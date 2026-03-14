#include "gw_app.h"
#include "ui_conf.h"
#include "ui_types.h"
#include "ui_time.h"
#include "ui_packets.h"
#include "ui_rf_plan_kr920.h"
#include "ui_lpm.h"
#include "ui_uart.h"
#include "ui_radio.h"
#include "ui_ble.h"
#include "gw_storage.h"
#include "gw_catm1.h"
#include "gw_sensors.h"
#include "gw_ble_report.h"
#include "stm32_timer.h"
#include "stm32_seq.h"
#include "radio.h"
#include <string.h>

/* -------------------------------------------------------------------------- */
/* Gateway 상태 */
/* -------------------------------------------------------------------------- */
typedef enum {
    GW_STATE_IDLE = 0,
    GW_STATE_BEACON_TX,
    GW_STATE_RX_SLOTS,
} GW_State_t;

static GW_State_t s_state = GW_STATE_IDLE;
static bool s_inited = false;
static UTIL_TIMER_Object_t s_tmr_wakeup;
static UTIL_TIMER_Object_t s_tmr_led1_pulse;
static UTIL_TIMER_Object_t s_tmr_ble_test_expire;
static volatile uint32_t s_evt_flags = 0;

#define GW_EVT_WAKEUP           (1u << 0)
#define GW_EVT_BEACON_ONESHOT   (1u << 1)
#define GW_EVT_RADIO_TX_DONE    (1u << 2)
#define GW_EVT_RADIO_TX_TIMEOUT (1u << 3)
#define GW_EVT_RADIO_RX_DONE    (1u << 4)
#define GW_EVT_RADIO_RX_TIMEOUT (1u << 5)
#define GW_EVT_RADIO_RX_ERROR   (1u << 6)
#define GW_EVT_BLE_TEST_EXPIRE  (1u << 7)

#define GW_FLASH_TX_BACKLOG_MAX (24u * 5u)
#define GW_BLE_TEST_SESSION_MS  (60u * 60u * 1000u)
#define GW_RX_WINDOW_GUARD_MS   (300u)

static bool s_test_mode = false;
static bool s_ble_test_session_active = false;
static uint16_t s_beacon_counter = 0;
static uint8_t s_slot_idx = 0;
static uint8_t s_slot_cnt = 0;
static uint8_t s_rx_expected_nodes = 0u;
static uint32_t s_rx_window_deadline_ms = 0u;
static uint64_t s_rx_nodes_seen_mask = 0u;
static uint32_t s_data_freq_hz = 0;
static bool s_rx_cycle_minute_test = false;
static uint32_t s_rx_cycle_stamp_sec = 0u;
static uint32_t s_catm1_retry_not_before_ms = 0u;
static bool s_beacon_oneshot_pending = false;
static GW_HourRec_t s_hour_rec;
static GW_HourRec_t s_last_cycle_rec;
static bool s_last_cycle_valid = false;
static uint32_t s_last_cycle_minute_id = 0u;
static uint32_t s_last_save_minute_id = 0xFFFFFFFFu;
static bool s_catm1_uplink_pending = false;
static uint32_t s_last_catm1_slot_id = 0xFFFFFFFFu;
static uint32_t s_last_2m_prep_slot_id = 0xFFFFFFFFu;
static uint32_t s_flash_tx_boot_tail_index = 0u;
static uint32_t s_flash_tx_next_send_index = 0u;
static bool s_boot_time_sync_pending = false;
static bool s_beacon_recovery_mode = false;
static uint8_t s_beacon_burst_remaining = 0u;
static uint32_t s_beacon_burst_anchor_sec = 0u;
static uint32_t s_beacon_burst_gap_ms = 0u;

#define GW_BEACON_BURST_COUNT_NORMAL   (3u)
#define GW_BEACON_BURST_COUNT_RECOVERY (5u)
#define GW_BEACON_BURST_GAP_MS         (250u)
#define GW_BEACON_REMINDER_BURST_COUNT (2u)
#define GW_BEACON_REMINDER_GAP_MS      (200u)

static uint8_t s_beacon_tx_payload[UI_BEACON_PAYLOAD_LEN];
static uint8_t s_rx_shadow[UI_NODE_PAYLOAD_LEN];
static uint16_t s_rx_shadow_size = 0u;
static int16_t s_rx_shadow_rssi = 0;
static int8_t s_rx_shadow_snr = 0;

static void prv_schedule_wakeup(void);
static void prv_requeue_events(uint32_t ev_mask);
static bool prv_arm_rx_slot(void);
static void prv_rx_next_slot(void);
static bool prv_is_two_minute_mode_active(void);
static void prv_close_rx_cycle_and_commit(void);
static bool prv_start_pending_beacon_burst(void);
static void prv_cancel_pending_beacon_burst(void);
static bool prv_get_reminder_offset_sec(uint32_t* out_offset_sec);
static void prv_send_ble_test_report_if_active(const GW_HourRec_t* rec);
static void prv_start_ble_test_session(void);
static void prv_stop_ble_test_session(bool disable_ble_now);
static void prv_get_effective_setting_ascii(uint8_t out_setting_ascii[3]);

static void prv_cancel_pending_beacon_burst(void)
{
    s_beacon_burst_remaining = 0u;
    s_beacon_burst_anchor_sec = 0u;
    s_beacon_burst_gap_ms = 0u;
    s_beacon_oneshot_pending = false;
}

static void prv_tmr_ble_test_expire_cb(void *context)
{
    (void)context;
    s_evt_flags |= GW_EVT_BLE_TEST_EXPIRE;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

static uint8_t prv_count_valid_nodes_in_rec(const GW_HourRec_t* rec)
{
    uint8_t count = 0u;
    if (rec == NULL) {
        return 0u;
    }
    for (uint32_t i = 0u; i < UI_MAX_NODES; i++) {
        const GW_NodeRec_t* n = &rec->nodes[i];
        if ((n->batt_lvl != UI_NODE_BATT_LVL_INVALID) ||
            (n->temp_c != UI_NODE_TEMP_INVALID_C) ||
            (n->x != (int16_t)0xFFFFu) ||
            (n->y != (int16_t)0xFFFFu) ||
            (n->z != (int16_t)0xFFFFu) ||
            (n->adc != 0xFFFFu) ||
            (n->pulse_cnt != 0xFFFFFFFFu)) {
            count++;
        }
    }
    return count;
}

static void prv_update_beacon_recovery_mode_from_rec(const GW_HourRec_t* rec)
{
    s_beacon_recovery_mode = (prv_count_valid_nodes_in_rec(rec) == 0u);
}

static uint8_t prv_get_beacon_burst_count(void)
{
    return 1u;
}

static void prv_prepare_beacon_burst(uint32_t anchor_sec, uint8_t total_count, uint32_t gap_ms)
{
    if (total_count == 0u) {
        total_count = 1u;
    }
    if (gap_ms == 0u) {
        gap_ms = GW_BEACON_BURST_GAP_MS;
    }
    s_beacon_burst_anchor_sec = anchor_sec;
    s_beacon_burst_remaining = total_count;
    s_beacon_burst_gap_ms = gap_ms;
    s_beacon_oneshot_pending = true;
}

static void prv_led1(bool on)
{
#if UI_HAVE_LED1
    HAL_GPIO_WritePin(LED1_GPIO_Port, LED1_Pin, on ? GPIO_PIN_SET : GPIO_PIN_RESET);
#else
    (void)on;
#endif
}

static void prv_led1_pulse_off_cb(void *context)
{
    (void)context;
    prv_led1(false);
}

static void prv_led1_pulse_10ms(void)
{
    prv_led1(true);
    (void)UTIL_TIMER_Stop(&s_tmr_led1_pulse);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_led1_pulse, 10u);
    (void)UTIL_TIMER_Start(&s_tmr_led1_pulse);
}

static bool prv_radio_ready_for_tx(void)
{
    if ((Radio.SetChannel == NULL) || (Radio.Send == NULL) ||
        (Radio.Sleep == NULL) || (Radio.SetTxConfig == NULL)) {
        return false;
    }
    return true;
}

static bool prv_radio_ready_for_rx(void)
{
    if ((Radio.SetChannel == NULL) || (Radio.Rx == NULL) ||
        (Radio.Sleep == NULL) || (Radio.SetRxConfig == NULL)) {
        return false;
    }
    return true;
}

static uint8_t prv_pack_gw_volt_x10(uint16_t raw_x10)
{
    if (raw_x10 == 0xFFFFu) {
        return 0xFFu;
    }
    if (raw_x10 > 254u) {
        return 254u;
    }
    return (uint8_t)raw_x10;
}

static int8_t prv_pack_gw_temp_c(int16_t raw_x10)
{
    int16_t c;
    if ((uint16_t)raw_x10 == 0xFFFFu) {
        return UI_NODE_TEMP_INVALID_C;
    }
    if (raw_x10 >= 0) {
        c = (int16_t)((raw_x10 + 5) / 10);
    } else {
        c = (int16_t)((raw_x10 - 5) / 10);
    }
    if (c < -50) {
        c = -50;
    }
    if (c > 100) {
        c = 100;
    }
    return (int8_t)c;
}

static void prv_hour_rec_init(uint32_t epoch_sec)
{
    uint16_t gw_volt_x10_raw = 0xFFFFu;
    int16_t gw_temp_x10_raw = (int16_t)0xFFFFu;

    s_hour_rec.gw_volt_x10 = 0xFFu;
    s_hour_rec.gw_temp_c = UI_NODE_TEMP_INVALID_C;
    s_hour_rec.epoch_sec = epoch_sec;

    (void)GW_Sensors_MeasureGw(&gw_volt_x10_raw, &gw_temp_x10_raw);
    s_hour_rec.gw_volt_x10 = prv_pack_gw_volt_x10(gw_volt_x10_raw);
    s_hour_rec.gw_temp_c = prv_pack_gw_temp_c(gw_temp_x10_raw);
    for (uint32_t i = 0; i < UI_MAX_NODES; i++) {
        s_hour_rec.nodes[i].batt_lvl = UI_NODE_BATT_LVL_INVALID;
        s_hour_rec.nodes[i].temp_c = UI_NODE_TEMP_INVALID_C;
        s_hour_rec.nodes[i].x = (int16_t)0xFFFFu;
        s_hour_rec.nodes[i].y = (int16_t)0xFFFFu;
        s_hour_rec.nodes[i].z = (int16_t)0xFFFFu;
        s_hour_rec.nodes[i].adc = 0xFFFFu;
        s_hour_rec.nodes[i].pulse_cnt = 0xFFFFFFFFu;
    }
}

static uint32_t prv_gw_offset_sec(void)
{
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t gw = cfg->gw_num;
    if (gw > 2u) gw = 2u;
    return (uint32_t)gw * 2u;
}

static uint32_t prv_get_beacon_offset_sec(void)
{
    /* 1분 테스트 모드는 00초 고정 비콘 */
    if (s_test_mode) {
        return 0u;
    }
    /* 2분/정상 모드는 기존 GW NUM phase 유지 */
    return prv_gw_offset_sec();
}

static uint64_t prv_next_event_centi(uint64_t now_centi, uint32_t interval_sec, uint32_t offset_sec)
{
    uint32_t now_sec = (uint32_t)(now_centi / 100u);
    uint32_t centi = (uint32_t)(now_centi % 100u);
    uint32_t cand_sec = now_sec + ((centi == 0u) ? 0u : 1u);
    uint32_t rem = (interval_sec == 0u) ? 0u : (cand_sec % interval_sec);
    uint32_t next_sec;
    if (interval_sec == 0u) {
        next_sec = cand_sec + offset_sec;
    } else if (rem <= offset_sec) {
        next_sec = cand_sec - rem + offset_sec;
    } else {
        next_sec = cand_sec - rem + interval_sec + offset_sec;
    }
    return (uint64_t)next_sec * 100u;
}

static bool prv_is_event_due_now(uint64_t now_centi,
                                 uint32_t interval_sec,
                                 uint32_t offset_sec,
                                 uint32_t late_grace_centi,
                                 uint64_t* due_event_centi)
{
    uint64_t next_evt;
    uint64_t step;
    uint64_t prev_evt;

    if ((interval_sec == 0u) || (due_event_centi == NULL)) {
        return false;
    }
    next_evt = prv_next_event_centi(now_centi, interval_sec, offset_sec);
    step = (uint64_t)interval_sec * 100u;
    if (next_evt == now_centi) {
        *due_event_centi = next_evt;
        return true;
    }
    if (next_evt < step) {
        return false;
    }
    prev_evt = next_evt - step;
    if ((now_centi >= prev_evt) &&
        (now_centi < (prev_evt + (uint64_t)late_grace_centi))) {
        *due_event_centi = prev_evt;
        return true;
    }
    return false;
}

static void prv_get_effective_setting_ascii(uint8_t out_setting_ascii[3])
{
    const UI_Config_t* cfg = UI_GetConfig();

    if (out_setting_ascii == NULL) {
        return;
    }

    if (s_ble_test_session_active) {
        out_setting_ascii[0] = (uint8_t)'0';
        out_setting_ascii[1] = (uint8_t)'1';
        out_setting_ascii[2] = (uint8_t)'M';
        return;
    }

    if (cfg == NULL) {
        out_setting_ascii[0] = (uint8_t)'0';
        out_setting_ascii[1] = (uint8_t)'0';
        out_setting_ascii[2] = (uint8_t)'H';
        return;
    }

    out_setting_ascii[0] = cfg->setting_ascii[0];
    out_setting_ascii[1] = cfg->setting_ascii[1];
    out_setting_ascii[2] = cfg->setting_ascii[2];
}

static bool prv_get_setting_value_unit_ascii_first(uint8_t* out_value, char* out_unit)
{
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t value;
    char unit;

    if ((out_value == NULL) || (out_unit == NULL)) {
        return false;
    }
    if (s_ble_test_session_active) {
        *out_value = 1u;
        *out_unit = 'M';
        return true;
    }
    if (cfg == NULL) {
        return false;
    }
    if ((cfg->setting_ascii[0] >= (uint8_t)'0') &&
        (cfg->setting_ascii[0] <= (uint8_t)'9') &&
        (cfg->setting_ascii[1] >= (uint8_t)'0') &&
        (cfg->setting_ascii[1] <= (uint8_t)'9')) {
        unit = (char)cfg->setting_ascii[2];
        if ((unit == 'M') || (unit == 'H')) {
            value = (uint8_t)(((cfg->setting_ascii[0] - (uint8_t)'0') * 10u) +
                              (cfg->setting_ascii[1] - (uint8_t)'0'));
            if (value > 0u) {
                *out_value = value;
                *out_unit = unit;
                return true;
            }
        }
    }
    value = cfg->setting_value;
    unit = cfg->setting_unit;
    if ((value > 0u) && ((unit == 'M') || (unit == 'H'))) {
        *out_value = value;
        *out_unit = unit;
        return true;
    }
    return false;
}

static void prv_update_test_mode(void)
{
    uint8_t value;
    char unit;
    s_test_mode = (prv_get_setting_value_unit_ascii_first(&value, &unit) &&
                   (value == 1u) && (unit == 'M'));
}

static bool prv_is_two_minute_mode_active(void)
{
    uint8_t value;
    char unit;
    return (prv_get_setting_value_unit_ascii_first(&value, &unit) &&
            (value == 2u) && (unit == 'M'));
}

static uint32_t prv_get_setting_cycle_sec(void)
{
    uint8_t value;
    char unit;
    if (!prv_get_setting_value_unit_ascii_first(&value, &unit)) {
        return 0u;
    }
    if (unit == 'M') {
        return (uint32_t)value * 60u;
    }
    return (uint32_t)value * 3600u;
}

static uint32_t prv_get_normal_cycle_sec(void)
{
    uint32_t cycle_sec = prv_get_setting_cycle_sec();
    if (cycle_sec == 0u) {
        return UI_GW_RX_PERIOD_S_NORMAL;
    }
    if (prv_is_two_minute_mode_active()) {
        return 120u;
    }
    if (cycle_sec < UI_BEACON_PERIOD_S) {
        return UI_BEACON_PERIOD_S;
    }
    return cycle_sec;
}

static uint32_t prv_get_hop_period_sec(void)
{
    return s_test_mode ? 60u : prv_get_normal_cycle_sec();
}

static uint32_t prv_get_beacon_interval_sec(void)
{
    if (s_test_mode) {
        return 60u;
    }
    if (prv_is_two_minute_mode_active()) {
        return 120u;
    }
    return UI_BEACON_PERIOD_S;
}

static uint32_t prv_get_rx_interval_sec(void)
{
    return s_test_mode ? 60u : prv_get_normal_cycle_sec();
}

static uint32_t prv_get_rx_start_offset_sec(void)
{
    if (s_test_mode) {
        return 20u;
    }
    if (prv_is_two_minute_mode_active()) {
        return 30u;
    }
    /* 일반 모드 ND TX base offset은 60초(+ node_num*2초)다.
     * 최신 코드에서 GW RX 시작을 30초로 당겨 놓으면서,
     * max_nodes가 작은 경우 GW가 ND00/ND01 전송 전에 RX를 끝내는 문제가 생겼다.
     * 정상 모드는 설정 매크로(UI_GW_RX_START_OFFSET_S)와 다시 맞춘다. */
    return UI_GW_RX_START_OFFSET_S;
}

static bool prv_get_reminder_offset_sec(uint32_t* out_offset_sec)
{
    (void)out_offset_sec;
    return false;
}

static bool prv_get_two_minute_prep_offset_sec(uint32_t* out_offset_sec)
{
    (void)out_offset_sec;
    return false;
}

static uint32_t prv_get_current_cycle_timestamp_sec(void)
{
    uint32_t now_sec = UI_Time_NowSec2016();
    uint32_t cycle_sec = prv_get_setting_cycle_sec();
    if (cycle_sec != 0u) {
        return ((now_sec / cycle_sec) * cycle_sec);
    }
    return now_sec;
}

static bool prv_is_minute_test_active(void)
{
    uint8_t value;
    char unit;
    return (s_test_mode &&
            prv_get_setting_value_unit_ascii_first(&value, &unit) &&
            (value == 1u) && (unit == 'M'));
}

static uint64_t prv_next_test50_centi(uint64_t now_centi)
{
    return prv_next_event_centi(now_centi, 60u, 40u);
}

static void prv_mark_cycle_complete(const GW_HourRec_t* rec)
{
    if (rec == NULL) {
        return;
    }
    s_last_cycle_rec = *rec;
    s_last_cycle_valid = true;
    s_last_cycle_minute_id = rec->epoch_sec / 60u;
}

static uint32_t prv_flash_tx_clamp_floor_to_boot_tail(uint32_t floor_index)
{
    if (floor_index < s_flash_tx_boot_tail_index) {
        floor_index = s_flash_tx_boot_tail_index;
    }
    return floor_index;
}

static void prv_flash_tx_reset_to_tail(void)
{
    uint32_t tail = GW_Storage_GetTotalRecordCount();
    s_flash_tx_boot_tail_index = tail;
    s_flash_tx_next_send_index = tail;
}

static uint32_t prv_flash_tx_pending_count(void)
{
    uint32_t tail = GW_Storage_GetTotalRecordCount();
    if (s_flash_tx_boot_tail_index > tail) {
        s_flash_tx_boot_tail_index = tail;
    }
    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index) {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }
    if (s_flash_tx_next_send_index > tail) {
        s_flash_tx_next_send_index = tail;
        return 0u;
    }
    return (tail - s_flash_tx_next_send_index);
}

static void prv_flash_tx_note_saved(bool saved_ok)
{
    uint32_t tail;
    uint32_t pending;
    uint32_t min_keep_index;
    if (!saved_ok) {
        return;
    }
    tail = GW_Storage_GetTotalRecordCount();
    if (s_flash_tx_boot_tail_index > tail) {
        s_flash_tx_boot_tail_index = tail;
    }
    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index) {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }
    if (s_flash_tx_next_send_index > tail) {
        s_flash_tx_next_send_index = tail;
    }
    pending = tail - s_flash_tx_next_send_index;
    if (pending > GW_FLASH_TX_BACKLOG_MAX) {
        min_keep_index = tail - GW_FLASH_TX_BACKLOG_MAX;
        min_keep_index = prv_flash_tx_clamp_floor_to_boot_tail(min_keep_index);
        if (s_flash_tx_next_send_index < min_keep_index) {
            s_flash_tx_next_send_index = min_keep_index;
        }
    }
}

static void prv_flash_tx_note_sent(uint32_t sent_count)
{
    uint32_t pending = prv_flash_tx_pending_count();
    uint32_t tail = GW_Storage_GetTotalRecordCount();
    if (sent_count > pending) {
        sent_count = pending;
    }
    s_flash_tx_next_send_index += sent_count;
    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index) {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }
    if (s_flash_tx_next_send_index > tail) {
        s_flash_tx_next_send_index = tail;
    }
}

static void prv_flash_tx_resync_after_storage_change(void)
{
    uint32_t tail = GW_Storage_GetTotalRecordCount();
    uint32_t min_keep_index;
    if (s_flash_tx_boot_tail_index > tail) {
        s_flash_tx_boot_tail_index = tail;
    }
    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index) {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }
    if (s_flash_tx_next_send_index > tail) {
        s_flash_tx_next_send_index = tail;
    }
    if ((tail - s_flash_tx_next_send_index) > GW_FLASH_TX_BACKLOG_MAX) {
        min_keep_index = tail - GW_FLASH_TX_BACKLOG_MAX;
        min_keep_index = prv_flash_tx_clamp_floor_to_boot_tail(min_keep_index);
        if (s_flash_tx_next_send_index < min_keep_index) {
            s_flash_tx_next_send_index = min_keep_index;
        }
    }
}

static bool prv_save_hour_rec_verified(const GW_HourRec_t* rec)
{
    uint32_t before_cnt;
    uint32_t after_cnt;
    uint32_t try_idx;

    if (rec == NULL) {
        return false;
    }
    before_cnt = GW_Storage_GetTotalRecordCount();
    for (try_idx = 0u; try_idx < 2u; try_idx++) {
        if (!GW_Storage_SaveHourRec(rec)) {
            continue;
        }
        after_cnt = GW_Storage_GetTotalRecordCount();
        if (after_cnt > before_cnt) {
            return true;
        }
    }
    return false;
}

static bool prv_flash_tail_matches_rec(const GW_HourRec_t* rec)
{
    GW_FileRec_t last_rec;
    uint32_t tail;
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t cur_gw_num = (cfg != NULL) ? cfg->gw_num : 0u;

    if (rec == NULL) {
        return false;
    }

    tail = GW_Storage_GetTotalRecordCount();
    if (tail == 0u) {
        return false;
    }
    if (!GW_Storage_ReadRecordByGlobalIndex(tail - 1u, &last_rec, NULL)) {
        return false;
    }

    return ((last_rec.gw_num == cur_gw_num) &&
            (last_rec.rec.epoch_sec == rec->epoch_sec));
}

static void prv_handle_test50_actions_core(uint32_t now_sec, bool treat_as_minute_test)
{
    uint32_t now_minute_id = now_sec / 60u;

    if (!treat_as_minute_test) {
        return;
    }
    if ((!s_last_cycle_valid) || (s_last_cycle_minute_id != now_minute_id)) {
        return;
    }
    if (s_last_save_minute_id != now_minute_id) {
        bool saved_ok = prv_save_hour_rec_verified(&s_last_cycle_rec);
        prv_flash_tx_note_saved(saved_ok);
        s_last_save_minute_id = now_minute_id;
        GW_Storage_PurgeOldFiles(s_last_cycle_rec.epoch_sec);
        prv_flash_tx_resync_after_storage_change();
    }
}

static void prv_handle_test50_actions(uint32_t now_sec)
{
    prv_handle_test50_actions_core(now_sec, prv_is_minute_test_active());
}

static void prv_send_ble_test_report_if_active(const GW_HourRec_t* rec)
{
    if (rec == NULL) {
        return;
    }
    if (!UI_BLE_IsActive() || !UI_BLE_IsPersistent()) {
        return;
    }

    UI_BLE_EnsureSerialReady();
    (void)GW_BleReport_SendMinuteTestRecord(rec);
}

static void prv_start_ble_test_session(void)
{
    s_ble_test_session_active = true;
    UI_BLE_SetPersistent(true);
    UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);

    (void)UTIL_TIMER_Stop(&s_tmr_ble_test_expire);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_ble_test_expire, GW_BLE_TEST_SESSION_MS);
    (void)UTIL_TIMER_Start(&s_tmr_ble_test_expire);

    prv_update_test_mode();
    if (s_state == GW_STATE_IDLE) {
        prv_schedule_wakeup();
    }
}

static void prv_stop_ble_test_session(bool disable_ble_now)
{
    s_ble_test_session_active = false;
    (void)UTIL_TIMER_Stop(&s_tmr_ble_test_expire);
    UI_BLE_SetPersistent(false);

    if (disable_ble_now && UI_BLE_IsActive()) {
        UI_BLE_Disable();
    }

    prv_update_test_mode();
    if (s_state == GW_STATE_IDLE) {
        prv_schedule_wakeup();
    }
}

static bool prv_is_catm1_periodic_active(void)
{
    uint32_t cycle_sec;
    if (!UI_Time_IsValid()) {
        return false;
    }
    if (s_test_mode) {
        return false;
    }
    if (prv_is_two_minute_mode_active()) {
        return true;
    }
    cycle_sec = prv_get_setting_cycle_sec();
    return (cycle_sec >= UI_BEACON_PERIOD_S);
}

static uint32_t prv_get_catm1_period_sec(void)
{
    if (!prv_is_catm1_periodic_active()) {
        return 0u;
    }
    return prv_get_setting_cycle_sec();
}

static uint32_t prv_get_catm1_offset_sec(void)
{
    return prv_is_two_minute_mode_active() ? 90u : UI_CATM1_PERIODIC_OFFSET_S;
}

static uint32_t prv_catm1_slot_id_from_epoch_sec(uint32_t epoch_sec, uint32_t period_sec)
{
    if (period_sec == 0u) {
        return 0xFFFFFFFFu;
    }
    return (epoch_sec / period_sec);
}

static bool prv_last_cycle_matches_slot(uint32_t period_sec, uint32_t slot_id)
{
    if ((!s_last_cycle_valid) || (period_sec == 0u)) {
        return false;
    }
    return (prv_catm1_slot_id_from_epoch_sec(s_last_cycle_rec.epoch_sec, period_sec) == slot_id);
}

static void prv_request_catm1_uplink(void)
{
    s_catm1_uplink_pending = true;
}

static const GW_HourRec_t* prv_get_catm1_uplink_record(void)
{
    if (s_last_cycle_valid) {
        return &s_last_cycle_rec;
    }
    return &s_hour_rec;
}

static bool prv_run_catm1_uplink_now(void)
{
    const GW_HourRec_t* rec;
    uint32_t pending;
    uint32_t now_ms;

    if (!s_catm1_uplink_pending) {
        return false;
    }
    if (s_state != GW_STATE_IDLE) {
        return false;
    }

    now_ms = HAL_GetTick();
    if ((s_catm1_retry_not_before_ms != 0u) &&
        ((int32_t)(now_ms - s_catm1_retry_not_before_ms) < 0)) {
        return false;
    }
    if (GW_Catm1_IsBusy()) {
        return false;
    }

    rec = prv_get_catm1_uplink_record();
    pending = prv_flash_tx_pending_count();
    if (pending > 0u) {
        GW_FileRec_t first_rec;
        uint32_t sent_count = 0u;
        uint32_t batch_count;
        bool sent_ok = false;

        if ((rec != NULL) && !prv_flash_tail_matches_rec(rec)) {
            bool saved_ok = prv_save_hour_rec_verified(rec);
            prv_flash_tx_note_saved(saved_ok);
        }

        pending = prv_flash_tx_pending_count();
        batch_count = pending;
        if (batch_count > GW_FLASH_TX_BACKLOG_MAX) {
            batch_count = GW_FLASH_TX_BACKLOG_MAX;
        }
        if ((batch_count == 0u) ||
            !GW_Storage_ReadRecordByGlobalIndex(s_flash_tx_next_send_index, &first_rec, NULL)) {
            prv_flash_tx_reset_to_tail();
            pending = 0u;
        } else {
            sent_ok = GW_Catm1_SendStoredRange(s_flash_tx_next_send_index, batch_count, &sent_count);
            if (sent_count > 0u) {
                prv_flash_tx_note_sent(sent_count);
                s_catm1_retry_not_before_ms = 0u;
            } else if (!sent_ok) {
                /* 서버 정지/미응답 시 현재 wakeup에서는 재시도하지 않고 종료한다.
                 * backlog는 flash에 그대로 두고, 다음 정상 uplink 요청 때만 다시 시도한다. */
                s_catm1_retry_not_before_ms = 0u;
            }
            prv_flash_tx_resync_after_storage_change();
            s_catm1_uplink_pending = (prv_flash_tx_pending_count() > 0u);
            if ((!sent_ok) && (sent_count == 0u)) {
                s_catm1_uplink_pending = false;
            }
            prv_schedule_wakeup();
            return true;
        }
    }

    if (rec != NULL) {
        bool sent_ok = GW_Catm1_SendSnapshot(rec);
        if (sent_ok) {
            s_catm1_retry_not_before_ms = 0u;
            s_catm1_uplink_pending = (prv_flash_tx_pending_count() > 0u);
        } else {
            /* SendSnapshot() 실패 시 rec는 flash에 남길 수 있지만,
             * 같은 wakeup에서 CAT-M1 uplink를 연속 재시도하지 않는다. */
            s_catm1_retry_not_before_ms = 0u;
            s_catm1_uplink_pending = false;
        }
        prv_schedule_wakeup();
        return true;
    }

    return false;
}

static void prv_requeue_events(uint32_t ev_mask)
{
    if (ev_mask != 0u) {
        s_evt_flags |= ev_mask;
        UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
    }
}

static bool prv_arm_rx_slot(void)
{
    if (!prv_radio_ready_for_rx()) {
        return false;
    }
    if (!UI_Radio_PrepareRx(UI_NODE_PAYLOAD_LEN)) {
        return false;
    }
    Radio.SetChannel(s_data_freq_hz);
    Radio.Rx(UI_SLOT_DURATION_MS);
    return true;
}

static void prv_schedule_after_ms(uint32_t delay_ms)
{
    if (delay_ms == 0u) {
        delay_ms = 1u;
    }
    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_wakeup, delay_ms);
    (void)UTIL_TIMER_Start(&s_tmr_wakeup);
}

static bool prv_rx_all_expected_nodes_seen(void)
{
    uint64_t full_mask;

    if ((s_rx_expected_nodes == 0u) || (s_rx_expected_nodes >= 64u)) {
        return false;
    }

    full_mask = (1ULL << s_rx_expected_nodes) - 1ULL;
    return ((s_rx_nodes_seen_mask & full_mask) == full_mask);
}

static void prv_rx_note_node_received(uint8_t node_num)
{
    if (node_num < 64u) {
        s_rx_nodes_seen_mask |= (1ULL << node_num);
    }
}

static bool prv_rx_window_expired(void)
{
    if (s_rx_window_deadline_ms == 0u) {
        return true;
    }
    return ((int32_t)(HAL_GetTick() - s_rx_window_deadline_ms) >= 0);
}

static void prv_schedule_next_second_tick(uint64_t now_centi)
{
    uint32_t centi = (uint32_t)(now_centi % 100u);
    uint32_t wait_centi = (centi == 0u) ? 1u : (100u - centi);
    prv_schedule_after_ms(wait_centi * 10u);
}

static bool prv_start_beacon_tx(uint32_t now_sec)
{
    UI_DateTime_t dt;
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t setting_ascii[3];

    UI_Time_Epoch2016_ToCalendar(now_sec, &dt);
    prv_get_effective_setting_ascii(setting_ascii);
    (void)UI_Pkt_BuildBeacon(s_beacon_tx_payload, cfg->net_id, &dt, setting_ascii);
    if (!prv_radio_ready_for_tx()) {
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        return false;
    }
    if (!UI_Radio_PrepareTx(UI_BEACON_PAYLOAD_LEN)) {
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        return false;
    }
    UI_LPM_LockStop();
    s_state = GW_STATE_BEACON_TX;
    prv_led1_pulse_10ms();
    Radio.SetChannel(UI_RF_GetBeaconFreqHz());
    Radio.Send(s_beacon_tx_payload, UI_BEACON_PAYLOAD_LEN);
    return true;
}

static bool prv_start_pending_beacon_burst(void)
{
    if ((!s_beacon_oneshot_pending) || (s_beacon_burst_remaining == 0u)) {
        return false;
    }
    if (!prv_start_beacon_tx(s_beacon_burst_anchor_sec)) {
        prv_cancel_pending_beacon_burst();
        return false;
    }
    s_beacon_burst_remaining--;
    return true;
}

static void prv_tmr_wakeup_cb(void *context)
{
    (void)context;
    s_evt_flags |= GW_EVT_WAKEUP;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_App_Process(void)
{
    if (!s_inited) {
        return;
    }

    uint32_t ev = s_evt_flags;
    if (s_boot_time_sync_pending && (s_state == GW_STATE_IDLE) && !GW_Catm1_IsBusy()) {
        s_boot_time_sync_pending = false;
        (void)GW_Catm1_SyncTimeOnce();
        prv_hour_rec_init(prv_get_current_cycle_timestamp_sec());
        if (ev == 0u) {
            prv_schedule_wakeup();
            return;
        }
    }
    if (ev == 0u) {
        return;
    }
    s_evt_flags &= ~ev;
    prv_update_test_mode();

    if ((ev & GW_EVT_BLE_TEST_EXPIRE) != 0u) {
        prv_stop_ble_test_session(true);
        ev &= ~GW_EVT_BLE_TEST_EXPIRE;
        if (ev == 0u) {
            return;
        }
    }

    if ((ev & GW_EVT_RADIO_TX_DONE) != 0u) {
        if (s_state == GW_STATE_BEACON_TX) {
            Radio.Sleep();
            s_state = GW_STATE_IDLE;
            UI_LPM_UnlockStop();
            s_beacon_counter++;
            if (s_beacon_burst_remaining > 0u) {
                s_beacon_oneshot_pending = true;
                prv_schedule_after_ms(s_beacon_burst_gap_ms);
            } else {
                prv_cancel_pending_beacon_burst();
                prv_schedule_wakeup();
            }
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_TX_DONE));
        return;
    }

    if ((ev & GW_EVT_RADIO_TX_TIMEOUT) != 0u) {
        UI_Radio_MarkRecoverNeeded();
        Radio.Sleep();
        if (s_state != GW_STATE_IDLE) {
            s_state = GW_STATE_IDLE;
            UI_LPM_UnlockStop();
        }
        prv_cancel_pending_beacon_burst();
        prv_schedule_wakeup();
        prv_requeue_events(ev & ~(GW_EVT_RADIO_TX_TIMEOUT));
        return;
    }

    if ((ev & GW_EVT_RADIO_RX_DONE) != 0u) {
        if (s_state == GW_STATE_RX_SLOTS) {
            prv_led1_pulse_10ms();
            UI_NodeData_t nd;
            if (UI_Pkt_ParseNodeData(s_rx_shadow, s_rx_shadow_size, &nd)) {
                if (nd.node_num < UI_MAX_NODES) {
                    const UI_Config_t* cfg = UI_GetConfig();
                    if (memcmp(nd.net_id, cfg->net_id, UI_NET_ID_LEN) == 0) {
                        GW_NodeRec_t* r = &s_hour_rec.nodes[nd.node_num];
                        r->batt_lvl = nd.batt_lvl;
                        r->temp_c = nd.temp_c;
                        r->x = nd.x;
                        r->y = nd.y;
                        r->z = nd.z;
                        r->adc = nd.adc;
                        r->pulse_cnt = nd.pulse_cnt;
                        if (nd.node_num < s_rx_expected_nodes) {
                            prv_rx_note_node_received(nd.node_num);
                        }
                    }
                }
            }
            prv_rx_next_slot();
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_RX_DONE));
        return;
    }

    if ((ev & GW_EVT_RADIO_RX_TIMEOUT) != 0u) {
        if (s_state == GW_STATE_RX_SLOTS) {
            prv_rx_next_slot();
        } else {
            UI_Radio_MarkRecoverNeeded();
            Radio.Sleep();
            if (s_state != GW_STATE_IDLE) {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                prv_schedule_wakeup();
            }
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_RX_TIMEOUT));
        return;
    }

    if ((ev & GW_EVT_RADIO_RX_ERROR) != 0u) {
        if (s_state == GW_STATE_RX_SLOTS) {
            prv_rx_next_slot();
        } else {
            UI_Radio_MarkRecoverNeeded();
            Radio.Sleep();
            if (s_state != GW_STATE_IDLE) {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                prv_schedule_wakeup();
            }
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_RX_ERROR));
        return;
    }

    if ((ev & GW_EVT_BEACON_ONESHOT) != 0u) {
        s_beacon_oneshot_pending = true;
    }
    if (s_beacon_oneshot_pending) {
        if (s_state == GW_STATE_IDLE) {
            if (prv_start_pending_beacon_burst()) {
                return;
            }
            prv_schedule_wakeup();
            return;
        }
        return;
    }

    if (s_catm1_uplink_pending && (s_state == GW_STATE_IDLE)) {
        if (prv_run_catm1_uplink_now()) {
            return;
        }
    }

    if ((ev & GW_EVT_WAKEUP) != 0u) {
        if (s_state != GW_STATE_IDLE) {
            prv_schedule_wakeup();
            return;
        }

        uint64_t now_centi = UI_Time_NowCenti2016();
        uint32_t now_sec = (uint32_t)(now_centi / 100u);
        uint32_t beacon_interval = prv_get_beacon_interval_sec();
        uint32_t beacon_off = prv_get_beacon_offset_sec();
        uint64_t next_beacon = prv_next_event_centi(now_centi, beacon_interval, beacon_off);
        uint32_t rx_interval = prv_get_rx_interval_sec();
        uint32_t rx_start = prv_get_rx_start_offset_sec();
        uint64_t next_rx = prv_next_event_centi(now_centi, rx_interval, rx_start);
        uint32_t reminder_off = 0u;
        bool have_reminder = prv_get_reminder_offset_sec(&reminder_off);
        uint32_t two_min_prep_off = 0u;
        bool have_two_min_prep = prv_get_two_minute_prep_offset_sec(&two_min_prep_off);
        uint64_t next_test50 = 0xFFFFFFFFFFFFFFFFull;
        uint64_t next_catm1 = 0xFFFFFFFFFFFFFFFFull;
        uint64_t due_beacon = 0u;
        uint64_t due_rx = 0u;
        uint64_t due_reminder = 0u;
        uint64_t due_prep = 0u;
        uint64_t due_test50 = 0u;
        uint64_t due_catm1 = 0u;
        bool beacon_due_now = prv_is_event_due_now(now_centi, beacon_interval, beacon_off, 120u, &due_beacon);
        bool rx_due_now = prv_is_event_due_now(now_centi, rx_interval, rx_start, 120u, &due_rx);
        bool reminder_due_now = have_reminder && prv_is_event_due_now(now_centi, rx_interval, reminder_off, 120u, &due_reminder);
        bool prep_due_now = have_two_min_prep && prv_is_event_due_now(now_centi, rx_interval, two_min_prep_off, 120u, &due_prep);
        bool test50_due_now = false;
        bool catm1_due_now = false;

        if (prv_is_minute_test_active()) {
            next_test50 = prv_next_test50_centi(now_centi);
            test50_due_now = prv_is_event_due_now(now_centi, 60u, 40u, 120u, &due_test50);
        }
        if (prv_is_catm1_periodic_active()) {
            uint32_t catm1_period = prv_get_catm1_period_sec();
            next_catm1 = prv_next_event_centi(now_centi, catm1_period, prv_get_catm1_offset_sec());
            catm1_due_now = prv_is_event_due_now(now_centi, catm1_period, prv_get_catm1_offset_sec(), 120u, &due_catm1);
        }

        if (catm1_due_now) {
            uint32_t catm1_period = prv_get_catm1_period_sec();
            uint32_t slot_id = prv_catm1_slot_id_from_epoch_sec((uint32_t)(due_catm1 / 100u), catm1_period);
            if (s_last_catm1_slot_id != slot_id) {
                s_last_catm1_slot_id = slot_id;
                if (prv_is_two_minute_mode_active()) {
                    if (s_last_cycle_valid || (prv_flash_tx_pending_count() > 0u)) {
                        prv_request_catm1_uplink();
                    }
                } else if (prv_last_cycle_matches_slot(catm1_period, slot_id)) {
                    prv_request_catm1_uplink();
                }
            }
        }

        if (reminder_due_now && !beacon_due_now && !rx_due_now) {
            uint32_t reminder_sec = (uint32_t)(due_reminder / 100u);
            prv_prepare_beacon_burst(reminder_sec, GW_BEACON_REMINDER_BURST_COUNT, GW_BEACON_REMINDER_GAP_MS);
            if (prv_start_pending_beacon_burst()) {
                return;
            }
            prv_schedule_wakeup();
            return;
        }

        if (prep_due_now) {
            uint32_t prep_slot_id = prv_catm1_slot_id_from_epoch_sec((uint32_t)(due_prep / 100u), rx_interval);
            if (s_last_2m_prep_slot_id != prep_slot_id) {
                s_last_2m_prep_slot_id = prep_slot_id;
                prv_hour_rec_init(prv_get_current_cycle_timestamp_sec());
            }
            if (!beacon_due_now && !rx_due_now) {
                prv_schedule_wakeup();
                return;
            }
        }

        if (prv_is_minute_test_active() && (test50_due_now || ((next_test50 < next_beacon) && (next_test50 < next_rx)))) {
            prv_handle_test50_actions(now_sec);
            prv_schedule_wakeup();
            return;
        }

        if (s_catm1_uplink_pending && !beacon_due_now && !rx_due_now) {
            if (prv_run_catm1_uplink_now()) {
                return;
            }
        }

        if (beacon_due_now) {
            uint32_t beacon_sec = (uint32_t)(due_beacon / 100u);
            prv_prepare_beacon_burst(beacon_sec, prv_get_beacon_burst_count(), GW_BEACON_BURST_GAP_MS);
            if (prv_start_pending_beacon_burst()) {
                return;
            }
            prv_schedule_wakeup();
            return;
        }

        if (rx_due_now) {
            uint32_t hop_period = prv_get_hop_period_sec();
            const UI_Config_t* cfg = UI_GetConfig();
            uint32_t cycle_stamp_sec;

            s_data_freq_hz = UI_RF_GetDataFreqHz((uint32_t)(due_rx / 100u), hop_period, 0u);
            s_slot_cnt = (uint8_t)cfg->max_nodes;
            if (s_test_mode && (s_slot_cnt > UI_TESTMODE_MAX_NODES)) {
                s_slot_cnt = UI_TESTMODE_MAX_NODES;
            }
            if (prv_is_two_minute_mode_active() && (s_slot_cnt > 5u)) {
                s_slot_cnt = 5u;
            }
            s_slot_idx = 0;
            s_rx_expected_nodes = s_slot_cnt;
            s_rx_nodes_seen_mask = 0u;
            s_rx_window_deadline_ms = HAL_GetTick() + ((uint32_t)s_slot_cnt * UI_SLOT_DURATION_MS) + GW_RX_WINDOW_GUARD_MS;
            if (!prv_radio_ready_for_rx()) {
                prv_schedule_wakeup();
                return;
            }
            cycle_stamp_sec = prv_get_current_cycle_timestamp_sec();
            s_rx_cycle_minute_test = prv_is_minute_test_active();
            s_rx_cycle_stamp_sec = cycle_stamp_sec;
            UI_LPM_LockStop();
            s_state = GW_STATE_RX_SLOTS;
            prv_hour_rec_init(cycle_stamp_sec);
            if (!prv_arm_rx_slot()) {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                s_rx_cycle_minute_test = false;
                s_rx_cycle_stamp_sec = 0u;
                s_rx_window_deadline_ms = 0u;
                s_rx_expected_nodes = 0u;
                s_rx_nodes_seen_mask = 0u;
                prv_schedule_wakeup();
            }
            return;
        }
    }

    prv_schedule_wakeup();
}

static void GW_TaskMain(void)
{
    GW_App_Process();
}

static void prv_schedule_wakeup(void)
{
    uint64_t now_centi;
    uint64_t next;
    uint64_t next_beacon;
    uint64_t next_rx;
    uint32_t beacon_interval;
    uint32_t beacon_off;
    uint32_t rx_interval;
    uint32_t rx_start;
    uint32_t reminder_off = 0u;
    uint32_t two_min_prep_off = 0u;
    uint64_t delta_centi;
    uint32_t delta_ms;
    bool have_reminder;
    bool have_two_min_prep;

    if (s_state != GW_STATE_IDLE) {
        return;
    }

    prv_update_test_mode();
    now_centi = UI_Time_NowCenti2016();
    if (s_beacon_oneshot_pending) {
        prv_schedule_after_ms(10u);
        return;
    }
    if (s_catm1_uplink_pending) {
        uint32_t now_ms = HAL_GetTick();
        if ((s_catm1_retry_not_before_ms != 0u) &&
            ((int32_t)(now_ms - s_catm1_retry_not_before_ms) < 0)) {
            prv_schedule_after_ms(s_catm1_retry_not_before_ms - now_ms);
        } else {
            s_catm1_retry_not_before_ms = 0u;
            prv_schedule_after_ms(10u);
        }
        return;
    }

    beacon_interval = prv_get_beacon_interval_sec();
    beacon_off = prv_get_beacon_offset_sec();
    rx_interval = prv_get_rx_interval_sec();
    rx_start = prv_get_rx_start_offset_sec();
    have_reminder = prv_get_reminder_offset_sec(&reminder_off);
    have_two_min_prep = prv_get_two_minute_prep_offset_sec(&two_min_prep_off);

    next_beacon = prv_next_event_centi(now_centi, beacon_interval, beacon_off);
    next_rx = prv_next_event_centi(now_centi, rx_interval, rx_start);
    next = (next_beacon < next_rx) ? next_beacon : next_rx;

    if (have_reminder) {
        uint64_t next_reminder = prv_next_event_centi(now_centi, rx_interval, reminder_off);
        if (next_reminder < next) {
            next = next_reminder;
        }
    }
    if (have_two_min_prep) {
        uint64_t next_prep = prv_next_event_centi(now_centi, rx_interval, two_min_prep_off);
        if (next_prep < next) {
            next = next_prep;
        }
    }
    if (prv_is_minute_test_active()) {
        uint64_t next_test50 = prv_next_test50_centi(now_centi);
        if (next_test50 < next) {
            next = next_test50;
        }
    }
    if (prv_is_catm1_periodic_active()) {
        uint64_t next_catm1 = prv_next_event_centi(now_centi, prv_get_catm1_period_sec(), prv_get_catm1_offset_sec());
        if (next_catm1 < next) {
            next = next_catm1;
        }
    }

    delta_centi = (next > now_centi) ? (next - now_centi) : 1u;
    delta_ms = (uint32_t)(delta_centi * 10u);
    if (delta_ms == 0u) {
        delta_ms = 1u;
    }
    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_wakeup, delta_ms);
    (void)UTIL_TIMER_Start(&s_tmr_wakeup);
}

void GW_App_Init(void)
{
    if (s_inited) {
        return;
    }
#if (UI_USE_SEQ_MULTI_TASKS == 1u)
    UTIL_SEQ_RegTask(UI_TASK_BIT_GW_MAIN, 0, GW_TaskMain);
#endif
    (void)UTIL_TIMER_Create(&s_tmr_wakeup, 100u, UTIL_TIMER_ONESHOT, prv_tmr_wakeup_cb, NULL);
    (void)UTIL_TIMER_Create(&s_tmr_led1_pulse, 10u, UTIL_TIMER_ONESHOT, prv_led1_pulse_off_cb, NULL);
    (void)UTIL_TIMER_Create(&s_tmr_ble_test_expire, GW_BLE_TEST_SESSION_MS, UTIL_TIMER_ONESHOT, prv_tmr_ble_test_expire_cb, NULL);
    GW_Storage_Init();
    prv_flash_tx_reset_to_tail();
    GW_Catm1_Init();
    prv_led1(false);
    s_state = GW_STATE_IDLE;
    s_evt_flags = 0;
    s_beacon_counter = 0;
    s_test_mode = false;
    s_ble_test_session_active = false;
    s_rx_cycle_minute_test = false;
    s_rx_cycle_stamp_sec = 0u;
    s_rx_window_deadline_ms = 0u;
    s_rx_expected_nodes = 0u;
    s_rx_nodes_seen_mask = 0u;
    s_catm1_retry_not_before_ms = 0u;
    prv_cancel_pending_beacon_burst();
    s_beacon_recovery_mode = false;
    s_last_cycle_valid = false;
    s_last_cycle_minute_id = 0u;
    s_last_save_minute_id = 0xFFFFFFFFu;
    s_catm1_uplink_pending = false;
    s_catm1_retry_not_before_ms = 0u;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    s_boot_time_sync_pending = true;
    prv_hour_rec_init(prv_get_current_cycle_timestamp_sec());
    s_inited = true;
    prv_schedule_wakeup();
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void UI_Hook_OnConfigChanged(void)
{
    if (!s_inited) {
        return;
    }
    prv_cancel_pending_beacon_burst();
    s_catm1_uplink_pending = false;
    s_catm1_retry_not_before_ms = 0u;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    prv_update_test_mode();
    prv_schedule_wakeup();
}

void UI_Hook_OnSettingChanged(uint8_t value, char unit)
{
    (void)value;
    (void)unit;
    if (!s_inited) {
        return;
    }
    s_last_save_minute_id = 0xFFFFFFFFu;
    s_catm1_retry_not_before_ms = 0u;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    prv_update_test_mode();
    prv_prepare_beacon_burst(UI_Time_NowSec2016(), prv_get_beacon_burst_count(), GW_BEACON_BURST_GAP_MS);
    s_evt_flags |= GW_EVT_BEACON_ONESHOT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void UI_Hook_OnTimeChanged(void)
{
    if (!s_inited) {
        return;
    }
    prv_cancel_pending_beacon_burst();
    s_catm1_uplink_pending = false;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    prv_schedule_wakeup();
}

void UI_Hook_OnTestStartRequested(void)
{
    if (!s_inited) {
        return;
    }

    prv_start_ble_test_session();
    if (s_last_cycle_valid) {
        prv_send_ble_test_report_if_active(&s_last_cycle_rec);
    } else {
        prv_send_ble_test_report_if_active(&s_hour_rec);
    }
}

void UI_Hook_OnBeaconOnceRequested(void)
{
    if (!s_inited) {
        return;
    }
    prv_prepare_beacon_burst(UI_Time_NowSec2016(), prv_get_beacon_burst_count(), GW_BEACON_BURST_GAP_MS);
    s_evt_flags |= GW_EVT_BEACON_ONESHOT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_Radio_OnTxDone(void)
{
    s_evt_flags |= GW_EVT_RADIO_TX_DONE;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_Radio_OnTxTimeout(void)
{
    UI_Radio_MarkRecoverNeeded();
    s_evt_flags |= GW_EVT_RADIO_TX_TIMEOUT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

static void prv_close_rx_cycle_and_commit(void)
{
    s_state = GW_STATE_IDLE;
    UI_LPM_UnlockStop();
    s_rx_window_deadline_ms = 0u;
    s_rx_expected_nodes = 0u;
    s_rx_nodes_seen_mask = 0u;

    if (s_rx_cycle_stamp_sec != 0u) {
        s_hour_rec.epoch_sec = s_rx_cycle_stamp_sec;
    } else {
        s_hour_rec.epoch_sec = prv_get_current_cycle_timestamp_sec();
    }
    prv_mark_cycle_complete(&s_hour_rec);
    prv_update_beacon_recovery_mode_from_rec(&s_hour_rec);
    if (s_rx_cycle_minute_test) {
        uint32_t now_sec = UI_Time_NowSec2016();
        prv_handle_test50_actions_core(now_sec, true);
        prv_send_ble_test_report_if_active(&s_hour_rec);
        prv_request_catm1_uplink();
        s_rx_cycle_minute_test = false;
        s_rx_cycle_stamp_sec = 0u;
        if (prv_run_catm1_uplink_now()) {
            return;
        }
    } else {
        bool saved_ok = prv_save_hour_rec_verified(&s_hour_rec);
        prv_flash_tx_note_saved(saved_ok);
        GW_Storage_PurgeOldFiles(s_hour_rec.epoch_sec);
        prv_flash_tx_resync_after_storage_change();
        s_rx_cycle_minute_test = false;
        s_rx_cycle_stamp_sec = 0u;
    }
    prv_schedule_wakeup();
}

static void prv_rx_next_slot(void)
{
    Radio.Sleep();
    s_slot_idx++;

    if (prv_rx_window_expired() || prv_rx_all_expected_nodes_seen()) {
        prv_close_rx_cycle_and_commit();
        return;
    }

    if (!prv_radio_ready_for_rx()) {
        s_rx_cycle_minute_test = false;
        s_rx_cycle_stamp_sec = 0u;
        s_rx_window_deadline_ms = 0u;
        s_rx_expected_nodes = 0u;
        s_rx_nodes_seen_mask = 0u;
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        prv_schedule_wakeup();
        return;
    }
    if (!prv_arm_rx_slot()) {
        s_rx_cycle_minute_test = false;
        s_rx_cycle_stamp_sec = 0u;
        s_rx_window_deadline_ms = 0u;
        s_rx_expected_nodes = 0u;
        s_rx_nodes_seen_mask = 0u;
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        prv_schedule_wakeup();
        return;
    }
}

void GW_Radio_OnRxDone(uint8_t *payload, uint16_t size, int16_t rssi, int8_t snr)
{
    if (size > sizeof(s_rx_shadow)) {
        size = sizeof(s_rx_shadow);
    }
    if ((payload != NULL) && (size > 0u)) {
        memcpy(s_rx_shadow, payload, size);
    }
    s_rx_shadow_size = size;
    s_rx_shadow_rssi = rssi;
    s_rx_shadow_snr = snr;
    s_evt_flags |= GW_EVT_RADIO_RX_DONE;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_Radio_OnRxTimeout(void)
{
    UI_Radio_MarkRecoverNeeded();
    s_evt_flags |= GW_EVT_RADIO_RX_TIMEOUT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_Radio_OnRxError(void)
{
    UI_Radio_MarkRecoverNeeded();
    s_evt_flags |= GW_EVT_RADIO_RX_ERROR;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}
