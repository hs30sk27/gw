#include "gw_app.h"

#include "ui_conf.h"
#include "ui_types.h"
#include "ui_time.h"
#include "ui_packets.h"
#include "ui_rf_plan_kr920.h"
#include "ui_lpm.h"
#include "ui_uart.h"
#include "ui_radio.h"

#include "gw_storage.h"
#include "gw_catm1.h"
#include "gw_sensors.h"

#include "stm32_timer.h"
#include "stm32_seq.h"
#include "radio.h"

#include <string.h>

/* -------------------------------------------------------------------------- */
/* Gateway 상태                                                               */
/* -------------------------------------------------------------------------- */
typedef enum
{
    GW_STATE_IDLE = 0,
    GW_STATE_BEACON_TX,
    GW_STATE_RX_SLOTS,
} GW_State_t;

static GW_State_t s_state = GW_STATE_IDLE;
static bool s_inited = false;

static UTIL_TIMER_Object_t s_tmr_wakeup;
static UTIL_TIMER_Object_t s_tmr_led1_pulse;

static volatile uint32_t s_evt_flags = 0;
#define GW_EVT_WAKEUP            (1u << 0)
#define GW_EVT_BEACON_ONESHOT    (1u << 1)
#define GW_EVT_RADIO_TX_DONE     (1u << 2)
#define GW_EVT_RADIO_TX_TIMEOUT  (1u << 3)
#define GW_EVT_RADIO_RX_DONE     (1u << 4)
#define GW_EVT_RADIO_RX_TIMEOUT  (1u << 5)
#define GW_EVT_RADIO_RX_ERROR    (1u << 6)

#define GW_FLASH_TX_BACKLOG_MAX   (24u * 5u)

static bool s_test_mode = false;

static uint16_t s_beacon_counter = 0; /* 노드에서 참고할 수 있도록: 노드가 비콘 받을 때마다 ++ */

static uint8_t  s_slot_idx = 0;
static uint8_t  s_slot_cnt = 0;

/* 수신 주파수(호핑 결과) */
static uint32_t s_data_freq_hz = 0;
static uint32_t s_rx_window_anchor_sec = 0u;

/* 사용자 명령/SETTING에 의해 요청된 1회 비콘 */
static bool s_beacon_oneshot_pending = false;

/* 수신 데이터 저장(1시간 단위) */
static GW_HourRec_t s_hour_rec;

/* RAM 링버퍼 대신, 플래시에 저장된 global record index로 미전송 backlog를 관리한다.
 * reset 후에는 현재 tail부터 시작하므로 이전 메시지는 무시된다. */
/* 현재 분 수신 중 레코드 / 50초 전송 및 저장용 마지막 완료 레코드 */
static GW_HourRec_t s_last_cycle_rec;
static bool s_last_cycle_valid = false;
static uint32_t s_last_cycle_minute_id = 0u;
static uint32_t s_last_save_minute_id = 0xFFFFFFFFu;
static bool     s_catm1_uplink_pending = false;
static uint32_t s_last_catm1_slot_id = 0xFFFFFFFFu;
static uint32_t s_last_2m_prep_slot_id = 0xFFFFFFFFu;
static uint32_t s_flash_tx_boot_tail_index = 0u;  /* boot 시점 flash tail (부팅 이전 old data cut line) */
static uint32_t s_flash_tx_next_send_index = 0u;  /* boot 이후 신규 저장분 중 다음 oldest unsent index */
static bool     s_boot_time_sync_pending = false;
static bool     s_beacon_recovery_mode = false;
static uint8_t  s_beacon_burst_remaining = 0u;
static uint32_t s_beacon_burst_anchor_sec = 0u;
static uint32_t s_beacon_burst_gap_ms = 0u;

/*
 * Radio.Send() 경로에서 payload 버퍼는 TX 완료 전까지 유효해야 한다.
 * 현장 증상(BEACON ON 무반응 / SETTING:1M 후 HardFault)은
 * 로컬 스택 버퍼를 넘길 때와 일치할 수 있어, 정적 버퍼로 고정한다.
 */
#define GW_BEACON_BURST_COUNT_NORMAL      (3u)
#define GW_BEACON_BURST_COUNT_RECOVERY    (5u)
#define GW_BEACON_BURST_GAP_MS            (250u)
#define GW_BEACON_REMINDER_BURST_COUNT    (2u)
#define GW_BEACON_REMINDER_GAP_MS         (200u)

static uint8_t s_beacon_tx_payload[UI_BEACON_PAYLOAD_LEN];
static uint8_t s_rx_shadow[UI_NODE_PAYLOAD_LEN];
static uint16_t s_rx_shadow_size = 0u;
static int16_t s_rx_shadow_rssi = 0;
static int8_t s_rx_shadow_snr = 0;

static bool prv_is_two_minute_mode_active(void);
static bool prv_start_pending_beacon_burst(void);
static void prv_cancel_pending_beacon_burst(void);
static bool prv_get_reminder_offset_sec(uint32_t* out_offset_sec);


static void prv_cancel_pending_beacon_burst(void)
{
    s_beacon_burst_remaining = 0u;
    s_beacon_burst_anchor_sec = 0u;
    s_beacon_burst_gap_ms = 0u;
    s_beacon_oneshot_pending = false;
}

static uint8_t prv_count_valid_nodes_in_rec(const GW_HourRec_t* rec)
{
    uint8_t count = 0u;

    if (rec == NULL)
    {
        return 0u;
    }

    for (uint32_t i = 0u; i < UI_MAX_NODES; i++)
    {
        const GW_NodeRec_t* n = &rec->nodes[i];
        if ((n->batt_lvl != UI_NODE_BATT_LVL_INVALID) ||
            (n->temp_c != UI_NODE_TEMP_INVALID_C) ||
            (n->x != (int16_t)0xFFFFu) ||
            (n->y != (int16_t)0xFFFFu) ||
            (n->z != (int16_t)0xFFFFu) ||
            (n->adc != 0xFFFFu) ||
            (n->pulse_cnt != 0xFFFFFFFFu))
        {
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
    /* 최신 요구: 정시 비콘은 1회만 송신한다.
     * recovery/reminder로 비콘을 늘리지 않고, ND 수신 타이밍 쪽에서 miss를 줄인다. */
    return 1u;
}

static void prv_prepare_beacon_burst(uint32_t anchor_sec, uint8_t total_count, uint32_t gap_ms)
{
    if (total_count == 0u)
    {
        total_count = 1u;
    }
    if (gap_ms == 0u)
    {
        gap_ms = GW_BEACON_BURST_GAP_MS;
    }

    s_beacon_burst_anchor_sec = anchor_sec;
    s_beacon_burst_remaining = total_count;
    s_beacon_burst_gap_ms = gap_ms;
    s_beacon_oneshot_pending = true;
}

static void prv_led0(bool on)
{
#if defined(LED0_GPIO_Port) && defined(LED0_Pin)
    HAL_GPIO_WritePin(LED0_GPIO_Port, LED0_Pin, on ? GPIO_PIN_SET : GPIO_PIN_RESET);
#else
    (void)on;
#endif
}

static void prv_led1(bool on)
{
#if UI_HAVE_LED1
    HAL_GPIO_WritePin(LED1_GPIO_Port, LED1_Pin, on ? GPIO_PIN_SET : GPIO_PIN_RESET);
#else
    (void)on;
#endif
}

static void prv_power_on_led_blink(void)
{
    for (uint32_t i = 0u; i < 2u; i++)
    {
        prv_led0(true);
        prv_led1(true);
        HAL_Delay(100u);
        prv_led0(false);
        prv_led1(false);
        HAL_Delay(100u);
    }
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
    if ((Radio.SetChannel == NULL) || (Radio.Send == NULL) || (Radio.Sleep == NULL) || (Radio.SetTxConfig == NULL))
    {
        return false;
    }
    return true;
}

static bool prv_radio_ready_for_rx(void)
{
    if ((Radio.SetChannel == NULL) || (Radio.Rx == NULL) || (Radio.Sleep == NULL) || (Radio.SetRxConfig == NULL))
    {
        return false;
    }
    return true;
}

static uint8_t prv_pack_gw_volt_x10(uint16_t raw_x10)
{
    if (raw_x10 == 0xFFFFu)
    {
        return 0xFFu;
    }
    if (raw_x10 > 254u)
    {
        return 254u;
    }
    return (uint8_t)raw_x10;
}

static int8_t prv_pack_gw_temp_c(int16_t raw_x10)
{
    int16_t c;

    if ((uint16_t)raw_x10 == 0xFFFFu)
    {
        return UI_NODE_TEMP_INVALID_C;
    }

    if (raw_x10 >= 0)
    {
        c = (int16_t)((raw_x10 + 5) / 10);
    }
    else
    {
        c = (int16_t)((raw_x10 - 5) / 10);
    }

    if (c < -50)
    {
        c = -50;
    }
    if (c > 100)
    {
        c = 100;
    }

    return (int8_t)c;
}

static void prv_hour_rec_init(uint32_t epoch_sec)
{
    uint16_t gw_volt_x10_raw = 0xFFFFu;
    int16_t  gw_temp_x10_raw = (int16_t)0xFFFFu;

    /* GW 자체 전압/내부온도도 hour record에 포함(요구사항) */
    s_hour_rec.gw_volt_x10 = 0xFFu;
    s_hour_rec.gw_temp_c   = UI_NODE_TEMP_INVALID_C;
    s_hour_rec.epoch_sec   = epoch_sec;

    /*
     * 배터리(최소 전류) 정책:
     *  - 필요한 순간에만 ADC Init
     *  - 측정 후 즉시 ADC DeInit
     */
    (void)GW_Sensors_MeasureGw(&gw_volt_x10_raw, &gw_temp_x10_raw);
    s_hour_rec.gw_volt_x10 = prv_pack_gw_volt_x10(gw_volt_x10_raw);
    s_hour_rec.gw_temp_c   = prv_pack_gw_temp_c(gw_temp_x10_raw);

    for (uint32_t i = 0; i < UI_MAX_NODES; i++)
    {
        s_hour_rec.nodes[i].batt_lvl  = UI_NODE_BATT_LVL_INVALID;
        s_hour_rec.nodes[i].temp_c    = UI_NODE_TEMP_INVALID_C;
        s_hour_rec.nodes[i].x          = (int16_t)0xFFFFu;
        s_hour_rec.nodes[i].y          = (int16_t)0xFFFFu;
        s_hour_rec.nodes[i].z          = (int16_t)0xFFFFu;
        s_hour_rec.nodes[i].adc        = 0xFFFFu;
        s_hour_rec.nodes[i].pulse_cnt  = 0xFFFFFFFFu;
    }
}

/* -------------------------------------------------------------------------- */
/* 시간 정렬(다음 이벤트 계산)                                                 */
/* -------------------------------------------------------------------------- */
static uint32_t prv_gw_offset_sec(void)
{
    /* gw0=0s, gw1=2s, gw2=4s */
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t gw = cfg->gw_num;
    if (gw > 2u) gw = 2u;
    return (uint32_t)gw * 2u;
}

static uint32_t prv_get_nd_group_span_sec(void)
{
    /*
     * GW 간 beacon 위상은 기존대로 유지하고,
     * ND data window만 GW group별로 분리한다.
     *
     * - 01M : 최대 5 node   -> 10초 폭 (2초 슬롯 x 5)
     * - 02M : 최대 10 node  -> 20초 폭 (2초 슬롯 x 10)
     * - normal : 기본 ND NUM:10 기준 11 slot -> 22초 폭
     */
    if (s_test_mode)
    {
        return 10u;
    }
    if (prv_is_two_minute_mode_active())
    {
        return 20u;
    }
    return 22u;
}

static uint32_t prv_get_nd_group_offset_sec(void)
{
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t gw = cfg->gw_num;

    if (gw > 2u)
    {
        gw = 2u;
    }

    return (uint32_t)gw * prv_get_nd_group_span_sec();
}

static uint32_t prv_get_nd_window_base_offset_sec(void)
{
    /*
     * 비콘 위상(0/2/4초)은 유지하고,
     * ND data RX/TX base time만 GW group별로 옮겨서 충돌을 줄인다.
     *
     * 01M : GW0=10s, GW1=20s, GW2=30s
     * 02M : GW0=30s, GW1=50s, GW2=70s
     * normal : GW0=60s, GW1=82s, GW2=104s
     */
    uint32_t base = 60u;

    if (s_test_mode)
    {
        base = 10u;
    }
    else if (prv_is_two_minute_mode_active())
    {
        base = 30u;
    }

    return base + prv_get_nd_group_offset_sec();
}

static uint32_t prv_get_beacon_offset_sec(void)
{
    /* 비콘 phase는 기존 GW NUM 기준(0/2/4초)을 유지한다. */
    return prv_gw_offset_sec();
}

static uint64_t prv_next_event_centi(uint64_t now_centi, uint32_t interval_sec, uint32_t offset_sec)
{
    uint32_t now_sec = (uint32_t)(now_centi / 100u);
    uint32_t centi   = (uint32_t)(now_centi % 100u);

    /* centi가 0이 아니면 다음 초부터 계산 */
    uint32_t cand_sec = now_sec + ((centi == 0u) ? 0u : 1u);

    uint32_t rem = (interval_sec == 0u) ? 0u : (cand_sec % interval_sec);
    uint32_t next_sec;

    if (interval_sec == 0u)
    {
        next_sec = cand_sec + offset_sec;
    }
    else if (rem <= offset_sec)
    {
        next_sec = cand_sec - rem + offset_sec;
    }
    else
    {
        next_sec = cand_sec - rem + interval_sec + offset_sec;
    }

    return (uint64_t)next_sec * 100u;
}

/*
 * 타이머 callback 지연 때문에 wakeup이 목표 초를 조금 지나서 들어와도
 * 현재 주기의 beacon/RX 시작을 놓치지 않기 위한 판정.
 *
 * 예) 목표가 01:00:00.00인데 실제 task 처리가 01:00:00.03에 실행되면
 *     prv_next_event_centi()는 다음 분(01:01:00)으로 넘어가 버린다.
 *     그래서 직전 event를 다시 계산해 grace window 안이면 현재 event로 본다.
 */
static bool prv_is_event_due_now(uint64_t now_centi,
                                 uint32_t interval_sec,
                                 uint32_t offset_sec,
                                 uint32_t late_grace_centi,
                                 uint64_t* due_event_centi)
{
    if ((interval_sec == 0u) || (due_event_centi == NULL))
    {
        return false;
    }

    uint64_t next_evt = prv_next_event_centi(now_centi, interval_sec, offset_sec);
    uint64_t step     = (uint64_t)interval_sec * 100u;

    /* exact boundary도 due로 인정해야 한다.
     * 그렇지 않으면 +03분 CATM1 wakeup 같은 정확한 경계에서
     * due 판정이 false가 되고, 아래 스케줄러가 next_beacon/next_rx 비교로
     * 잘못된 비콘 송신을 시작할 수 있다. */
    if (next_evt == now_centi)
    {
        *due_event_centi = next_evt;
        return true;
    }

    if (next_evt < step)
    {
        return false;
    }

    uint64_t prev_evt = next_evt - step;
    if ((now_centi >= prev_evt) && (now_centi < (prev_evt + (uint64_t)late_grace_centi)))
    {
        *due_event_centi = prev_evt;
        return true;
    }

    return false;
}

static bool prv_get_setting_value_unit_ascii_first(uint8_t* out_value, char* out_unit)
{
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t value;
    char unit;

    if ((out_value == NULL) || (out_unit == NULL) || (cfg == NULL))
    {
        return false;
    }

    if ((cfg->setting_ascii[0] >= (uint8_t)'0') && (cfg->setting_ascii[0] <= (uint8_t)'9') &&
        (cfg->setting_ascii[1] >= (uint8_t)'0') && (cfg->setting_ascii[1] <= (uint8_t)'9'))
    {
        unit = (char)cfg->setting_ascii[2];
        if ((unit == 'M') || (unit == 'H'))
        {
            value = (uint8_t)(((cfg->setting_ascii[0] - (uint8_t)'0') * 10u) +
                              (cfg->setting_ascii[1] - (uint8_t)'0'));
            if (value > 0u)
            {
                *out_value = value;
                *out_unit = unit;
                return true;
            }
        }
    }

    value = cfg->setting_value;
    unit = cfg->setting_unit;
    if ((value > 0u) && ((unit == 'M') || (unit == 'H')))
    {
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

    /* 최신 요구: SETTING:1M 만 1분 시험 모드로 처리 */
    s_test_mode = (prv_get_setting_value_unit_ascii_first(&value, &unit) && (value == 1u) && (unit == 'M'));
}

static bool prv_is_two_minute_mode_active(void)
{
    uint8_t value;
    char unit;
    return (prv_get_setting_value_unit_ascii_first(&value, &unit) && (value == 2u) && (unit == 'M'));
}

static uint32_t prv_get_setting_cycle_sec(void)
{
    uint8_t value;
    char unit;

    if (!prv_get_setting_value_unit_ascii_first(&value, &unit))
    {
        return 0u;
    }

    if (unit == 'M')
    {
        return (uint32_t)value * 60u;
    }

    return (uint32_t)value * 3600u;
}

static uint32_t prv_get_normal_cycle_sec(void)
{
    uint32_t cycle_sec = prv_get_setting_cycle_sec();

    if (cycle_sec == 0u)
    {
        return UI_GW_RX_PERIOD_S_NORMAL;
    }

    /* SETTING:2M 전용 모드
     * - beacon : 2분마다 00초(+GW offset)
     * - RX     : +30초부터 최대 10노드 수신
     * - TCP    : +01분 30초 */
    if (prv_is_two_minute_mode_active())
    {
        return 120u;
    }

    /* +01분 RX, +03분 CATM1 규칙은 최소 5분 주기에서만 의미가 있으므로,
     * 더 짧은 값이 들어오면 5분으로 올려서 안전하게 처리한다. */
    if (cycle_sec < UI_BEACON_PERIOD_S)
    {
        return UI_BEACON_PERIOD_S;
    }

    return cycle_sec;
}

static uint32_t prv_get_hop_period_sec(void)
{
    /* 최신 요구:
     * - 한 번의 ND data RX window 동안에는 모든 node를 같은 채널로 받는다.
     * - 다음 data 시간대(다음 window)로 넘어갈 때만 채널을 hop한다.
     * 따라서 hop period는 2초 slot이 아니라 현재 data cycle 주기를 사용한다. */
    return s_test_mode ? 60u : prv_get_normal_cycle_sec();
}

static uint32_t prv_get_beacon_interval_sec(void)
{
    if (s_test_mode)
    {
        return 60u;
    }
    if (prv_is_two_minute_mode_active())
    {
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
    /* beacon 시각은 유지하고, ND data window만 GW group별로 분리한다. */
    (void)UI_GW_TEST_RX_START_S;
    (void)UI_GW_RX_START_OFFSET_S;
    return prv_get_nd_window_base_offset_sec();
}

static bool prv_get_reminder_offset_sec(uint32_t* out_offset_sec)
{
    (void)out_offset_sec;
    /* 최신 요구: 정시 비콘만 1회 송신하고 reminder beacon은 사용하지 않는다. */
    return false;
}

static bool prv_get_two_minute_prep_offset_sec(uint32_t* out_offset_sec)
{
    (void)out_offset_sec;
    /* 2M 모드에서는 별도 prep 이벤트 없이 +30초에 바로 RX 슬롯을 연다. */
    return false;
}

static uint32_t prv_get_current_cycle_timestamp_sec(void)
{
    uint32_t now_sec = UI_Time_NowSec2016();
    uint32_t cycle_sec = prv_get_setting_cycle_sec();

    /* timestamp는 "노드 데이터를 받을 준비를 시작하는 현재 시각" 기준으로 잡는다.
     * 예) SET:05M 이고 RX 준비가 17:11:00에 시작되면 17:10:00으로 저장한다.
     * 즉, now %% setting_cycle 을 적용해 설정 주기 floor로 맞추고 초는 항상 00초가 된다. */
    if (cycle_sec != 0u)
    {
        return ((now_sec / cycle_sec) * cycle_sec);
    }

    return now_sec;
}

static bool prv_is_minute_test_active(void)
{
    uint8_t value;
    char unit;
    return (s_test_mode && prv_get_setting_value_unit_ascii_first(&value, &unit) && (value == 1u) && (unit == 'M'));
}

static uint64_t prv_next_test50_centi(uint64_t now_centi)
{
    return prv_next_event_centi(now_centi, 60u, 40u);
}

static void prv_mark_cycle_complete(const GW_HourRec_t* rec)
{
    if (rec == NULL)
    {
        return;
    }
    s_last_cycle_rec = *rec;
    s_last_cycle_valid = true;
    s_last_cycle_minute_id = rec->epoch_sec / 60u;
}

static uint32_t prv_flash_tx_clamp_floor_to_boot_tail(uint32_t floor_index)
{
    if (floor_index < s_flash_tx_boot_tail_index)
    {
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

    if (s_flash_tx_boot_tail_index > tail)
    {
        s_flash_tx_boot_tail_index = tail;
    }

    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index)
    {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }

    if (s_flash_tx_next_send_index > tail)
    {
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

    if (!saved_ok)
    {
        return;
    }

    tail = GW_Storage_GetTotalRecordCount();

    if (s_flash_tx_boot_tail_index > tail)
    {
        s_flash_tx_boot_tail_index = tail;
    }

    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index)
    {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }
    if (s_flash_tx_next_send_index > tail)
    {
        s_flash_tx_next_send_index = tail;
    }

    pending = tail - s_flash_tx_next_send_index;
    if (pending > GW_FLASH_TX_BACKLOG_MAX)
    {
        min_keep_index = tail - GW_FLASH_TX_BACKLOG_MAX;
        min_keep_index = prv_flash_tx_clamp_floor_to_boot_tail(min_keep_index);
        if (s_flash_tx_next_send_index < min_keep_index)
        {
            s_flash_tx_next_send_index = min_keep_index;
        }
    }
}

static void prv_flash_tx_note_sent(uint32_t sent_count)
{
    uint32_t pending = prv_flash_tx_pending_count();
    uint32_t tail = GW_Storage_GetTotalRecordCount();

    if (sent_count > pending)
    {
        sent_count = pending;
    }

    s_flash_tx_next_send_index += sent_count;
    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index)
    {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }
    if (s_flash_tx_next_send_index > tail)
    {
        s_flash_tx_next_send_index = tail;
    }
}

static void prv_flash_tx_resync_after_storage_change(void)
{
    uint32_t tail = GW_Storage_GetTotalRecordCount();
    uint32_t min_keep_index;

    if (s_flash_tx_boot_tail_index > tail)
    {
        s_flash_tx_boot_tail_index = tail;
    }

    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index)
    {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }

    if (s_flash_tx_next_send_index > tail)
    {
        s_flash_tx_next_send_index = tail;
    }

    if ((tail - s_flash_tx_next_send_index) > GW_FLASH_TX_BACKLOG_MAX)
    {
        min_keep_index = tail - GW_FLASH_TX_BACKLOG_MAX;
        min_keep_index = prv_flash_tx_clamp_floor_to_boot_tail(min_keep_index);
        if (s_flash_tx_next_send_index < min_keep_index)
        {
            s_flash_tx_next_send_index = min_keep_index;
        }
    }
}

static bool prv_save_hour_rec_verified(const GW_HourRec_t* rec)
{
    uint32_t before_cnt;
    uint32_t after_cnt;
    uint32_t try_idx;

    if (rec == NULL)
    {
        return false;
    }

    before_cnt = GW_Storage_GetTotalRecordCount();

    for (try_idx = 0u; try_idx < 2u; try_idx++)
    {
        if (!GW_Storage_SaveHourRec(rec))
        {
            continue;
        }

        after_cnt = GW_Storage_GetTotalRecordCount();
        if (after_cnt > before_cnt)
        {
            return true;
        }
    }

    return false;
}

static void prv_handle_test50_actions(uint32_t now_sec)
{
    uint32_t now_minute_id = now_sec / 60u;

    if (!prv_is_minute_test_active())
    {
        return;
    }

    if ((!s_last_cycle_valid) || (s_last_cycle_minute_id != now_minute_id))
    {
        return;
    }

    if (s_last_save_minute_id != now_minute_id)
    {
        bool saved_ok = prv_save_hour_rec_verified(&s_last_cycle_rec);
        prv_flash_tx_note_saved(saved_ok);
        s_last_save_minute_id = now_minute_id;
        GW_Storage_PurgeOldFiles(s_last_cycle_rec.epoch_sec);
        prv_flash_tx_resync_after_storage_change();
    }

    /* SETTING:1M 단축 모드에서는 40초 TCP uplink가 우선이다.
     * 분 테스트 결과는 저장 후 TCP uplink만 수행한다. */
}

/* -------------------------------------------------------------------------- */
/* Wakeup scheduling                                                           */
/* -------------------------------------------------------------------------- */
static void prv_schedule_wakeup(void);
static void prv_requeue_events(uint32_t ev_mask);
static bool prv_arm_rx_slot(void);
static void prv_rx_next_slot(void);

static bool prv_is_catm1_periodic_active(void)
{
    uint32_t cycle_sec;

    if (!UI_Time_IsValid())
    {
        return false;
    }

    if (s_test_mode)
    {
        return false;
    }

    if (prv_is_two_minute_mode_active())
    {
        return true;
    }

    cycle_sec = prv_get_setting_cycle_sec();
    return (cycle_sec >= UI_BEACON_PERIOD_S);
}

static uint32_t prv_get_catm1_period_sec(void)
{
    if (!prv_is_catm1_periodic_active())
    {
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
    if (period_sec == 0u)
    {
        return 0xFFFFFFFFu;
    }
    return (epoch_sec / period_sec);
}

static bool prv_last_cycle_matches_slot(uint32_t period_sec, uint32_t slot_id)
{
    if ((!s_last_cycle_valid) || (period_sec == 0u))
    {
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
    if (s_last_cycle_valid)
    {
        return &s_last_cycle_rec;
    }
    return &s_hour_rec;
}

static bool prv_run_catm1_uplink_now(void)
{
    const GW_HourRec_t* rec;
    uint32_t pending;

    if (!s_catm1_uplink_pending)
    {
        return false;
    }

    if (s_state != GW_STATE_IDLE)
    {
        return false;
    }

    s_catm1_uplink_pending = false;

    pending = prv_flash_tx_pending_count();
    if (pending > 0u)
    {
        GW_FileRec_t first_rec;
        uint32_t sent_count = 0u;
        uint32_t batch_count = pending;

        if (batch_count > GW_FLASH_TX_BACKLOG_MAX)
        {
            batch_count = GW_FLASH_TX_BACKLOG_MAX;
        }

        if (!GW_Storage_ReadRecordByGlobalIndex(s_flash_tx_next_send_index, &first_rec, NULL))
        {
            /* 런타임 중 파일 삭제/포맷 등으로 global index view가 깨지면
             * 현재 tail로 재동기화해서 과거 backlog는 버린다. */
            prv_flash_tx_reset_to_tail();
            pending = 0u;
        }
        else
        {
            (void)GW_Catm1_SendStoredRange(s_flash_tx_next_send_index,
                                           batch_count,
                                           &sent_count);
            if (sent_count > 0u)
            {
                prv_flash_tx_note_sent(sent_count);
            }
            prv_flash_tx_resync_after_storage_change();
            prv_schedule_wakeup();
            return true;
        }
    }

    rec = prv_get_catm1_uplink_record();
    (void)GW_Catm1_SendSnapshot(rec);
    prv_schedule_wakeup();
    return true;
}

static void prv_requeue_events(uint32_t ev_mask)
{
    if (ev_mask != 0u)
    {
        s_evt_flags |= ev_mask;
        UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
    }
}

static bool prv_arm_rx_slot(void)
{
    if (!prv_radio_ready_for_rx())
    {
        return false;
    }
    if (!UI_Radio_PrepareRx(UI_NODE_PAYLOAD_LEN))
    {
        return false;
    }

    /* 한 RX window 동안에는 같은 채널을 유지한다.
     * 채널 변경은 다음 window anchor에서만 일어난다. */
    s_data_freq_hz = UI_RF_GetDataFreqHz(s_rx_window_anchor_sec, prv_get_hop_period_sec(), 0u);

    /* 중요: Radio.Sleep()는 동작 종료 후에만 호출한다.
     * 이전 버전은 Rx 직전에 Sleep()을 넣어서 Rx 시작 직후 timeout을 유발할 수 있었다. */
    Radio.SetChannel(s_data_freq_hz);
    Radio.Rx(UI_SLOT_DURATION_MS);
    return true;
}

static void prv_schedule_after_ms(uint32_t delay_ms)
{
    if (delay_ms == 0u)
    {
        delay_ms = 1u;
    }

    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_wakeup, delay_ms);
    (void)UTIL_TIMER_Start(&s_tmr_wakeup);
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
    UI_Time_Epoch2016_ToCalendar(now_sec, &dt);

    const UI_Config_t* cfg = UI_GetConfig();
    (void)UI_Pkt_BuildBeacon(s_beacon_tx_payload, cfg->net_id, &dt, cfg->setting_ascii);

    if (!prv_radio_ready_for_tx())
    {
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        return false;
    }
    if (!UI_Radio_PrepareTx(UI_BEACON_PAYLOAD_LEN))
    {
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        return false;
    }

    UI_LPM_LockStop();
    s_state = GW_STATE_BEACON_TX;

    prv_led1_pulse_10ms();

    /* 중요: Radio.Sleep()는 Tx 직전에 호출하지 않는다.
     * 이전 버전은 Sleep() 후 Send()를 바로 호출해서 beacon timeout 가능성이 있었다. */
    Radio.SetChannel(UI_RF_GetBeaconFreqHz());
    Radio.Send(s_beacon_tx_payload, UI_BEACON_PAYLOAD_LEN);
    return true;
}

static bool prv_start_pending_beacon_burst(void)
{
    if ((!s_beacon_oneshot_pending) || (s_beacon_burst_remaining == 0u))
    {
        return false;
    }

    if (!prv_start_beacon_tx(s_beacon_burst_anchor_sec))
    {
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
    if (!s_inited)
    {
        return;
    }

    uint32_t ev = s_evt_flags;

    if (s_boot_time_sync_pending && (s_state == GW_STATE_IDLE) && !GW_Catm1_IsBusy())
    {
        /* 부팅 직후 Init 문맥에서 곧바로 CATM1을 붙이면 SMS Ready 직후 세션이 정리되는
         * 현장 증상이 있어, scheduler가 돈 뒤 첫 idle 문맥에서 1회만 시간 동기화를 수행한다. */
        s_boot_time_sync_pending = false;
        (void)GW_Catm1_SyncTimeOnce();
        prv_hour_rec_init(prv_get_current_cycle_timestamp_sec());

        if (ev == 0u)
        {
            prv_schedule_wakeup();
            return;
        }
    }

    if (ev == 0u)
    {
        return;
    }
    s_evt_flags &= ~ev;

    prv_update_test_mode();

    if ((ev & GW_EVT_RADIO_TX_DONE) != 0u)
    {
        if (s_state == GW_STATE_BEACON_TX)
        {
            Radio.Sleep();
            s_state = GW_STATE_IDLE;
            UI_LPM_UnlockStop();
            s_beacon_counter++;

            if (s_beacon_burst_remaining > 0u)
            {
                s_beacon_oneshot_pending = true;
                prv_schedule_after_ms(s_beacon_burst_gap_ms);
            }
            else
            {
                prv_cancel_pending_beacon_burst();
                prv_schedule_wakeup();
            }
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_TX_DONE));
        return;
    }

    if ((ev & GW_EVT_RADIO_TX_TIMEOUT) != 0u)
    {
        UI_Radio_MarkRecoverNeeded();
        Radio.Sleep();
        if (s_state != GW_STATE_IDLE)
        {
            s_state = GW_STATE_IDLE;
            UI_LPM_UnlockStop();
        }
        prv_cancel_pending_beacon_burst();
        prv_schedule_wakeup();
        prv_requeue_events(ev & ~(GW_EVT_RADIO_TX_TIMEOUT));
        return;
    }

    if ((ev & GW_EVT_RADIO_RX_DONE) != 0u)
    {
        if (s_state == GW_STATE_RX_SLOTS)
        {
            /* 실제 수신 완료가 발생한 경우 LED1을 10ms pulse */
            prv_led1_pulse_10ms();

            UI_NodeData_t nd;
            if (UI_Pkt_ParseNodeData(s_rx_shadow, s_rx_shadow_size, &nd))
            {
                if (nd.node_num < UI_MAX_NODES)
                {
                    const UI_Config_t* cfg = UI_GetConfig();
                    if (memcmp(nd.net_id, cfg->net_id, UI_NET_ID_LEN) == 0)
                    {
                        GW_NodeRec_t* r = &s_hour_rec.nodes[nd.node_num];
                        r->batt_lvl = nd.batt_lvl;
                        r->temp_c   = nd.temp_c;
                        r->x         = nd.x;
                        r->y         = nd.y;
                        r->z         = nd.z;
                        r->adc       = nd.adc;
                        r->pulse_cnt = nd.pulse_cnt;
                    }
                }
            }
            prv_rx_next_slot();
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_RX_DONE));
        return;
    }

    if ((ev & GW_EVT_RADIO_RX_TIMEOUT) != 0u)
    {
        if (s_state == GW_STATE_RX_SLOTS)
        {
            prv_rx_next_slot();
        }
        else
        {
            UI_Radio_MarkRecoverNeeded();
            Radio.Sleep();
            if (s_state != GW_STATE_IDLE)
            {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                prv_schedule_wakeup();
            }
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_RX_TIMEOUT));
        return;
    }

    if ((ev & GW_EVT_RADIO_RX_ERROR) != 0u)
    {
        if (s_state == GW_STATE_RX_SLOTS)
        {
            prv_rx_next_slot();
        }
        else
        {
            UI_Radio_MarkRecoverNeeded();
            Radio.Sleep();
            if (s_state != GW_STATE_IDLE)
            {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                prv_schedule_wakeup();
            }
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_RX_ERROR));
        return;
    }

    /* 비콘 원샷 요청 처리 */
    if ((ev & GW_EVT_BEACON_ONESHOT) != 0u)
    {
        s_beacon_oneshot_pending = true;
    }

    if (s_beacon_oneshot_pending)
    {
        if (s_state == GW_STATE_IDLE)
        {
            if (prv_start_pending_beacon_burst())
            {
                return;
            }

            /* Radio API가 준비되지 않았으면 즉시 crash하지 않고 다음 task로 복귀 */
            prv_schedule_wakeup();
            return;
        }

        /* Radio 동작 중이면 완료 후 다시 처리 */
        return;
    }

    if (s_catm1_uplink_pending && (s_state == GW_STATE_IDLE))
    {
        if (prv_run_catm1_uplink_now())
        {
            return;
        }
    }

    /* Wakeup 이벤트 처리 */
    if ((ev & GW_EVT_WAKEUP) != 0u)
    {
        if (s_state != GW_STATE_IDLE)
        {
            /* 동작 중에는 무시(Stop lock 상태) */
            prv_schedule_wakeup();
            return;
        }

        uint64_t now_centi = UI_Time_NowCenti2016();
        uint32_t now_sec   = (uint32_t)(now_centi / 100u);
        uint32_t beacon_interval = prv_get_beacon_interval_sec();
        uint32_t beacon_off      = prv_get_beacon_offset_sec();
        uint64_t next_beacon     = prv_next_event_centi(now_centi, beacon_interval, beacon_off);

        uint32_t rx_interval     = prv_get_rx_interval_sec();
        uint32_t rx_start        = prv_get_rx_start_offset_sec();
        uint64_t next_rx         = prv_next_event_centi(now_centi, rx_interval, rx_start);
        uint32_t reminder_off    = 0u;
        bool have_reminder       = prv_get_reminder_offset_sec(&reminder_off);
        uint32_t two_min_prep_off = 0u;
        bool have_two_min_prep = prv_get_two_minute_prep_offset_sec(&two_min_prep_off);

        uint64_t next_test50 = 0xFFFFFFFFFFFFFFFFull;
        uint64_t next_catm1  = 0xFFFFFFFFFFFFFFFFull;
        uint64_t due_beacon  = 0u;
        uint64_t due_rx      = 0u;
        uint64_t due_reminder = 0u;
        uint64_t due_prep    = 0u;
        uint64_t due_test50  = 0u;
        uint64_t due_catm1   = 0u;
        bool beacon_due_now  = prv_is_event_due_now(now_centi, beacon_interval, beacon_off, 120u, &due_beacon);
        bool rx_due_now      = prv_is_event_due_now(now_centi, rx_interval, rx_start, 120u, &due_rx);
        bool reminder_due_now = have_reminder && prv_is_event_due_now(now_centi, rx_interval, reminder_off, 120u, &due_reminder);
        bool prep_due_now    = have_two_min_prep && prv_is_event_due_now(now_centi, rx_interval, two_min_prep_off, 120u, &due_prep);
        bool test50_due_now  = false;
        bool catm1_due_now   = false;

        if (prv_is_minute_test_active())
        {
            next_test50 = prv_next_test50_centi(now_centi);
            test50_due_now = prv_is_event_due_now(now_centi, 60u, 40u, 120u, &due_test50);
        }

        if (prv_is_catm1_periodic_active())
        {
            uint32_t catm1_period = prv_get_catm1_period_sec();
            next_catm1 = prv_next_event_centi(now_centi, catm1_period, prv_get_catm1_offset_sec());
            catm1_due_now = prv_is_event_due_now(now_centi, catm1_period, prv_get_catm1_offset_sec(), 120u, &due_catm1);
        }

        if (catm1_due_now)
        {
            uint32_t catm1_period = prv_get_catm1_period_sec();
            uint32_t slot_id = prv_catm1_slot_id_from_epoch_sec((uint32_t)(due_catm1 / 100u), catm1_period);
            if (s_last_catm1_slot_id != slot_id)
            {
                s_last_catm1_slot_id = slot_id;

                if (prv_is_two_minute_mode_active())
                {
                    /* SETTING:2M은 beacon phase는 유지하고,
                     * data window만 GW group별로 30/50/70초로 분리한다.
                     * 해당 slot의 마지막 cycle record가 있으면 01분 30초에 uplink를 반드시 시도한다. */
                    if (s_last_cycle_valid || (prv_flash_tx_pending_count() > 0u))
                    {
                        prv_request_catm1_uplink();
                    }
                }
                else if (prv_last_cycle_matches_slot(catm1_period, slot_id))
                {
                    prv_request_catm1_uplink();
                }
            }
        }

        if (reminder_due_now && !beacon_due_now && !rx_due_now)
        {
            uint32_t reminder_sec = (uint32_t)(due_reminder / 100u);
            prv_prepare_beacon_burst(reminder_sec, GW_BEACON_REMINDER_BURST_COUNT, GW_BEACON_REMINDER_GAP_MS);
            if (prv_start_pending_beacon_burst())
            {
                return;
            }

            prv_schedule_wakeup();
            return;
        }

        if (prep_due_now)
        {
            uint32_t prep_slot_id = prv_catm1_slot_id_from_epoch_sec((uint32_t)(due_prep / 100u), rx_interval);
            if (s_last_2m_prep_slot_id != prep_slot_id)
            {
                s_last_2m_prep_slot_id = prep_slot_id;
                prv_hour_rec_init(prv_get_current_cycle_timestamp_sec());
            }

            if (!beacon_due_now && !rx_due_now)
            {
                prv_schedule_wakeup();
                return;
            }
        }

        if (prv_is_minute_test_active() && (test50_due_now || ((next_test50 < next_beacon) && (next_test50 < next_rx))))
        {
            prv_handle_test50_actions(now_sec);
            prv_schedule_wakeup();
            return;
        }

        if (s_catm1_uplink_pending && !beacon_due_now && !rx_due_now)
        {
            if (prv_run_catm1_uplink_now())
            {
                return;
            }
        }

        /* (1) 비콘: 실제 due 시점에만 송신한다.
         * wakeup 원인이 +03분 CATM1 또는 daily event여도,
         * next_beacon이 next_rx보다 이르다는 이유만으로 비콘을 보내지 않는다. */
        if (beacon_due_now)
        {
            uint32_t beacon_sec = (uint32_t)(due_beacon / 100u);

            prv_prepare_beacon_burst(beacon_sec, prv_get_beacon_burst_count(), GW_BEACON_BURST_GAP_MS);
            if (prv_start_pending_beacon_burst())
            {
                return;
            }

            prv_schedule_wakeup();
            return;
        }

        /* (2) 데이터 수신 슬롯 시작: 실제 due 시점에만 시작 */
        if (rx_due_now)
        {
            s_rx_window_anchor_sec = (uint32_t)(due_rx / 100u);
            s_data_freq_hz = 0u;

            const UI_Config_t* cfg = UI_GetConfig();
            s_slot_cnt = (uint8_t)cfg->max_nodes;
            /* SETTING:1M (테스트 단축)
             * - beacon 위상은 유지
             * - RX/data window는 GW group별로 10/20/30초 base로 분리
             * - 2초 슬롯 기준 최대 5노드만 확인(총 10초) -> 40초 TCP 전송과 맞춘다. */
            if (s_test_mode && (s_slot_cnt > 5u)) { s_slot_cnt = 5u; }

            /* SETTING:2M
             * - beacon 위상은 유지
             * - RX/data window는 GW group별로 30/50/70초 base로 분리
             * - 2초 슬롯 기준 최대 10노드만 수신 */
            if (prv_is_two_minute_mode_active() && (s_slot_cnt > 10u)) { s_slot_cnt = 10u; }
            s_slot_idx = 0;

            if (!prv_radio_ready_for_rx())
            {
                prv_schedule_wakeup();
                return;
            }

            uint32_t cycle_stamp_sec = prv_get_current_cycle_timestamp_sec();

            UI_LPM_LockStop();
            s_state = GW_STATE_RX_SLOTS;

            prv_hour_rec_init(cycle_stamp_sec);

            if (!prv_arm_rx_slot())
            {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                prv_schedule_wakeup();
            }
            return;
        }
    }

    /* default: 다음 wakeup 예약 */
    prv_schedule_wakeup();
}

static void GW_TaskMain(void)
{
    /* 멀티 task 모드에서만 등록/호출됨 */
    GW_App_Process();
}

static void prv_schedule_wakeup(void)
{
    if (s_state != GW_STATE_IDLE)
    {
        /* Radio busy: 이벤트는 radio callback에서 처리 */
        return;
    }

    prv_update_test_mode();

    uint64_t now_centi = UI_Time_NowCenti2016();

    if (s_beacon_oneshot_pending)
    {
        prv_schedule_after_ms(10u);
        return;
    }

    if (s_catm1_uplink_pending)
    {
        prv_schedule_after_ms(10u);
        return;
    }

    uint32_t beacon_interval = prv_get_beacon_interval_sec();
    uint32_t beacon_off      = prv_get_beacon_offset_sec();

    uint32_t rx_interval     = prv_get_rx_interval_sec();
    uint32_t rx_start        = prv_get_rx_start_offset_sec();
    uint32_t reminder_off    = 0u;
    bool have_reminder       = prv_get_reminder_offset_sec(&reminder_off);
    uint32_t two_min_prep_off = 0u;
    bool have_two_min_prep = prv_get_two_minute_prep_offset_sec(&two_min_prep_off);

    uint64_t next_beacon = prv_next_event_centi(now_centi, beacon_interval, beacon_off);
    uint64_t next_rx     = prv_next_event_centi(now_centi, rx_interval, rx_start);
    uint64_t next = (next_beacon < next_rx) ? next_beacon : next_rx;

    if (have_reminder)
    {
        uint64_t next_reminder = prv_next_event_centi(now_centi, rx_interval, reminder_off);
        if (next_reminder < next)
        {
            next = next_reminder;
        }
    }

    if (have_two_min_prep)
    {
        uint64_t next_prep = prv_next_event_centi(now_centi, rx_interval, two_min_prep_off);
        if (next_prep < next)
        {
            next = next_prep;
        }
    }

    if (prv_is_minute_test_active())
    {
        uint64_t next_test50 = prv_next_test50_centi(now_centi);
        if (next_test50 < next)
        {
            next = next_test50;
        }
    }

    if (prv_is_catm1_periodic_active())
    {
        uint64_t next_catm1 = prv_next_event_centi(now_centi, prv_get_catm1_period_sec(), prv_get_catm1_offset_sec());
        if (next_catm1 < next)
        {
            next = next_catm1;
        }
    }

    uint64_t delta_centi = (next > now_centi) ? (next - now_centi) : 1u;
    uint32_t delta_ms = (uint32_t)(delta_centi * 10u);
    if (delta_ms == 0u) delta_ms = 1u;

    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_wakeup, delta_ms);
    (void)UTIL_TIMER_Start(&s_tmr_wakeup);
}

/* -------------------------------------------------------------------------- */
/* Public API                                                                 */
/* -------------------------------------------------------------------------- */
void GW_App_Init(void)
{
    if (s_inited)
    {
        return;
    }
    /* Task 등록 (task bit이 충분한 경우에만 분리) */
#if (UI_USE_SEQ_MULTI_TASKS == 1u)
    UTIL_SEQ_RegTask(UI_TASK_BIT_GW_MAIN, 0, GW_TaskMain);
#endif

    (void)UTIL_TIMER_Create(&s_tmr_wakeup, 100u, UTIL_TIMER_ONESHOT, prv_tmr_wakeup_cb, NULL);
    (void)UTIL_TIMER_Create(&s_tmr_led1_pulse, 10u, UTIL_TIMER_ONESHOT, prv_led1_pulse_off_cb, NULL);

    GW_Storage_Init();
    prv_flash_tx_reset_to_tail(); /* reset 후에는 기존 backlog 무시 */
    GW_Catm1_Init();
    prv_power_on_led_blink();
    prv_led0(false);
    prv_led1(false);
    s_state = GW_STATE_IDLE;
    s_evt_flags = 0;
    s_beacon_counter = 0;
    s_test_mode = false;
    prv_cancel_pending_beacon_burst();
    s_beacon_recovery_mode = false;
    s_last_cycle_valid = false;
    s_last_cycle_minute_id = 0u;
    s_last_save_minute_id = 0xFFFFFFFFu;
    s_catm1_uplink_pending = false;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    s_boot_time_sync_pending = true;

    /* 부팅 직후 record는 현재 RTC 기준으로 먼저 준비하고,
     * 실제 SIM7080 시간 동기화는 첫 idle task 문맥에서 1회 수행 후 다시 갱신한다. */
    prv_hour_rec_init(prv_get_current_cycle_timestamp_sec());

    s_inited = true;
    prv_schedule_wakeup();
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

/* -------------------------------------------------------------------------- */
/* UI_CMD hook overrides                                                      */
/* -------------------------------------------------------------------------- */
void UI_Hook_OnConfigChanged(void)
{
    if (!s_inited)
    {
        return;
    }

    /* 설정 변경 시 스케줄 갱신 */
    prv_cancel_pending_beacon_burst();
    s_catm1_uplink_pending = false;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    prv_update_test_mode();
    prv_schedule_wakeup();
}

void UI_Hook_OnSettingChanged(uint8_t value, char unit)
{
    (void)value;
    (void)unit;

    if (!s_inited)
    {
        return;
    }

    /* SETTING 변경은 곧바로 스케줄 재계산 + 비콘 burst 송신 시도 */
    s_last_save_minute_id = 0xFFFFFFFFu;
    s_catm1_uplink_pending = false;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    prv_update_test_mode();
    prv_prepare_beacon_burst(UI_Time_NowSec2016(), prv_get_beacon_burst_count(), GW_BEACON_BURST_GAP_MS);
    s_evt_flags |= GW_EVT_BEACON_ONESHOT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void UI_Hook_OnTimeChanged(void)
{
    if (!s_inited)
    {
        return;
    }

    /* 시간 변경 시 스케줄 즉시 재계산 */
    prv_cancel_pending_beacon_burst();
    s_catm1_uplink_pending = false;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    prv_schedule_wakeup();
}

void UI_Hook_OnBeaconOnceRequested(void)
{
    if (!s_inited)
    {
        return;
    }

    /* 즉시 송신 요청 -> beacon burst */
    prv_prepare_beacon_burst(UI_Time_NowSec2016(), prv_get_beacon_burst_count(), GW_BEACON_BURST_GAP_MS);
    s_evt_flags |= GW_EVT_BEACON_ONESHOT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

/* -------------------------------------------------------------------------- */
/* Radio event handlers                                                       */
/* -------------------------------------------------------------------------- */
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

static void prv_rx_next_slot(void)
{
    Radio.Sleep();

    s_slot_idx++;

    if (s_slot_idx < s_slot_cnt)
    {
        if (!prv_radio_ready_for_rx())
        {
            s_state = GW_STATE_IDLE;
            UI_LPM_UnlockStop();
            prv_schedule_wakeup();
            return;
        }
        if (!prv_arm_rx_slot())
        {
            s_state = GW_STATE_IDLE;
            UI_LPM_UnlockStop();
            prv_schedule_wakeup();
            return;
        }
    }
    else
    {
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();

        /* RX 시작과 종료가 모두 같은 설정 주기 안에 있으므로,
         * 종료 직전에도 한 번 더 현재 시간을 설정 주기 floor로 맞춰 고정 timestamp를 방지한다. */
        s_hour_rec.epoch_sec = prv_get_current_cycle_timestamp_sec();
        prv_mark_cycle_complete(&s_hour_rec);
        prv_update_beacon_recovery_mode_from_rec(&s_hour_rec);

        if (prv_is_minute_test_active())
        {
            /* SETTING:1M 단축 요구사항:
             * - beacon phase는 기존 GW NUM 기준 유지
             * - RX/data window는 GW group별 10/20/30초 base 사용
             * - 40초 TCP 이전에 RX 종료 즉시 저장 후 uplink를 요청한다. */
            uint32_t now_sec = UI_Time_NowSec2016();
            prv_handle_test50_actions(now_sec);
            prv_request_catm1_uplink();

            /* 1M 모드 단축: RX 종료(≈40초) 직후 즉시 TCP uplink를 시작해야 한다.
             * pending flag만 세우고 다음 wakeup까지 기다리면 +1분으로 밀릴 수 있으므로,
             * 여기서 바로 실행한다. */
            if (prv_run_catm1_uplink_now())
            {
                return;
            }
        }
        else
        {
            bool saved_ok = prv_save_hour_rec_verified(&s_hour_rec);
            prv_flash_tx_note_saved(saved_ok);
            GW_Storage_PurgeOldFiles(s_hour_rec.epoch_sec);
            prv_flash_tx_resync_after_storage_change();
        }

        prv_schedule_wakeup();
    }
}

void GW_Radio_OnRxDone(uint8_t *payload, uint16_t size, int16_t rssi, int8_t snr)
{
    if (size > sizeof(s_rx_shadow))
    {
        size = sizeof(s_rx_shadow);
    }
    if ((payload != NULL) && (size > 0u))
    {
        memcpy(s_rx_shadow, payload, size);
    }
    s_rx_shadow_size = size;
    s_rx_shadow_rssi = rssi;
    s_rx_shadow_snr  = snr;
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

