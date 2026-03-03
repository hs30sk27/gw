#include "gw_app.h"


#include "ui_conf.h"
#include "ui_types.h"
#include "ui_time.h"
#include "ui_packets.h"
#include "ui_rf_plan_kr920.h"
#include "ui_lpm.h"
#include "ui_ble.h"
#include "ui_uart.h"
#include "ui_fault.h"
#include "ui_radio.h"

#include "gw_storage.h"
#include "gw_ble_report.h"
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
static UTIL_TIMER_Object_t s_tmr_ble_keepalive;

static volatile uint32_t s_evt_flags = 0;
#define GW_EVT_WAKEUP            (1u << 0)
#define GW_EVT_BEACON_ONESHOT    (1u << 1)
#define GW_EVT_RADIO_TX_DONE     (1u << 2)
#define GW_EVT_RADIO_TX_TIMEOUT  (1u << 3)
#define GW_EVT_RADIO_RX_DONE     (1u << 4)
#define GW_EVT_RADIO_RX_TIMEOUT  (1u << 5)
#define GW_EVT_RADIO_RX_ERROR    (1u << 6)
#define GW_EVT_TEST50_PUSH      (1u << 7)
#define GW_EVT_BLE_KEEPALIVE    (1u << 8)

static bool s_test_mode = false;

static uint16_t s_beacon_counter = 0; /* 노드에서 참고할 수 있도록: 노드가 비콘 받을 때마다 ++ */

static uint8_t  s_slot_idx = 0;
static uint8_t  s_slot_cnt = 0;

/* 수신 주파수(호핑 결과) */
static uint32_t s_data_freq_hz = 0;

/* 사용자 명령/SETTING에 의해 요청된 1회 비콘 */
static bool s_beacon_oneshot_pending = false;

/* 수신 데이터 저장(1시간 단위) */
static GW_HourRec_t s_hour_rec;

/* 최근 24개(1일) 버퍼 (RAM) */
static GW_HourRec_t s_hour_ring[24];
static uint8_t s_hour_ring_idx = 0;

/* 현재 분 수신 중 레코드 / 50초 전송 및 저장용 마지막 완료 레코드 */
static GW_HourRec_t s_last_cycle_rec;
static bool s_last_cycle_valid = false;
static uint32_t s_last_cycle_minute_id = 0u;
static uint32_t s_last_ble_push_minute_id = 0xFFFFFFFFu;
static uint32_t s_last_save_minute_id = 0xFFFFFFFFu;
static bool     s_catm1_uplink_pending = false;
static bool     s_catm1_uplink_with_gnss = false;
static uint32_t s_last_catm1_slot_id = 0xFFFFFFFFu;
static uint32_t s_last_daily_catm1_day_id = 0xFFFFFFFFu;

/*
 * Radio.Send() 경로에서 payload 버퍼는 TX 완료 전까지 유효해야 한다.
 * 현장 증상(BEACON ON 무반응 / SETTING:1M 후 HardFault)은
 * 로컬 스택 버퍼를 넘길 때와 일치할 수 있어, 정적 버퍼로 고정한다.
 */
static uint8_t s_beacon_tx_payload[UI_BEACON_PAYLOAD_LEN];
static uint8_t s_rx_shadow[UI_NODE_PAYLOAD_LEN];
static uint16_t s_rx_shadow_size = 0u;
static int16_t s_rx_shadow_rssi = 0;
static int8_t s_rx_shadow_snr = 0;

#define GW_BLE_KEEPALIVE_REARM_MS   (60000u)

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
    if ((Radio.SetChannel == NULL) || (Radio.Send == NULL) || (Radio.Sleep == NULL) || (Radio.SetTxConfig == NULL))
    {
        UI_FAULT_MARK("GW_RADIO_NULL",
                      (Radio.SetChannel == NULL) ? 1u : 0u,
                      ((Radio.Send == NULL) ? 1u : 0u) | ((Radio.SetTxConfig == NULL) ? 2u : 0u));
        return false;
    }
    return true;
}

static bool prv_radio_ready_for_rx(void)
{
    if ((Radio.SetChannel == NULL) || (Radio.Rx == NULL) || (Radio.Sleep == NULL) || (Radio.SetRxConfig == NULL))
    {
        UI_FAULT_MARK("GW_RADIO_NULL",
                      (Radio.SetChannel == NULL) ? 1u : 0u,
                      ((Radio.Rx == NULL) ? 1u : 0u) | ((Radio.SetRxConfig == NULL) ? 2u : 0u));
        return false;
    }
    return true;
}

static void prv_hour_rec_init(uint32_t epoch_sec)
{
    /* GW 자체 전압/내부온도도 hour record에 포함(요구사항) */
    s_hour_rec.gw_volt_x10 = 0xFFFFu;
    s_hour_rec.gw_temp_x10 = (int16_t)0xFFFFu;
    s_hour_rec.epoch_sec   = epoch_sec;

    /*
     * 배터리(최소 전류) 정책:
     *  - 필요한 순간에만 ADC Init
     *  - 측정 후 즉시 ADC DeInit
     */
    (void)GW_Sensors_MeasureGw(&s_hour_rec.gw_volt_x10, &s_hour_rec.gw_temp_x10);

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

static void prv_update_test_mode(void)
{
    const UI_Config_t* cfg = UI_GetConfig();

    /* 최신 요구: SETTING:1M 만 1분 시험 모드로 처리 */
    s_test_mode = ((cfg->setting_value == 1u) && (cfg->setting_unit == 'M'));
}

static uint32_t prv_get_setting_cycle_sec(void)
{
    const UI_Config_t* cfg = UI_GetConfig();

    if ((cfg->setting_value == 0u) || ((cfg->setting_unit != 'M') && (cfg->setting_unit != 'H')))
    {
        return 0u;
    }

    if (cfg->setting_unit == 'M')
    {
        return (uint32_t)cfg->setting_value * 60u;
    }

    return (uint32_t)cfg->setting_value * 3600u;
}

static uint32_t prv_get_normal_cycle_sec(void)
{
    uint32_t cycle_sec = prv_get_setting_cycle_sec();

    if (cycle_sec == 0u)
    {
        return UI_GW_RX_PERIOD_S_NORMAL;
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
    return s_test_mode ? 60u : prv_get_normal_cycle_sec();
}

static uint32_t prv_get_beacon_interval_sec(void)
{
    return s_test_mode ? 60u : UI_BEACON_PERIOD_S;
}

static uint32_t prv_get_rx_interval_sec(void)
{
    return s_test_mode ? 60u : prv_get_normal_cycle_sec();
}

static uint32_t prv_get_rx_start_offset_sec(void)
{
    return s_test_mode ? UI_GW_TEST_RX_START_S : UI_GW_RX_START_OFFSET_S;
}

static bool prv_is_minute_test_active(void)
{
    const UI_Config_t* cfg = UI_GetConfig();
    return (s_test_mode && (cfg->setting_value == 1u) && (cfg->setting_unit == 'M'));
}

static uint64_t prv_next_test50_centi(uint64_t now_centi)
{
    return prv_next_event_centi(now_centi, 60u, 50u);
}

static void prv_hold_ble_for_minute_test(void)
{
    if (prv_is_minute_test_active())
    {
        /* 1M 모드에서는 BLE를 계속 유지 */
        UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);
    }
}

static bool prv_setting_requests_ble_keepalive(void)
{
    const UI_Config_t* cfg = UI_GetConfig();
    return ((cfg->setting_value == 1u) && (cfg->setting_unit == 'M'));
}

static void prv_schedule_ble_keepalive(void)
{
    (void)UTIL_TIMER_Stop(&s_tmr_ble_keepalive);

    if (prv_setting_requests_ble_keepalive())
    {
        (void)UTIL_TIMER_SetPeriod(&s_tmr_ble_keepalive, GW_BLE_KEEPALIVE_REARM_MS);
        (void)UTIL_TIMER_Start(&s_tmr_ble_keepalive);
    }
}

static void prv_ble_keepalive_kick(void)
{
    UI_FAULT_CP(UI_CP_GW_KEEPALIVE, "GW_BLE_KA", UI_BLE_IsActive() ? 1u : 0u, prv_setting_requests_ble_keepalive() ? 1u : 0u);
    UI_Fault_Bp_GwKeepalive();

    if (prv_setting_requests_ble_keepalive() && UI_BLE_IsActive())
    {
        UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);
    }

    prv_schedule_ble_keepalive();
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

static void prv_handle_test50_actions(uint32_t now_sec)
{
    uint32_t now_minute_id = now_sec / 60u;

    if (!prv_is_minute_test_active())
    {
        return;
    }

    prv_hold_ble_for_minute_test();

    if ((!s_last_cycle_valid) || (s_last_cycle_minute_id != now_minute_id))
    {
        return;
    }

    if (s_last_save_minute_id != now_minute_id)
    {
        (void)GW_Storage_SaveHourRec(&s_last_cycle_rec);
        s_last_save_minute_id = now_minute_id;
        GW_Storage_PurgeOldFiles(s_last_cycle_rec.epoch_sec);
    }

    if (s_last_ble_push_minute_id != now_minute_id)
    {
        (void)GW_BleReport_SendMinuteTestRecord(&s_last_cycle_rec);
        s_last_ble_push_minute_id = now_minute_id;
    }
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
    return UI_CATM1_PERIODIC_OFFSET_S;
}

static uint32_t prv_daily_day_id_from_epoch_sec(uint32_t epoch_sec)
{
    return (epoch_sec / 86400u);
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

static void prv_request_catm1_uplink(bool with_gnss)
{
    s_catm1_uplink_pending = true;
    if (with_gnss)
    {
        s_catm1_uplink_with_gnss = true;
    }
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
    bool with_gnss;

    if (!s_catm1_uplink_pending)
    {
        return false;
    }

    if (s_state != GW_STATE_IDLE)
    {
        return false;
    }

    rec = prv_get_catm1_uplink_record();
    with_gnss = s_catm1_uplink_with_gnss;

    s_catm1_uplink_pending = false;
    s_catm1_uplink_with_gnss = false;

    UI_FAULT_MARK("GW_CATM1", with_gnss ? 1u : 0u, rec->epoch_sec);
    (void)GW_Catm1_SendSnapshot(rec, with_gnss);
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
        UI_FAULT_MARK("GW_RX_CFGFAIL", s_data_freq_hz, UI_SLOT_DURATION_MS);
        return false;
    }

    UI_FAULT_MARK("GW_RX_PREP", s_data_freq_hz, UI_SLOT_DURATION_MS);
    /* 중요: Radio.Sleep()는 동작 종료 후에만 호출한다.
     * 이전 버전은 Rx 직전에 Sleep()을 넣어서 Rx 시작 직후 fault/timeout을 유발할 수 있었다. */
    UI_FAULT_MARK("GW_RX_SET0", s_data_freq_hz, 0u);
    Radio.SetChannel(s_data_freq_hz);
    UI_FAULT_MARK("GW_RX_SET1", s_data_freq_hz, 0u);
    UI_FAULT_CP(UI_CP_GW_RX_ARM, "GW_RX_ARM", s_data_freq_hz, UI_SLOT_DURATION_MS);
    UI_Fault_Bp_GwRxArm();
    UI_FAULT_MARK("GW_RX_CALL0", s_data_freq_hz, UI_SLOT_DURATION_MS);
    Radio.Rx(UI_SLOT_DURATION_MS);
    UI_FAULT_MARK("GW_RX_CALL1", s_data_freq_hz, UI_SLOT_DURATION_MS);
    return true;
}

static void prv_schedule_after_ms(uint32_t delay_ms)
{
    if (delay_ms == 0u)
    {
        delay_ms = 1u;
    }

    UI_FAULT_MARK("GW_TMR_ARM", delay_ms, 0u);
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

    UI_FAULT_MARK("GW_BCN_BUILD", now_sec, s_beacon_counter);

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
        UI_FAULT_MARK("GW_BCN_CFGFAIL", now_sec, UI_BEACON_PAYLOAD_LEN);
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        return false;
    }

    UI_LPM_LockStop();
    s_state = GW_STATE_BEACON_TX;

    prv_led1_pulse_10ms();

    UI_FAULT_MARK("GW_BCN_PREP", UI_RF_GetBeaconFreqHz(), UI_BEACON_PAYLOAD_LEN);
    /* 중요: Radio.Sleep()는 Tx 직전에 호출하지 않는다.
     * 이전 버전은 Sleep() 후 Send()를 바로 호출해서 beacon timeout 가능성이 있었다. */
    UI_FAULT_MARK("GW_BCN_SET0", UI_RF_GetBeaconFreqHz(), UI_BEACON_PAYLOAD_LEN);
    Radio.SetChannel(UI_RF_GetBeaconFreqHz());
    UI_FAULT_MARK("GW_BCN_SET1", UI_RF_GetBeaconFreqHz(), UI_BEACON_PAYLOAD_LEN);
    UI_FAULT_CP(UI_CP_GW_BEACON_SEND, "GW_BCN_SD", s_beacon_tx_payload[0], s_beacon_tx_payload[1]);
    UI_Fault_Bp_GwBeaconSend();
    UI_FAULT_MARK("GW_BCN_SEND0", s_beacon_tx_payload[0], s_beacon_tx_payload[1]);
    Radio.Send(s_beacon_tx_payload, UI_BEACON_PAYLOAD_LEN);
    UI_FAULT_MARK("GW_BCN_SEND1", s_beacon_tx_payload[0], s_beacon_tx_payload[1]);
    return true;
}

static void prv_tmr_wakeup_cb(void *context)
{
    (void)context;
    UI_FAULT_CP(UI_CP_GW_WAKE_CB, "GW_WAKE_CB", s_state, s_evt_flags);
    UI_Fault_Bp_GwWakeCb();
    UI_FAULT_MARK("GW_TMR_CB", s_state, s_evt_flags);
    s_evt_flags |= GW_EVT_WAKEUP;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

static void prv_tmr_ble_keepalive_cb(void *context)
{
    (void)context;
    s_evt_flags |= GW_EVT_BLE_KEEPALIVE;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_App_Process(void)
{
    if (!s_inited)
    {
        return;
    }

    uint32_t ev = s_evt_flags;
    if (ev == 0u)
    {
        return;
    }
    s_evt_flags &= ~ev;

    UI_FAULT_MARK("GW_PROC", ev, s_state);
    prv_update_test_mode();
    prv_hold_ble_for_minute_test();

    if ((ev & GW_EVT_BLE_KEEPALIVE) != 0u)
    {
        prv_ble_keepalive_kick();
        ev &= ~GW_EVT_BLE_KEEPALIVE;
        if (ev == 0u)
        {
            return;
        }
    }

    if ((ev & GW_EVT_RADIO_TX_DONE) != 0u)
    {
        UI_FAULT_MARK("GW_TXDONE", s_state, s_beacon_oneshot_pending ? 1u : 0u);
        if (s_state == GW_STATE_BEACON_TX)
        {
            Radio.Sleep();
            s_state = GW_STATE_IDLE;
            UI_LPM_UnlockStop();
            if (s_beacon_oneshot_pending)
            {
                s_beacon_oneshot_pending = false;
            }
            s_beacon_counter++;
            prv_schedule_wakeup();
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_TX_DONE));
        return;
    }

    if ((ev & GW_EVT_RADIO_TX_TIMEOUT) != 0u)
    {
        UI_FAULT_MARK("GW_TXTO", s_state, 0u);
        UI_Radio_MarkRecoverNeeded();
        Radio.Sleep();
        if (s_state != GW_STATE_IDLE)
        {
            s_state = GW_STATE_IDLE;
            UI_LPM_UnlockStop();
        }
        s_beacon_oneshot_pending = false;
        prv_schedule_wakeup();
        prv_requeue_events(ev & ~(GW_EVT_RADIO_TX_TIMEOUT));
        return;
    }

    if ((ev & GW_EVT_RADIO_RX_DONE) != 0u)
    {
        UI_FAULT_MARK("GW_RXDONE", (uint32_t)(uint16_t)s_rx_shadow_rssi, (uint32_t)(uint8_t)s_rx_shadow_snr);
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
        UI_FAULT_MARK("GW_RXTO", s_state, 0u);
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
        UI_FAULT_MARK("GW_RXERR", s_state, 0u);
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
            uint64_t now_centi = UI_Time_NowCenti2016();
            uint32_t now_sec   = (uint32_t)(now_centi / 100u);

            UI_FAULT_MARK("GW_BCN_ONE", (uint32_t)(now_centi % 100u), now_sec);
            if (prv_start_beacon_tx(now_sec))
            {
                return;
            }

            /* Radio API가 준비되지 않았으면 즉시 crash하지 않고 다음 task로 복귀 */
            s_beacon_oneshot_pending = false;
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
        uint32_t beacon_off      = prv_gw_offset_sec();
        uint64_t next_beacon     = prv_next_event_centi(now_centi, beacon_interval, beacon_off);

        uint32_t rx_interval     = prv_get_rx_interval_sec();
        uint32_t rx_start        = prv_get_rx_start_offset_sec();
        uint64_t next_rx         = prv_next_event_centi(now_centi, rx_interval, rx_start);

        uint64_t next_test50 = 0xFFFFFFFFFFFFFFFFull;
        uint64_t next_catm1  = 0xFFFFFFFFFFFFFFFFull;
        uint64_t next_daily  = 0xFFFFFFFFFFFFFFFFull;
        uint64_t due_beacon  = 0u;
        uint64_t due_rx      = 0u;
        uint64_t due_test50  = 0u;
        uint64_t due_catm1   = 0u;
        uint64_t due_daily   = 0u;
        bool beacon_due_now  = prv_is_event_due_now(now_centi, beacon_interval, beacon_off, 120u, &due_beacon);
        bool rx_due_now      = prv_is_event_due_now(now_centi, rx_interval, rx_start, 120u, &due_rx);
        bool test50_due_now  = false;
        bool catm1_due_now   = false;
        bool daily_due_now   = false;

        if (prv_is_minute_test_active())
        {
            next_test50 = prv_next_test50_centi(now_centi);
            test50_due_now = prv_is_event_due_now(now_centi, 60u, 50u, 120u, &due_test50);
        }

        if (prv_is_catm1_periodic_active())
        {
            uint32_t catm1_period = prv_get_catm1_period_sec();
            next_catm1 = prv_next_event_centi(now_centi, catm1_period, prv_get_catm1_offset_sec());
            catm1_due_now = prv_is_event_due_now(now_centi, catm1_period, prv_get_catm1_offset_sec(), 120u, &due_catm1);
        }

        if (UI_Time_IsValid())
        {
            next_daily = prv_next_event_centi(now_centi, 86400u, UI_CATM1_DAILY_OFFSET_S);
            daily_due_now = prv_is_event_due_now(now_centi, 86400u, UI_CATM1_DAILY_OFFSET_S, 120u, &due_daily);
        }

        if (daily_due_now)
        {
            uint32_t day_id = prv_daily_day_id_from_epoch_sec((uint32_t)(due_daily / 100u));
            if (s_last_daily_catm1_day_id != day_id)
            {
                s_last_daily_catm1_day_id = day_id;
                prv_request_catm1_uplink(true);
            }
        }

        if (catm1_due_now)
        {
            uint32_t catm1_period = prv_get_catm1_period_sec();
            uint32_t slot_id = prv_catm1_slot_id_from_epoch_sec((uint32_t)(due_catm1 / 100u), catm1_period);
            if (s_last_catm1_slot_id != slot_id)
            {
                s_last_catm1_slot_id = slot_id;
                if (prv_last_cycle_matches_slot(catm1_period, slot_id))
                {
                    prv_request_catm1_uplink(false);
                }
            }
        }

        if (prv_is_minute_test_active() && (test50_due_now || ((next_test50 < next_beacon) && (next_test50 < next_rx))))
        {
            UI_FAULT_MARK("GW_TEST50", (uint32_t)(test50_due_now ? (due_test50 / 100u) : (next_test50 / 100u)), now_sec);
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

            UI_FAULT_MARK("GW_BCN_SCHED", beacon_sec, s_beacon_counter);
            if (prv_start_beacon_tx(beacon_sec))
            {
                return;
            }

            prv_schedule_wakeup();
            return;
        }

        /* (2) 데이터 수신 슬롯 시작: 실제 due 시점에만 시작 */
        if (rx_due_now)
        {
            uint32_t hop_period = prv_get_hop_period_sec();
            s_data_freq_hz = UI_RF_GetDataFreqHz((uint32_t)(due_rx / 100u), hop_period, 0u);

            const UI_Config_t* cfg = UI_GetConfig();
            s_slot_cnt = (uint8_t)cfg->max_nodes;
            if (s_test_mode && (s_slot_cnt > 10u)) { s_slot_cnt = 10u; }

            s_slot_idx = 0;

            if (!prv_radio_ready_for_rx())
            {
                prv_schedule_wakeup();
                return;
            }

            UI_LPM_LockStop();
            s_state = GW_STATE_RX_SLOTS;

            prv_hour_rec_init((uint32_t)(due_rx / 100u));

            UI_FAULT_MARK("GW_RX_START", s_data_freq_hz, UI_SLOT_DURATION_MS);
            if (!prv_arm_rx_slot())
            {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                prv_schedule_wakeup();
            }
            UI_FAULT_MARK("GW_RX_ARMED", s_data_freq_hz, UI_SLOT_DURATION_MS);
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
    uint32_t beacon_off      = prv_gw_offset_sec();

    uint32_t rx_interval     = prv_get_rx_interval_sec();
    uint32_t rx_start        = prv_get_rx_start_offset_sec();

    uint64_t next_beacon = prv_next_event_centi(now_centi, beacon_interval, beacon_off);
    uint64_t next_rx     = prv_next_event_centi(now_centi, rx_interval, rx_start);
    uint64_t next = (next_beacon < next_rx) ? next_beacon : next_rx;

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

    if (UI_Time_IsValid())
    {
        uint64_t next_daily = prv_next_event_centi(now_centi, 86400u, UI_CATM1_DAILY_OFFSET_S);
        if (next_daily < next)
        {
            next = next_daily;
        }
    }

    uint64_t delta_centi = (next > now_centi) ? (next - now_centi) : 1u;
    uint32_t delta_ms = (uint32_t)(delta_centi * 10u);
    UI_FAULT_MARK("GW_WAKE_SET", (uint32_t)next, delta_ms);
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
    (void)UTIL_TIMER_Create(&s_tmr_ble_keepalive, GW_BLE_KEEPALIVE_REARM_MS, UTIL_TIMER_ONESHOT, prv_tmr_ble_keepalive_cb, NULL);

    GW_Storage_Init();
    GW_Catm1_Init();

    UI_FAULT_CP(UI_CP_GW_INIT, "GW_INIT", 0u, 0u);
    prv_led1(false);
    s_state = GW_STATE_IDLE;
    s_evt_flags = 0;
    s_beacon_counter = 0;
    s_test_mode = false;
    s_beacon_oneshot_pending = false;
    s_last_cycle_valid = false;
    s_last_cycle_minute_id = 0u;
    s_last_ble_push_minute_id = 0xFFFFFFFFu;
    s_last_save_minute_id = 0xFFFFFFFFu;
    s_catm1_uplink_pending = false;
    s_catm1_uplink_with_gnss = false;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_daily_catm1_day_id = 0xFFFFFFFFu;

    /* ring 초기화 */
    memset(s_hour_ring, 0xFF, sizeof(s_hour_ring));
    s_hour_ring_idx = 0;

    prv_hour_rec_init(UI_Time_NowSec2016());

    s_inited = true;
    prv_schedule_ble_keepalive();
    prv_schedule_wakeup();
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

    UI_FAULT_MARK("GW_CFG_CHG", 0u, 0u);
    /* 설정 변경 시 스케줄 갱신 */
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

    UI_FAULT_MARK("GW_SET_CHG", value, (uint32_t)unit);
    prv_ble_keepalive_kick();
    /* SETTING 변경은 곧바로 스케줄 재계산 + 1회 비콘 송신 시도 */
    s_last_ble_push_minute_id = 0xFFFFFFFFu;
    s_last_save_minute_id = 0xFFFFFFFFu;
    s_catm1_uplink_pending = false;
    s_catm1_uplink_with_gnss = false;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_daily_catm1_day_id = 0xFFFFFFFFu;
    prv_update_test_mode();
    s_beacon_oneshot_pending = true;
    s_evt_flags |= GW_EVT_BEACON_ONESHOT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void UI_Hook_OnTimeChanged(void)
{
    if (!s_inited)
    {
        return;
    }

    UI_FAULT_MARK("GW_TIME_CHG", 0u, 0u);
    /* 시간 변경 시 스케줄 즉시 재계산 */
    s_beacon_oneshot_pending = false;
    s_catm1_uplink_pending = false;
    s_catm1_uplink_with_gnss = false;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_daily_catm1_day_id = 0xFFFFFFFFu;
    prv_schedule_wakeup();
}

void UI_Hook_OnBeaconOnceRequested(void)
{
    if (!s_inited)
    {
        return;
    }

    UI_FAULT_MARK("GW_BCN_REQ", 0u, 0u);
    /* 즉시 송신 요청 */
    s_beacon_oneshot_pending = true;
    s_evt_flags |= GW_EVT_BEACON_ONESHOT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

/* -------------------------------------------------------------------------- */
/* Radio event handlers                                                       */
/* -------------------------------------------------------------------------- */
void GW_Radio_OnTxDone(void)
{
    UI_FAULT_MARK("GW_TXDONE_I", s_state, s_beacon_oneshot_pending ? 1u : 0u);
    s_evt_flags |= GW_EVT_RADIO_TX_DONE;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_Radio_OnTxTimeout(void)
{
    UI_FAULT_MARK("GW_TXTO_I", s_state, 0u);
    UI_Radio_MarkRecoverNeeded();
    s_evt_flags |= GW_EVT_RADIO_TX_TIMEOUT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

static void prv_rx_next_slot(void)
{
    UI_FAULT_MARK("GW_RX_SLOT", s_slot_idx, s_slot_cnt);
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
        UI_FAULT_MARK("GW_RX_ARMED", s_data_freq_hz, s_slot_idx);
    }
    else
    {
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();

        s_hour_ring[s_hour_ring_idx] = s_hour_rec;
        s_hour_ring_idx = (uint8_t)((s_hour_ring_idx + 1u) % 24u);

        prv_mark_cycle_complete(&s_hour_rec);

        if (prv_is_minute_test_active())
        {
            uint32_t now_sec = UI_Time_NowSec2016();
            if ((now_sec % 60u) >= 50u)
            {
                prv_handle_test50_actions(now_sec);
            }
        }
        else
        {
            (void)GW_Storage_SaveHourRec(&s_hour_rec);
            GW_Storage_PurgeOldFiles(s_hour_rec.epoch_sec);
        }

        prv_schedule_wakeup();
    }
}

void GW_Radio_OnRxDone(uint8_t *payload, uint16_t size, int16_t rssi, int8_t snr)
{
    UI_FAULT_CP(UI_CP_GW_RX_DONE, "GW_RXDONE", size, s_state);
    UI_Fault_Bp_GwRxDone();
    UI_FAULT_MARK("GW_RXDONE_I", size, s_state);
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
    UI_FAULT_MARK("GW_RXTO_I", s_state, 0u);
    UI_Radio_MarkRecoverNeeded();
    s_evt_flags |= GW_EVT_RADIO_RX_TIMEOUT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_Radio_OnRxError(void)
{
    UI_FAULT_MARK("GW_RXERR_I", s_state, 0u);
    UI_Radio_MarkRecoverNeeded();
    s_evt_flags |= GW_EVT_RADIO_RX_ERROR;
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

