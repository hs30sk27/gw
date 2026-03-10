#include "gw_catm1.h"
#include "ui_conf.h"
#include "ui_types.h"
#include "ui_time.h"
#include "ui_lpm.h"
#include "ui_ringbuf.h"
#include "ui_ble.h"
#include "main.h"
#include "stm32wlxx_hal.h"
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

extern UART_HandleTypeDef hlpuart1;
extern bool GW_Storage_SaveHourRec(const GW_HourRec_t* rec);
extern uint32_t GW_Storage_GetTotalRecordCount(void);

static volatile bool s_catm1_busy = false;
static volatile bool s_catm1_session_at_ok = false;
static uint8_t s_catm1_rx_byte = 0u;
static uint8_t s_catm1_rb_mem[UI_CATM1_RX_RING_SIZE];
static UI_RingBuf_t s_catm1_rb;
static bool s_catm1_rb_ready = false;
static volatile uint32_t s_catm1_last_rx_ms = 0u;
static volatile uint32_t s_catm1_last_poweroff_ms = 0u;
static volatile bool s_catm1_waiting_boot_sms_ready = false;
static volatile bool s_catm1_boot_sms_ready_seen = false;
static int64_t s_time_sync_delta_sec_buf[GW_CATM1_TIME_SYNC_DELTA_BUF_LEN];
static uint8_t s_time_sync_delta_wr = 0u;
static uint8_t s_time_sync_delta_count = 0u;

static bool s_failed_snapshot_queued_valid = false;
static uint32_t s_failed_snapshot_queued_epoch_sec = 0u;
static uint8_t s_failed_snapshot_queued_gw_num = 0u;

#ifndef GW_CATM1_NTP_HOST
#define GW_CATM1_NTP_HOST "pool.ntp.org"
#endif
#ifndef GW_CATM1_NTP_TZ_QH
#define GW_CATM1_NTP_TZ_QH (36)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_RETRY
#define GW_CATM1_STARTUP_CCLK_RETRY (20u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_GAP_MS
#define GW_CATM1_STARTUP_CCLK_GAP_MS (1500u)
#endif
#ifndef GW_CATM1_NTP_TIMEOUT_MS
#define GW_CATM1_NTP_TIMEOUT_MS (65000u)
#endif
#ifndef GW_CATM1_BOOT_URC_WAIT_MS
#define GW_CATM1_BOOT_URC_WAIT_MS (15000u)
#endif
#ifndef GW_CATM1_BOOT_QUIET_MS
#define GW_CATM1_BOOT_QUIET_MS (400u)
#endif
#ifndef GW_CATM1_SIM_READY_TIMEOUT_MS
#define GW_CATM1_SIM_READY_TIMEOUT_MS (20000u)
#endif
#ifndef GW_CATM1_START_SESSION_TIMEOUT_MS
#define GW_CATM1_START_SESSION_TIMEOUT_MS (45000u)
#endif
#ifndef GW_CATM1_STARTUP_SYNC_ATTEMPTS
#define GW_CATM1_STARTUP_SYNC_ATTEMPTS (3u)
#endif
#ifndef GW_CATM1_STARTUP_SYNC_RETRY_GAP_MS
#define GW_CATM1_STARTUP_SYNC_RETRY_GAP_MS (1500u)
#endif
#ifndef GW_CATM1_STARTUP_REG_WAIT_MS
#define GW_CATM1_STARTUP_REG_WAIT_MS (15000u)
#endif
#ifndef GW_CATM1_STARTUP_PS_WAIT_MS
#define GW_CATM1_STARTUP_PS_WAIT_MS (10000u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_FIRST_TRY
#define GW_CATM1_STARTUP_CCLK_FIRST_TRY (2u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_POST_REG_TRY
#define GW_CATM1_STARTUP_CCLK_POST_REG_TRY (3u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_POST_ATTACH_TRY
#define GW_CATM1_STARTUP_CCLK_POST_ATTACH_TRY (2u)
#endif
#ifndef GW_CATM1_AT_SYNC_MAX_TRY
#define GW_CATM1_AT_SYNC_MAX_TRY (4u)
#endif
#ifndef GW_CATM1_TCP_OPEN_HARD_TIMEOUT_MS
#define GW_CATM1_TCP_OPEN_HARD_TIMEOUT_MS (12000u)
#endif
#ifndef GW_CATM1_TCP_OPEN_SUCCESS_URC_TIMEOUT_MS
#define GW_CATM1_TCP_OPEN_SUCCESS_URC_TIMEOUT_MS (2000u)
#endif
#ifndef GW_CATM1_CCLK_QUERY_COMP_MAX_CENTI
#define GW_CATM1_CCLK_QUERY_COMP_MAX_CENTI (250u)
#endif
#ifndef GW_CATM1_POST_SMS_READY_SETTLE_MS
#define GW_CATM1_POST_SMS_READY_SETTLE_MS (2000u)
#endif
#ifndef GW_CATM1_POST_SMS_READY_AT_SYNC_WINDOW_MS
#define GW_CATM1_POST_SMS_READY_AT_SYNC_WINDOW_MS (12000u)
#endif
#ifndef GW_CATM1_POST_SMS_READY_RETRY_GAP_MS
#define GW_CATM1_POST_SMS_READY_RETRY_GAP_MS (300u)
#endif
#ifndef GW_CATM1_POWER_CYCLE_GUARD_MS
#define GW_CATM1_POWER_CYCLE_GUARD_MS (3500u)
#endif
#ifndef GW_CATM1_SESSION_RESYNC_QUIET_MS
#define GW_CATM1_SESSION_RESYNC_QUIET_MS (200u)
#endif
#ifndef GW_CATM1_SESSION_RESYNC_MAX_WAIT_MS
#define GW_CATM1_SESSION_RESYNC_MAX_WAIT_MS (1200u)
#endif
#ifndef GW_CATM1_NET_REG_MIN_TIMEOUT_MS
#define GW_CATM1_NET_REG_MIN_TIMEOUT_MS (60000u)
#endif
#ifndef GW_CATM1_PS_ATTACH_MIN_TIMEOUT_MS
#define GW_CATM1_PS_ATTACH_MIN_TIMEOUT_MS (30000u)
#endif
#ifndef GW_CATM1_QUERY_FAIL_RESYNC_STREAK
#define GW_CATM1_QUERY_FAIL_RESYNC_STREAK (3u)
#endif
#ifndef GW_CATM1_POST_CPIN_NETWORK_SETTLE_MS
#define GW_CATM1_POST_CPIN_NETWORK_SETTLE_MS (1500u)
#endif

#ifndef GW_TCP_INTERNAL_TEMP_COMP_C
#define GW_TCP_INTERNAL_TEMP_COMP_C ((int8_t)-4)
#endif

static bool prv_tcp_blocked_by_ble(void)
{
#if UI_HAVE_BT_EN
    return UI_BLE_IsActive();
#else
    return false;
#endif
}

static int8_t prv_apply_tcp_temp_comp_c(int8_t temp_c)
{
    int16_t v;

    if (temp_c == UI_NODE_TEMP_INVALID_C) {
        return UI_NODE_TEMP_INVALID_C;
    }

    v = (int16_t)temp_c + (int16_t)GW_TCP_INTERNAL_TEMP_COMP_C;
    if (v < (int16_t)UI_NODE_TEMP_MIN_C) {
        v = (int16_t)UI_NODE_TEMP_MIN_C;
    }
    if (v > (int16_t)UI_NODE_TEMP_MAX_C) {
        v = (int16_t)UI_NODE_TEMP_MAX_C;
    }
    return (int8_t)v;
}

static char prv_gw_voltage_flag(uint8_t gw_volt_x10)
{
    if (gw_volt_x10 == 0xFFu) {
        return '-';
    }
    return (gw_volt_x10 >= UI_NODE_BATT_LOW_THRESHOLD_X10) ? 'Y' : 'N';
}

static char prv_node_voltage_flag(uint8_t batt_lvl)
{
    if (batt_lvl == UI_NODE_BATT_LVL_INVALID) {
        return '-';
    }
    return (batt_lvl == UI_NODE_BATT_LVL_NORMAL) ? 'Y' : 'N';
}

static bool prv_is_leap_year(uint16_t year)
{
    return ((year % 4u) == 0u) && ((((year % 100u) != 0u) || ((year % 400u) == 0u)));
}

static uint8_t prv_days_in_month(uint16_t year, uint8_t month)
{
    static const uint8_t s_days[12] = { 31u, 28u, 31u, 30u, 31u, 30u, 31u, 31u, 30u, 31u, 30u, 31u };

    if ((month == 0u) || (month > 12u)) {
        return 30u;
    }
    if ((month == 2u) && prv_is_leap_year(year)) {
        return 29u;
    }
    return s_days[month - 1u];
}

static void prv_format_epoch2016(uint32_t epoch_sec, char* out, size_t out_sz)
{
    uint32_t days;
    uint32_t rem;
    uint16_t year = 2016u;
    uint8_t month = 1u;
    uint8_t day;
    uint8_t hour;
    uint8_t min;
    uint8_t sec;

    if ((out == NULL) || (out_sz == 0u)) {
        return;
    }

    days = epoch_sec / 86400u;
    rem = epoch_sec % 86400u;

    while (1) {
        uint32_t diy = prv_is_leap_year(year) ? 366u : 365u;
        if (days < diy) {
            break;
        }
        days -= diy;
        year++;
    }

    while (1) {
        uint8_t dim = prv_days_in_month(year, month);
        if (days < dim) {
            break;
        }
        days -= dim;
        month++;
    }

    day = (uint8_t)(days + 1u);
    hour = (uint8_t)(rem / 3600u);
    rem %= 3600u;
    min = (uint8_t)(rem / 60u);
    sec = (uint8_t)(rem % 60u);

    (void)snprintf(out, out_sz, "%04u-%02u-%02u %02u:%02u:%02u",
                   (unsigned)year, (unsigned)month, (unsigned)day,
                   (unsigned)hour, (unsigned)min, (unsigned)sec);
}

static bool prv_activate_pdp(void);
static void prv_enable_network_time_auto_update(void);
static void prv_note_failed_snapshot_sent(void);
static bool prv_wait_eps_registered(void);
static bool prv_wait_eps_registered_until(uint32_t timeout_ms);
static bool prv_wait_ps_attached(void);
static bool prv_wait_ps_attached_until(uint32_t timeout_ms);
static bool prv_try_session_resync(void);

static void prv_catm1_rb_reset(void)
{
    UI_RingBuf_Init(&s_catm1_rb, s_catm1_rb_mem, UI_CATM1_RX_RING_SIZE);
    s_catm1_rb_ready = true;
    s_catm1_last_rx_ms = HAL_GetTick();
}

static void prv_time_sync_delta_reset(void)
{
    uint32_t i;
    for (i = 0u; i < GW_CATM1_TIME_SYNC_DELTA_BUF_LEN; i++) {
        s_time_sync_delta_sec_buf[i] = 0;
    }
    s_time_sync_delta_wr = 0u;
    s_time_sync_delta_count = 0u;
}

static void prv_time_sync_delta_push(int64_t delta_sec)
{
    s_time_sync_delta_sec_buf[s_time_sync_delta_wr] = delta_sec;
    s_time_sync_delta_wr = (uint8_t)((s_time_sync_delta_wr + 1u) % GW_CATM1_TIME_SYNC_DELTA_BUF_LEN);
    if (s_time_sync_delta_count < GW_CATM1_TIME_SYNC_DELTA_BUF_LEN) {
        s_time_sync_delta_count++;
    }
}

static void prv_catm1_rx_start_it(void)
{
    HAL_StatusTypeDef st;
    if (!s_catm1_rb_ready) {
        prv_catm1_rb_reset();
    }
    st = HAL_UART_Receive_IT(&hlpuart1, &s_catm1_rx_byte, 1u);
    if ((st != HAL_OK) && (st != HAL_BUSY)) {
    }
}

static bool prv_catm1_rb_pop_wait(uint8_t* out, uint32_t timeout_ms)
{
    uint32_t start = HAL_GetTick();
    if (out == NULL) {
        return false;
    }
    if (!s_catm1_rb_ready) {
        prv_catm1_rb_reset();
    }
    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms) {
        if (UI_RingBuf_Pop(&s_catm1_rb, out)) {
            return true;
        }
        HAL_Delay(1u);
    }
    return UI_RingBuf_Pop(&s_catm1_rb, out);
}

static void prv_delay_ms(uint32_t ms)
{
    HAL_Delay(ms);
}

static void prv_wait_rx_quiet(uint32_t quiet_ms, uint32_t max_wait_ms)
{
    uint32_t start = HAL_GetTick();
    uint32_t last_rx = start;
    uint8_t ch = 0u;

    while ((uint32_t)(HAL_GetTick() - start) < max_wait_ms) {
        if (prv_catm1_rb_pop_wait(&ch, 20u)) {
            if (ch != 0u) {
                last_rx = HAL_GetTick();
            }
        }
        if ((uint32_t)(HAL_GetTick() - last_rx) >= quiet_ms) {
            break;
        }
    }
}

static void prv_power_leds_write(GPIO_PinState state)
{
#if defined(LED0_GPIO_Port) && defined(LED0_Pin)
    HAL_GPIO_WritePin(LED0_GPIO_Port, LED0_Pin, state);
#endif
#if defined(LED1_GPIO_Port) && defined(LED1_Pin)
    HAL_GPIO_WritePin(LED1_GPIO_Port, LED1_Pin, state);
#endif
}

static void prv_power_leds_blink_twice(void)
{
    uint32_t i;
    for (i = 0u; i < 2u; i++) {
        prv_power_leds_write(GPIO_PIN_SET);
        HAL_Delay(100u);
        prv_power_leds_write(GPIO_PIN_RESET);
        HAL_Delay(100u);
    }
}

static bool prv_lpuart_is_inited(void)
{
    return ((hlpuart1.gState != HAL_UART_STATE_RESET) || (hlpuart1.RxState != HAL_UART_STATE_RESET));
}

static bool prv_lpuart_ensure(void)
{
    if (hlpuart1.Instance == NULL) {
        return false;
    }
    if (!prv_lpuart_is_inited()) {
        if (HAL_UART_Init(&hlpuart1) != HAL_OK) {
            return false;
        }
    }
    if (!s_catm1_rb_ready) {
        prv_catm1_rb_reset();
    }
    prv_catm1_rx_start_it();
    return true;
}

static void prv_lpuart_release(void)
{
    if (prv_lpuart_is_inited()) {
        (void)HAL_UART_DeInit(&hlpuart1);
    }
    if (s_catm1_rb_ready) {
        prv_catm1_rb_reset();
    }
}

static void prv_get_server(uint8_t ip[4], uint16_t* port)
{
    const UI_Config_t* cfg = UI_GetConfig();
    bool ip_zero;

    ip[0] = cfg->tcpip_ip[0];
    ip[1] = cfg->tcpip_ip[1];
    ip[2] = cfg->tcpip_ip[2];
    ip[3] = cfg->tcpip_ip[3];
    *port = cfg->tcpip_port;

    ip_zero = ((ip[0] == 0u) && (ip[1] == 0u) && (ip[2] == 0u) && (ip[3] == 0u));
    if (ip_zero || (*port < UI_TCPIP_MIN_PORT)) {
        ip[0] = UI_TCPIP_DEFAULT_IP0;
        ip[1] = UI_TCPIP_DEFAULT_IP1;
        ip[2] = UI_TCPIP_DEFAULT_IP2;
        ip[3] = UI_TCPIP_DEFAULT_IP3;
        *port = UI_TCPIP_DEFAULT_PORT;
    }
}

static bool prv_uart_send_bytes(const void* data, uint16_t len, uint32_t timeout_ms)
{
    if (s_catm1_waiting_boot_sms_ready && !s_catm1_boot_sms_ready_seen) {
        return false;
    }
    if (!prv_lpuart_ensure()) {
        return false;
    }
    return (HAL_UART_Transmit(&hlpuart1, (uint8_t*)(uintptr_t)data, len, timeout_ms) == HAL_OK);
}

static bool prv_uart_send_text(const char* s, uint32_t timeout_ms)
{
    return prv_uart_send_bytes(s, (uint16_t)strlen(s), timeout_ms);
}

static void prv_uart_flush_rx(void)
{
    if (!prv_lpuart_ensure()) {
        return;
    }
    prv_catm1_rb_reset();
    prv_catm1_rx_start_it();
}

static bool prv_uart_wait_for(char* out, size_t out_sz, uint32_t timeout_ms,
                              const char* tok1, const char* tok2, const char* tok3)
{
    uint8_t ch;
    uint32_t start = HAL_GetTick();
    size_t n = 0u;

    if ((out == NULL) || (out_sz == 0u)) {
        return false;
    }

    out[0] = '\0';
    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms) {
        if (!prv_catm1_rb_pop_wait(&ch, 20u)) {
            continue;
        }
        if (ch == 0u) {
            continue;
        }
        if ((n + 1u) < out_sz) {
            out[n++] = (char)ch;
            out[n] = '\0';
        } else if (out_sz > 16u) {
            size_t keep = (out_sz / 2u);
            memmove(out, &out[out_sz - keep - 1u], keep);
            n = keep;
            out[n++] = (char)ch;
            out[n] = '\0';
        }
        if ((strstr(out, "ERROR") != NULL) || (strstr(out, "+CME ERROR") != NULL)) {
            return false;
        }
        if ((tok1 != NULL) && (strstr(out, tok1) != NULL)) {
            return true;
        }
        if ((tok2 != NULL) && (strstr(out, tok2) != NULL)) {
            return true;
        }
        if ((tok3 != NULL) && (strstr(out, tok3) != NULL)) {
            return true;
        }
    }
    return false;
}

static bool prv_send_cmd_wait(const char* cmd, const char* tok1, const char* tok2, const char* tok3,
                              uint32_t timeout_ms, char* out, size_t out_sz)
{
    prv_uart_flush_rx();
    if (!prv_uart_send_text(cmd, UI_CATM1_AT_TIMEOUT_MS)) {
        return false;
    }
    return prv_uart_wait_for(out, out_sz, timeout_ms, tok1, tok2, tok3);
}

static bool prv_send_query_wait_ok(const char* cmd, uint32_t timeout_ms, char* out, size_t out_sz)
{
    return prv_send_cmd_wait(cmd, "OK", NULL, NULL, timeout_ms, out, out_sz);
}

static bool prv_send_query_wait_prefix_ok(const char* cmd, const char* prefix,
                                          uint32_t timeout_ms, char* out, size_t out_sz)
{
    uint8_t ch;
    uint32_t start = HAL_GetTick();
    uint32_t last_rx_tick = start;
    bool saw_ok_after = false;
    size_t n = 0u;

    if ((cmd == NULL) || (out == NULL) || (out_sz == 0u)) {
        return false;
    }
    if (prefix == NULL) {
        return prv_send_query_wait_ok(cmd, timeout_ms, out, out_sz);
    }

    prv_uart_flush_rx();
    if (!prv_uart_send_text(cmd, UI_CATM1_AT_TIMEOUT_MS)) {
        return false;
    }

    out[0] = '\0';
    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms) {
        const char* pfx;
        const char* line_end;
        const char* ok_after;

        if (!prv_catm1_rb_pop_wait(&ch, UI_CATM1_QUERY_RX_POLL_MS)) {
            if (saw_ok_after && ((uint32_t)(HAL_GetTick() - last_rx_tick) >= UI_CATM1_QUERY_OK_IDLE_MS)) {
                return true;
            }
            continue;
        }
        if (ch == 0u) {
            continue;
        }
        last_rx_tick = HAL_GetTick();
        if ((n + 1u) < out_sz) {
            out[n++] = (char)ch;
            out[n] = '\0';
        } else if (out_sz > 16u) {
            size_t keep = (out_sz / 2u);
            memmove(out, &out[out_sz - keep - 1u], keep);
            n = keep;
            out[n++] = (char)ch;
            out[n] = '\0';
        }
        if ((strstr(out, "ERROR") != NULL) || (strstr(out, "+CME ERROR") != NULL)) {
            return false;
        }
        pfx = strstr(out, prefix);
        if (pfx == NULL) {
            continue;
        }
        line_end = strstr(pfx, "\r\n");
        if (line_end == NULL) {
            line_end = strchr(pfx, '\n');
        }
        if (line_end == NULL) {
            continue;
        }
        ok_after = strstr(line_end, "\r\nOK\r\n");
        if (ok_after == NULL) {
            ok_after = strstr(line_end, "\nOK\r\n");
        }
        if (ok_after == NULL) {
            ok_after = strstr(line_end, "\nOK\n");
        }
        if (ok_after != NULL) {
            saw_ok_after = true;
        }
    }
    return saw_ok_after;
}

static bool prv_send_query_wait_cnact_ok(char* out, size_t out_sz)
{
    uint8_t ch;
    uint32_t start = HAL_GetTick();
    bool saw_cnact = false;
    size_t n = 0u;

    if ((out == NULL) || (out_sz == 0u)) {
        return false;
    }

    prv_uart_flush_rx();
    if (!prv_uart_send_text("AT+CNACT?\r\n", UI_CATM1_AT_TIMEOUT_MS)) {
        return false;
    }

    out[0] = '\0';
    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_CNACT_QUERY_TIMEOUT_MS) {
        if (!prv_catm1_rb_pop_wait(&ch, UI_CATM1_QUERY_RX_POLL_MS)) {
            continue;
        }
        if (ch == 0u) {
            continue;
        }
        if ((n + 1u) < out_sz) {
            out[n++] = (char)ch;
            out[n] = '\0';
        } else if (out_sz > 16u) {
            size_t keep = (out_sz / 2u);
            memmove(out, &out[out_sz - keep - 1u], keep);
            n = keep;
            out[n++] = (char)ch;
            out[n] = '\0';
        }
        if ((strstr(out, "ERROR") != NULL) || (strstr(out, "+CME ERROR") != NULL)) {
            return false;
        }
        if (strstr(out, "+CNACT:") != NULL) {
            saw_cnact = true;
        }
        if (saw_cnact) {
            if ((strstr(out, "\r\nOK\r\n") != NULL) || (strstr(out, "\nOK\r\n") != NULL) || (strstr(out, "\nOK\n") != NULL)) {
                return true;
            }
        }
    }
    return false;
}

static bool prv_send_at_sync(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t i;
    uint32_t max_try = GW_CATM1_AT_SYNC_MAX_TRY;

    if ((UI_CATM1_AT_SYNC_RETRY != 0u) && (UI_CATM1_AT_SYNC_RETRY < max_try)) {
        max_try = UI_CATM1_AT_SYNC_RETRY;
    }
    if (max_try == 0u) {
        max_try = GW_CATM1_AT_SYNC_MAX_TRY;
    }

    for (i = 0u; i < max_try; i++) {
        rsp[0] = '\0';
        prv_uart_flush_rx();
        if (!prv_uart_send_text("AT\r\n", UI_CATM1_AT_TIMEOUT_MS)) {
            prv_delay_ms(UI_CATM1_AT_SYNC_GAP_MS);
            continue;
        }
        if (prv_uart_wait_for(rsp, sizeof(rsp), UI_CATM1_AT_TIMEOUT_MS, "OK", NULL, NULL)) {
            s_catm1_session_at_ok = true;
            (void)prv_send_cmd_wait("ATE0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
            (void)prv_send_cmd_wait("AT+CMEE=2\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
            return true;
        }
        prv_delay_ms(UI_CATM1_AT_SYNC_GAP_MS);
    }
    return false;
}

static bool prv_wait_sim_ready(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t start = HAL_GetTick();

    while ((uint32_t)(HAL_GetTick() - start) < GW_CATM1_SIM_READY_TIMEOUT_MS) {
        if (prv_send_query_wait_prefix_ok("AT+CPIN?\r\n", "+CPIN:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            (strstr(rsp, "+CPIN: READY") != NULL)) {
            return true;
        }
        prv_wait_rx_quiet(200u, 800u);
    }
    return false;
}

static bool prv_wait_boot_sms_ready(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool ready = false;

    rsp[0] = '\0';
    ready = prv_uart_wait_for(rsp, sizeof(rsp), GW_CATM1_BOOT_URC_WAIT_MS, "SMS Ready", NULL, NULL);
    if (ready) {
        s_catm1_boot_sms_ready_seen = true;
        s_catm1_waiting_boot_sms_ready = false;
    }
    return ready;
}

static bool prv_try_session_resync(void)
{
    prv_wait_rx_quiet(GW_CATM1_SESSION_RESYNC_QUIET_MS,
                      GW_CATM1_SESSION_RESYNC_MAX_WAIT_MS);
    return prv_send_at_sync();
}

static bool prv_wait_at_sync_after_sms_ready(void)
{
    uint32_t start = HAL_GetTick();
    uint32_t elapsed = 0u;

    prv_wait_rx_quiet(GW_CATM1_BOOT_QUIET_MS, GW_CATM1_BOOT_QUIET_MS + 400u);

    elapsed = (uint32_t)(HAL_GetTick() - start);
    if (elapsed < GW_CATM1_POST_SMS_READY_SETTLE_MS) {
        prv_delay_ms(GW_CATM1_POST_SMS_READY_SETTLE_MS - elapsed);
    }

    while ((uint32_t)(HAL_GetTick() - start) < GW_CATM1_POST_SMS_READY_AT_SYNC_WINDOW_MS) {
        prv_uart_flush_rx();
        if (prv_send_at_sync()) {
            return true;
        }

        prv_wait_rx_quiet(200u, 1000u);
        elapsed = (uint32_t)(HAL_GetTick() - start);
        if (elapsed >= GW_CATM1_POST_SMS_READY_AT_SYNC_WINDOW_MS) {
            break;
        }

        {
            uint32_t gap_ms = GW_CATM1_POST_SMS_READY_RETRY_GAP_MS;
            uint32_t remain_ms = GW_CATM1_POST_SMS_READY_AT_SYNC_WINDOW_MS - elapsed;
            if (gap_ms > remain_ms) {
                gap_ms = remain_ms;
            }
            if (gap_ms != 0u) {
                prv_delay_ms(gap_ms);
            }
        }
    }

    return false;
}

static bool prv_start_session(bool enable_time_auto_update)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool cpin_ready = false;

    /* TCP/session 시작은 항상 fresh boot로만 진행한다.
     * stale session_at_ok 때문에 SMS Ready 이전에 AT가 먼저 나가는 경로를 차단한다. */
    prv_uart_flush_rx();
    s_catm1_session_at_ok = false;
    s_catm1_boot_sms_ready_seen = false;
    s_catm1_waiting_boot_sms_ready = true;

    GW_Catm1_PowerOn();
    prv_delay_ms(UI_CATM1_BOOT_WAIT_MS);

    if (!prv_wait_boot_sms_ready()) {
        return false;
    }
    if (!prv_wait_at_sync_after_sms_ready()) {
        return false;
    }

    rsp[0] = '\0';
    if (prv_send_query_wait_prefix_ok("AT+CPIN?\r\n", "+CPIN:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
        (strstr(rsp, "+CPIN: READY") != NULL)) {
        cpin_ready = true;
    }

    if (!cpin_ready) {
        cpin_ready = prv_wait_sim_ready();
    }
    if (!cpin_ready) {
        return false;
    }

    /* CPIN READY 직후에는 NAS/attach가 아직 흔들릴 수 있어 바로 CTZU/CEREG를 쏘지 않고
     * 잠깐 안정 시간을 둔다. */
    prv_wait_rx_quiet(200u, 1200u);
    prv_delay_ms(GW_CATM1_POST_CPIN_NETWORK_SETTLE_MS);

    /* 등록 상태 변화 URC는 best-effort로 켜 두되, 실패해도 세션은 계속 진행한다. */
    rsp[0] = '\0';
    (void)prv_send_cmd_wait("AT+CEREG=2\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));

    /* CTZU/CLTS는 등록 직전 단계에서 URC를 쏟아 query를 흔들 수 있어
     * 실제 시간 동기화 경로(prv_sync_time_from_modem_startup)로 defer 한다. */
    if (enable_time_auto_update) {
        prv_wait_rx_quiet(100u, 400u);
    }

    rsp[0] = '\0';
    (void)prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    (void)prv_send_query_wait_prefix_ok("AT+CGATT?\r\n", "+CGATT:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));

    prv_wait_rx_quiet(200u, 1200u);
    return true;
}

static void prv_enable_network_time_auto_update(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    if (!prv_send_cmd_wait("AT+CTZU=1\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        (void)prv_send_cmd_wait("AT+CLTS=1\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    }
}

static bool prv_is_invalid_cclk_rsp(const char* rsp)
{
    const char* p;
    int yy = 0;

    if (rsp == NULL) {
        return false;
    }
    p = strstr(rsp, "+CCLK:");
    if (p == NULL) {
        return false;
    }
    while (strstr(p + 1, "+CCLK:") != NULL) {
        p = strstr(p + 1, "+CCLK:");
    }
    p = strchr(p, '"');
    if (p == NULL) {
        return false;
    }
    if (sscanf(p + 1, "%2d/", &yy) != 1) {
        return false;
    }
    return ((yy < 0) || (yy >= 80));
}

static bool prv_parse_cclk_epoch(const char* rsp, uint64_t* out_epoch_centi)
{
    const char* p;
    int yy = 0;
    int mon = 0;
    int day = 0;
    int hh = 0;
    int mm = 0;
    int ss = 0;
    int tz_q15 = 0;
    char sign = '+';
    int n;
    UI_DateTime_t dt = {0};

    if ((rsp == NULL) || (out_epoch_centi == NULL)) {
        return false;
    }
    p = strstr(rsp, "+CCLK:");
    if (p == NULL) {
        return false;
    }
    while (strstr(p + 1, "+CCLK:") != NULL) {
        p = strstr(p + 1, "+CCLK:");
    }
    p = strchr(p, '"');
    if (p == NULL) {
        return false;
    }

    n = sscanf(p + 1, "%2d/%2d/%2d,%2d:%2d:%2d%c%2d", &yy, &mon, &day, &hh, &mm, &ss, &sign, &tz_q15);
    if (n < 6) {
        return false;
    }
    if ((yy < 0) || (yy >= 80)) {
        return false;
    }
    if ((mon < 1) || (mon > 12) || (day < 1) || (day > 31) ||
        (hh < 0) || (hh > 23) || (mm < 0) || (mm > 59) || (ss < 0) || (ss > 59)) {
        return false;
    }

    (void)sign;
    (void)tz_q15;
    dt.year = (uint16_t)(2000 + yy);
    dt.month = (uint8_t)mon;
    dt.day = (uint8_t)day;
    dt.hour = (uint8_t)hh;
    dt.min = (uint8_t)mm;
    dt.sec = (uint8_t)ss;
    dt.centi = 0u;
    *out_epoch_centi = (uint64_t)UI_Time_Epoch2016_FromCalendar(&dt) * 100u;
    return true;
}

static uint64_t prv_compensate_cclk_epoch(uint64_t epoch_centi, uint32_t elapsed_ms)
{
    uint32_t comp_centi = (elapsed_ms + 9u) / 10u;
    if (comp_centi > GW_CATM1_CCLK_QUERY_COMP_MAX_CENTI) {
        comp_centi = GW_CATM1_CCLK_QUERY_COMP_MAX_CENTI;
    }
    return epoch_centi + (uint64_t)comp_centi;
}

static bool prv_query_network_time_epoch_retry(uint32_t max_try, uint32_t gap_ms,
                                               bool break_on_invalid, uint64_t* out_epoch_centi)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t i;

    if ((out_epoch_centi == NULL) || (max_try == 0u)) {
        return false;
    }

    for (i = 0u; i < max_try; i++) {
        uint32_t query_start_ms = HAL_GetTick();

        if (!prv_send_query_wait_prefix_ok("AT+CCLK?\r\n", "+CCLK:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
            (void)prv_try_session_resync();
            if ((i + 1u) < max_try) {
                prv_delay_ms(gap_ms);
            }
            continue;
        }
        if (prv_parse_cclk_epoch(rsp, out_epoch_centi)) {
            *out_epoch_centi = prv_compensate_cclk_epoch(*out_epoch_centi,
                                                         (uint32_t)(HAL_GetTick() - query_start_ms));
            return true;
        }
        if (break_on_invalid && prv_is_invalid_cclk_rsp(rsp)) {
            break;
        }
        if ((i + 1u) < max_try) {
            prv_delay_ms(gap_ms);
        }
    }
    return false;
}

static bool prv_query_network_time_epoch(uint64_t* out_epoch_centi)
{
    uint32_t max_try = UI_CATM1_TIME_SYNC_RETRY;
    if (max_try > 2u) {
        max_try = 2u;
    }
    return prv_query_network_time_epoch_retry(max_try, UI_CATM1_TIME_SYNC_GAP_MS, true, out_epoch_centi);
}

static bool prv_ntp_sync_time(uint64_t* out_epoch_centi)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    char cmd[96];

    if (out_epoch_centi == NULL) {
        return false;
    }

    (void)prv_send_cmd_wait("AT+CNTPCID=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    (void)snprintf(cmd, sizeof(cmd), "AT+CNTP=\"%s\",%d,0,1\r\n", GW_CATM1_NTP_HOST, GW_CATM1_NTP_TZ_QH);
    if (!prv_send_cmd_wait(cmd, "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    if (!prv_send_cmd_wait("AT+CNTP\r\n", "+CNTP:", NULL, NULL, GW_CATM1_NTP_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    prv_delay_ms(1000u);
    return prv_query_network_time_epoch_retry(6u, 1000u, false, out_epoch_centi);
}

static bool prv_sync_time_from_modem_startup(void)
{
    uint64_t epoch_centi = 0u;
    int64_t delta_sec;
    bool cpin_ready = false;
    bool reg_ok = false;
    bool ps_ok = false;
    char rsp[UI_CATM1_RX_BUF_SZ];

    rsp[0] = '\0';
    if (prv_send_query_wait_prefix_ok("AT+CPIN?\r\n", "+CPIN:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
        (strstr(rsp, "+CPIN: READY") != NULL)) {
        cpin_ready = true;
    }
    if (!cpin_ready) {
        return false;
    }

    prv_enable_network_time_auto_update();
    (void)prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    (void)prv_send_query_wait_prefix_ok("AT+CGATT?\r\n", "+CGATT:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));

    if (prv_query_network_time_epoch_retry(GW_CATM1_STARTUP_CCLK_FIRST_TRY,
                                           GW_CATM1_STARTUP_CCLK_GAP_MS,
                                           true,
                                           &epoch_centi)) {
        goto apply_time;
    }

    reg_ok = prv_wait_eps_registered_until(GW_CATM1_STARTUP_REG_WAIT_MS);
    if (reg_ok && prv_query_network_time_epoch_retry(GW_CATM1_STARTUP_CCLK_POST_REG_TRY,
                                                     1000u,
                                                     false,
                                                     &epoch_centi)) {
        goto apply_time;
    }

    if (reg_ok) {
        ps_ok = prv_wait_ps_attached_until(GW_CATM1_STARTUP_PS_WAIT_MS);
        if (ps_ok && prv_query_network_time_epoch_retry(GW_CATM1_STARTUP_CCLK_POST_ATTACH_TRY,
                                                        1000u,
                                                        false,
                                                        &epoch_centi)) {
            goto apply_time;
        }
        if (ps_ok && prv_activate_pdp() && prv_ntp_sync_time(&epoch_centi)) {
            goto apply_time;
        }
    }

    return false;

apply_time:
    delta_sec = (int64_t)(epoch_centi / 100u) - (int64_t)UI_Time_NowSec2016();
    prv_time_sync_delta_push(delta_sec);
    UI_Time_SetEpochCenti2016(epoch_centi);
    prv_enable_network_time_auto_update();
    return true;
}

static bool prv_sync_time_from_modem(void)
{
    uint64_t epoch_centi = 0u;
    int64_t delta_sec;

    if (!prv_query_network_time_epoch(&epoch_centi)) {
        return false;
    }
    delta_sec = (int64_t)(epoch_centi / 100u) - (int64_t)UI_Time_NowSec2016();
    prv_time_sync_delta_push(delta_sec);
    UI_Time_SetEpochCenti2016(epoch_centi);
    return true;
}

static bool prv_query_gnss_loc_line(char* out, size_t out_sz)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    char* p;
    char* e;

    if ((out == NULL) || (out_sz == 0u)) {
        return false;
    }

    out[0] = '\0';
    if (!prv_send_cmd_wait("AT+CGNSPWR=1\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    prv_delay_ms(UI_CATM1_GNSS_WAIT_MS);
    if (!prv_send_cmd_wait("AT+CGNSINF\r\n", "+CGNSINF:", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        (void)prv_send_cmd_wait("AT+CGNSPWR=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
        return false;
    }

    p = strstr(rsp, "+CGNSINF:");
    if (p != NULL) {
        while (strstr(p + 1, "+CGNSINF:") != NULL) {
            p = strstr(p + 1, "+CGNSINF:");
        }
        e = strpbrk(p, "\r\n");
        if (e != NULL) {
            *e = '\0';
        }
        (void)snprintf(out, out_sz, "LOC:%s", p);
    }

    (void)prv_send_cmd_wait("AT+CGNSPWR=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    return (out[0] != '\0');
}

static bool prv_parse_cereg_stat(const char* rsp, uint8_t* stat)
{
    const char* p;
    unsigned n = 0u;
    unsigned v = 0u;

    if ((rsp == NULL) || (stat == NULL)) {
        return false;
    }
    p = strstr(rsp, "+CEREG:");
    if (p == NULL) {
        return false;
    }
    while (strstr(p + 1, "+CEREG:") != NULL) {
        p = strstr(p + 1, "+CEREG:");
    }
    if ((sscanf(p, "+CEREG: %u,%u", &n, &v) != 2) && (sscanf(p, "+CEREG:%u,%u", &n, &v) != 2)) {
        return false;
    }
    (void)n;
    *stat = (uint8_t)v;
    return true;
}

static bool prv_wait_eps_registered(void)
{
    return prv_wait_eps_registered_until(UI_CATM1_NET_REG_TIMEOUT_MS);
}

static bool prv_wait_eps_registered_until(uint32_t timeout_ms)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t stat = 0u;
    uint32_t start = HAL_GetTick();
    uint32_t effective_timeout_ms = timeout_ms;
    uint8_t query_fail_streak = 0u;

    if (effective_timeout_ms < GW_CATM1_NET_REG_MIN_TIMEOUT_MS) {
        effective_timeout_ms = GW_CATM1_NET_REG_MIN_TIMEOUT_MS;
    }

    while ((uint32_t)(HAL_GetTick() - start) < effective_timeout_ms) {
        if (prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cereg_stat(rsp, &stat)) {
            query_fail_streak = 0u;

            if ((stat == 1u) || (stat == 5u) || (stat == 9u) || (stat == 10u)) {
                return true;
            }

            /* 0/2는 미등록/검색중, 6/7/8은 SMS only/emergency 상태라 데이터는 아직 불가하지만
             * 실제 현장에서는 잠시 후 5로 올라가는 경우가 있어 즉시 실패로 보지 않는다.
             * 3/4도 세션을 바로 끊지 않고 timeout까지 기다렸다가 판단한다. */
        } else {
            query_fail_streak++;
            if (query_fail_streak >= GW_CATM1_QUERY_FAIL_RESYNC_STREAK) {
                (void)prv_try_session_resync();
                query_fail_streak = 0u;
            } else {
                prv_wait_rx_quiet(150u, 700u);
            }
        }
        prv_delay_ms(UI_CATM1_NET_REG_POLL_MS);
    }
    return false;
}

static bool prv_parse_cgatt_state(const char* rsp, uint8_t* state)
{
    const char* p;
    unsigned v = 0u;

    if ((rsp == NULL) || (state == NULL)) {
        return false;
    }
    p = strstr(rsp, "+CGATT:");
    if (p == NULL) {
        return false;
    }
    while (strstr(p + 1, "+CGATT:") != NULL) {
        p = strstr(p + 1, "+CGATT:");
    }
    if ((sscanf(p, "+CGATT: %u", &v) != 1) && (sscanf(p, "+CGATT:%u", &v) != 1)) {
        return false;
    }
    *state = (uint8_t)v;
    return true;
}

static bool prv_wait_ps_attached(void)
{
    return prv_wait_ps_attached_until(UI_CATM1_PS_ATTACH_TIMEOUT_MS);
}

static bool prv_wait_ps_attached_until(uint32_t timeout_ms)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t state = 0u;
    uint32_t start = HAL_GetTick();
    uint32_t effective_timeout_ms = timeout_ms;
    uint8_t query_fail_streak = 0u;

    if (effective_timeout_ms < GW_CATM1_PS_ATTACH_MIN_TIMEOUT_MS) {
        effective_timeout_ms = GW_CATM1_PS_ATTACH_MIN_TIMEOUT_MS;
    }

    while ((uint32_t)(HAL_GetTick() - start) < effective_timeout_ms) {
        if (prv_send_query_wait_prefix_ok("AT+CGATT?\r\n", "+CGATT:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cgatt_state(rsp, &state)) {
            query_fail_streak = 0u;
            if (state == 1u) {
                return true;
            }
        } else {
            query_fail_streak++;
            if (query_fail_streak >= GW_CATM1_QUERY_FAIL_RESYNC_STREAK) {
                (void)prv_try_session_resync();
                query_fail_streak = 0u;
            } else {
                prv_wait_rx_quiet(150u, 700u);
            }
        }
        prv_delay_ms(UI_CATM1_PS_ATTACH_POLL_MS);
    }
    return false;
}

static bool prv_parse_cnact_ctx0(const char* rsp, uint8_t* state, uint8_t ip[4])
{
    const char* p;
    unsigned cid = 0u;
    unsigned vstate = 0u;
    unsigned a = 0u, b = 0u, c = 0u, d = 0u;

    if ((rsp == NULL) || (state == NULL) || (ip == NULL)) {
        return false;
    }
    p = rsp;
    while ((p = strstr(p, "+CNACT:")) != NULL) {
        if ((((sscanf(p, "+CNACT: %u,%u,\"%u.%u.%u.%u\"", &cid, &vstate, &a, &b, &c, &d) == 6) ||
              (sscanf(p, "+CNACT:%u,%u,\"%u.%u.%u.%u\"", &cid, &vstate, &a, &b, &c, &d) == 6))) &&
            (cid == 0u)) {
            *state = (uint8_t)vstate;
            ip[0] = (uint8_t)a;
            ip[1] = (uint8_t)b;
            ip[2] = (uint8_t)c;
            ip[3] = (uint8_t)d;
            return true;
        }
        p += 7;
    }
    return false;
}

static bool prv_activate_pdp(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    char cmd[96];
    uint8_t state = 0u;
    uint8_t ip[4] = {0u, 0u, 0u, 0u};
    uint32_t start;

    (void)snprintf(cmd, sizeof(cmd), "AT+CNCFG=0,1,\"%s\",\"\",\"\",1\r\n", UI_CATM1_1NCE_APN);
    if (!prv_send_cmd_wait(cmd, "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    if (prv_send_query_wait_cnact_ok(rsp, sizeof(rsp)) &&
        prv_parse_cnact_ctx0(rsp, &state, ip) &&
        (state == 1u) && ((ip[0] | ip[1] | ip[2] | ip[3]) != 0u)) {
        return true;
    }

    if (!prv_send_cmd_wait("AT+CNACT=0,1\r\n", "OK", "+APP PDP: 0,ACTIVE", NULL, UI_CATM1_NET_ACT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    prv_delay_ms(UI_CATM1_QUERY_OK_IDLE_MS);
    if (prv_send_query_wait_cnact_ok(rsp, sizeof(rsp)) &&
        prv_parse_cnact_ctx0(rsp, &state, ip) &&
        (state == 1u) && ((ip[0] | ip[1] | ip[2] | ip[3]) != 0u)) {
        return true;
    }

    start = HAL_GetTick();
    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_NET_ACT_TIMEOUT_MS) {
        prv_delay_ms(300u);
        if (prv_send_query_wait_cnact_ok(rsp, sizeof(rsp)) &&
            prv_parse_cnact_ctx0(rsp, &state, ip) &&
            (state == 1u) && ((ip[0] | ip[1] | ip[2] | ip[3]) != 0u)) {
            return true;
        }
    }
    return false;
}

static bool prv_parse_caopen_result(const char* rsp, uint8_t* out_cid, uint8_t* out_result)
{
    const char* p;
    unsigned cid = 0u;
    unsigned result = 0u;

    if (rsp == NULL) {
        return false;
    }

    p = rsp;
    while ((p = strstr(p, "+CAOPEN:")) != NULL) {
        if (((sscanf(p, "+CAOPEN: %u,%u", &cid, &result) == 2) ||
             (sscanf(p, "+CAOPEN:%u,%u", &cid, &result) == 2))) {
            if (out_cid != NULL) {
                *out_cid = (uint8_t)cid;
            }
            if (out_result != NULL) {
                *out_result = (uint8_t)result;
            }
            return true;
        }
        p += 8; /* strlen("+CAOPEN:") */
    }

    return false;
}

static bool prv_open_tcp(const uint8_t ip[4], uint16_t port)
{
    char cmd[96];
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t ch = 0u;
    uint8_t cid = 0xFFu;
    uint8_t result = 0xFFu;
    uint32_t start;
    uint32_t timeout_ms = GW_CATM1_TCP_OPEN_SUCCESS_URC_TIMEOUT_MS;
    size_t n = 0u;

    if ((timeout_ms == 0u) || (timeout_ms > GW_CATM1_TCP_OPEN_HARD_TIMEOUT_MS)) {
        timeout_ms = GW_CATM1_TCP_OPEN_HARD_TIMEOUT_MS;
    }

    if (!prv_send_cmd_wait("AT+CACID=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    /* 이전 세션에 남은 소켓이 있으면 새 open이 오래 붙잡힐 수 있어 먼저 정리한다. */
    (void)prv_send_cmd_wait("AT+CACLOSE=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));

    (void)snprintf(cmd, sizeof(cmd), "AT+CAOPEN=0,0,\"TCP\",\"%u.%u.%u.%u\",%u\r\n",
                   (unsigned)ip[0], (unsigned)ip[1], (unsigned)ip[2], (unsigned)ip[3], (unsigned)port);

    prv_uart_flush_rx();
    if (!prv_uart_send_text(cmd, UI_CATM1_AT_TIMEOUT_MS)) {
        return false;
    }

    rsp[0] = '\0';
    start = HAL_GetTick();
    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms) {
        if (!prv_catm1_rb_pop_wait(&ch, 20u)) {
            continue;
        }
        if (ch == 0u) {
            continue;
        }

        if ((n + 1u) < sizeof(rsp)) {
            rsp[n++] = (char)ch;
            rsp[n] = '\0';
        } else if (sizeof(rsp) > 16u) {
            size_t keep = (sizeof(rsp) / 2u);
            memmove(rsp, &rsp[sizeof(rsp) - keep - 1u], keep);
            n = keep;
            rsp[n++] = (char)ch;
            rsp[n] = '\0';
        }

        if ((strstr(rsp, "ERROR") != NULL) || (strstr(rsp, "+CME ERROR") != NULL)) {
            /* caller cleanup에서 즉시 power down 처리 */
            return false;
        }

        if (prv_parse_caopen_result(rsp, &cid, &result) && (cid == 0u)) {
            if (result == 0u) {
                return true;
            }
            /* +CAOPEN: 0,0 이 아니면 즉시 전송 취소 후 power down */
            return false;
        }
    }

    /* +CAOPEN: 0,0 이 2초 안에 없으면 caller cleanup에서 power down */
    return false;
}

static bool prv_query_caack(uint32_t* out_total, uint32_t* out_unack)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    unsigned a = 0u;
    unsigned b = 0u;
    uint32_t total = 0u;
    uint32_t unack = 0u;

    if (out_total != NULL) {
        *out_total = 0u;
    }
    if (out_unack != NULL) {
        *out_unack = 0u;
    }

    if (!prv_send_query_wait_prefix_ok("AT+CAACK=0\r\n", "+CAACK:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    if (sscanf(rsp, "%*[^:]: %u,%u", &a, &b) != 2) {
        return false;
    }
    if (b > a) {
        total = (uint32_t)b;
        unack = (uint32_t)a;
    } else {
        total = (uint32_t)a;
        unack = (uint32_t)b;
    }
    if (out_total != NULL) {
        *out_total = total;
    }
    if (out_unack != NULL) {
        *out_unack = unack;
    }
    return true;
}

static bool prv_wait_caack_drained(uint32_t base_total, uint32_t add_len)
{
    uint32_t start = HAL_GetTick();
    uint32_t need_total = base_total + add_len;

    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_ACK_WAIT_MS) {
        uint32_t total = 0u;
        uint32_t unack = 0u;
        if (prv_query_caack(&total, &unack)) {
            if ((total >= need_total) && (unack == 0u)) {
                return true;
            }
        }
        prv_delay_ms(200u);
    }
    return false;
}

static bool prv_send_tcp_payload(const char* payload)
{
    char cmd[48];
    char rsp[UI_CATM1_RX_BUF_SZ];
    size_t len;
    uint32_t base_total = 0u;
    uint32_t base_unack = 0u;
    bool have_base;

    if (payload == NULL) {
        return false;
    }
    if (prv_tcp_blocked_by_ble()) {
        return false;
    }
    len = strlen(payload);
    if ((len == 0u) || (len > 1460u)) {
        return false;
    }

    have_base = prv_query_caack(&base_total, &base_unack);
    if (!have_base) {
        base_total = 0u;
        base_unack = 0u;
    }

    (void)snprintf(cmd, sizeof(cmd), "AT+CASEND=0,%u,%u\r\n", (unsigned)len, (unsigned)UI_CATM1_SEND_INPUT_TIMEOUT_MS);
    if (!prv_send_cmd_wait(cmd, ">", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    if (!prv_uart_send_bytes(payload, (uint16_t)len, UI_CATM1_SEND_INPUT_TIMEOUT_MS + 2000u)) {
        return false;
    }
    if (!prv_uart_wait_for(rsp, sizeof(rsp), UI_CATM1_SEND_INPUT_TIMEOUT_MS + 3000u, "OK", NULL, NULL)) {
        return false;
    }
    return prv_wait_caack_drained(base_total, (uint32_t)len);
}

static bool prv_node_valid(const GW_NodeRec_t* r)
{
    if (r == NULL) {
        return false;
    }
    if (r->batt_lvl != UI_NODE_BATT_LVL_INVALID) {
        return true;
    }
    if (r->temp_c != UI_NODE_TEMP_INVALID_C) {
        return true;
    }
    if ((uint16_t)r->x != 0xFFFFu) {
        return true;
    }
    if ((uint16_t)r->y != 0xFFFFu) {
        return true;
    }
    if ((uint16_t)r->z != 0xFFFFu) {
        return true;
    }
    if (r->adc != 0xFFFFu) {
        return true;
    }
    if (r->pulse_cnt != 0xFFFFFFFFu) {
        return true;
    }
    return false;
}

static uint32_t prv_get_snapshot_node_limit(void)
{
    const UI_Config_t* cfg = UI_GetConfig();
    uint32_t limit = UI_MAX_NODES;

    if (cfg != NULL) {
        limit = (uint32_t)cfg->max_nodes;
    }
    if (limit == 0u) {
        limit = 1u;
    }
    if (limit > UI_MAX_NODES) {
        limit = UI_MAX_NODES;
    }
    return limit;
}

static bool prv_is_ascii_space(char ch)
{
    return (ch == ' ') || (ch == '\t') || (ch == '\r') || (ch == '\n');
}

static void prv_trim_ascii_inplace(char* s)
{
    size_t start = 0u;
    size_t len;

    if (s == NULL) {
        return;
    }

    while (prv_is_ascii_space(s[start])) {
        start++;
    }

    if (start > 0u) {
        memmove(s, &s[start], strlen(&s[start]) + 1u);
    }

    len = strlen(s);
    while ((len > 0u) && prv_is_ascii_space(s[len - 1u])) {
        s[len - 1u] = '\0';
        len--;
    }
}

static void prv_get_loc_ascii_safe(char* out, size_t out_sz)
{
    const char* src;
    char tmp[192];
    size_t si = 0u;
    size_t di = 0u;
    char* sep = NULL;

    if ((out == NULL) || (out_sz == 0u)) {
        return;
    }

    src = UI_GetLocAscii();
    if ((src == NULL) || (src[0] == '\0')) {
        (void)snprintf(out, out_sz, "-");
        return;
    }

    while ((src[si] != '\0') && ((di + 1u) < sizeof(tmp))) {
        char ch = src[si++];

        if ((ch == '\r') || (ch == '\n')) {
            ch = ' ';
        }
        tmp[di++] = ch;
    }
    tmp[di] = '\0';
    prv_trim_ascii_inplace(tmp);

    if (tmp[0] == '\0') {
        (void)snprintf(out, out_sz, "-");
        return;
    }

    if ((tmp[0] == '(') && (tmp[strlen(tmp) - 1u] == ')')) {
        (void)snprintf(out, out_sz, "%s", tmp);
        return;
    }

    sep = strchr(tmp, ';');
    if (sep == NULL) {
        sep = strchr(tmp, ',');
    }

    if ((sep != NULL) &&
        (strchr(sep + 1, ';') == NULL) &&
        (strchr(sep + 1, ',') == NULL)) {
        char left[96];
        char right[96];
        size_t left_len = (size_t)(sep - tmp);

        memset(left, 0, sizeof(left));
        memset(right, 0, sizeof(right));
        if (left_len >= sizeof(left)) {
            left_len = sizeof(left) - 1u;
        }
        memcpy(left, tmp, left_len);
        left[left_len] = '\0';
        (void)snprintf(right, sizeof(right), "%s", sep + 1);
        prv_trim_ascii_inplace(left);
        prv_trim_ascii_inplace(right);

        if ((left[0] != '\0') && (right[0] != '\0')) {
            (void)snprintf(out, out_sz, "(%s,%s)", left, right);
            return;
        }
    }

    di = 0u;
    for (si = 0u; (tmp[si] != '\0') && ((di + 1u) < out_sz); si++) {
        char ch = tmp[si];

        if (ch == ',') {
            ch = ';';
        }
        out[di++] = ch;
    }
    out[di] = '\0';

    if (out[0] == '\0') {
        (void)snprintf(out, out_sz, "-");
    }
}

static void prv_append_fmt(char* out, size_t out_sz, size_t* io_len, const char* fmt, ...)
{
    va_list ap;
    int n;

    if ((out == NULL) || (io_len == NULL) || (*io_len >= out_sz)) {
        return;
    }

    va_start(ap, fmt);
    n = vsnprintf(&out[*io_len], out_sz - *io_len, fmt, ap);
    va_end(ap);

    if (n <= 0) {
        return;
    }
    if ((size_t)n >= (out_sz - *io_len)) {
        *io_len = out_sz - 1u;
        out[*io_len] = '\0';
        return;
    }
    *io_len += (size_t)n;
}

static size_t prv_build_snapshot_payload(const GW_HourRec_t* rec, char* out, size_t out_sz)
{
    const UI_Config_t* cfg = UI_GetConfig();
    size_t len = 0u;
    uint32_t i;
    uint32_t node_limit;
    bool truncated = false;
    char set0, set1, set2;
    char loc_buf[192];
    char ts_buf[32];
    char gw_volt_flag;
    int gw_temp_c;

    if ((rec == NULL) || (out == NULL) || (out_sz < 64u) || (cfg == NULL)) {
        return 0u;
    }

    out[0] = '\0';
    node_limit = prv_get_snapshot_node_limit();
    prv_get_loc_ascii_safe(loc_buf, sizeof(loc_buf));
    set0 = (char)cfg->setting_ascii[0];
    set1 = (char)cfg->setting_ascii[1];
    set2 = (char)cfg->setting_ascii[2];
    prv_format_epoch2016(rec->epoch_sec, ts_buf, sizeof(ts_buf));
    gw_volt_flag = prv_gw_voltage_flag(rec->gw_volt_x10);
    gw_temp_c = (int)prv_apply_tcp_temp_comp_c(rec->gw_temp_c);

    prv_append_fmt(out, out_sz, &len,
                   "T:%s,NID:%.*s,GW:%u,L:%s,S:%c%c%c,GV:%c,GT:%d",
                   ts_buf,
                   (int)UI_NET_ID_LEN,
                   (const char*)cfg->net_id,
                   (unsigned)cfg->gw_num,
                   loc_buf,
                   set0, set1, set2,
                   gw_volt_flag,
                   gw_temp_c);

    for (i = 0u; i < node_limit; i++) {
        const GW_NodeRec_t* r = &rec->nodes[i];
        char node_volt_flag;
        int node_temp_c;

        if (!prv_node_valid(r)) {
            continue;
        }
        if ((out_sz - len) < 128u) {
            truncated = true;
            break;
        }

        node_volt_flag = prv_node_voltage_flag(r->batt_lvl);
        node_temp_c = (int)r->temp_c;
        prv_append_fmt(out, out_sz, &len,
                       ",ND:%02lu,V:%c,T:%d,X:%d,Y:%d,Z:%d,A:%u,P:%lu",
                       (unsigned long)i,
                       node_volt_flag,
                       node_temp_c,
                       (int)r->x,
                       (int)r->y,
                       (int)r->z,
                       (unsigned)r->adc,
                       (unsigned long)r->pulse_cnt);
    }
    if (truncated) {
        prv_append_fmt(out, out_sz, &len, ",TRUNC:1");
    }
    prv_append_fmt(out, out_sz, &len, "\r\n");
    return len;
}

void GW_Catm1_UartRxCpltCallback(UART_HandleTypeDef *huart)
{
    if (huart != &hlpuart1) {
        return;
    }
    if (!s_catm1_rb_ready) {
        prv_catm1_rb_reset();
    }
    if (s_catm1_rx_byte != 0u) {
        (void)UI_RingBuf_Push(&s_catm1_rb, s_catm1_rx_byte);
        s_catm1_last_rx_ms = HAL_GetTick();
    }
    prv_catm1_rx_start_it();
}

void GW_Catm1_UartErrorCallback(UART_HandleTypeDef *huart)
{
    if (huart != &hlpuart1) {
        return;
    }
    prv_catm1_rx_start_it();
}

void GW_Catm1_Init(void)
{
    prv_catm1_rb_reset();
    s_catm1_session_at_ok = false;
    s_catm1_boot_sms_ready_seen = false;
    s_catm1_waiting_boot_sms_ready = false;
    prv_power_leds_blink_twice();
}

void GW_Catm1_PowerOn(void)
{
    uint32_t now = HAL_GetTick();
    s_catm1_session_at_ok = false;
    s_catm1_boot_sms_ready_seen = false;
    s_catm1_waiting_boot_sms_ready = true;

    if (s_catm1_last_poweroff_ms != 0u) {
        uint32_t elapsed = (uint32_t)(now - s_catm1_last_poweroff_ms);
        if (elapsed < GW_CATM1_POWER_CYCLE_GUARD_MS) {
            prv_delay_ms(GW_CATM1_POWER_CYCLE_GUARD_MS - elapsed);
        }
    }
#if defined(PWR_KEY_Pin)
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
#if defined(CATM1_PWR_Pin)
    HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_SET);
#endif
#if defined(PWR_KEY_Pin)
    prv_delay_ms(UI_CATM1_PWRKEY_GUARD_MS);
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_ACTIVE_STATE);
    prv_delay_ms(UI_CATM1_PWRKEY_ON_PULSE_MS);
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
}

void GW_Catm1_PowerOff(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool normal_pd = false;

#if defined(PWR_KEY_Pin)
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
    if (prv_lpuart_is_inited() && s_catm1_session_at_ok) {
        if (prv_send_cmd_wait("AT+CPOWD=1\r\n", "NORMAL POWER DOWN", "OK", NULL,
                              UI_CATM1_PWRDOWN_TIMEOUT_MS, rsp, sizeof(rsp))) {
            normal_pd = true;
            prv_delay_ms(UI_CATM1_PWRDOWN_WAIT_MS);
        }
    }
#if defined(PWR_KEY_Pin)
    if (!normal_pd) {
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_ACTIVE_STATE);
        prv_delay_ms(UI_CATM1_PWRKEY_OFF_PULSE_MS);
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
        prv_delay_ms(UI_CATM1_PWRDOWN_WAIT_MS);
    }
#endif
#if defined(CATM1_PWR_Pin)
    HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_RESET);
#endif
    s_catm1_session_at_ok = false;
    s_catm1_boot_sms_ready_seen = false;
    s_catm1_waiting_boot_sms_ready = false;
    s_catm1_last_poweroff_ms = HAL_GetTick();
}

bool GW_Catm1_IsBusy(void)
{
    return s_catm1_busy;
}

void GW_Catm1_SetBusy(bool busy)
{
    s_catm1_busy = busy;
}

void GW_Catm1_ClearTimeSyncDeltaBuf(void)
{
    prv_time_sync_delta_reset();
}

uint8_t GW_Catm1_GetTimeSyncDeltaCount(void)
{
    return s_time_sync_delta_count;
}

uint8_t GW_Catm1_CopyTimeSyncDeltaBuf(int64_t* out_buf, uint8_t max_items)
{
    uint8_t stored;
    uint8_t cnt;
    uint8_t i;
    uint8_t oldest;
    uint8_t start;
    uint8_t skip;

    if ((out_buf == NULL) || (max_items == 0u)) {
        return 0u;
    }

    stored = s_time_sync_delta_count;
    cnt = stored;
    if (cnt > max_items) {
        cnt = max_items;
    }
    oldest = (stored < GW_CATM1_TIME_SYNC_DELTA_BUF_LEN) ? 0u : s_time_sync_delta_wr;
    skip = (uint8_t)(stored - cnt);
    start = (uint8_t)((oldest + skip) % GW_CATM1_TIME_SYNC_DELTA_BUF_LEN);
    for (i = 0u; i < cnt; i++) {
        out_buf[i] = s_time_sync_delta_sec_buf[(uint8_t)((start + i) % GW_CATM1_TIME_SYNC_DELTA_BUF_LEN)];
    }
    return cnt;
}

bool GW_Catm1_SyncTimeOnce(void)
{
    bool success = false;

    if (GW_Catm1_IsBusy()) {
        return false;
    }

    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);
    if (!prv_lpuart_ensure()) {
        goto cleanup;
    }
    if (!prv_start_session(true)) {
        goto cleanup;
    }
    success = prv_sync_time_from_modem_startup();

cleanup:
    GW_Catm1_PowerOff();
    prv_lpuart_release();
    GW_Catm1_SetBusy(false);
    UI_LPM_UnlockStop();
    return success;
}

bool GW_Catm1_QueryAndStoreLoc(char* out_line, size_t out_sz)
{
    char loc_line[GW_LOC_LINE_MAX];
    GW_LocRec_t loc_rec;
    bool success = false;

    if (GW_Catm1_IsBusy()) {
        return false;
    }
    if ((out_line != NULL) && (out_sz > 0u)) {
        out_line[0] = '\0';
    }

    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);
    if (!prv_lpuart_ensure()) {
        goto cleanup;
    }
    if (!prv_start_session(true)) {
        goto cleanup;
    }
    if (prv_wait_eps_registered() && prv_wait_ps_attached()) {
        (void)prv_sync_time_from_modem_startup();
    }
    if (!prv_query_gnss_loc_line(loc_line, sizeof(loc_line))) {
        goto cleanup;
    }

    memset(&loc_rec, 0, sizeof(loc_rec));
    loc_rec.saved_epoch_sec = UI_Time_NowSec2016();
    (void)snprintf(loc_rec.line, sizeof(loc_rec.line), "%s", loc_line);
    if (!GW_Storage_SaveLocRec(&loc_rec)) {
        goto cleanup;
    }
    if ((out_line != NULL) && (out_sz > 0u)) {
        (void)snprintf(out_line, out_sz, "%s", loc_rec.line);
    }
    success = true;

cleanup:
    GW_Catm1_PowerOff();
    prv_lpuart_release();
    GW_Catm1_SetBusy(false);
    UI_LPM_UnlockStop();
    return success;
}

static void prv_note_failed_snapshot_sent(void)
{
    s_failed_snapshot_queued_valid = false;
    s_failed_snapshot_queued_epoch_sec = 0u;
    s_failed_snapshot_queued_gw_num = 0u;
}

static bool prv_store_failed_snapshot_to_flash(const GW_HourRec_t* rec)
{
    uint32_t before_cnt;
    uint32_t after_cnt;
    uint32_t try_idx;
    const UI_Config_t* cfg;
    uint8_t cur_gw_num;

    if (rec == NULL) {
        return false;
    }

    cfg = UI_GetConfig();
    cur_gw_num = (cfg != NULL) ? cfg->gw_num : 0u;

    if (s_failed_snapshot_queued_valid &&
        (s_failed_snapshot_queued_epoch_sec == rec->epoch_sec) &&
        (s_failed_snapshot_queued_gw_num == cur_gw_num)) {
        return true;
    }

    before_cnt = GW_Storage_GetTotalRecordCount();
    for (try_idx = 0u; try_idx < 2u; try_idx++) {
        if (!GW_Storage_SaveHourRec(rec)) {
            continue;
        }
        after_cnt = GW_Storage_GetTotalRecordCount();
        if (after_cnt > before_cnt) {
            s_failed_snapshot_queued_valid = true;
            s_failed_snapshot_queued_epoch_sec = rec->epoch_sec;
            s_failed_snapshot_queued_gw_num = cur_gw_num;
            return true;
        }
    }

    return false;
}

bool GW_Catm1_SendSnapshot(const GW_HourRec_t* rec)
{
    uint8_t ip[4];
    uint16_t port;
    char payload[UI_CATM1_SERVER_PAYLOAD_MAX + 1u];
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool success = false;
    bool opened = false;
    bool pdp_active = false;
    bool should_store_on_fail = false;
    size_t len;
    GW_HourRec_t live_rec;

    if (rec == NULL) {
        return false;
    }
    if (prv_tcp_blocked_by_ble()) {
        return false;
    }

    live_rec = *rec;
    len = prv_build_snapshot_payload(&live_rec, payload, sizeof(payload));
    if (len == 0u) {
        return false;
    }

    if (prv_tcp_blocked_by_ble()) {
        return false;
    }

    should_store_on_fail = true;
    prv_get_server(ip, &port);
    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);
    if (!prv_lpuart_ensure()) {
        goto cleanup;
    }
    if (!prv_start_session(true)) {
        goto cleanup;
    }
    if (!prv_wait_eps_registered()) {
        goto cleanup;
    }
    if (!prv_wait_ps_attached()) {
        goto cleanup;
    }
    if (!prv_activate_pdp()) {
        goto cleanup;
    }
    pdp_active = true;
    if (!prv_open_tcp(ip, port)) {
        goto cleanup;
    }
    opened = true;
    if (!prv_send_tcp_payload(payload)) {
        goto cleanup;
    }
    success = true;
    prv_note_failed_snapshot_sent();

cleanup:
    if (opened) {
        (void)prv_send_cmd_wait("AT+CACLOSE=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    }
    (void)pdp_active;
    GW_Catm1_PowerOff();
    prv_lpuart_release();
    GW_Catm1_SetBusy(false);
    UI_LPM_UnlockStop();
    if ((!success) && should_store_on_fail) {
        (void)prv_store_failed_snapshot_to_flash(&live_rec);
    }
    return success;
}

bool GW_Catm1_SendStoredRange(uint32_t first_rec_index, uint32_t max_count, uint32_t* out_sent_count)
{
    uint8_t ip[4];
    uint16_t port;
    char payload[UI_CATM1_SERVER_PAYLOAD_MAX + 1u];
    char rsp[UI_CATM1_RX_BUF_SZ];
    GW_FileRec_t file_rec;
    bool success = false;
    bool pdp_active = false;
    bool opened = false;
    uint32_t i;

    if (out_sent_count != NULL) {
        *out_sent_count = 0u;
    }
    if ((max_count == 0u) || (out_sent_count == NULL)) {
        return false;
    }
    if (!GW_Storage_ReadRecordByGlobalIndex(first_rec_index, &file_rec, NULL)) {
        return false;
    }
    if (prv_tcp_blocked_by_ble()) {
        return false;
    }

    prv_get_server(ip, &port);
    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);
    if (!prv_lpuart_ensure()) {
        goto cleanup;
    }
    if (!prv_start_session(true)) {
        goto cleanup;
    }
    if (!prv_wait_eps_registered()) {
        goto cleanup;
    }
    if (!prv_wait_ps_attached()) {
        goto cleanup;
    }
    if (!prv_activate_pdp()) {
        goto cleanup;
    }
    pdp_active = true;
    if (!prv_open_tcp(ip, port)) {
        goto cleanup;
    }
    opened = true;

    for (i = 0u; i < max_count; i++) {
        size_t len;

        if (prv_tcp_blocked_by_ble()) {
            break;
        }
        if (!GW_Storage_ReadRecordByGlobalIndex(first_rec_index + i, &file_rec, NULL)) {
            break;
        }
        len = prv_build_snapshot_payload(&file_rec.rec, payload, sizeof(payload));
        if (len == 0u) {
            break;
        }
        if (!prv_send_tcp_payload(payload)) {
            break;
        }
        (*out_sent_count)++;
        prv_note_failed_snapshot_sent();
    }
    success = ((*out_sent_count) > 0u);

cleanup:
    if (opened) {
        (void)prv_send_cmd_wait("AT+CACLOSE=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    }
    (void)pdp_active;
    GW_Catm1_PowerOff();
    prv_lpuart_release();
    GW_Catm1_SetBusy(false);
    UI_LPM_UnlockStop();
    return success;
}
