#include "gw_catm1.h"
#include "ui_conf.h"
#include "ui_types.h"
#include "ui_time.h"
#include "ui_lpm.h"
#include "ui_ringbuf.h"

#include "main.h"
#include "stm32wlxx_hal.h"

#include <stdio.h>
#include <string.h>
#include <stdarg.h>

/* LPUART1 핸들 */
extern UART_HandleTypeDef hlpuart1;

static volatile bool s_catm1_busy = false;
/* 현재 power-on session에서 AT OK를 한 번이라도 받았는지.
 * OK를 못 받는 상태에서는 AT+CPOWD=1 시도가 오래 걸리므로 즉시 hard power-off로 간다. */
static volatile bool s_catm1_session_at_ok = false;
static uint8_t s_catm1_rx_byte = 0u;
static uint8_t s_catm1_rb_mem[UI_CATM1_RX_RING_SIZE];
static UI_RingBuf_t s_catm1_rb;
static bool s_catm1_rb_ready = false;
static volatile uint32_t s_catm1_last_rx_ms = 0u;
static bool s_catm1_uart_ready = false;
static int64_t s_time_sync_delta_sec_buf[GW_CATM1_TIME_SYNC_DELTA_BUF_LEN];
static uint8_t s_time_sync_delta_wr = 0u;
static uint8_t s_time_sync_delta_count = 0u;

#ifndef GW_CATM1_NTP_HOST
#define GW_CATM1_NTP_HOST             "pool.ntp.org"
#endif
#ifndef GW_CATM1_NTP_TZ_QH
#define GW_CATM1_NTP_TZ_QH            (36)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_RETRY
#define GW_CATM1_STARTUP_CCLK_RETRY   (6u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_GAP_MS
#define GW_CATM1_STARTUP_CCLK_GAP_MS  (500u)
#endif
#ifndef GW_CATM1_NTP_TIMEOUT_MS
#define GW_CATM1_NTP_TIMEOUT_MS       (65000u)
#endif
#ifndef GW_CATM1_BOOT_URC_WAIT_MS
#define GW_CATM1_BOOT_URC_WAIT_MS     (4000u)
#endif
#ifndef GW_CATM1_BOOT_QUIET_MS
#define GW_CATM1_BOOT_QUIET_MS        (400u)
#endif
#ifndef GW_CATM1_SIM_READY_TIMEOUT_MS
#define GW_CATM1_SIM_READY_TIMEOUT_MS (5000u)
#endif
#ifndef GW_CATM1_HARD_POWEROFF_WAIT_MS
#define GW_CATM1_HARD_POWEROFF_WAIT_MS (1500u)
#endif
#ifndef GW_CATM1_START_SESSION_TIMEOUT_MS
#define GW_CATM1_START_SESSION_TIMEOUT_MS (12000u)
#endif
#ifndef GW_CATM1_STARTUP_SYNC_ATTEMPTS
#define GW_CATM1_STARTUP_SYNC_ATTEMPTS   (2u)
#endif
#ifndef GW_CATM1_STARTUP_SYNC_RETRY_GAP_MS
#define GW_CATM1_STARTUP_SYNC_RETRY_GAP_MS (500u)
#endif
#ifndef GW_CATM1_STARTUP_REG_WAIT_MS
#define GW_CATM1_STARTUP_REG_WAIT_MS    (4000u)
#endif
#ifndef GW_CATM1_STARTUP_PS_WAIT_MS
#define GW_CATM1_STARTUP_PS_WAIT_MS     (3000u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_FIRST_TRY
#define GW_CATM1_STARTUP_CCLK_FIRST_TRY (1u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_POST_REG_TRY
#define GW_CATM1_STARTUP_CCLK_POST_REG_TRY (2u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_POST_ATTACH_TRY
#define GW_CATM1_STARTUP_CCLK_POST_ATTACH_TRY (1u)
#endif

#ifndef GW_CATM1_AT_SYNC_MAX_TRY
#define GW_CATM1_AT_SYNC_MAX_TRY        (4u)
#endif

#ifndef GW_CATM1_RUNTIME_REG_WAIT_MS
#define GW_CATM1_RUNTIME_REG_WAIT_MS    (6000u)
#endif
#ifndef GW_CATM1_RUNTIME_PS_WAIT_MS
#define GW_CATM1_RUNTIME_PS_WAIT_MS     (4000u)
#endif
#ifndef GW_CATM1_RUNTIME_PDP_WAIT_MS
#define GW_CATM1_RUNTIME_PDP_WAIT_MS    (4000u)
#endif

static bool prv_activate_pdp(void);
static void prv_enable_network_time_auto_update(void);
static bool prv_wait_eps_registered(void);
static bool prv_wait_eps_registered_until(uint32_t timeout_ms);
static bool prv_wait_ps_attached(void);
static bool prv_wait_ps_attached_until(uint32_t timeout_ms);

static void prv_catm1_rb_reset(void)
{
    UI_RingBuf_Init(&s_catm1_rb, s_catm1_rb_mem, UI_CATM1_RX_RING_SIZE);
    s_catm1_rb_ready = true;
    s_catm1_last_rx_ms = HAL_GetTick();
}

static void prv_time_sync_delta_reset(void)
{
    uint32_t i;

    for (i = 0u; i < GW_CATM1_TIME_SYNC_DELTA_BUF_LEN; i++)
    {
        s_time_sync_delta_sec_buf[i] = 0;
    }

    s_time_sync_delta_wr = 0u;
    s_time_sync_delta_count = 0u;
}

static void prv_time_sync_delta_push(int64_t delta_sec)
{
    s_time_sync_delta_sec_buf[s_time_sync_delta_wr] = delta_sec;
    s_time_sync_delta_wr = (uint8_t)((s_time_sync_delta_wr + 1u) % GW_CATM1_TIME_SYNC_DELTA_BUF_LEN);
    if (s_time_sync_delta_count < GW_CATM1_TIME_SYNC_DELTA_BUF_LEN)
    {
        s_time_sync_delta_count++;
    }
}

static void prv_lpuart_clear_errors(void)
{
    if (hlpuart1.Instance == NULL)
    {
        return;
    }

#if defined(__HAL_UART_CLEAR_PEFLAG)
    __HAL_UART_CLEAR_PEFLAG(&hlpuart1);
#endif
#if defined(__HAL_UART_CLEAR_FEFLAG)
    __HAL_UART_CLEAR_FEFLAG(&hlpuart1);
#endif
#if defined(__HAL_UART_CLEAR_NEFLAG)
    __HAL_UART_CLEAR_NEFLAG(&hlpuart1);
#endif
#if defined(__HAL_UART_CLEAR_OREFLAG)
    __HAL_UART_CLEAR_OREFLAG(&hlpuart1);
#endif
#if defined(__HAL_UART_CLEAR_IDLEFLAG)
    __HAL_UART_CLEAR_IDLEFLAG(&hlpuart1);
#endif
}

static bool prv_lpuart_force_reinit(void)
{
    if (hlpuart1.Instance == NULL)
    {
        return false;
    }

    (void)HAL_UART_Abort(&hlpuart1);
    (void)HAL_UART_DeInit(&hlpuart1);
    HAL_Delay(2u);

    if (HAL_UART_Init(&hlpuart1) != HAL_OK)
    {
        s_catm1_uart_ready = false;
        return false;
    }

    prv_lpuart_clear_errors();
    s_catm1_uart_ready = true;
    return true;
}

static void prv_catm1_rx_start_it(void)
{
    HAL_StatusTypeDef st;

    if (!s_catm1_rb_ready)
    {
        prv_catm1_rb_reset();
    }

    prv_lpuart_clear_errors();
    st = HAL_UART_Receive_IT(&hlpuart1, &s_catm1_rx_byte, 1u);
    if (st == HAL_BUSY)
    {
        (void)HAL_UART_AbortReceive(&hlpuart1);
        prv_lpuart_clear_errors();
        st = HAL_UART_Receive_IT(&hlpuart1, &s_catm1_rx_byte, 1u);
    }

    if (st != HAL_OK)
    {
        s_catm1_uart_ready = false;
    }
}

static bool prv_catm1_rb_pop_wait(uint8_t* out, uint32_t timeout_ms)
{
    uint32_t start = HAL_GetTick();

    if (out == NULL)
    {
        return false;
    }

    if (!s_catm1_rb_ready)
    {
        prv_catm1_rb_reset();
    }

    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms)
    {
        if (UI_RingBuf_Pop(&s_catm1_rb, out))
        {
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

    while ((uint32_t)(HAL_GetTick() - start) < max_wait_ms)
    {
        if (prv_catm1_rb_pop_wait(&ch, 20u))
        {
            if (ch != 0u)
            {
                last_rx = HAL_GetTick();
            }
        }

        if ((uint32_t)(HAL_GetTick() - last_rx) >= quiet_ms)
        {
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

    for (i = 0u; i < 2u; i++)
    {
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
    if (hlpuart1.Instance == NULL)
    {
        return false;
    }

    if ((!s_catm1_uart_ready) || !prv_lpuart_is_inited())
    {
        if (!prv_lpuart_force_reinit())
        {
            return false;
        }
    }

    if (!s_catm1_rb_ready)
    {
        prv_catm1_rb_reset();
    }

    prv_catm1_rx_start_it();
    return s_catm1_uart_ready;
}

static void prv_lpuart_release(void)
{
    if (hlpuart1.Instance != NULL)
    {
        (void)HAL_UART_Abort(&hlpuart1);
    }

    if (prv_lpuart_is_inited())
    {
        (void)HAL_UART_DeInit(&hlpuart1);
    }

    s_catm1_uart_ready = false;

    if (s_catm1_rb_ready)
    {
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
    if (ip_zero || (*port < UI_TCPIP_MIN_PORT))
    {
        ip[0] = UI_TCPIP_DEFAULT_IP0;
        ip[1] = UI_TCPIP_DEFAULT_IP1;
        ip[2] = UI_TCPIP_DEFAULT_IP2;
        ip[3] = UI_TCPIP_DEFAULT_IP3;
        *port = UI_TCPIP_DEFAULT_PORT;
    }
}

static bool prv_uart_send_bytes(const void* data, uint16_t len, uint32_t timeout_ms)
{
    if (!prv_lpuart_ensure())
    {
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
    if (!prv_lpuart_ensure())
    {
        return;
    }

    prv_catm1_rb_reset();
    prv_catm1_rx_start_it();
}

static bool prv_uart_wait_for(char* out,
                              size_t out_sz,
                              uint32_t timeout_ms,
                              const char* tok1,
                              const char* tok2,
                              const char* tok3)
{
    uint8_t ch;
    uint32_t start = HAL_GetTick();
    size_t n = 0u;

    if ((out == NULL) || (out_sz == 0u))
    {
        return false;
    }

    out[0] = '\0';

    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms)
    {
        if (!prv_catm1_rb_pop_wait(&ch, 20u))
        {
            continue;
        }

        if (ch == 0u)
        {
            continue;
        }

        if ((n + 1u) < out_sz)
        {
            out[n++] = (char)ch;
            out[n] = '\0';
        }
        else if (out_sz > 16u)
        {
            size_t keep = (out_sz / 2u);
            memmove(out, &out[out_sz - keep - 1u], keep);
            n = keep;
            out[n++] = (char)ch;
            out[n] = '\0';
        }

        if ((strstr(out, "ERROR") != NULL) || (strstr(out, "+CME ERROR") != NULL))
        {
            return false;
        }
        if ((tok1 != NULL) && (strstr(out, tok1) != NULL))
        {
            return true;
        }
        if ((tok2 != NULL) && (strstr(out, tok2) != NULL))
        {
            return true;
        }
        if ((tok3 != NULL) && (strstr(out, tok3) != NULL))
        {
            return true;
        }
    }

    return false;
}

static bool prv_send_cmd_wait(const char* cmd,
                              const char* tok1,
                              const char* tok2,
                              const char* tok3,
                              uint32_t timeout_ms,
                              char* out,
                              size_t out_sz)
{
    prv_uart_flush_rx();
    if (!prv_uart_send_text(cmd, UI_CATM1_AT_TIMEOUT_MS))
    {
        return false;
    }
    return prv_uart_wait_for(out, out_sz, timeout_ms, tok1, tok2, tok3);
}

static bool prv_send_query_wait_ok(const char* cmd,
                                   uint32_t timeout_ms,
                                   char* out,
                                   size_t out_sz)
{
    return prv_send_cmd_wait(cmd, "OK", NULL, NULL, timeout_ms, out, out_sz);
}

static bool prv_send_query_wait_prefix_ok(const char* cmd,
                                          const char* prefix,
                                          uint32_t timeout_ms,
                                          char* out,
                                          size_t out_sz)
{
    uint8_t ch;
    uint32_t start = HAL_GetTick();
    uint32_t last_rx_tick = start;
    bool saw_ok_after = false;
    size_t n = 0u;

    if ((cmd == NULL) || (out == NULL) || (out_sz == 0u))
    {
        return false;
    }

    if (prefix == NULL)
    {
        return prv_send_query_wait_ok(cmd, timeout_ms, out, out_sz);
    }

    prv_uart_flush_rx();
    if (!prv_uart_send_text(cmd, UI_CATM1_AT_TIMEOUT_MS))
    {
        return false;
    }

    out[0] = '\0';

    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms)
    {
        const char* pfx;
        const char* line_end;
        const char* ok_after;

        if (!prv_catm1_rb_pop_wait(&ch, UI_CATM1_QUERY_RX_POLL_MS))
        {
            if (saw_ok_after &&
                ((uint32_t)(HAL_GetTick() - last_rx_tick) >= UI_CATM1_QUERY_OK_IDLE_MS))
            {
                return true;
            }
            continue;
        }
        if (ch == 0u)
        {
            continue;
        }

        last_rx_tick = HAL_GetTick();

        if ((n + 1u) < out_sz)
        {
            out[n++] = (char)ch;
            out[n] = '\0';
        }
        else if (out_sz > 16u)
        {
            size_t keep = (out_sz / 2u);
            memmove(out, &out[out_sz - keep - 1u], keep);
            n = keep;
            out[n++] = (char)ch;
            out[n] = '\0';
        }

        if ((strstr(out, "ERROR") != NULL) || (strstr(out, "+CME ERROR") != NULL))
        {
            return false;
        }

        pfx = strstr(out, prefix);
        if (pfx == NULL)
        {
            continue;
        }

        line_end = strstr(pfx, "\r\n");
        if (line_end == NULL)
        {
            line_end = strchr(pfx, '\n');
        }
        if (line_end == NULL)
        {
            continue;
        }

        ok_after = strstr(line_end, "\r\nOK\r\n");
        if (ok_after == NULL)
        {
            ok_after = strstr(line_end, "\nOK\r\n");
        }
        if (ok_after == NULL)
        {
            ok_after = strstr(line_end, "\nOK\n");
        }
        if (ok_after != NULL)
        {
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

    if ((out == NULL) || (out_sz == 0u))
    {
        return false;
    }

    prv_uart_flush_rx();
    if (!prv_uart_send_text("AT+CNACT?\r\n", UI_CATM1_AT_TIMEOUT_MS))
    {
        return false;
    }

    out[0] = '\0';

    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_CNACT_QUERY_TIMEOUT_MS)
    {
        if (!prv_catm1_rb_pop_wait(&ch, UI_CATM1_QUERY_RX_POLL_MS))
        {
            continue;
        }

        if (ch == 0u)
        {
            continue;
        }

        if ((n + 1u) < out_sz)
        {
            out[n++] = (char)ch;
            out[n] = '\0';
        }
        else if (out_sz > 16u)
        {
            size_t keep = (out_sz / 2u);
            memmove(out, &out[out_sz - keep - 1u], keep);
            n = keep;
            out[n++] = (char)ch;
            out[n] = '\0';
        }

        if ((strstr(out, "ERROR") != NULL) || (strstr(out, "+CME ERROR") != NULL))
        {
            return false;
        }

        if (strstr(out, "+CNACT:") != NULL)
        {
            saw_cnact = true;
        }

        if (saw_cnact)
        {
            if ((strstr(out, "\r\nOK\r\n") != NULL) ||
                (strstr(out, "\nOK\r\n") != NULL) ||
                (strstr(out, "\nOK\n") != NULL))
            {
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

    if ((UI_CATM1_AT_SYNC_RETRY != 0u) && (UI_CATM1_AT_SYNC_RETRY < max_try))
    {
        max_try = UI_CATM1_AT_SYNC_RETRY;
    }

    if (max_try == 0u)
    {
        max_try = GW_CATM1_AT_SYNC_MAX_TRY;
    }

    for (i = 0u; i < max_try; i++)
    {
        rsp[0] = '\0';
        prv_uart_flush_rx();

        if (!prv_uart_send_text("AT\r\n", UI_CATM1_AT_TIMEOUT_MS))
        {
            prv_delay_ms(UI_CATM1_AT_SYNC_GAP_MS);
            continue;
        }

        if (prv_uart_wait_for(rsp, sizeof(rsp), UI_CATM1_AT_TIMEOUT_MS, "OK", NULL, NULL))
        {
            s_catm1_session_at_ok = true;
            (void)prv_send_cmd_wait("ATE0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
            (void)prv_send_cmd_wait("AT+CMEE=2\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
            return true;
        }

        /* RDY / +CFUN / SMS Ready는 재부팅 진행중 URC이므로 동기 완료로 간주하지 않는다. */
        prv_delay_ms(UI_CATM1_AT_SYNC_GAP_MS);
    }

    return false;
}

static bool prv_wait_sim_ready(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t start = HAL_GetTick();

    while ((uint32_t)(HAL_GetTick() - start) < GW_CATM1_SIM_READY_TIMEOUT_MS)
    {
        if (prv_send_query_wait_prefix_ok("AT+CPIN?\r\n", "+CPIN:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            (strstr(rsp, "+CPIN: READY") != NULL))
        {
            return true;
        }

        prv_wait_rx_quiet(200u, 800u);
    }

    return false;
}

static bool prv_wait_boot_sms_ready(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];

    rsp[0] = '\0';

    /* power-on 직후에는 RDY / +CFUN / +CPIN 보다 마지막의 "SMS Ready"를
     * 모뎀 부팅 완료 기준으로 사용한다.
     * 이 문구가 제한 시간 안에 안 오면 그 세션은 더 진행하지 않고 종료한다. */
    return prv_uart_wait_for(rsp,
                             sizeof(rsp),
                             GW_CATM1_BOOT_URC_WAIT_MS,
                             "SMS Ready",
                             NULL,
                             NULL);
}

static bool prv_start_session(bool enable_time_auto_update)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool cpin_ready = false;

    /* 새 power-on session은 이전 URC 찌꺼기를 버리고 시작한다. */
    prv_uart_flush_rx();

    GW_Catm1_PowerOn();
    prv_delay_ms(UI_CATM1_BOOT_WAIT_MS);

    /* 새 session 시작: 아직 AT OK를 못 받음 */
    s_catm1_session_at_ok = false;

    /* SMS Ready는 현장 기준 3초 내 도착하므로, 짧은 timeout 안에 안 오면 더 진행하지 않고 종료한다. */
    if (!prv_wait_boot_sms_ready())
    {
        return false;
    }

    prv_wait_rx_quiet(GW_CATM1_BOOT_QUIET_MS, GW_CATM1_BOOT_QUIET_MS + 200u);
    prv_uart_flush_rx();

    /* 요구사항(시간 동기화 포함): AT OK를 4회 내에 못 잡으면 즉시 전원 OFF. */
    if (!prv_send_at_sync())
    {
        return false;
    }

    rsp[0] = '\0';
    if (prv_send_query_wait_prefix_ok("AT+CPIN?\r\n", "+CPIN:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
        (strstr(rsp, "+CPIN: READY") != NULL))
    {
        cpin_ready = true;
    }

    if (enable_time_auto_update)
    {
        prv_enable_network_time_auto_update();
    }

    /* power-on time sync 진입 전에 최소한의 상태 query를 한 번씩 찍어 두고,
     * 이후 상세 대기/재시도는 prv_sync_time_from_modem_startup()에서 bounded하게 수행한다. */
    (void)prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    (void)prv_send_query_wait_prefix_ok("AT+CGATT?\r\n", "+CGATT:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));

    if (!cpin_ready)
    {
        cpin_ready = prv_wait_sim_ready();
    }

    if (!cpin_ready)
    {
        return false;
    }

    prv_wait_rx_quiet(200u, 1200u);
    return true;
}

static void prv_enable_network_time_auto_update(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];

    /* 사용자가 요청한 AT+CTZU=1을 우선 시도한다.
     * 펌웨어/통신사 조합에 따라 미지원일 수 있으므로,
     * 실패 시에도 세션을 중단하지 않고 계속 진행한다.
     * 일부 SIM7080 문서/펌웨어는 CLTS 경로를 사용하므로 fallback으로 함께 시도한다. */
    if (!prv_send_cmd_wait("AT+CTZU=1\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
    {
        (void)prv_send_cmd_wait("AT+CLTS=1\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    }
}

static bool prv_is_invalid_cclk_rsp(const char* rsp)
{
    const char* p;
    int yy = 0;

    if (rsp == NULL)
    {
        return false;
    }

    p = strstr(rsp, "+CCLK:");
    if (p == NULL)
    {
        return false;
    }
    while (strstr(p + 1, "+CCLK:") != NULL)
    {
        p = strstr(p + 1, "+CCLK:");
    }

    p = strchr(p, '"');
    if (p == NULL)
    {
        return false;
    }

    if (sscanf(p + 1, "%2d/", &yy) != 1)
    {
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

    if ((rsp == NULL) || (out_epoch_centi == NULL))
    {
        return false;
    }

    p = strstr(rsp, "+CCLK:");
    if (p == NULL)
    {
        return false;
    }
    while (strstr(p + 1, "+CCLK:") != NULL)
    {
        p = strstr(p + 1, "+CCLK:");
    }

    p = strchr(p, '"');
    if (p == NULL)
    {
        return false;
    }

    n = sscanf(p + 1, "%2d/%2d/%2d,%2d:%2d:%2d%c%2d", &yy, &mon, &day, &hh, &mm, &ss, &sign, &tz_q15);
    if (n < 6)
    {
        return false;
    }

    /* SIM7080의 미동기 기본값은 80/.. 형태가 나올 수 있으므로 방지 */
    if ((yy < 0) || (yy >= 80))
    {
        return false;
    }

    if ((mon < 1) || (mon > 12) || (day < 1) || (day > 31) ||
        (hh < 0) || (hh > 23) || (mm < 0) || (mm > 59) || (ss < 0) || (ss > 59))
    {
        return false;
    }

    (void)sign;
    (void)tz_q15;

    dt.year  = (uint16_t)(2000 + yy);
    dt.month = (uint8_t)mon;
    dt.day   = (uint8_t)day;
    dt.hour  = (uint8_t)hh;
    dt.min   = (uint8_t)mm;
    dt.sec   = (uint8_t)ss;
    dt.centi = 0u;

    *out_epoch_centi = (uint64_t)UI_Time_Epoch2016_FromCalendar(&dt) * 100u;
    return true;
}

static bool prv_query_network_time_epoch_retry(uint32_t max_try,
                                              uint32_t gap_ms,
                                              bool break_on_invalid,
                                              uint64_t* out_epoch_centi)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t i;

    if ((out_epoch_centi == NULL) || (max_try == 0u))
    {
        return false;
    }

    for (i = 0u; i < max_try; i++)
    {
        if (!prv_send_query_wait_prefix_ok("AT+CCLK?\r\n", "+CCLK:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
        {
            if ((i + 1u) < max_try)
            {
                prv_delay_ms(gap_ms);
            }
            continue;
        }

        if (prv_parse_cclk_epoch(rsp, out_epoch_centi))
        {
            return true;
        }

        if (break_on_invalid && prv_is_invalid_cclk_rsp(rsp))
        {
            break;
        }

        if ((i + 1u) < max_try)
        {
            prv_delay_ms(gap_ms);
        }
    }

    return false;
}

static bool prv_query_network_time_epoch(uint64_t* out_epoch_centi)
{
    uint32_t max_try = UI_CATM1_TIME_SYNC_RETRY;

    if (max_try > 2u)
    {
        max_try = 2u;
    }

    return prv_query_network_time_epoch_retry(max_try,
                                              UI_CATM1_TIME_SYNC_GAP_MS,
                                              true,
                                              out_epoch_centi);
}

static bool prv_ntp_sync_time(uint64_t* out_epoch_centi)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    char cmd[96];

    if (out_epoch_centi == NULL)
    {
        return false;
    }

    (void)prv_send_cmd_wait("AT+CNTPCID=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    (void)snprintf(cmd, sizeof(cmd), "AT+CNTP=\"%s\",%d,0,1\r\n", GW_CATM1_NTP_HOST, GW_CATM1_NTP_TZ_QH);
    if (!prv_send_cmd_wait(cmd, "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
    {
        return false;
    }

    if (!prv_send_cmd_wait("AT+CNTP\r\n", "+CNTP:", NULL, NULL, GW_CATM1_NTP_TIMEOUT_MS, rsp, sizeof(rsp)))
    {
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

    /* power-on time sync 요구 순서:
     * AT OK 이후 CPIN -> CTZU/CLTS -> CEREG -> CGATT -> CCLK, 필요 시 NTP 1회.
     * 실패하더라도 같은 세션 안에서만 짧게 시도하고 종료한다. */
    rsp[0] = '\0';
    if (prv_send_query_wait_prefix_ok("AT+CPIN?\r\n", "+CPIN:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
        (strstr(rsp, "+CPIN: READY") != NULL))
    {
        cpin_ready = true;
    }

    if (!cpin_ready)
    {
        return false;
    }

    prv_enable_network_time_auto_update();
    (void)prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    (void)prv_send_query_wait_prefix_ok("AT+CGATT?\r\n", "+CGATT:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));

    if (prv_query_network_time_epoch_retry(GW_CATM1_STARTUP_CCLK_FIRST_TRY,
                                           GW_CATM1_STARTUP_CCLK_GAP_MS,
                                           true,
                                           &epoch_centi))
    {
        goto apply_time;
    }

    reg_ok = prv_wait_eps_registered_until(GW_CATM1_STARTUP_REG_WAIT_MS);
    if (reg_ok &&
        prv_query_network_time_epoch_retry(GW_CATM1_STARTUP_CCLK_POST_REG_TRY,
                                           1000u,
                                           false,
                                           &epoch_centi))
    {
        goto apply_time;
    }

    if (reg_ok)
    {
        ps_ok = prv_wait_ps_attached_until(GW_CATM1_STARTUP_PS_WAIT_MS);

        if (ps_ok &&
            prv_query_network_time_epoch_retry(GW_CATM1_STARTUP_CCLK_POST_ATTACH_TRY,
                                               1000u,
                                               false,
                                               &epoch_centi))
        {
            goto apply_time;
        }

        if (ps_ok && prv_activate_pdp() && prv_ntp_sync_time(&epoch_centi))
        {
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

    if (!prv_query_network_time_epoch(&epoch_centi))
    {
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

    if ((out == NULL) || (out_sz == 0u))
    {
        return false;
    }

    out[0] = '\0';

    if (!prv_send_cmd_wait("AT+CGNSPWR=1\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
    {
        return false;
    }

    prv_delay_ms(UI_CATM1_GNSS_WAIT_MS);

    if (!prv_send_cmd_wait("AT+CGNSINF\r\n", "+CGNSINF:", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
    {
        (void)prv_send_cmd_wait("AT+CGNSPWR=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
        return false;
    }

    p = strstr(rsp, "+CGNSINF:");
    if (p != NULL)
    {
        while (strstr(p + 1, "+CGNSINF:") != NULL)
        {
            p = strstr(p + 1, "+CGNSINF:");
        }
        e = strpbrk(p, "\r\n");
        if (e != NULL)
        {
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

    if ((rsp == NULL) || (stat == NULL))
    {
        return false;
    }

    p = strstr(rsp, "+CEREG:");
    if (p == NULL)
    {
        return false;
    }
    while (strstr(p + 1, "+CEREG:") != NULL)
    {
        p = strstr(p + 1, "+CEREG:");
    }

    if ((sscanf(p, "+CEREG: %u,%u", &n, &v) != 2) &&
        (sscanf(p, "+CEREG:%u,%u", &n, &v) != 2))
    {
        return false;
    }

    (void)n;
    *stat = (uint8_t)v;
    return true;
}

static bool prv_wait_eps_registered(void)
{
    uint32_t timeout_ms = UI_CATM1_NET_REG_TIMEOUT_MS;

    if ((timeout_ms == 0u) || (timeout_ms > GW_CATM1_RUNTIME_REG_WAIT_MS))
    {
        timeout_ms = GW_CATM1_RUNTIME_REG_WAIT_MS;
    }

    return prv_wait_eps_registered_until(timeout_ms);
}

static bool prv_wait_eps_registered_until(uint32_t timeout_ms)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t stat = 0u;
    uint32_t start = HAL_GetTick();

    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms)
    {
        if (prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cereg_stat(rsp, &stat))
        {
            /* 1/5: EPS data registered, 9/10: registered with CSFB not preferred.
             * 3: denied, 6/7: SMS only, 8: emergency only -> 데이터 세션 불가. */
            if ((stat == 1u) || (stat == 5u) || (stat == 9u) || (stat == 10u))
            {
                return true;
            }

            if ((stat == 3u) || (stat == 6u) || (stat == 7u) || (stat == 8u))
            {
                return false;
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

    if ((rsp == NULL) || (state == NULL))
    {
        return false;
    }

    p = strstr(rsp, "+CGATT:");
    if (p == NULL)
    {
        return false;
    }
    while (strstr(p + 1, "+CGATT:") != NULL)
    {
        p = strstr(p + 1, "+CGATT:");
    }

    if ((sscanf(p, "+CGATT: %u", &v) != 1) && (sscanf(p, "+CGATT:%u", &v) != 1))
    {
        return false;
    }

    *state = (uint8_t)v;
    return true;
}

static bool prv_wait_ps_attached(void)
{
    uint32_t timeout_ms = UI_CATM1_PS_ATTACH_TIMEOUT_MS;

    if ((timeout_ms == 0u) || (timeout_ms > GW_CATM1_RUNTIME_PS_WAIT_MS))
    {
        timeout_ms = GW_CATM1_RUNTIME_PS_WAIT_MS;
    }

    return prv_wait_ps_attached_until(timeout_ms);
}

static bool prv_wait_ps_attached_until(uint32_t timeout_ms)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t state = 0u;
    uint32_t start = HAL_GetTick();

    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms)
    {
        if (prv_send_query_wait_prefix_ok("AT+CGATT?\r\n", "+CGATT:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cgatt_state(rsp, &state))
        {
            if (state == 1u)
            {
                return true;
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

    if ((rsp == NULL) || (state == NULL) || (ip == NULL))
    {
        return false;
    }

    p = rsp;
    while ((p = strstr(p, "+CNACT:")) != NULL)
    {
        if ((((sscanf(p, "+CNACT: %u,%u,\"%u.%u.%u.%u\"", &cid, &vstate, &a, &b, &c, &d) == 6) ||
              (sscanf(p, "+CNACT:%u,%u,\"%u.%u.%u.%u\"", &cid, &vstate, &a, &b, &c, &d) == 6))) &&
            (cid == 0u))
        {
            *state = (uint8_t)vstate;
            ip[0] = (uint8_t)a;
            ip[1] = (uint8_t)b;
            ip[2] = (uint8_t)c;
            ip[3] = (uint8_t)d;
            return true;
        }

        p += 7; /* strlen("+CNACT:") */
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
    uint32_t act_timeout_ms = UI_CATM1_NET_ACT_TIMEOUT_MS;

    if ((act_timeout_ms == 0u) || (act_timeout_ms > GW_CATM1_RUNTIME_PDP_WAIT_MS))
    {
        act_timeout_ms = GW_CATM1_RUNTIME_PDP_WAIT_MS;
    }

    /* 1NCE: APN=iot.1nce.net, user/pass blank, PAP */
    (void)snprintf(cmd, sizeof(cmd), "AT+CNCFG=0,1,\"%s\",\"\",\"\",1\r\n", UI_CATM1_1NCE_APN);
    if (!prv_send_cmd_wait(cmd, "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
    {
        return false;
    }

    /* 이미 살아 있는 PDP가 있으면 그대로 사용 */
    if (prv_send_query_wait_cnact_ok(rsp, sizeof(rsp)) &&
        prv_parse_cnact_ctx0(rsp, &state, ip) &&
        (state == 1u) && ((ip[0] | ip[1] | ip[2] | ip[3]) != 0u))
    {
        return true;
    }

    if (!prv_send_cmd_wait("AT+CNACT=0,1\r\n", "OK", "+APP PDP: 0,ACTIVE", NULL, act_timeout_ms, rsp, sizeof(rsp)))
    {
        return false;
    }

    /* 모듈이 query 응답을 매우 빠르게 끝내는 경우가 있어
     * OK 뒤 13 ms quiet guard 후 1회 확인한다. */
    prv_delay_ms(UI_CATM1_QUERY_OK_IDLE_MS);

    if (prv_send_query_wait_cnact_ok(rsp, sizeof(rsp)) &&
        prv_parse_cnact_ctx0(rsp, &state, ip) &&
        (state == 1u) && ((ip[0] | ip[1] | ip[2] | ip[3]) != 0u))
    {
        return true;
    }

    start = HAL_GetTick();
    while ((uint32_t)(HAL_GetTick() - start) < act_timeout_ms)
    {
        prv_delay_ms(300u);

        if (prv_send_query_wait_cnact_ok(rsp, sizeof(rsp)) &&
            prv_parse_cnact_ctx0(rsp, &state, ip) &&
            (state == 1u) && ((ip[0] | ip[1] | ip[2] | ip[3]) != 0u))
        {
            return true;
        }
    }

    return false;
}

static bool prv_open_tcp(const uint8_t ip[4], uint16_t port)
{
    char cmd[96];
    char rsp[UI_CATM1_RX_BUF_SZ];

    if (!prv_send_cmd_wait("AT+CACID=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
    {
        return false;
    }

    (void)snprintf(cmd,
                   sizeof(cmd),
                   "AT+CAOPEN=0,0,\"TCP\",\"%u.%u.%u.%u\",%u\r\n",
                   (unsigned)ip[0],
                   (unsigned)ip[1],
                   (unsigned)ip[2],
                   (unsigned)ip[3],
                   (unsigned)port);

    return prv_send_cmd_wait(cmd, "+CAOPEN: 0,0", NULL, NULL, UI_CATM1_TCP_OPEN_TIMEOUT_MS, rsp, sizeof(rsp));
}

static bool prv_query_caack(uint32_t* out_total, uint32_t* out_unack)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    unsigned a = 0u;
    unsigned b = 0u;
    uint32_t total = 0u;
    uint32_t unack = 0u;

    if (out_total != NULL) { *out_total = 0u; }
    if (out_unack != NULL) { *out_unack = 0u; }

    /* CAACK는 prefix만 잡히면 곧바로 return 하면 숫자 부분이 아직 안 들어와 parsing 실패가 날 수 있다.
     * prefix line + OK까지 받아서 안정적으로 parse 한다. */
    if (!prv_send_query_wait_prefix_ok("AT+CAACK=0\r\n", "+CAACK:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
    {
        return false;
    }

    if (sscanf(rsp, "%*[^:]: %u,%u", &a, &b) != 2)
    {
        return false;
    }

    /* SIMCOM 펌웨어/문서에 따라 필드 순서가 total,unack 또는 unack,total 로 혼재 가능성이 있다.
     * 일반적으로 total >= unack 이므로 큰 값을 total로 취급한다. */
    if (b > a)
    {
        total = (uint32_t)b;
        unack = (uint32_t)a;
    }
    else
    {
        total = (uint32_t)a;
        unack = (uint32_t)b;
    }

    if (out_total != NULL) { *out_total = total; }
    if (out_unack != NULL) { *out_unack = unack; }
    return true;
}

static bool prv_wait_caack_drained(uint32_t base_total, uint32_t add_len)
{
    uint32_t start = HAL_GetTick();
    uint32_t need_total = base_total + add_len;

    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_ACK_WAIT_MS)
    {
        uint32_t total = 0u;
        uint32_t unack = 0u;

        if (prv_query_caack(&total, &unack))
        {
            if ((total >= need_total) && (unack == 0u))
            {
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

    if (payload == NULL)
    {
        return false;
    }

    len = strlen(payload);
    if ((len == 0u) || (len > 1460u))
    {
        return false;
    }

    /* 일부 현장망에서는 CAACK 드레인이 2~3초를 넘는 경우가 있어,
     * CASEND OK 이후 UI_CATM1_ACK_WAIT_MS(기본 10s) 동안 CAACK(unack==0)까지 기다린다.
     * total은 소켓 누적치이므로 send 전 base_total을 잡아 상대 비교한다. */
    have_base = prv_query_caack(&base_total, &base_unack);
    if (!have_base)
    {
        base_total = 0u;
        base_unack = 0u;
    }

    (void)snprintf(cmd,
                   sizeof(cmd),
                   "AT+CASEND=0,%u,%u\r\n",
                   (unsigned)len,
                   (unsigned)UI_CATM1_SEND_INPUT_TIMEOUT_MS);

    if (!prv_send_cmd_wait(cmd, ">", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
    {
        return false;
    }

    if (!prv_uart_send_bytes(payload, (uint16_t)len, UI_CATM1_SEND_INPUT_TIMEOUT_MS + 2000u))
    {
        return false;
    }

    if (!prv_uart_wait_for(rsp, sizeof(rsp), UI_CATM1_SEND_INPUT_TIMEOUT_MS + 3000u, "OK", NULL, NULL))
    {
        return false;
    }

    return prv_wait_caack_drained(base_total, (uint32_t)len);
}

static bool prv_node_valid(const GW_NodeRec_t* r)
{
    if (r == NULL)
    {
        return false;
    }
    if (r->batt_lvl != UI_NODE_BATT_LVL_INVALID) { return true; }
    if (r->temp_c != UI_NODE_TEMP_INVALID_C) { return true; }
    if ((uint16_t)r->x != 0xFFFFu) { return true; }
    if ((uint16_t)r->y != 0xFFFFu) { return true; }
    if ((uint16_t)r->z != 0xFFFFu) { return true; }
    if (r->adc != 0xFFFFu) { return true; }
    if (r->pulse_cnt != 0xFFFFFFFFu) { return true; }
    return false;
}

static uint8_t prv_valid_node_count(const GW_HourRec_t* rec)
{
    uint8_t cnt = 0u;
    uint32_t i;

    for (i = 0u; i < UI_MAX_NODES; i++)
    {
        if (prv_node_valid(&rec->nodes[i]))
        {
            cnt++;
        }
    }
    return cnt;
}

static void prv_append_fmt(char* out, size_t out_sz, size_t* io_len, const char* fmt, ...)
{
    va_list ap;
    int n;

    if ((out == NULL) || (io_len == NULL) || (*io_len >= out_sz))
    {
        return;
    }

    va_start(ap, fmt);
    n = vsnprintf(&out[*io_len], out_sz - *io_len, fmt, ap);
    va_end(ap);

    if (n <= 0)
    {
        return;
    }

    if ((size_t)n >= (out_sz - *io_len))
    {
        *io_len = out_sz - 1u;
        out[*io_len] = '\0';
        return;
    }

    *io_len += (size_t)n;
}

static bool prv_get_setting_value_unit(uint8_t* out_value, char* out_unit)
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

static uint32_t prv_get_payload_timestamp_epoch_sec(uint32_t rec_epoch_sec)
{
    uint8_t value;
    char unit;
    uint32_t cycle_sec = 0u;

    if (!prv_get_setting_value_unit(&value, &unit))
    {
        return rec_epoch_sec;
    }

    if (unit == 'M')
    {
        cycle_sec = (uint32_t)value * 60u;
    }
    else if (unit == 'H')
    {
        cycle_sec = (uint32_t)value * 3600u;
    }

    if (cycle_sec == 0u)
    {
        return rec_epoch_sec;
    }

    /* payload timestamp는 현재/저장 epoch를 설정 문자열 기준 absolute epoch floor로 정렬한다.
     * 05M -> 00/05/10..., 10M -> 00/10/20..., 01H -> HH:00:00 */
    return ((rec_epoch_sec / cycle_sec) * cycle_sec);
}

static size_t prv_build_snapshot_payload(const GW_HourRec_t* rec, char* out, size_t out_sz)
{
    const UI_Config_t* cfg = UI_GetConfig();
    UI_DateTime_t dt;
    size_t len = 0u;
    uint32_t i;
    uint8_t valid_cnt;
    bool truncated = false;
    char set0, set1, set2;

    if ((rec == NULL) || (out == NULL) || (out_sz < 64u))
    {
        return 0u;
    }

    out[0] = '\0';
    UI_Time_Epoch2016_ToCalendar(prv_get_payload_timestamp_epoch_sec(rec->epoch_sec), &dt);
    valid_cnt = prv_valid_node_count(rec);
    set0 = (char)cfg->setting_ascii[0];
    set1 = (char)cfg->setting_ascii[1];
    set2 = (char)cfg->setting_ascii[2];

    prv_append_fmt(out, out_sz, &len,
                   "DATA,GW:%u,NETID:%.*s,SET:%c%c%c,T:%04u-%02u-%02u %02u:%02u:%02u,GV:%u,GT:%d,N:%u\r\n",
                   (unsigned)cfg->gw_num,
                   (int)UI_NET_ID_LEN,
                   (const char*)cfg->net_id,
                   set0,
                   set1,
                   set2,
                   (unsigned)dt.year,
                   (unsigned)dt.month,
                   (unsigned)dt.day,
                   (unsigned)dt.hour,
                   (unsigned)dt.min,
                   (unsigned)dt.sec,
                   (unsigned)rec->gw_volt_x10,
                   (int)rec->gw_temp_c,
                   (unsigned)valid_cnt);

    for (i = 0u; i < UI_MAX_NODES; i++)
    {
        const GW_NodeRec_t* r = &rec->nodes[i];
        if (!prv_node_valid(r))
        {
            continue;
        }

        if ((out_sz - len) < 96u)
        {
            truncated = true;
            break;
        }

        prv_append_fmt(out, out_sz, &len,
                       "ND:%02lu,B:%u,T:%d,X:%d,Y:%d,Z:%d,A:%u,P:%lu\r\n",
                       (unsigned long)i,
                       (unsigned)r->batt_lvl,
                       (int)r->temp_c,
                       (int)r->x,
                       (int)r->y,
                       (int)r->z,
                       (unsigned)r->adc,
                       (unsigned long)r->pulse_cnt);
    }

    if (truncated)
    {
        prv_append_fmt(out, out_sz, &len, "TRUNC:1\r\n");
    }

    return len;
}

void GW_Catm1_UartRxCpltCallback(UART_HandleTypeDef *huart)
{
    if (huart != &hlpuart1)
    {
        return;
    }

    if (!s_catm1_rb_ready)
    {
        prv_catm1_rb_reset();
    }

    if (s_catm1_rx_byte != 0u)
    {
        (void)UI_RingBuf_Push(&s_catm1_rb, s_catm1_rx_byte);
        s_catm1_last_rx_ms = HAL_GetTick();
    }

    prv_catm1_rx_start_it();
}

void GW_Catm1_UartErrorCallback(UART_HandleTypeDef *huart)
{
    if ((huart != &hlpuart1) || (hlpuart1.Instance == NULL))
    {
        return;
    }

    (void)HAL_UART_AbortReceive(&hlpuart1);
    prv_lpuart_clear_errors();
    prv_catm1_rx_start_it();
}

void GW_Catm1_Init(void)
{
    /* 필요 시점에만 UART/전원을 올린다. */
    s_catm1_uart_ready = false;
    prv_catm1_rb_reset();
}

void GW_Catm1_PowerOn(void)
{
    s_catm1_session_at_ok = false;
    prv_power_leds_blink_twice();

#if defined(PWR_KEY_Pin)
    /* PWRKEY는 pulse 후 반드시 inactive state로 되돌린다.
     * active state에 계속 머물면 SIM7080 내부 reset timer(약 12.6s)에 의해
     * RDY / +CFUN / +CPIN 재부팅 루프가 날 수 있다. */
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

    prv_power_leds_blink_twice();

#if defined(PWR_KEY_Pin)
    /* 종료 직전에도 inactive state를 보장해서 의도치 않은 reset pulse를 막는다. */
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif

    if (prv_lpuart_is_inited() && s_catm1_session_at_ok)
    {
        /* SIM7080 종료는 AT+CPOWD=1 우선 적용 */
        if (prv_send_cmd_wait("AT+CPOWD=1\r\n",
                              "NORMAL POWER DOWN",
                              "OK",
                              NULL,
                              UI_CATM1_PWRDOWN_TIMEOUT_MS,
                              rsp,
                              sizeof(rsp)))
        {
            normal_pd = true;
            prv_delay_ms(UI_CATM1_PWRDOWN_WAIT_MS);
        }
    }

#if defined(PWR_KEY_Pin)
    if (!normal_pd)
    {
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_ACTIVE_STATE);
        prv_delay_ms(UI_CATM1_PWRKEY_OFF_PULSE_MS);
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);

        /* boot 실패/무응답 경로에서는 CATM1_PWR를 곧바로 차단하므로
         * 긴 power-down 대기 대신 짧은 settle time만 둔다. */
        prv_delay_ms(GW_CATM1_HARD_POWEROFF_WAIT_MS);
    }
#endif

#if defined(CATM1_PWR_Pin)
    HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_RESET);
#endif
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

    if ((out_buf == NULL) || (max_items == 0u))
    {
        return 0u;
    }

    stored = s_time_sync_delta_count;
    cnt = stored;
    if (cnt > max_items)
    {
        cnt = max_items;
    }

    oldest = (stored < GW_CATM1_TIME_SYNC_DELTA_BUF_LEN) ? 0u : s_time_sync_delta_wr;
    skip = (uint8_t)(stored - cnt);
    start = (uint8_t)((oldest + skip) % GW_CATM1_TIME_SYNC_DELTA_BUF_LEN);

    for (i = 0u; i < cnt; i++)
    {
        out_buf[i] = s_time_sync_delta_sec_buf[(uint8_t)((start + i) % GW_CATM1_TIME_SYNC_DELTA_BUF_LEN)];
    }

    return cnt;
}

bool GW_Catm1_SyncTimeOnce(void)
{
    bool success = false;

    if (GW_Catm1_IsBusy())
    {
        return false;
    }

    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);

    if (!prv_lpuart_ensure())
    {
        goto cleanup;
    }

    /* 부팅 직후 시간 동기화는 power-on session 1회에서 끝낸다.
     * CTZU/CLTS는 session 시작 단계에서 먼저 켜고,
     * 그 뒤 CCLK/NTP 순으로 현재 시간을 읽어온다. */
    if (!prv_start_session(true))
    {
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

    if (GW_Catm1_IsBusy())
    {
        return false;
    }

    if ((out_line != NULL) && (out_sz > 0u))
    {
        out_line[0] = '\0';
    }

    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);

    if (!prv_lpuart_ensure())
    {
        goto cleanup;
    }

    if (!prv_start_session(true))
    {
        goto cleanup;
    }

    if (prv_wait_eps_registered() && prv_wait_ps_attached())
    {
        (void)prv_sync_time_from_modem_startup();
    }

    if (!prv_query_gnss_loc_line(loc_line, sizeof(loc_line)))
    {
        goto cleanup;
    }

    memset(&loc_rec, 0, sizeof(loc_rec));
    loc_rec.saved_epoch_sec = UI_Time_NowSec2016();
    (void)snprintf(loc_rec.line, sizeof(loc_rec.line), "%s", loc_line);

    if (!GW_Storage_SaveLocRec(&loc_rec))
    {
        goto cleanup;
    }

    if ((out_line != NULL) && (out_sz > 0u))
    {
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

bool GW_Catm1_SendSnapshot(const GW_HourRec_t* rec)
{
    uint8_t ip[4];
    uint16_t port;
    char payload[UI_CATM1_SERVER_PAYLOAD_MAX + 1u];
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool success = false;
    bool opened = false;
    bool pdp_active = false;
    size_t len;
    GW_HourRec_t live_rec;

    if (rec == NULL)
    {
        return false;
    }

    /* snapshot도 RX 준비 시점에 확정된 rec->epoch_sec를 그대로 사용한다.
     * 이렇게 해야 저장 파일과 TCP payload timestamp가 동일하게 움직인다. */
    live_rec = *rec;

    len = prv_build_snapshot_payload(&live_rec, payload, sizeof(payload));
    if (len == 0u)
    {
        return false;
    }

    prv_get_server(ip, &port);

    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);

    if (!prv_lpuart_ensure())
    {
        goto cleanup;
    }

    if (!prv_start_session(true))
    {
        goto cleanup;
    }

    /* 자동 시간 갱신 설정은 best-effort로 켜 두되,
     * 세션 중 CFUN 재부팅은 하지 않는다.
     * 따라서 현재 세션에서 CCLK가 아직 80/... 이면 시간 보정은 건너뛴다. */

    /* 런타임에서 CLTS/CTZU 설정 직후 CFUN=1,1까지 강제로 수행하면
     * 세션 도중 RDY 재부팅이 섞일 수 있다.
     * 따라서 데이터 전송 경로에서는 모듈 재부팅을 유발하지 않고,
     * 망 등록(EPS)과 PS attach가 성립한 뒤 CCLK?를 읽어 시간이 유효하면 반영한다. */
    if (!prv_wait_eps_registered())
    {
        goto cleanup;
    }

    if (!prv_wait_ps_attached())
    {
        goto cleanup;
    }

    if (!prv_activate_pdp())
    {
        goto cleanup;
    }
    pdp_active = true;

    if (!prv_open_tcp(ip, port))
    {
        goto cleanup;
    }
    opened = true;

    if (!prv_send_tcp_payload(payload))
    {
        goto cleanup;
    }

    success = true;

cleanup:
    if (opened)
    {
        (void)prv_send_cmd_wait("AT+CACLOSE=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    }
    /* 세션 종료 후 모듈을 끌 것이므로 PDP deactivate는 생략한다. */
    (void)pdp_active;
    GW_Catm1_PowerOff();
    prv_lpuart_release();
    GW_Catm1_SetBusy(false);
    UI_LPM_UnlockStop();
    return success;
}

bool GW_Catm1_SendStoredRange(uint32_t first_rec_index,
                              uint32_t max_count,
                              uint32_t* out_sent_count)
{
    uint8_t ip[4];
    uint16_t port;
    char payload[UI_CATM1_SERVER_PAYLOAD_MAX + 1u];
    char rsp[UI_CATM1_RX_BUF_SZ];
    GW_FileRec_t file_rec;
    bool success = false;
    bool pdp_active = false;
    uint32_t i;

    if (out_sent_count != NULL)
    {
        *out_sent_count = 0u;
    }

    if ((max_count == 0u) || (out_sent_count == NULL))
    {
        return false;
    }

    if (!GW_Storage_ReadRecordByGlobalIndex(first_rec_index, &file_rec, NULL))
    {
        return false;
    }

    prv_get_server(ip, &port);

    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);

    if (!prv_lpuart_ensure())
    {
        goto cleanup;
    }

    if (!prv_start_session(true))
    {
        goto cleanup;
    }

    if (!prv_wait_eps_registered())
    {
        goto cleanup;
    }

    if (!prv_wait_ps_attached())
    {
        goto cleanup;
    }

    if (!prv_activate_pdp())
    {
        goto cleanup;
    }
    pdp_active = true;

    for (i = 0u; i < max_count; i++)
    {
        bool opened = false;
        size_t len;

        if (!GW_Storage_ReadRecordByGlobalIndex(first_rec_index + i, &file_rec, NULL))
        {
            break;
        }

        len = prv_build_snapshot_payload(&file_rec.rec, payload, sizeof(payload));
        if (len == 0u)
        {
            break;
        }

        if (!prv_open_tcp(ip, port))
        {
            break;
        }
        opened = true;

        if (!prv_send_tcp_payload(payload))
        {
            if (opened)
            {
                (void)prv_send_cmd_wait("AT+CACLOSE=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
            }
            break;
        }

        (void)prv_send_cmd_wait("AT+CACLOSE=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
        opened = false;

        (*out_sent_count)++;
    }

    success = ((*out_sent_count) > 0u);

cleanup:
    /* 세션 종료 후 모듈을 끌 것이므로 PDP deactivate는 생략한다. */
    (void)pdp_active;
    GW_Catm1_PowerOff();
    prv_lpuart_release();
    GW_Catm1_SetBusy(false);
    UI_LPM_UnlockStop();
    return success;
}
