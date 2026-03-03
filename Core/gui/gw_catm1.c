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
static uint8_t s_catm1_rx_byte = 0u;
static uint8_t s_catm1_rb_mem[UI_CATM1_RX_RING_SIZE];
static UI_RingBuf_t s_catm1_rb;
static bool s_catm1_rb_ready = false;
static volatile uint32_t s_catm1_last_rx_ms = 0u;

static void prv_catm1_rb_reset(void)
{
    UI_RingBuf_Init(&s_catm1_rb, s_catm1_rb_mem, UI_CATM1_RX_RING_SIZE);
    s_catm1_rb_ready = true;
    s_catm1_last_rx_ms = HAL_GetTick();
}

static void prv_catm1_rx_start_it(void)
{
    HAL_StatusTypeDef st;

    if (!s_catm1_rb_ready)
    {
        prv_catm1_rb_reset();
    }

    st = HAL_UART_Receive_IT(&hlpuart1, &s_catm1_rx_byte, 1u);
    if ((st != HAL_OK) && (st != HAL_BUSY))
    {
        /* ignore: next command/session will retry */
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

    if (!prv_lpuart_is_inited())
    {
        if (HAL_UART_Init(&hlpuart1) != HAL_OK)
        {
            return false;
        }
    }

    if (!s_catm1_rb_ready)
    {
        prv_catm1_rb_reset();
    }

    prv_catm1_rx_start_it();
    return true;
}

static void prv_lpuart_release(void)
{
    if (prv_lpuart_is_inited())
    {
        (void)HAL_UART_DeInit(&hlpuart1);
    }
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

    for (i = 0u; i < UI_CATM1_AT_SYNC_RETRY; i++)
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

    if (!prv_send_query_wait_prefix_ok("AT+CPIN?\r\n", "+CPIN:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
    {
        return false;
    }

    return (strstr(rsp, "+CPIN: READY") != NULL);
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

static bool prv_query_network_time_epoch(uint64_t* out_epoch_centi)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t i;

    if (out_epoch_centi == NULL)
    {
        return false;
    }

    for (i = 0u; i < UI_CATM1_TIME_SYNC_RETRY; i++)
    {
        if (prv_send_query_wait_prefix_ok("AT+CCLK?\r\n", "+CCLK:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cclk_epoch(rsp, out_epoch_centi))
        {
            return true;
        }

        prv_delay_ms(UI_CATM1_TIME_SYNC_GAP_MS);
    }

    return false;
}

static bool prv_sync_time_from_modem(void)
{
    uint64_t epoch_centi = 0u;

    if (!prv_query_network_time_epoch(&epoch_centi))
    {
        return false;
    }

    UI_Time_SetEpochCenti2016(epoch_centi);
    return true;
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
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t stat = 0u;
    uint32_t start = HAL_GetTick();

    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_NET_REG_TIMEOUT_MS)
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
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t state = 0u;
    uint32_t start = HAL_GetTick();

    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_PS_ATTACH_TIMEOUT_MS)
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

    if (!prv_send_cmd_wait("AT+CNACT=0,1\r\n", "OK", "+APP PDP: 0,ACTIVE", NULL, UI_CATM1_NET_ACT_TIMEOUT_MS, rsp, sizeof(rsp)))
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
    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_NET_ACT_TIMEOUT_MS)
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

static bool prv_query_caack_ok(uint16_t sent_len)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    unsigned total = 0u;
    unsigned unack = 0u;
    uint32_t retry;

    for (retry = 0u; retry < 10u; retry++)
    {
        if (!prv_send_cmd_wait("AT+CAACK=0\r\n", "+CAACK:", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)))
        {
            prv_delay_ms(200u);
            continue;
        }

        if (sscanf(rsp, "%*[^:]: %u,%u", &total, &unack) == 2)
        {
            if ((total >= (unsigned)sent_len) && (unack == 0u))
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

    if (payload == NULL)
    {
        return false;
    }

    len = strlen(payload);
    if ((len == 0u) || (len > 1460u))
    {
        return false;
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

    return prv_query_caack_ok((uint16_t)len);
}

static bool prv_query_gnss_line(char* out, size_t out_sz)
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
        e = strpbrk(p, "\r\n");
        if (e != NULL)
        {
            *e = '\0';
        }
        (void)snprintf(out, out_sz, "GNSS,%s\r\n", p);
    }

    (void)prv_send_cmd_wait("AT+CGNSPWR=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    return (out[0] != '\0');
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
    UI_Time_Epoch2016_ToCalendar(rec->epoch_sec, &dt);
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
                   (int)rec->gw_temp_x10,
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

    (void)UI_RingBuf_Push(&s_catm1_rb, s_catm1_rx_byte);
    s_catm1_last_rx_ms = HAL_GetTick();
    prv_catm1_rx_start_it();
}

void GW_Catm1_UartErrorCallback(UART_HandleTypeDef *huart)
{
    if (huart != &hlpuart1)
    {
        return;
    }

    prv_catm1_rx_start_it();
}

void GW_Catm1_Init(void)
{
    /* 필요 시점에만 UART/전원을 올린다. */
    prv_catm1_rb_reset();
}

void GW_Catm1_PowerOn(void)
{
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

#if defined(PWR_KEY_Pin)
    /* 종료 직전에도 inactive state를 보장해서 의도치 않은 reset pulse를 막는다. */
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif

    if (prv_lpuart_is_inited())
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
        prv_delay_ms(UI_CATM1_PWRDOWN_WAIT_MS);
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

bool GW_Catm1_SendSnapshot(const GW_HourRec_t* rec, bool include_daily_gnss)
{
    uint8_t ip[4];
    uint16_t port;
    char payload[UI_CATM1_SERVER_PAYLOAD_MAX + 1u];
    char gnss_line[256];
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool success = false;
    bool opened = false;
    bool pdp_active = false;
    size_t len;

    len = prv_build_snapshot_payload(rec, payload, sizeof(payload));
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

    GW_Catm1_PowerOn();
    prv_delay_ms(UI_CATM1_BOOT_WAIT_MS);

    if (!prv_send_at_sync())
    {
        goto cleanup;
    }

    if (!prv_wait_sim_ready())
    {
        goto cleanup;
    }

    /* 런타임에서 CLTS=1 / CFUN=1,1을 강제로 수행하면 세션 도중 RDY 재부팅이 섞일 수 있다.
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

    (void)prv_sync_time_from_modem();

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

    if (include_daily_gnss && prv_query_gnss_line(gnss_line, sizeof(gnss_line)))
    {
        (void)prv_send_tcp_payload(gnss_line);
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
