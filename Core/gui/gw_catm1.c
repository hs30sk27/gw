/* gw_1/Core/gui/gw_catm1.c
 *
 * 아래 블록/함수를 기존 파일에서 같은 이름으로 교체해서 사용.
 * - GW_CATM1_BOOT_QUIET_MS 매크로 블록
 * - static bool prv_send_at_sync(void)
 * - static bool prv_start_session(bool enable_time_auto_update)
 */

/* --------------------------------------------------------------------------
 * 기존 매크로 블록 교체
 * -------------------------------------------------------------------------- */
#ifndef GW_CATM1_BOOT_QUIET_MS
#define GW_CATM1_BOOT_QUIET_MS        (80u)
#endif

#ifndef GW_CATM1_SMS_READY_SYNC_MAX_TRY
#define GW_CATM1_SMS_READY_SYNC_MAX_TRY        (3u)
#endif

#ifndef GW_CATM1_SMS_READY_AT_TIMEOUT_MS
#define GW_CATM1_SMS_READY_AT_TIMEOUT_MS       (220u)
#endif

#ifndef GW_CATM1_SMS_READY_SYNC_GAP_MS
#define GW_CATM1_SMS_READY_SYNC_GAP_MS         (40u)
#endif

#ifndef GW_CATM1_SMS_READY_POST_CMD_TIMEOUT_MS
#define GW_CATM1_SMS_READY_POST_CMD_TIMEOUT_MS (120u)
#endif


/* --------------------------------------------------------------------------
 * 기존 static bool prv_send_at_sync(void) 전체 교체
 * -------------------------------------------------------------------------- */
static bool prv_send_at_sync(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t i;
    uint32_t max_try = GW_CATM1_SMS_READY_SYNC_MAX_TRY;

    if ((UI_CATM1_AT_SYNC_RETRY != 0u) && (UI_CATM1_AT_SYNC_RETRY < max_try))
    {
        max_try = UI_CATM1_AT_SYNC_RETRY;
    }

    if (max_try == 0u)
    {
        max_try = GW_CATM1_SMS_READY_SYNC_MAX_TRY;
    }

    for (i = 0u; i < max_try; i++)
    {
        rsp[0] = '\0';
        prv_uart_flush_rx();

        if (!prv_uart_send_text("AT\r\n", GW_CATM1_SMS_READY_AT_TIMEOUT_MS))
        {
            prv_delay_ms(GW_CATM1_SMS_READY_SYNC_GAP_MS);
            continue;
        }

        if (prv_uart_wait_for(rsp,
                              sizeof(rsp),
                              GW_CATM1_SMS_READY_AT_TIMEOUT_MS,
                              "OK",
                              NULL,
                              NULL))
        {
            s_catm1_session_at_ok = true;

            (void)prv_send_cmd_wait("ATE0\r\n",
                                    "OK",
                                    NULL,
                                    NULL,
                                    GW_CATM1_SMS_READY_POST_CMD_TIMEOUT_MS,
                                    rsp,
                                    sizeof(rsp));

            (void)prv_send_cmd_wait("AT+CMEE=2\r\n",
                                    "OK",
                                    NULL,
                                    NULL,
                                    GW_CATM1_SMS_READY_POST_CMD_TIMEOUT_MS,
                                    rsp,
                                    sizeof(rsp));
            return true;
        }

        prv_delay_ms(GW_CATM1_SMS_READY_SYNC_GAP_MS);
    }

    return false;
}


/* --------------------------------------------------------------------------
 * 기존 static bool prv_start_session(bool enable_time_auto_update) 전체 교체
 * -------------------------------------------------------------------------- */
static bool prv_start_session(bool enable_time_auto_update)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool cpin_ready = false;

    prv_uart_flush_rx();

    GW_Catm1_PowerOn();
    prv_delay_ms(UI_CATM1_BOOT_WAIT_MS);

    s_catm1_session_at_ok = false;

    if (!prv_wait_boot_sms_ready())
    {
        return false;
    }

    /* SMS Ready 직후 400ms 대기 대신 짧게 정리 후 바로 진입 */
    prv_wait_rx_quiet(GW_CATM1_BOOT_QUIET_MS, GW_CATM1_BOOT_QUIET_MS + 80u);
    prv_uart_flush_rx();

    /* 1초 이내 진입 목표: 빠른 AT sync */
    if (!prv_send_at_sync())
    {
        return false;
    }

    rsp[0] = '\0';
    if (prv_send_query_wait_prefix_ok("AT+CPIN?\r\n",
                                      "+CPIN:",
                                      UI_CATM1_AT_TIMEOUT_MS,
                                      rsp,
                                      sizeof(rsp)) &&
        (strstr(rsp, "+CPIN: READY") != NULL))
    {
        cpin_ready = true;
    }

    if (enable_time_auto_update)
    {
        prv_enable_network_time_auto_update();
    }

    (void)prv_send_query_wait_prefix_ok("AT+CEREG?\r\n",
                                        "+CEREG:",
                                        UI_CATM1_AT_TIMEOUT_MS,
                                        rsp,
                                        sizeof(rsp));
    (void)prv_send_query_wait_prefix_ok("AT+CGATT?\r\n",
                                        "+CGATT:",
                                        UI_CATM1_AT_TIMEOUT_MS,
                                        rsp,
                                        sizeof(rsp));

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
