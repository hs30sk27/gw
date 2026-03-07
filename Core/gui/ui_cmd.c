#include "ui_cmd.h"
#include "ui_types.h"
#include "ui_uart.h"
#include "ui_time.h"
#include "ui_ble.h"
#include "gw_file_cmd.h"
#include "gw_catm1.h"
#include "gw_storage.h"

#include "stm32wlxx_hal.h" /* __weak */
#include <string.h>
#include <stdio.h>
#include <ctype.h>

/* -------------------------------------------------------------------------- */
/* Hook functions (GW/ND에서 필요 시 override)                                 */
/* -------------------------------------------------------------------------- */
__weak void UI_Hook_OnConfigChanged(void) {}
__weak void UI_Hook_OnTimeChanged(void) {}
__weak void UI_Hook_OnBeaconOnceRequested(void) {}
__weak void UI_Hook_OnBleEndRequested(void) {}
__weak void UI_Hook_OnSettingChanged(uint8_t value, char unit)
{
    (void)value;
    (void)unit;
}

/* -------------------------------------------------------------------------- */
static void prv_send_ok(void)
{
    UI_UART_SendString("OK\r\n");
}

static void prv_send_error(void)
{
    UI_UART_SendString("ERROR\r\n");
}

static const char* prv_skip_spaces(const char* s)
{
    while (s && *s && isspace((unsigned char)*s)) { s++; }
    return s;
}

/* CR/LF 제거용 (line은 이미 null-terminated 가정) */
static void prv_rstrip(char* s)
{
    size_t n = strlen(s);
    while (n > 0u)
    {
        char c = s[n-1u];
        if (c == '\r' || c == '\n' || c == ' ' || c == '\t')
        {
            s[n-1u] = '\0';
            n--;
        }
        else
        {
            break;
        }
    }
}

static int prv_parse_u8_dec(const char* s, uint8_t* out)
{
    /* 0..255 */
    unsigned v = 0;
    int cnt = 0;
    while (*s && isdigit((unsigned char)*s) && cnt < 3)
    {
        v = v*10u + (unsigned)(*s - '0');
        s++;
        cnt++;
    }
    if (cnt == 0) return 0;
    if (v > 255u) return 0;
    *out = (uint8_t)v;
    return cnt;
}

static bool prv_cmd_equals_relaxed(const char* s, const char* base)
{
    if ((s == NULL) || (base == NULL))
    {
        return false;
    }

    while (*base != '\0')
    {
        if (toupper((unsigned char)*s) != toupper((unsigned char)*base))
        {
            return false;
        }
        s++;
        base++;
    }

    while ((*s == ' ') || (*s == '\t'))
    {
        s++;
    }

    if (*s == ':')
    {
        s++;
        while (isspace((unsigned char)*s))
        {
            s++;
        }
    }

    return (*s == '\0');
}


static bool prv_commit_config_changed(void)
{
    if (!UI_Config_Save())
    {
        prv_send_error();
        return false;
    }

    prv_send_ok();
    UI_Hook_OnConfigChanged();
    return true;
}

static bool prv_commit_setting_changed(uint8_t value, char unit)
{
    if (!UI_Config_Save())
    {
        prv_send_error();
        return false;
    }

    prv_send_ok();
    UI_Hook_OnSettingChanged(value, unit);
    return true;
}

static void prv_send_setting_read(void)
{
    const UI_Config_t* cfg = UI_GetConfig();
    char netid[UI_NET_ID_LEN + 1u];
    char line[256];

    memcpy(netid, cfg->net_id, UI_NET_ID_LEN);
    netid[UI_NET_ID_LEN] = '\0';

    (void)snprintf(line, sizeof(line), "NETID:%s\r\n", netid);
    UI_UART_SendString(line);

    (void)snprintf(line, sizeof(line), "GW NUM:%u\r\n", cfg->gw_num);
    UI_UART_SendString(line);

    uint8_t last_nd_num = (cfg->max_nodes > 0u) ? (uint8_t)(cfg->max_nodes - 1u) : 0u;
    (void)snprintf(line, sizeof(line), "ND NUM:%u\r\n", last_nd_num);
    UI_UART_SendString(line);

    (void)snprintf(line, sizeof(line), "SETTING:%c%c%c\r\n",
                   (char)cfg->setting_ascii[0],
                   (char)cfg->setting_ascii[1],
                   (char)cfg->setting_ascii[2]);
    UI_UART_SendString(line);

    (void)snprintf(line, sizeof(line), "TCPIP:%u.%u.%u.%u:%u\r\n",
                   cfg->tcpip_ip[0], cfg->tcpip_ip[1], cfg->tcpip_ip[2], cfg->tcpip_ip[3], cfg->tcpip_port);
    UI_UART_SendString(line);

    (void)snprintf(line, sizeof(line), "LOC:%s\r\n",
                   (cfg->loc_ascii[0] != '\0') ? cfg->loc_ascii : "0,0");
    UI_UART_SendString(line);
}

static void prv_store_loc_to_compat_storage(const char* raw_ascii)
{
    GW_LocRec_t rec;

    if ((raw_ascii == NULL) || (raw_ascii[0] == '\0'))
    {
        return;
    }

    memset(&rec, 0, sizeof(rec));
    rec.saved_epoch_sec = UI_Time_NowSec2016();
    (void)snprintf(rec.line, sizeof(rec.line), "LOC:%s", raw_ascii);
    (void)GW_Storage_SaveLocRec(&rec);
}

void UI_Cmd_ProcessLine(const char* line_in)
{
    if (line_in == NULL) { return; }

    /* line_in은 상위에서 buffer를 넘겨주므로 안전하게 로컬 복사 */
    char line[UI_UART_LINE_MAX];
    (void)snprintf(line, sizeof(line), "%s", line_in);
    prv_rstrip(line);

    /*
     * 최종 요구사항:
     *   - UART1 내부 명령은 반드시 "<CMD>CRLF" 형태로만 처리
     *   - 블루투스 시작 시 튀는 데이터(쓰레기)에는 응답하지 않음
     */
    const char* s0 = prv_skip_spaces(line);
    if (s0 == NULL || *s0 != '<')
    {
        return;
    }
    size_t n0 = strlen(s0);
    if (n0 < 3u || s0[n0 - 1u] != '>')
    {
        return;
    }

    /* 프레임 '<', '>' 제거 */
    char* s = (char*)prv_skip_spaces(line);
    if (*s == '<')
    {
        s++;
    }

    s = (char*)prv_skip_spaces(s);
    prv_rstrip(s);
    size_t n = strlen(s);
    if (n > 0u && s[n-1u] == '>')
    {
        s[n-1u] = '\0';
        prv_rstrip(s);
    }

    const char* p = s;
    if (*p == '\0') { return; }

    /* 요구사항: BLE active 연장은 유효한 <...> 프레임일 때만 수행 */
    UI_BLE_ExtendMs(UI_BLE_ACTIVE_MS);

    /* -------------------- SETTING READ ------------------ */
    if (prv_cmd_equals_relaxed(p, "SETTING READ"))
    {
        prv_send_setting_read();
        prv_send_ok();
        return;
    }

    /* -------------------- TIME CHECK -------------------- */
    if ((strcmp(p, "TIME CHECK") == 0) || (strcmp(p, "TIME CHECK:") == 0))
    {
        char ts[48];
        UI_Time_FormatNow(ts, sizeof(ts));
        UI_UART_SendString(ts);
        UI_UART_SendString("\r\n");
        prv_send_ok();
        return;
    }

    /* -------------------- TIME:... ---------------------- */
    if (strncmp(p, "TIME:", 5) == 0)
    {
        if (UI_Time_SetFromString(p))
        {
            prv_send_ok();
            UI_Hook_OnTimeChanged();
        }
        else
        {
            prv_send_error();
        }
        return;
    }

    /* -------------------- NETID:XXXXXXXXXX -------------- */
    if (strncmp(p, "NETID:", 6) == 0)
    {
        const char* id = p + 6;
        if (strlen(id) < UI_NET_ID_LEN)
        {
            prv_send_error();
            return;
        }

        uint8_t net_id[UI_NET_ID_LEN];
        for (uint32_t i = 0; i < UI_NET_ID_LEN; i++)
        {
            net_id[i] = (uint8_t)id[i];
        }
        UI_SetNetId(net_id);
        (void)prv_commit_config_changed();
        return;
    }

    /* -------------------- GW NUM:x ---------------------- */
    if (strncmp(p, "GW NUM:", 7) == 0)
    {
        uint8_t gw = 0;
        if (prv_parse_u8_dec(p + 7, &gw) > 0)
        {
            UI_SetGwNum(gw);
            (void)prv_commit_config_changed();
        }
        else
        {
            prv_send_error();
        }
        return;
    }

    /* -------------------- SETTING:xxM/H ----------------- */
    if (strncmp(p, "SETTING:", 8) == 0)
    {
        const char* q = p + 8;
        uint8_t v = 0;
        int nn = prv_parse_u8_dec(q, &v);
        if (nn <= 0)
        {
            prv_send_error();
            return;
        }
        q += nn;
        char unit = *q;
        if ((unit != 'M') && (unit != 'H'))
        {
            prv_send_error();
            return;
        }

        if (v == 0u)
        {
            prv_send_error();
            return;
        }

        UI_SetSetting(v, unit);
        (void)prv_commit_setting_changed(v, unit);
        return;
    }

    /* -------------------- BEACON ON --------------------- */
    if ((strcmp(p, "BEACON ON") == 0) || (strcmp(p, "BEACON ON:") == 0))
    {
        prv_send_ok();
        UI_Hook_OnBeaconOnceRequested();
        return;
    }

    /* -------------------- ND NUM:xx --------------------- */
    if (strncmp(p, "ND NUM:", 7) == 0)
    {
        uint8_t v = 0;
        if (prv_parse_u8_dec(p + 7, &v) <= 0)
        {
            prv_send_error();
            return;
        }

        /* GW: ND NUM:xx = 마지막 ND 번호(0-based).
         * 예) ND NUM:0 -> RX 1 slot, ND NUM:4 -> RX 5 slots */
        if (v >= UI_MAX_NODES)
        {
            prv_send_error();
            return;
        }

        UI_SetMaxNodes((uint8_t)(v + 1u));
        (void)prv_commit_config_changed();
        return;
    }

    /* -------------------- TCPIP:xxx.xxx.xxx.xxx:yyyyy --- */
    if (strncmp(p, "TCPIP:", 6) == 0)
    {
        const char* q = p + 6;
        unsigned a,b,c,d,port;
        if (sscanf(q, "%3u.%3u.%3u.%3u:%5u", &a,&b,&c,&d,&port) != 5)
        {
            prv_send_error();
            return;
        }
        if (a > 255u || b > 255u || c > 255u || d > 255u || port > 65535u)
        {
            prv_send_error();
            return;
        }
        uint8_t ip[4] = {(uint8_t)a,(uint8_t)b,(uint8_t)c,(uint8_t)d};
        UI_SetTcpIp(ip, (uint16_t)port);
        (void)prv_commit_config_changed();
        return;
    }

    /* -------------------- FILE LIST ---------------------- */
    if ((strcmp(p, "FILE LIST") == 0) || (strcmp(p, "FILE LIST:") == 0))
    {
        if (GW_FileCmd_List())
        {
            prv_send_ok();
        }
        else
        {
            prv_send_error();
        }
        return;
    }

    /* -------------------- FILE READ:ALL|n --------------- */
    if (strncmp(p, "FILE READ:", 10) == 0)
    {
        if (GW_FileCmd_ReadArg(p + 10))
        {
            prv_send_ok();
        }
        else
        {
            prv_send_error();
        }
        return;
    }

    /* -------------------- FILE DEL:ALL|n ---------------- */
    if (strncmp(p, "FILE DEL:", 9) == 0)
    {
        if (GW_FileCmd_DeleteArg(p + 9))
        {
            prv_send_ok();
        }
        else
        {
            prv_send_error();
        }
        return;
    }

    /* -------------------- LOC / READ LOC ---------------- */
    /* GW 전용: <LOC:ASCII GNSS> 를 받으면 내부 Flash 설정에도 저장한다. */
    if ((strncmp(p, "LOC:", 4) == 0) && (p[4] != '\0'))
    {
        char* raw_loc = (char*)prv_skip_spaces(p + 4);
        prv_rstrip(raw_loc);
        if (*raw_loc == '\0')
        {
            prv_send_error();
            return;
        }

        UI_SetLocAscii(raw_loc);
        prv_store_loc_to_compat_storage(raw_loc);
        if (UI_Config_Save())
        {
            UI_UART_SendString("LOC:");
            UI_UART_SendString(raw_loc);
            UI_UART_SendString("\r\n");
            prv_send_ok();
        }
        else
        {
            prv_send_error();
        }
        return;
    }

    if ((strcmp(p, "LOC") == 0) || (strcmp(p, "LOC:") == 0))
    {
        char loc_line[GW_LOC_LINE_MAX];
        if (GW_Catm1_QueryAndStoreLoc(loc_line, sizeof(loc_line)))
        {
            char* raw_loc = (char*)loc_line;
            if (strncmp(raw_loc, "LOC:", 4) == 0)
            {
                raw_loc += 4;
            }
            raw_loc = (char*)prv_skip_spaces(raw_loc);
            prv_rstrip(raw_loc);
            if (*raw_loc == '\0')
            {
                prv_send_error();
                return;
            }

            UI_SetLocAscii(raw_loc);
            prv_store_loc_to_compat_storage(raw_loc);
            if (!UI_Config_Save())
            {
                prv_send_error();
                return;
            }
            UI_UART_SendString("LOC:");
            UI_UART_SendString(raw_loc);
            UI_UART_SendString("\r\n");
            prv_send_ok();
        }
        else
        {
            prv_send_error();
        }
        return;
    }

    if ((strcmp(p, "READ LOC") == 0) || (strcmp(p, "READ LOC:") == 0))
    {
        const UI_Config_t* cfg = UI_GetConfig();
        if (cfg->loc_ascii[0] != '\0')
        {
            UI_UART_SendString("LOC:");
            UI_UART_SendString(cfg->loc_ascii);
            UI_UART_SendString("\r\n");
            prv_send_ok();
        }
        else
        {
            GW_LocRec_t loc_rec;
            if (GW_Storage_ReadLocRec(&loc_rec) && (loc_rec.line[0] != '\0'))
            {
                UI_UART_SendString(loc_rec.line);
                UI_UART_SendString("\r\n");
                prv_send_ok();
            }
            else
            {
                prv_send_error();
            }
        }
        return;
    }

    /* -------------------- BLE END ------------------------ */
    if ((strncmp(p, "BLE END", 7) == 0) || (strncmp(p, "BLE END:", 8) == 0))
    {
        prv_send_ok();
        UI_Hook_OnBleEndRequested();
        return;
    }

    prv_send_error();
}
