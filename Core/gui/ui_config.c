/*
 * ui_config.c
 *
 * GW runtime config + MCU internal flash persistence.
 * Uses the last 2KB flash page at 0x0803F800.
 */

#include "ui_types.h"
#include "stm32wlxx_hal.h"
#include <string.h>

#define UI_CFG_FLASH_ADDR            (0x0803F800u)
#define UI_CFG_FLASH_PAGE_SIZE       (2048u)
#define UI_CFG_MAGIC                 (0x55494346u) /* UICF */
#define UI_CFG_VERSION               (2u)

typedef struct __attribute__((packed))
{
    uint32_t    magic;
    uint16_t    version;
    uint16_t    size;
    UI_Config_t cfg;
    uint32_t    crc32;
} UI_ConfigFlash_t;

static UI_Config_t s_cfg;
static uint8_t s_inited = 0u;

static uint32_t prv_crc32(const uint8_t* data, uint32_t len)
{
    uint32_t crc = 0xFFFFFFFFu;
    uint32_t i;
    uint32_t j;

    if (data == NULL)
    {
        return 0u;
    }

    for (i = 0u; i < len; i++)
    {
        crc ^= (uint32_t)data[i];
        for (j = 0u; j < 8u; j++)
        {
            if ((crc & 1u) != 0u)
            {
                crc = (crc >> 1u) ^ 0xEDB88320u;
            }
            else
            {
                crc >>= 1u;
            }
        }
    }

    return ~crc;
}

static void prv_set_setting_ascii(uint8_t value, char unit)
{
    s_cfg.setting_ascii[0] = (uint8_t)('0' + ((value / 10u) % 10u));
    s_cfg.setting_ascii[1] = (uint8_t)('0' + (value % 10u));
    s_cfg.setting_ascii[2] = (uint8_t)unit;
}

static void prv_apply_limits(void)
{
    if (s_cfg.max_nodes < 1u)
    {
        s_cfg.max_nodes = 1u;
    }
    if (s_cfg.max_nodes > UI_MAX_NODES)
    {
        s_cfg.max_nodes = UI_MAX_NODES;
    }
    if (s_cfg.node_num >= UI_MAX_NODES)
    {
        s_cfg.node_num = (UI_MAX_NODES - 1u);
    }
    if (s_cfg.setting_value > 99u)
    {
        s_cfg.setting_value = 99u;
    }
    if ((s_cfg.setting_unit != 'M') && (s_cfg.setting_unit != 'H'))
    {
        s_cfg.setting_unit = 'H';
    }
    if (s_cfg.tcpip_port < UI_TCPIP_MIN_PORT)
    {
        s_cfg.tcpip_ip[0] = UI_TCPIP_DEFAULT_IP0;
        s_cfg.tcpip_ip[1] = UI_TCPIP_DEFAULT_IP1;
        s_cfg.tcpip_ip[2] = UI_TCPIP_DEFAULT_IP2;
        s_cfg.tcpip_ip[3] = UI_TCPIP_DEFAULT_IP3;
        s_cfg.tcpip_port = UI_TCPIP_DEFAULT_PORT;
    }
    prv_set_setting_ascii(s_cfg.setting_value, s_cfg.setting_unit);
}

static void prv_init_defaults(void)
{
    uint32_t i;

    memset(&s_cfg, 0, sizeof(s_cfg));
    for (i = 0u; i < UI_NET_ID_LEN; i++)
    {
        s_cfg.net_id[i] = (uint8_t)'0';
    }
    s_cfg.gw_num = 0u;
    s_cfg.max_nodes = 50u;
    s_cfg.node_num = 0u;
    s_cfg.setting_value = 0u;
    s_cfg.setting_unit = 'H';
    s_cfg.tcpip_ip[0] = UI_TCPIP_DEFAULT_IP0;
    s_cfg.tcpip_ip[1] = UI_TCPIP_DEFAULT_IP1;
    s_cfg.tcpip_ip[2] = UI_TCPIP_DEFAULT_IP2;
    s_cfg.tcpip_ip[3] = UI_TCPIP_DEFAULT_IP3;
    s_cfg.tcpip_port = UI_TCPIP_DEFAULT_PORT;
    s_cfg.gnss_lat_e7 = 0;
    s_cfg.gnss_lon_e7 = 0;
    prv_apply_limits();
}

static bool prv_flash_read(UI_ConfigFlash_t* out)
{
    const UI_ConfigFlash_t* p = (const UI_ConfigFlash_t*)UI_CFG_FLASH_ADDR;
    uint32_t crc;

    if (out == NULL)
    {
        return false;
    }

    memcpy(out, p, sizeof(*out));
    if ((out->magic != UI_CFG_MAGIC) ||
        (out->version != UI_CFG_VERSION) ||
        (out->size != sizeof(UI_Config_t)))
    {
        return false;
    }

    crc = prv_crc32((const uint8_t*)&out->cfg, sizeof(out->cfg));
    return (crc == out->crc32);
}

static bool prv_flash_write(void)
{
    UI_ConfigFlash_t img;
    FLASH_EraseInitTypeDef erase;
    uint32_t page_error = 0u;
    uint32_t offset;
    HAL_StatusTypeDef st;

    memset(&img, 0xFF, sizeof(img));
    img.magic = UI_CFG_MAGIC;
    img.version = UI_CFG_VERSION;
    img.size = sizeof(UI_Config_t);
    img.cfg = s_cfg;
    img.crc32 = prv_crc32((const uint8_t*)&img.cfg, sizeof(img.cfg));

    if (HAL_FLASH_Unlock() != HAL_OK)
    {
        return false;
    }

    memset(&erase, 0, sizeof(erase));
    erase.TypeErase = FLASH_TYPEERASE_PAGES;
#if defined(FLASH_BANK_1)
    erase.Banks = FLASH_BANK_1;
#endif
    erase.Page = (UI_CFG_FLASH_ADDR - FLASH_BASE) / UI_CFG_FLASH_PAGE_SIZE;
    erase.NbPages = 1u;

    st = HAL_FLASHEx_Erase(&erase, &page_error);
    if (st != HAL_OK)
    {
        (void)HAL_FLASH_Lock();
        return false;
    }

    for (offset = 0u; offset < sizeof(img); offset += 8u)
    {
        uint64_t dw = 0xFFFFFFFFFFFFFFFFull;
        uint32_t copy_len = ((sizeof(img) - offset) >= 8u) ? 8u : (sizeof(img) - offset);
        memcpy(&dw, ((const uint8_t*)&img) + offset, copy_len);
        st = HAL_FLASH_Program(FLASH_TYPEPROGRAM_DOUBLEWORD, UI_CFG_FLASH_ADDR + offset, dw);
        if (st != HAL_OK)
        {
            (void)HAL_FLASH_Lock();
            return false;
        }
    }

    (void)HAL_FLASH_Lock();
    return true;
}

static void prv_ensure_init(void)
{
    UI_ConfigFlash_t img;

    if (s_inited != 0u)
    {
        return;
    }

    if (prv_flash_read(&img))
    {
        s_cfg = img.cfg;
        prv_apply_limits();
    }
    else
    {
        prv_init_defaults();
        (void)prv_flash_write();
    }

    s_inited = 1u;
}

const UI_Config_t* UI_GetConfig(void)
{
    prv_ensure_init();
    return &s_cfg;
}

void UI_SetNetId(const uint8_t net_id_10[UI_NET_ID_LEN])
{
    prv_ensure_init();
    memcpy(s_cfg.net_id, net_id_10, UI_NET_ID_LEN);
    (void)prv_flash_write();
}

void UI_SetGwNum(uint8_t gw_num)
{
    prv_ensure_init();
    s_cfg.gw_num = gw_num;
    (void)prv_flash_write();
}

void UI_SetMaxNodes(uint8_t max_nodes)
{
    prv_ensure_init();
    s_cfg.max_nodes = max_nodes;
    prv_apply_limits();
    (void)prv_flash_write();
}

void UI_SetNodeNum(uint8_t node_num)
{
    prv_ensure_init();
    s_cfg.node_num = node_num;
    prv_apply_limits();
    (void)prv_flash_write();
}

void UI_SetSetting(uint8_t value, char unit)
{
    prv_ensure_init();
    s_cfg.setting_value = value;
    s_cfg.setting_unit = unit;
    prv_apply_limits();
    (void)prv_flash_write();
}

void UI_SetTcpIp(const uint8_t ip[4], uint16_t port)
{
    prv_ensure_init();
    memcpy(s_cfg.tcpip_ip, ip, 4u);
    s_cfg.tcpip_port = port;
    prv_apply_limits();
    (void)prv_flash_write();
}

void UI_SetGnssPosE7(int32_t lat_e7, int32_t lon_e7)
{
    prv_ensure_init();
    s_cfg.gnss_lat_e7 = lat_e7;
    s_cfg.gnss_lon_e7 = lon_e7;
    (void)prv_flash_write();
}
