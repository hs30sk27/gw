/*
 * ui_types.h
 */
#ifndef UI_TYPES_H
#define UI_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include "ui_conf.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
    uint8_t  net_id[UI_NET_ID_LEN];
    uint8_t  gw_num;
    uint8_t  max_nodes;
    uint8_t  node_num;
    uint8_t  setting_value;
    char     setting_unit;
    uint8_t  setting_ascii[3];
    uint8_t  tcpip_ip[4];
    uint16_t tcpip_port;
    int32_t  gnss_lat_e7;
    int32_t  gnss_lon_e7;
} UI_Config_t;

const UI_Config_t* UI_GetConfig(void);
void UI_SetNetId(const uint8_t net_id_10[UI_NET_ID_LEN]);
void UI_SetGwNum(uint8_t gw_num);
void UI_SetMaxNodes(uint8_t max_nodes);
void UI_SetNodeNum(uint8_t node_num);
void UI_SetSetting(uint8_t value, char unit);
void UI_SetTcpIp(const uint8_t ip[4], uint16_t port);
void UI_SetGnssPosE7(int32_t lat_e7, int32_t lon_e7);

typedef enum
{
    UI_GPIO_EVT_NONE      = 0,
    UI_GPIO_EVT_TEST_KEY  = (1u << 0),
    UI_GPIO_EVT_OP_KEY    = (1u << 1),
    UI_GPIO_EVT_PULSE_IN  = (1u << 2),
} UI_GpioEventBits_t;

typedef enum
{
    UI_CMD_OK = 0,
    UI_CMD_ERROR,
} UI_CmdStatus_t;

#ifdef __cplusplus
}
#endif

#endif /* UI_TYPES_H */
