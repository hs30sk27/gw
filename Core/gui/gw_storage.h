/*
 * gw_storage.h
 */
#ifndef GW_STORAGE_H
#define GW_STORAGE_H

#include <stdint.h>
#include <stdbool.h>
#include "ui_conf.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GW_NODE_SENSOR_INFO_INVALID
#define GW_NODE_SENSOR_INFO_INVALID   (0xFFu)
#endif

#define GW_NODE_SENSOR_BIT_X          (1u << 0)
#define GW_NODE_SENSOR_BIT_Y          (1u << 1)
#define GW_NODE_SENSOR_BIT_Z          (1u << 2)
#define GW_NODE_SENSOR_BIT_ADC        (1u << 3)
#define GW_NODE_SENSOR_BIT_PULSE      (1u << 4)

typedef struct __attribute__((packed))
{
    uint8_t  batt_lvl;
    int8_t   temp_c;
    uint8_t  sensor_info;
    int16_t  x;
    int16_t  y;
    int16_t  z;
    uint16_t adc;
    uint32_t pulse_cnt;
} GW_NodeRec_t;

typedef struct __attribute__((packed))
{
    uint16_t gw_volt_x10;
    int16_t  gw_temp_x10;
    uint32_t epoch_sec;
    GW_NodeRec_t nodes[UI_MAX_NODES];
} GW_HourRec_t;

typedef struct __attribute__((packed))
{
    uint8_t  net_id[UI_NET_ID_LEN];
    uint8_t  gw_num;
    uint8_t  rec_type;
    uint32_t epoch_sec;
    GW_HourRec_t rec;
    uint16_t crc16;
} GW_FileRec_t;

typedef struct __attribute__((packed))
{
    uint32_t day_epoch_sec;
    uint16_t rec_index;
    uint16_t reserved;
} GW_StorageRecRef_t;

void GW_Storage_Init(void);

typedef struct
{
    uint16_t list_index;
    char     name[64];
    uint32_t size;
    uint16_t rec_count;
    uint32_t first_epoch_sec;
    uint32_t last_epoch_sec;
} GW_StorageFileInfo_t;

typedef bool (*GW_StorageReadCb_t)(const GW_StorageFileInfo_t* info,
                                   const GW_FileRec_t* rec,
                                   uint32_t rec_index,
                                   void* user);

bool GW_Storage_SaveHourRec(const GW_HourRec_t* rec);
void GW_Storage_PurgeOldFiles(uint32_t now_epoch_sec);
uint16_t GW_Storage_ListFiles(GW_StorageFileInfo_t* out, uint16_t max_items);
bool GW_Storage_ReadAllFiles(GW_StorageReadCb_t cb, void* user);
bool GW_Storage_ReadFileByIndex(uint16_t list_index_1based, GW_StorageReadCb_t cb, void* user);
bool GW_Storage_DeleteAllFiles(void);
bool GW_Storage_DeleteFileByIndex(uint16_t list_index_1based);

bool GW_Storage_FindRecordRefByEpoch(uint32_t epoch_sec, GW_StorageRecRef_t* out);
bool GW_Storage_ReadHourRecByRef(const GW_StorageRecRef_t* ref, GW_HourRec_t* out);

uint16_t GW_Storage_TcpQueue_Count(void);
uint32_t GW_Storage_TcpQueue_DropCount(void);
bool GW_Storage_TcpQueue_Push(const GW_StorageRecRef_t* ref, uint16_t max_keep);
bool GW_Storage_TcpQueue_Peek(uint16_t order, GW_StorageRecRef_t* out);
bool GW_Storage_TcpQueue_PopN(uint16_t count);
bool GW_Storage_TcpQueue_Clear(void);
bool GW_Storage_TcpQueue_RemoveDay(uint32_t day_epoch_sec);

bool GW_Storage_W25Q_PowerOn(void);
void GW_Storage_W25Q_PowerDown(void);
int  GW_Storage_W25Q_Read(uint32_t addr, void* buf, uint32_t size);
int  GW_Storage_W25Q_Prog(uint32_t addr, const void* buf, uint32_t size);
int  GW_Storage_W25Q_Erase4K(uint32_t addr);
int  GW_Storage_W25Q_Sync(void);

#ifdef __cplusplus
}
#endif

#endif /* GW_STORAGE_H */
