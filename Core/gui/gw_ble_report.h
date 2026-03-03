#ifndef GW_BLE_REPORT_H
#define GW_BLE_REPORT_H

#include <stdint.h>
#include <stdbool.h>
#include "gw_storage.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * 1-minute test mode BLE report
 * - called by GW main context at second 50
 * - sends a compact ASCII summary over UART1(BLE)
 */
bool GW_BleReport_SendMinuteTestRecord(const GW_HourRec_t* rec);

#ifdef __cplusplus
}
#endif

#endif /* GW_BLE_REPORT_H */
