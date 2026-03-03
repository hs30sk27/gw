/*
 * gw_catm1.h
 *
 * SIM7080(CATM1) minimal helper
 *
 * - 1NCE APN(iot.1nce.net) 기반 PDP active
 * - 세션마다 SIM7080 network time(CCLK)로 GW 시간을 보정
 * - TCP open -> CASEND -> CAACK 확인 -> close
 * - 하루 1회 GNSS(CGNSINF) 확인용 payload 전송 지원
 */

#ifndef GW_CATM1_H
#define GW_CATM1_H

#include <stdint.h>
#include <stdbool.h>
#include "stm32wlxx_hal.h"
#include "gw_storage.h"

#ifdef __cplusplus
extern "C" {
#endif

void GW_Catm1_Init(void);
void GW_Catm1_PowerOn(void);
void GW_Catm1_PowerOff(void);

/* LPUART1 RX interrupt callback routing (project uart_if.c / weak callback override용) */
void GW_Catm1_UartRxCpltCallback(UART_HandleTypeDef *huart);
void GW_Catm1_UartErrorCallback(UART_HandleTypeDef *huart);

/* 서버 전송 등 고전류 동작 여부(비콘/BLE 충돌 회피에 사용) */
bool GW_Catm1_IsBusy(void);
void GW_Catm1_SetBusy(bool busy);

/* one-shot session: power on -> TCP send -> optional GNSS send -> power off */
bool GW_Catm1_SendSnapshot(const GW_HourRec_t* rec, bool include_daily_gnss);

#ifdef __cplusplus
}
#endif

#endif /* GW_CATM1_H */
