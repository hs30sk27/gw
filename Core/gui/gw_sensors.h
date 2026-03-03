/*
 * gw_sensors.h
 *
 * Gateway 내부 센서(내부 온도/전원 전압) 측정 모듈
 *
 * 요구사항:
 *  - GW도 내부 온도 체크 필요(누락 보완)
 *  - 배터리(전류 최소): 필요할 때만 ADC Init, 측정 후 즉시 DeInit
 *  - hadc 사용
 */

#ifndef GW_SENSORS_H
#define GW_SENSORS_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * GW 내부 값 측정
 *  - volt_x10: 0.1V 단위 (예: 33 => 3.3V)
 *  - temp_x10: 0.1'C 단위 (예: 253 => 25.3'C)
 *
 * 성공 시 true, 실패 시 false(출력값은 호출부에서 0xFFFF 유지 권장)
 */
bool GW_Sensors_MeasureGw(uint16_t* volt_x10, int16_t* temp_x10);

#ifdef __cplusplus
}
#endif

#endif /* GW_SENSORS_H */
