/*
 * ui_hal_callbacks.c
 *
 * GUI 전용 HAL/UART dispatch helper.
 *
 * - 기본은 weak callback override를 제공하여, 프로젝트에 별도 uart_if.c가 없으면 바로 동작합니다.
 * - 프로젝트가 uart_if.c / stm32wlxx_it.c를 이미 소유하고 있으면,
 *   거기서 UI_HAL_UART_RxCpltDispatch(), UI_HAL_UART_ErrorDispatch(),
 *   UI_HAL_LPUART1_IrqDispatch()를 호출하면 됩니다.
 */

#include "ui_conf.h"
#include "ui_gpio.h"
#include "ui_uart.h"
#include "ui_hal_uart_dispatch.h"
#include "gw_catm1.h"
#include "stm32wlxx_hal.h"

extern UART_HandleTypeDef hlpuart1;

void UI_HAL_UART_RxCpltDispatch(UART_HandleTypeDef *huart)
{
    UI_UART_RxCpltCallback(huart);
    GW_Catm1_UartRxCpltCallback(huart);
}

void UI_HAL_UART_ErrorDispatch(UART_HandleTypeDef *huart)
{
    UI_UART_ErrorCallback(huart);
    GW_Catm1_UartErrorCallback(huart);
}

void UI_HAL_LPUART1_IrqDispatch(void)
{
    if (hlpuart1.Instance != NULL)
    {
        HAL_UART_IRQHandler(&hlpuart1);
    }
}

#if (UI_OVERRIDE_HAL_UART_WEAK_DISPATCH == 1u)
__attribute__((weak)) void HAL_UART_RxCpltCallback(UART_HandleTypeDef *huart)
{
    UI_HAL_UART_RxCpltDispatch(huart);
}

__attribute__((weak)) void HAL_UART_ErrorCallback(UART_HandleTypeDef *huart)
{
    UI_HAL_UART_ErrorDispatch(huart);
}

__attribute__((weak)) void LPUART1_IRQHandler(void)
{
    UI_HAL_LPUART1_IrqDispatch();
}
#endif

#if (UI_OVERRIDE_HAL_GPIO_EXTI_CALLBACK == 1u)
void HAL_GPIO_EXTI_Callback(uint16_t GPIO_Pin)
{
    UI_GPIO_ExtiCallback(GPIO_Pin);
}
#endif /* UI_OVERRIDE_HAL_GPIO_EXTI_CALLBACK */
