#include "ui_lpm.h"
#include "ui_conf.h"

/* Stop 진입 전 런타임 플래그 정리용 */
#include "ui_core.h"
#include "ui_gpio.h"
#include "ui_uart.h"
#include "ui_ble.h"
#include "ui_time.h"
#include "ui_radio.h"
#include "gw_storage.h"

#include "stm32_lpm.h"
#include "utilities_def.h" /* CFG_LPM_APPLI_Id */
#include "main.h"

#include "stm32wlxx_hal.h"
#include <stddef.h>

#if defined(HAL_SPI_MODULE_ENABLED)
#include "stm32wlxx_hal_spi.h"
#endif

#if defined(HAL_UART_MODULE_ENABLED)
#include "stm32wlxx_hal_uart.h"
#endif

#if defined(HAL_ADC_MODULE_ENABLED)
#include "stm32wlxx_hal_adc.h"
#endif
#include "ui_radio.h"

/* 주변장치 핸들(프로젝트 main.c에 존재) */
extern SPI_HandleTypeDef  hspi1;
extern UART_HandleTypeDef huart1;
extern UART_HandleTypeDef hlpuart1;
extern ADC_HandleTypeDef  hadc;

#if defined(DMA1_Channel2)
extern DMA_HandleTypeDef  hdma_usart1_tx;
#endif
extern void UI_Radio_EnterSleep(void);

static void prv_force_stop_pin_levels(void)
{
#if defined(W25Q128_CS_GPIO_Port) && defined(W25Q128_CS_Pin)
    HAL_GPIO_WritePin(W25Q128_CS_GPIO_Port, W25Q128_CS_Pin, GPIO_PIN_SET);
#endif
#if defined(CATM1_PWR_GPIO_Port) && defined(CATM1_PWR_Pin)
    HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_RESET);
#endif
#if defined(PWR_KEY_GPIO_Port) && defined(PWR_KEY_Pin)
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
#if defined(BT_EN_GPIO_Port) && defined(BT_EN_Pin)
    HAL_GPIO_WritePin(BT_EN_GPIO_Port, BT_EN_Pin, GPIO_PIN_RESET);
#endif
#if defined(LED0_GPIO_Port) && defined(LED0_Pin)
    HAL_GPIO_WritePin(LED0_GPIO_Port, LED0_Pin, GPIO_PIN_RESET);
#endif
#if defined(LED1_GPIO_Port) && defined(LED1_Pin)
    HAL_GPIO_WritePin(LED1_GPIO_Port, LED1_Pin, GPIO_PIN_RESET);
#endif
#if defined(RF_TXEN_GPIO_Port) && defined(RF_TXEN_Pin)
    HAL_GPIO_WritePin(RF_TXEN_GPIO_Port, RF_TXEN_Pin, GPIO_PIN_RESET);
#endif
#if defined(RF_RXEN_GPIO_Port) && defined(RF_RXEN_Pin)
    HAL_GPIO_WritePin(RF_RXEN_GPIO_Port, RF_RXEN_Pin, GPIO_PIN_RESET);
#endif
}

static void prv_disable_spi_clock(const SPI_HandleTypeDef *hspi)
{
#if defined(SPI1) && defined(__HAL_RCC_SPI1_CLK_DISABLE)
    if ((hspi != NULL) && (hspi->Instance == SPI1))
    {
        __HAL_RCC_SPI1_CLK_DISABLE();
        return;
    }
#endif
#if defined(SPI2) && defined(__HAL_RCC_SPI2_CLK_DISABLE)
    if ((hspi != NULL) && (hspi->Instance == SPI2))
    {
        __HAL_RCC_SPI2_CLK_DISABLE();
        return;
    }
#endif
#if defined(SPI3) && defined(__HAL_RCC_SPI3_CLK_DISABLE)
    if ((hspi != NULL) && (hspi->Instance == SPI3))
    {
        __HAL_RCC_SPI3_CLK_DISABLE();
        return;
    }
#endif
}

static void prv_disable_uart_clock(const UART_HandleTypeDef *huart)
{
#if defined(USART1) && defined(__HAL_RCC_USART1_CLK_DISABLE)
    if ((huart != NULL) && (huart->Instance == USART1))
    {
        __HAL_RCC_USART1_CLK_DISABLE();
        return;
    }
#endif
#if defined(USART2) && defined(__HAL_RCC_USART2_CLK_DISABLE)
    if ((huart != NULL) && (huart->Instance == USART2))
    {
        __HAL_RCC_USART2_CLK_DISABLE();
        return;
    }
#endif
#if defined(LPUART1) && defined(__HAL_RCC_LPUART1_CLK_DISABLE)
    if ((huart != NULL) && (huart->Instance == LPUART1))
    {
        __HAL_RCC_LPUART1_CLK_DISABLE();
        return;
    }
#endif
}

static void prv_disable_adc_clock(const ADC_HandleTypeDef *hadc_ptr)
{
#if defined(HAL_ADC_MODULE_ENABLED)
# if defined(ADC) && defined(__HAL_RCC_ADC_CLK_DISABLE)
    if ((hadc_ptr != NULL) && (hadc_ptr->Instance == ADC))
    {
        __HAL_RCC_ADC_CLK_DISABLE();
        return;
    }
# endif
# if defined(ADC1) && defined(__HAL_RCC_ADC_CLK_DISABLE)
    if ((hadc_ptr != NULL) && (hadc_ptr->Instance == ADC1))
    {
        __HAL_RCC_ADC_CLK_DISABLE();
        return;
    }
# endif
#endif
}

static volatile uint32_t s_stop_lock = 0;

void UI_LPM_Init(void)
{
    /* 요구사항: off-mode(standby) 비활성화 */
    UTIL_LPM_SetOffMode((1U << CFG_LPM_APPLI_Id), UTIL_LPM_DISABLE);
}

void UI_LPM_LockStop(void)
{
    s_stop_lock++;
    UTIL_LPM_SetStopMode((1U << CFG_LPM_APPLI_Id), UTIL_LPM_DISABLE);
}

void UI_LPM_UnlockStop(void)
{
    if (s_stop_lock > 0u)
    {
        s_stop_lock--;
    }

    if (s_stop_lock == 0u)
    {
        UTIL_LPM_SetStopMode((1U << CFG_LPM_APPLI_Id), UTIL_LPM_ENABLE);
    }
}

bool UI_LPM_IsStopLocked(void)
{
    return (s_stop_lock != 0u);
}

void UI_UART1_TxDma_DeInit(void)
{
#if defined(DMA1_Channel2)
    /*
     * 요구사항:
     *  - UART1 TX DMA는 사용하지 않음.
     *  - 불필요한 DMA 동작/전류 증가를 줄이기 위해 DeInit.
     */
    (void)HAL_DMA_DeInit(&hdma_usart1_tx);
#endif
}

void UI_LPM_BeforeStop_DeInitPeripherals(void)
{
    /*
     * 요구사항: stop 들어가기 전에 spi, uart1, lpuart1, adc deinit
     *
     * NOTE
     *  - RF 작업 종료 직후에는 GW 이벤트 경로에서 Radio.Sleep()으로 내리고,
     *    stop 직전에도 한 번 더 내려서 SubGHz가 깨어 남지 않도록 한다.
     */
    /* Reset 대비: Stop 진입 직전에 현재 시간을 Backup Register에 저장 */
    UI_Time_SaveToBackupNow();

    /* RF가 마지막 상태에 남아 있지 않도록 stop 직전 강제 sleep */
    UI_Radio_EnterSleep();



    /*
     * W25Q128은 평상시 LittleFS unmount에서 deep power-down으로 내리지만,
     * stop 직전에도 한 번 더 내려서 SPI DeInit 이후 flash가 standby 전류로
     * 남아 있지 않도록 한다.
     */
    GW_Storage_W25Q_PowerDown();

    /* 외부 부하가 남지 않도록 제어 핀을 저전력 상태로 고정 */
    prv_force_stop_pin_levels();

    /* SW 상태 정리: 다음 wake-up 이후 재진입 시 꼬임 방지 */
    UI_Core_ClearFlagsBeforeStop();
    UI_GPIO_ClearEvents();
    UI_UART_ResetRxBuffer();
    UI_BLE_ClearFlagsBeforeStop();
#if defined(HAL_ADC_MODULE_ENABLED)
    (void)HAL_ADC_DeInit(&hadc);
    prv_disable_adc_clock(&hadc);
#endif

#if defined(HAL_UART_MODULE_ENABLED)
    (void)HAL_UART_DeInit(&hlpuart1);
    prv_disable_uart_clock(&hlpuart1);

    (void)HAL_UART_DeInit(&huart1);
    prv_disable_uart_clock(&huart1);
#endif

#if defined(HAL_SPI_MODULE_ENABLED)
    (void)HAL_SPI_DeInit(&hspi1);
    prv_disable_spi_clock(&hspi1);
#endif
}

void UI_LPM_AfterStop_ReInitPeripherals(void)
{
    /*
     * 저전력(배터리) 정책:
     *  - Wake-up 직후 주변장치를 "무조건" ReInit 하지 않습니다.
     *  - 각 모듈이 필요할 때만 Init(Ensure) 하세요.
     *
     * 예)
     *  - BLE ON 시점: UI_UART_EnsureStarted() 호출
     *  - 센서 측정 시점: MX_ADC_Init(), MX_SPI1_Init() 등 필요 부분만 호출
     */
}

void UI_LPM_EnterStopNow(void)
{
    /* 동작 중이면 stop 진입 금지 */
    if (UI_LPM_IsStopLocked())
    {
        return;
    }

    UI_LPM_BeforeStop_DeInitPeripherals();
    HAL_SuspendTick();
    UTIL_LPM_EnterLowPower();
    HAL_ResumeTick();

    /* wakeup 후: 기본은 아무 것도 하지 않음(최소 전류) */
    UI_LPM_AfterStop_ReInitPeripherals();
}
