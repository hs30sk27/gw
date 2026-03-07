################################################################################
# Automatically-generated file. Do not edit!
# Toolchain: GNU Tools for STM32 (13.3.rel1)
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../Core/gui/gw_app.c \
../Core/gui/gw_ble_report.c \
../Core/gui/gw_catm1.c \
../Core/gui/gw_file_cmd.c \
../Core/gui/gw_sensors.c \
../Core/gui/gw_storage.c \
../Core/gui/gw_w25q128.c \
../Core/gui/ui_ble.c \
../Core/gui/ui_cmd.c \
../Core/gui/ui_config.c \
../Core/gui/ui_core.c \
../Core/gui/ui_crc16.c \
../Core/gui/ui_fault.c \
../Core/gui/ui_gpio.c \
../Core/gui/ui_hal_callbacks.c \
../Core/gui/ui_lpm.c \
../Core/gui/ui_packets.c \
../Core/gui/ui_radio.c \
../Core/gui/ui_rf_plan_kr920.c \
../Core/gui/ui_ringbuf.c \
../Core/gui/ui_time.c \
../Core/gui/ui_uart.c 

OBJS += \
./Core/gui/gw_app.o \
./Core/gui/gw_ble_report.o \
./Core/gui/gw_catm1.o \
./Core/gui/gw_file_cmd.o \
./Core/gui/gw_sensors.o \
./Core/gui/gw_storage.o \
./Core/gui/gw_w25q128.o \
./Core/gui/ui_ble.o \
./Core/gui/ui_cmd.o \
./Core/gui/ui_config.o \
./Core/gui/ui_core.o \
./Core/gui/ui_crc16.o \
./Core/gui/ui_fault.o \
./Core/gui/ui_gpio.o \
./Core/gui/ui_hal_callbacks.o \
./Core/gui/ui_lpm.o \
./Core/gui/ui_packets.o \
./Core/gui/ui_radio.o \
./Core/gui/ui_rf_plan_kr920.o \
./Core/gui/ui_ringbuf.o \
./Core/gui/ui_time.o \
./Core/gui/ui_uart.o 

C_DEPS += \
./Core/gui/gw_app.d \
./Core/gui/gw_ble_report.d \
./Core/gui/gw_catm1.d \
./Core/gui/gw_file_cmd.d \
./Core/gui/gw_sensors.d \
./Core/gui/gw_storage.d \
./Core/gui/gw_w25q128.d \
./Core/gui/ui_ble.d \
./Core/gui/ui_cmd.d \
./Core/gui/ui_config.d \
./Core/gui/ui_core.d \
./Core/gui/ui_crc16.d \
./Core/gui/ui_fault.d \
./Core/gui/ui_gpio.d \
./Core/gui/ui_hal_callbacks.d \
./Core/gui/ui_lpm.d \
./Core/gui/ui_packets.d \
./Core/gui/ui_radio.d \
./Core/gui/ui_rf_plan_kr920.d \
./Core/gui/ui_ringbuf.d \
./Core/gui/ui_time.d \
./Core/gui/ui_uart.d 


# Each subdirectory must supply rules for building sources it contributes
Core/gui/%.o Core/gui/%.su Core/gui/%.cyclo: ../Core/gui/%.c Core/gui/subdir.mk
	arm-none-eabi-gcc "$<" -mcpu=cortex-m4 -std=gnu11 -g3 -DDEBUG -DCORE_CM4 -DUSE_HAL_DRIVER -DSTM32WLE5xx -c -I../Core/Inc -I../SubGHz_Phy/App -I../SubGHz_Phy/Target -I../Drivers/STM32WLxx_HAL_Driver/Inc -I../Drivers/STM32WLxx_HAL_Driver/Inc/Legacy -I../Utilities/trace/adv_trace -I../Utilities/misc -I../Utilities/sequencer -I../Utilities/timer -I../Utilities/lpm/tiny_lpm -I../Drivers/CMSIS/Device/ST/STM32WLxx/Include -I../Middlewares/Third_Party/SubGHz_Phy -I../Middlewares/Third_Party/SubGHz_Phy/stm32_radio_driver -I../Drivers/CMSIS/Include -I"D:/work26/gw/Core/lfs" -I"D:/work26/gw/Core/gui" -Oz -ffunction-sections -fdata-sections -Wall -fstack-usage -fcyclomatic-complexity -MMD -MP -MF"$(@:%.o=%.d)" -MT"$@" --specs=nano.specs -mfloat-abi=soft -mthumb -o "$@"

clean: clean-Core-2f-gui

clean-Core-2f-gui:
	-$(RM) ./Core/gui/gw_app.cyclo ./Core/gui/gw_app.d ./Core/gui/gw_app.o ./Core/gui/gw_app.su ./Core/gui/gw_ble_report.cyclo ./Core/gui/gw_ble_report.d ./Core/gui/gw_ble_report.o ./Core/gui/gw_ble_report.su ./Core/gui/gw_catm1.cyclo ./Core/gui/gw_catm1.d ./Core/gui/gw_catm1.o ./Core/gui/gw_catm1.su ./Core/gui/gw_file_cmd.cyclo ./Core/gui/gw_file_cmd.d ./Core/gui/gw_file_cmd.o ./Core/gui/gw_file_cmd.su ./Core/gui/gw_sensors.cyclo ./Core/gui/gw_sensors.d ./Core/gui/gw_sensors.o ./Core/gui/gw_sensors.su ./Core/gui/gw_storage.cyclo ./Core/gui/gw_storage.d ./Core/gui/gw_storage.o ./Core/gui/gw_storage.su ./Core/gui/gw_w25q128.cyclo ./Core/gui/gw_w25q128.d ./Core/gui/gw_w25q128.o ./Core/gui/gw_w25q128.su ./Core/gui/ui_ble.cyclo ./Core/gui/ui_ble.d ./Core/gui/ui_ble.o ./Core/gui/ui_ble.su ./Core/gui/ui_cmd.cyclo ./Core/gui/ui_cmd.d ./Core/gui/ui_cmd.o ./Core/gui/ui_cmd.su ./Core/gui/ui_config.cyclo ./Core/gui/ui_config.d ./Core/gui/ui_config.o ./Core/gui/ui_config.su ./Core/gui/ui_core.cyclo ./Core/gui/ui_core.d ./Core/gui/ui_core.o ./Core/gui/ui_core.su ./Core/gui/ui_crc16.cyclo ./Core/gui/ui_crc16.d ./Core/gui/ui_crc16.o ./Core/gui/ui_crc16.su ./Core/gui/ui_fault.cyclo ./Core/gui/ui_fault.d ./Core/gui/ui_fault.o ./Core/gui/ui_fault.su ./Core/gui/ui_gpio.cyclo ./Core/gui/ui_gpio.d ./Core/gui/ui_gpio.o ./Core/gui/ui_gpio.su ./Core/gui/ui_hal_callbacks.cyclo ./Core/gui/ui_hal_callbacks.d ./Core/gui/ui_hal_callbacks.o ./Core/gui/ui_hal_callbacks.su ./Core/gui/ui_lpm.cyclo ./Core/gui/ui_lpm.d ./Core/gui/ui_lpm.o ./Core/gui/ui_lpm.su ./Core/gui/ui_packets.cyclo ./Core/gui/ui_packets.d ./Core/gui/ui_packets.o ./Core/gui/ui_packets.su ./Core/gui/ui_radio.cyclo ./Core/gui/ui_radio.d ./Core/gui/ui_radio.o ./Core/gui/ui_radio.su ./Core/gui/ui_rf_plan_kr920.cyclo ./Core/gui/ui_rf_plan_kr920.d ./Core/gui/ui_rf_plan_kr920.o ./Core/gui/ui_rf_plan_kr920.su ./Core/gui/ui_ringbuf.cyclo ./Core/gui/ui_ringbuf.d ./Core/gui/ui_ringbuf.o ./Core/gui/ui_ringbuf.su ./Core/gui/ui_time.cyclo ./Core/gui/ui_time.d ./Core/gui/ui_time.o ./Core/gui/ui_time.su ./Core/gui/ui_uart.cyclo ./Core/gui/ui_uart.d ./Core/gui/ui_uart.o ./Core/gui/ui_uart.su

.PHONY: clean-Core-2f-gui

