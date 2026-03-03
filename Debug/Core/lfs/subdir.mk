################################################################################
# Automatically-generated file. Do not edit!
# Toolchain: GNU Tools for STM32 (13.3.rel1)
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../Core/lfs/lfs.c \
../Core/lfs/lfs_util.c 

OBJS += \
./Core/lfs/lfs.o \
./Core/lfs/lfs_util.o 

C_DEPS += \
./Core/lfs/lfs.d \
./Core/lfs/lfs_util.d 


# Each subdirectory must supply rules for building sources it contributes
Core/lfs/%.o Core/lfs/%.su Core/lfs/%.cyclo: ../Core/lfs/%.c Core/lfs/subdir.mk
	arm-none-eabi-gcc "$<" -mcpu=cortex-m4 -std=gnu11 -g3 -DDEBUG -DCORE_CM4 -DUSE_HAL_DRIVER -DSTM32WLE5xx -c -I../Core/Inc -I../SubGHz_Phy/App -I../SubGHz_Phy/Target -I../Drivers/STM32WLxx_HAL_Driver/Inc -I../Drivers/STM32WLxx_HAL_Driver/Inc/Legacy -I../Utilities/trace/adv_trace -I../Utilities/misc -I../Utilities/sequencer -I../Utilities/timer -I../Utilities/lpm/tiny_lpm -I../Drivers/CMSIS/Device/ST/STM32WLxx/Include -I../Middlewares/Third_Party/SubGHz_Phy -I../Middlewares/Third_Party/SubGHz_Phy/stm32_radio_driver -I../Drivers/CMSIS/Include -I"D:/work26/gw/Core/lfs" -I"D:/work26/gw/Core/gui" -Oz -ffunction-sections -fdata-sections -Wall -fstack-usage -fcyclomatic-complexity -MMD -MP -MF"$(@:%.o=%.d)" -MT"$@" --specs=nano.specs -mfloat-abi=soft -mthumb -o "$@"

clean: clean-Core-2f-lfs

clean-Core-2f-lfs:
	-$(RM) ./Core/lfs/lfs.cyclo ./Core/lfs/lfs.d ./Core/lfs/lfs.o ./Core/lfs/lfs.su ./Core/lfs/lfs_util.cyclo ./Core/lfs/lfs_util.d ./Core/lfs/lfs_util.o ./Core/lfs/lfs_util.su

.PHONY: clean-Core-2f-lfs

