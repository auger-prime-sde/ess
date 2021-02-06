ARDUINO_DIR = /opt/arduino-1.8.5
BOARD_TAG = mega
MCU = atmega2560
AVRDUDE_ARD_BAUDRATE = 115200
AVRDUDE_ARD_PROGRAMMER = wiring
ARDUINO_LIBS = 
MONITOR_PORT = /dev/ttyACM0
CXXFLAGS = -fdump-class-hierarchy
# DEBUG = 1
OPTIMIZATION_LEVEL = 2
ifdef DEVICE
CXXFLAGS += -DDEVICE=$(DEVICE)
endif

include /usr/share/arduino/Arduino.mk
