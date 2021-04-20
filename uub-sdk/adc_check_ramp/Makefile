# Makefile for adc_check_ramp
# for more FPGA versions
#
# Petr Tobiska <tobiska@fzu.cz>
# 2021-04-19

# project specific configuration (src, elf etc.)
include project.mk


ELFS = $(patsubst %,build/adc_check_ramp-%.elf,$(FPGAVERSIONS))
SRCS := adc_check_ramp.c
TARNAME := adc_check_ramp.tgz
TAR_OPTS := --owner=root:0 --group=root:0 --mode='a+rx'
TAR_OPTS += --xform=s,,sbin/,  --xform=s,build/,,

OBJDIR := build
OBJS := $(patsubst %.c,$(OBJDIR)/%.o,$(SRCS))
C_DEPS := $(patsubst %.o,%.d,$(OBJS))

ifeq ($(DEBUG),y)
DEBUG_FLAGS := -O0 -g3
else
DEBUG_FLAGS := -O2
endif

LIBS := -lrt
CC := arm-xilinx-linux-gnueabi-gcc
CFLAGS = -Wall $(DEBUG_FLAGS) -c -fmessage-length=0
CFLAGS += -MT$@ -MMD -MP -MF$(@:%.o=%.d) -MT$(@:%.o=%.d)
ELFSIZE = $(ELF:%=%.size)

# All Target
all: $(ELFS) patch

# dependency rules
ifneq ($(MAKECMDGOALS),clean)
ifneq ($(strip $(C_DEPS)),)
-include $(C_DEPS)
endif
endif

# $(OBJDIR)/%.o: %.c | $(OBJDIR)
# 	$(CC) $(CFLAGS) -I$(FPGAVER) -o $@ $<

$(OBJDIR):
	mkdir $@

build/adc_check_ramp-%.elf : $(SRCS) | $(OBJDIR)
	$(CC) $(CFLAGS) -I$* -I. -o $(OBJDIR)/adc_check_ramp.o adc_check_ramp.c
	$(CC) -o $@ $(OBJDIR)/adc_check_ramp.o $(LIBS)
	rm $(OBJDIR)/adc_check_ramp.o

patch: $(TARNAME)

$(TARNAME): $(ELFS) test-adc
	tar czf $(TARNAME) $(TAR_OPTS) test-adc $(ELFS)

$(ELFSIZE): $(ELFS)
	@echo arm-xilinx-linux-gnueabi-size $< | tee $@

clean:
	rm -f $(ELFS) $(ELFSIZE) $(OBJS) $(C_DEPS)

.PHONY: all clean
