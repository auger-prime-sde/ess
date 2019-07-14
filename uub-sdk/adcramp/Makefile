# Makefile for UUB programs
#
# Petr Tobiska <tobiska@fzu.cz>
# 2019-07-14

# project specific configuration (src, elf etc.)
include project.mk

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
all: $(ELF) $(ELFSIZE)

# dependency rules
ifneq ($(MAKECMDGOALS),clean)
ifneq ($(strip $(C_DEPS)),)
-include $(C_DEPS)
endif
endif

$(OBJDIR)/%.o: %.c | $(OBJDIR)
	$(CC) $(CFLAGS)  -o $@ $<

$(OBJDIR):
	mkdir $@

$(ELF): $(OBJS)
	arm-xilinx-linux-gnueabi-gcc -o $@ $< $(LIBS)

$(ELFSIZE): $(ELF)
	arm-xilinx-linux-gnueabi-size $< | tee $@

clean:
	rm -f $(ELF) $(ELFSIZE) $(OBJS) $(C_DEPS)

.PHONY: all clean
