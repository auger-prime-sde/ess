# Makefile for UUB programs
#
# Petr Tobiska <tobiska@fzu.cz>
# 2019-07-14

# project specific configuration (src, elf etc.)
include project.mk


ELFS = $(patsubst %,build/netscope-%.elf,$(FPGAVERSIONS))
SRCS := netscope.c

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
all: $(ELFS)

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

build/netscope-%.elf : $(SRCS) | $(OBJDIR)
	$(CC) $(CFLAGS) -I$* -I. -o $(OBJDIR)/netscope.o netscope.c
	$(CC) -o $@ $(OBJDIR)/netscope.o $(LIBS)
	rm $(OBJDIR)/netscope.o

$(ELFSIZE): $(ELF)
	@echo arm-xilinx-linux-gnueabi-size $< | tee $@

clean:
	rm -f $(ELFS) $(ELFSIZE) $(OBJS) $(C_DEPS)

.PHONY: all clean
