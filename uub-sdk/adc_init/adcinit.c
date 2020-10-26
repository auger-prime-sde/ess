/*
 * adcinit.c
 * Initialization of ADC AD9268 via SPI
 *
 * Petr Tobiska <tobiska@fzu.cz>, 2020-10-16
 * based on yadc by Yann Aubert and uub_init by Roberto Assiro
 */

#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <getopt.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <linux/types.h>
#include <linux/spi/spidev.h>

#define NADC 5          // Number of ADCs

static void pabort(const char *s) {
  fputs(s, stderr);
  fputc('\n', stderr);
  exit(1);
}

int openspidev(int adc) {
  int fd;
  char filename[20];

  snprintf(filename, 19, "/dev/spidev32766.%d", adc);
  fd = open(filename, O_RDWR);
  if (fd < 0)
    pabort("can't open device");
  return fd;
}

void spi_init(int adcfd) {
  static uint8_t mode = 0;
  static uint8_t bits = 8;
  static uint32_t speed = 5000000;
  
  int ret;

  // spi mode
  ret = ioctl(adcfd, SPI_IOC_WR_MODE, &mode);
  if (ret == -1)
    pabort("can't set spi mode");
  
  ret = ioctl(adcfd, SPI_IOC_RD_MODE, &mode);
  if (ret == -1)
    pabort("can't get spi mode");
  
  // bits per word
  ret = ioctl(adcfd, SPI_IOC_WR_BITS_PER_WORD, &bits);
  if (ret == -1)
    pabort("can't set bits per word");

  ret = ioctl(adcfd, SPI_IOC_RD_BITS_PER_WORD, &bits);
  if (ret == -1)
    pabort("can't get bits per word");
  
  // max speed hz
  ret = ioctl(adcfd, SPI_IOC_WR_MAX_SPEED_HZ, &speed);
  if (ret == -1)
    pabort("can't set max speed hz");

  ret = ioctl(adcfd, SPI_IOC_RD_MAX_SPEED_HZ, &speed);
  if (ret == -1)
    pabort("can't get max speed hz");
}

int adc_read(int adcfd, int address) {
  struct spi_ioc_transfer xfer[2];
  unsigned char buf[32];
  int status;

  memset(xfer, 0, sizeof xfer);
  memset(buf, 0, sizeof buf);

  /* Read register 1 */
  buf[0] = 0x80 | ((address>>8) & 0xff);
  buf[1] = address & 0xff;

  xfer[0].tx_buf = (unsigned long) buf;
  xfer[0].len = 2;

  xfer[1].rx_buf = (unsigned long) buf;
  xfer[1].len = 1;

  status = ioctl(adcfd, SPI_IOC_MESSAGE(2), xfer);
  if (status < 0) {
    pabort("SPI_IOC_MESSAGE");
  }

  // printf("Address value : %03x %02x\n", address, buf[0]);
  return ((int) buf[0]);
}

int adc_write(int adcfd, int address, int cmd) {
  char cmdstr[3];

  cmdstr[0] = (address>>8) & 0xff;
  cmdstr[1] = address & 0xff;
  cmdstr[2] = cmd;

  if (write(adcfd, cmdstr, sizeof(cmdstr)) != sizeof(cmdstr)) {
    pabort("adc_write");
  }
  return 1;
}

/* write value into register and check that it is there */
void reg_set_and_check(int adcfd, int addr, unsigned char val) {
  int result;
  adc_write(adcfd, addr, val);
  result = adc_read(adcfd, addr);
  if( result != val)
    fprintf(stderr, "[%04x]%02x:%02x,", addr, val, result);
}

int main(int argc, char **argv) {
  int adc, fd;

  fprintf(stderr, "Initialization of ADCs on SPI-0: ");
  for( adc = 0; adc < NADC; adc++ ) {
    fd = openspidev(adc);
    spi_init(fd);

    fprintf(stderr, "%d", adc);
    // select both channels A and B
    reg_set_and_check(fd, 0x0005, 0x03);
    // digital reset operation (AD9268.pdf, p.41)
    reg_set_and_check(fd, 0x0008, 0x03);
    reg_set_and_check(fd, 0x0008, 0x00);
    // SPI config: Soft reset, read returns 0x18
    adc_write(fd, 0x0000, 0x3c);
    // output mode LVDS inverted
    reg_set_and_check(fd, 0x0014, 0xa4);
    // VREF select:  2.0V p-p
    reg_set_and_check(fd, 0x0018, 0x04);
    // test mode off (normal mode)
    reg_set_and_check(fd, 0x000d, 0x00);

    close(fd);
    usleep(100);
  }
  fputc('\n', stderr);
  return 0;
}
