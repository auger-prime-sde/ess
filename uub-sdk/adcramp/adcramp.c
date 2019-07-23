/*
 * adcramp.c
 * Switch on/off RAMP in ADC, controlled by UDP packets
 *
 * Petr Tobiska <tobiska@fzu.cz>, 2018-12-03
 * based on yadc by Yann Aubert
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

#define CTRLPORT 8886   // The port for cmd and resp
#define MSGLEN 18       // Length of UDP data (to avoid padding)
#define NADC 5          // Number of ADCs
#define WAITTIME   1000000L  // timeout in select, us

#define MASK_CMD   0x40
#define MASK_ON    0x20
#define MASK_ADC   0x1C
#define SH_ADC        2
#define MASK_CHS   0x03
#define CMD_QUIT   0x21     // quit
#define RESP_BASE  0x20
#define RESP_ERR   0x10

#define ADDR_CHS   0x05
#define ADDR_TEST  0x0D
#define RAMPON     0x0F
#define RAMPOFF    0x00

/* global variables */
char buf[MSGLEN];
struct sockaddr src_addr;
socklen_t addrlen;


static void pabort(const char *s) {
  FILE *ferr;

  if((ferr = fopen("adcramp.log", "a")) != NULL) {
    fputs(s, ferr);
    fputc('\n', ferr);
    fclose(ferr); }
  abort();
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
    perror("SPI_IOC_MESSAGE");
    return (-1);
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


/*
 * control socket: read cmds, write responses
 */
int opencontrolsock() {
  /* struct timeval read_timeout; */
  struct sockaddr_in sa;
  int sock;

  if ((sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1)
    pabort("creating socket failed");

  /* read_timeout.tv_sec = 0; */
  /* read_timeout.tv_usec = 500; */
  /* setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, */
  /* 		     &read_timeout, sizeof read_timeout); */

  memset((char *) &sa, 0, sizeof(struct sockaddr_in));
  sa.sin_family = AF_INET;
  sa.sin_port = htons(CTRLPORT);
  sa.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(sock, (struct sockaddr*)&sa, sizeof(struct sockaddr_in)) < 0)
    pabort("bind failed");

  return sock;
}

int main(int argc, char **argv) {
  int sock, adcfd[NADC];
  int adc, fd, i, len;
  char c, resp;
  fd_set rset;
  struct timeval tv;

  //  fprintf(stderr, "main begin\n");
  sock = opencontrolsock();
  //  fprintf(stderr, "sock open\n");
  for( adc = 0; adc < NADC; adc++ ) {
    fd = openspidev(adc);
    spi_init(fd);
    adcfd[adc] = fd; }

  /* main loop */
  //  fprintf(stderr, "entering main loop\n");
  c = 0;
  addrlen = sizeof(src_addr);
  do {
    FD_ZERO(&rset);
    FD_SET(sock, &rset);
    tv.tv_sec = 0;        // timeout for select: 100ms
    tv.tv_usec = WAITTIME;
  
    if((len = select(sock+1, &rset, NULL, NULL, &tv)) == 0) {
      /* fprintf(stderr, "timeout\n"); */
      continue;   // timeout
    }
    
    len = recvfrom(sock, (void *)buf, MSGLEN, 0, &src_addr, &addrlen);
    if(len != MSGLEN)
      continue;   // incomplete packet ???
    resp = RESP_BASE;
    buf[MSGLEN-1] = '\0';  // sentinel
    for( i = 0; buf[i] != '\0'; i++ ) {
      if(( c = buf[i] ) == CMD_QUIT) {
	resp ++;
	break; }
      else if( c & MASK_CMD ) {
	if((adc = (c & MASK_ADC) >> SH_ADC) >= NADC) {
	  resp |= RESP_ERR;
	  break; }
	adc = adcfd[adc];
	adc_write(adc, ADDR_CHS, c & MASK_CHS);
	adc_write(adc, ADDR_TEST, (c & MASK_ON) ? RAMPON : RAMPOFF);
	resp ++; }
      else {  // invalid cmd
	resp |= RESP_ERR;
	break; }
    } /* end for */
    buf[0] = resp;
    sendto(sock, buf, MSGLEN, MSG_DONTWAIT, &src_addr, addrlen);
  } while( c != CMD_QUIT );

  //  fprintf(stderr, "left main loop\n");
  close(sock);
  for( adc = 0; adc < NADC; adc ++)
    close(adcfd[adc]);
  
  return 0;
}
