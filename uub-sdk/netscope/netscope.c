/* UUB simple signals acquisition utility, dump it on UDP
   Petr Tobiska
   based on scope.c by R. Assiro

   version 2019-07-14
*/

#define TRIG_EXT
//#define TRIG_SB
//#define TRIG_SB_MULTI
//#define TRIG_COMPAT_SB

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/mman.h>
#include <ctype.h>
#include <termios.h>

#include "sde_trigger_defs.h"
#include "time_tagging.h"
#include "test_control_defs.h"
#ifndef TEST_CONTROL_BASE
  #define TEST_CONTROL_BASE XPAR_TEST_CONTROL_BLOCK_TEST_CONTROL_0_S00_AXI_BASEADDR
#endif

#define SIG_WAKEUP SIGRTMIN+14

#define SERVER "192.168.31.254"
#define DATAPORT 8888   //The port on which to send data
#define CTRLPORT 8887   //The port on which to send data
#define WAITTIME 10000   /* wait time [ns] between checking data available */
#define PACKETSIZE 1400  /* plus frag header */

/* from shwr_evt_defs.h */
#define SHWR_RAW_NCH_MAX 5
#define SHWR_NCH_MAX (2*SHWR_RAW_NCH_MAX)
#define SHWR_NSAMPLES 2048
static unsigned int shwr_addr[SHWR_RAW_NCH_MAX] = {
  TRIGGER_MEMORY_SHWR0_BASE,
  TRIGGER_MEMORY_SHWR1_BASE,
  TRIGGER_MEMORY_SHWR2_BASE,
  TRIGGER_MEMORY_SHWR3_BASE,
  TRIGGER_MEMORY_SHWR4_BASE
};

struct read_evt_global {
  uint32_t id_counter;
  uint32_t volatile *shwr_pt[SHWR_RAW_NCH_MAX];
  int shwr_mem_size;

  uint32_t volatile *regs;
  uint32_t volatile *tt_regs;
  uint32_t volatile *tstctl_regs;
  int regs_size;

  sigset_t sigset; /*used to wake the process periodically */
};

struct shwr_header {
  uint32_t id;
  uint32_t shwr_buf_status, shwr_buf_start, shwr_buf_trig_id;
  uint32_t ttag_shwr_seconds, ttag_shwr_nanosec;
  uint32_t rd;
};

struct frag_header {
  uint32_t id;
  uint16_t start;
  uint16_t end;
};

#define PAGESIZE (sysconf(_SC_PAGESIZE))
#define DATASIZE (sizeof(uint32_t) * SHWR_NSAMPLES * SHWR_RAW_NCH_MAX)
#define WBUFSIZE (sizeof(struct frag_header) + DATASIZE)

/* global variables */
static struct read_evt_global gl;
static struct shwr_header sh;
uint8_t workbuf[WBUFSIZE];
#define databuf (workbuf + sizeof(struct frag_header))
#define endbuf (workbuf + WBUFSIZE)

/* functions */

/*
 * open socket for sending data
 * construct server address in sa
 */
int opensock(struct sockaddr_in* sa) {
  int sock;
 
  if (( sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1) {
    fprintf(stderr, "creating socket failed\n");
    exit(1); }
    
  memset((char *) sa, 0, sizeof(struct sockaddr_in));
  sa->sin_family = AF_INET;
  sa->sin_port = htons(DATAPORT);

  if (inet_aton(SERVER, &(sa->sin_addr)) == 0) {
    fprintf(stderr, "inet_aton failed");
    exit(1); }

  return(sock);
}

/*
 * control socket: wait for any packet to stop 
 */
int opencontrolsock() {
  struct timeval read_timeout;
  struct sockaddr_in sa;
  int sock;

  if ((sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1) {
    fprintf(stderr, "creating socket failed\n");
    exit(1); }

  read_timeout.tv_sec = 0;
  read_timeout.tv_usec = 10;
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,
		     &read_timeout, sizeof read_timeout);

  memset((char *) &sa, 0, sizeof(struct sockaddr_in));
  sa.sin_family = AF_INET;
  sa.sin_port = htons(CTRLPORT);
  sa.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(sock, (struct sockaddr*)&sa, sizeof(struct sockaddr_in)) < 0) {
	fprintf(stderr, "bind failed\n");
    exit(1); }

  return sock;
}


/*
 * send data: shwr_header and databuf in pieces
 */
void senddata(int sock, struct sockaddr_in* sa) {
  unsigned psize;
  uint8_t * start;
  uint8_t * end;
  uint32_t id = sh.id;
  struct frag_header *fh;
  
  /* send header */
  psize = sizeof(struct shwr_header);
  sh.id |= 0x80000000;
  if(sendto(sock, (uint8_t *) &sh, psize, 0,
	    (struct sockaddr*)sa, sizeof(struct sockaddr_in)) != psize) {
    fprintf(stderr, "senddata header failed\n");
    exit(1); }

  for(start = end = workbuf; end < endbuf;
      start = end - sizeof(struct frag_header)) {
    if ((end = start + PACKETSIZE) > endbuf)
      end = endbuf;

    fh = (struct frag_header *)start;
    fh->id = id;
    fh->start = start - workbuf;
    fh->end = end - databuf;

    psize = end - start;
    if (sendto(sock, start, psize, 0,
	       (struct sockaddr*)sa, sizeof(struct sockaddr_in)) != psize) {
      fprintf(stderr, "senddata failed");
      exit(1); }
  }
}

/*
 * check if there is an incoming UDP datagram
 */
#define BUFSIZE 1
int controlrecv(int sock){
  /* struct sockaddr src_addr; */
  /* socklen_t addrlen; */
  char buf[BUFSIZE];
  ssize_t msglen;

  msglen = recvfrom(sock, (void *)buf, BUFSIZE, MSG_DONTWAIT, NULL, NULL);
  /* msglen = recvfrom(sock, (void *)buf, BUFSIZE, MSG_DONTWAIT, */
  /* 		  &src_addr, &addrlen); */
  return msglen;
}

/*
 *  round up <n> to multiple of <multiple>
 */
unsigned roundup(unsigned n, unsigned multiple) {
  return ((n + multiple-1) / multiple) * multiple;
}

/*
 * mmap regs and shwr_pt
 */
void read_evt_init() {
  int i, fd, size;
  void * pt;
  struct sigevent sev;
  timer_t t_alarm;
  struct itimerspec ts;

  if((fd = open("/dev/mem",O_RDWR)) < 0 ) {
    fprintf(stderr, "Error opening /dev/mem\n");
    exit(1); }

  size = roundup(256*sizeof(uint32_t), PAGESIZE);
  gl.regs_size = size;
  pt = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
	    SDE_TRIGGER_BASE);
  if(pt == MAP_FAILED) {
    fprintf(stderr, "Error mapping regs\n");
    exit(1); }
  gl.regs = (uint32_t *)pt;

  pt = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
	    TIME_TAGGING_BASE);
  if(pt == MAP_FAILED) {
    fprintf(stderr, "Error mapping tt_regs\n");
    exit(1); }
  gl.tt_regs = (uint32_t *)pt;

  pt = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
	    TEST_CONTROL_BASE);
  if(pt == MAP_FAILED) {
    fprintf(stderr, "Error mapping tstctl_regs\n");
    exit(1); }
  gl.tstctl_regs = (uint32_t *)pt;

  size = roundup(SHWR_MEM_DEPTH * SHWR_MEM_NBUF, PAGESIZE);
  gl.shwr_mem_size = size;
  for( i = 0; i < SHWR_RAW_NCH_MAX; i++ ) {
    pt = mmap(NULL,size, PROT_READ, MAP_SHARED, fd, shwr_addr[i]);
    if(pt == MAP_FAILED) {
      fprintf(stderr, "Error mapping shower buf %d\n", i);
      exit(1); }
    gl.shwr_pt[i] = (uint32_t *) pt;
  }
  close(fd);

  /*setting periodical process wakeup to check if there are event.
    It is ugly, but for now, it would work in this whay, until
    we figure a what to implement interruptions through the kernel */

  /*signal of alarm handler - it is going to be just blocked
    to be used with sigtimedwait system call.
  */
  if(sigemptyset(&gl.sigset) != 0){
    fprintf(stderr, "error while trying to set signals ... 1 \n");
    exit(1);
  }
  if(sigaddset(&gl.sigset, SIG_WAKEUP) != 0){
    fprintf(stderr, "error while trying to set signals ... 2\n");
    exit(1);
  }
  if(sigprocmask(SIG_BLOCK, &gl.sigset, NULL) != 0){
    fprintf(stderr, "error while trying to set signals ... 3 \n");
    exit(1);
  }

  /* periodical signal generation */
  sev.sigev_notify = SIGEV_SIGNAL;
  sev.sigev_signo = SIG_WAKEUP;
  if(timer_create(CLOCK_MONOTONIC, &sev, &t_alarm) != 0){
    fprintf(stderr, "timer creation error\n");
    exit(1);
  }

  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = WAITTIME;
  ts.it_value.tv_sec = 0;
  ts.it_value.tv_nsec = WAITTIME;  /*the next interruption */
  if(timer_settime(t_alarm, 0, &ts, NULL) != 0){
    exit(1);
  }

  gl.id_counter=0;
}

void read_evt_end() {
  int i;

  if(gl.regs != NULL)
    munmap((void *)gl.regs, gl.regs_size);

  if(gl.tt_regs != NULL)
    munmap((void *)gl.tt_regs, gl.regs_size);

  if(gl.tstctl_regs != NULL)
      munmap((void *)gl.tstctl_regs, gl.regs_size);

  for( i=0; i < SHWR_RAW_NCH_MAX; i++ ) {
    if(gl.shwr_pt[i] != NULL)
      munmap((void *)gl.shwr_pt[i], gl.shwr_mem_size); }
}

/*
 * read out FADC to buf and fill shwr_header
 */
int read_evt_read(struct shwr_header* sh, uint8_t *buf) {
  uint32_t volatile *st;
  void *pt_aux;
  uint32_t aux;
  uint32_t *fadc;
  int rd, sig, i;
  int offset;

  fadc = (uint32_t *)buf;

  st = &(gl.regs[SHWR_BUF_STATUS_ADDR]);

  /*wait for the periodical signal and check if there is a
    event trigger
  */
  aux = SHWR_BUF_NFULL_MASK << SHWR_BUF_NFULL_SHIFT;
  sig = SIG_WAKEUP;
  while( ((*st) & aux) == 0 && sig == SIG_WAKEUP)
    sig = sigwaitinfo(&gl.sigset, NULL);

  if(sig == SIG_WAKEUP){
    rd = (((*st) >> SHWR_BUF_RNUM_SHIFT) & SHWR_BUF_RNUM_MASK);
    offset = rd * SHWR_NSAMPLES;
    for(i = 0; i < SHWR_RAW_NCH_MAX; i++){
      pt_aux = (void *)(gl.shwr_pt[i] + offset);
      memcpy(fadc, pt_aux, sizeof(uint32_t)*SHWR_NSAMPLES);
      fadc += SHWR_NSAMPLES;
    }
    sh->id = gl.id_counter;
    sh->shwr_buf_status   = gl.regs[SHWR_BUF_STATUS_ADDR];
    sh->shwr_buf_start    = gl.regs[SHWR_BUF_START_ADDR];
    sh->shwr_buf_trig_id  = gl.regs[SHWR_BUF_TRIG_ID_ADDR];
    sh->ttag_shwr_seconds = gl.tt_regs[TTAG_SHWR_SECONDS_ADDR];
    sh->ttag_shwr_nanosec = gl.tt_regs[TTAG_SHWR_NANOSEC_ADDR];
    sh->rd = rd;
    /* release buffer */
    gl.regs[SHWR_BUF_CONTROL_ADDR] = rd;
    gl.id_counter++;
    return(0);
  }
  return(1);
}
  
int main(int argc, char ** argv) {
  struct sockaddr_in sa;
  int datasock, controlsock;

  // prepare UDP
  datasock = opensock(&sa);
  controlsock = opencontrolsock();
  
  read_evt_init();
#ifdef TRIG_EXT
  // set trigger to external
  gl.regs[SHWR_BUF_TRIG_MASK_ADDR] = COMPATIBILITY_SHWR_BUF_TRIG_EXT;
#endif
#ifdef TRIG_SB
  // set trigger to Full bandwidth single bin
  gl.regs[SHWR_BUF_TRIG_MASK_ADDR] = SHWR_BUF_TRIG_SB;
  gl.regs[SB_TRIG_THR0_ADDR] = 1000;
  gl.regs[SB_TRIG_THR1_ADDR] = 1000;
  gl.regs[SB_TRIG_THR2_ADDR] = 1000;
  gl.regs[SB_TRIG_SSD_ADDR] = 1000;
  gl.regs[SB_TRIG_ENAB_ADDR] = 0x1F;
#endif
#ifdef TRIG_SB_MULTI
  // set trigger to Full bandwidth single bin with multiplicity
  gl.regs[SHWR_BUF_TRIG_MASK_ADDR] = SHWR_BUF_TRIG_SB;
  gl.regs[SB_TRIG_THR0_ADDR] = 1000;
  gl.regs[SB_TRIG_THR1_ADDR] = 1000;
  gl.regs[SB_TRIG_THR2_ADDR] = 1000;
  gl.regs[SB_TRIG_SSD_ADDR] = 1000;
  gl.regs[SB_TRIG_ENAB_ADDR] = 0x7 | 0x30;
#endif
#ifdef TRIG_COMPAT_SB
  // set trigger to Compatibility single bin
  gl.regs[SHWR_BUF_TRIG_MASK_ADDR] = COMPATIBILITY_SHWR_BUF_TRIG_SB;
  gl.regs[COMPATIBILITY_SB_TRIG_THR0_ADDR] = 1000;
  gl.regs[COMPATIBILITY_SB_TRIG_THR1_ADDR] = 1000;
  gl.regs[COMPATIBILITY_SB_TRIG_THR2_ADDR] = 1000;
  gl.regs[COMPATIBILITY_SB_TRIG_ENAB_ADDR] = 0x78;
#endif
  // set fake GPS
  gl.tstctl_regs[USE_FAKE_ADDR] |= 1 << USE_FAKE_PPS_BIT;

  while(controlrecv(controlsock) <= 0) {
    read_evt_read(&sh, databuf);
    senddata(datasock, &sa);
    fprintf(stderr, "sent id %08x, rd %d, time %9d.%09d [s.tics], evt %1x\n",
	    sh.id, sh.rd, sh.ttag_shwr_seconds,
	    sh.ttag_shwr_nanosec & TTAG_NANOSEC_MASK,
	    sh.ttag_shwr_nanosec >> TTAG_EVTCTR_SHIFT);
  }

  read_evt_end();
  close(controlsock);
  close(datasock);
  
  return(0);
}
