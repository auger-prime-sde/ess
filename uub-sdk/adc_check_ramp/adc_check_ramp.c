/* adc_check_ramp
   Utility to check if ADCs are initialized correctly
    - set ADCs to ramp test mode
    - take one trace and evaluate
    - set ADCs to normal mode
   exit code: 0 = all ADCs are OK,
              1-31 = bit mask of failing ADCs
	      other value: error in SPI communication/read out etc., see EXIT_*
   stderr: debug messages
   Petr Tobiska <tobiska@fzu.cz>
*/

#define VERSION "2021-04-19"
#define REALTIME
#define BUFALIGN

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sched.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <linux/spi/spidev.h>

#include "sde_trigger_defs.h"
#include "time_tagging.h"
#include "test_control_defs.h"
#ifndef TEST_CONTROL_BASE
  #define TEST_CONTROL_BASE XPAR_TEST_CONTROL_BLOCK_TEST_CONTROL_0_S00_AXI_BASEADDR
#endif

#define SIG_WAKEUP SIGRTMIN+14

#define WAITTIME 10000   /* wait time [ns] between checking data available */

/* from shwr_evt_defs.h */
#define SHWR_MAX_VAL (1 << 12)
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

#define PAGESIZE (sysconf(_SC_PAGESIZE))
#define DATASIZE (SHWR_NSAMPLES * SHWR_RAW_NCH_MAX)

/* SPI for ADC */
#define MASK_CHS   0x03
#define ADDR_CHS   0x05
#define ADDR_TEST  0x0D
#define RAMPON     0x0F
#define RAMPOFF    0x00
/* exit codes */
#define EXIT_NOPER      32
#define EXIT_OPENSPI    33
#define EXIT_SPIWRMODE  34
#define EXIT_SPIRDMODE  35
#define EXIT_SPIBITPERW 36
#define EXIT_SPIWRSPEED 37
#define EXIT_SPIRDSPEED 38
#define EXIT_SPIWRITE   39
#define EXIT_SPIREAD    40
#define EXIT_BUFALIGN   50
#define EXIT_EVTDEVMEM  60
#define EXIT_EVTMAPTRIG 61
#define EXIT_EVTMAPTIME 62
#define EXIT_EVTMAPTEST 63
#define EXIT_EVTMAPSHWR 64
#define EXIT_EVTSIGNAL  65
#define EXIT_EVTTIMER   66
#define EXIT_EVTSETTIME 67
 
/* global variables */
static struct read_evt_global gl;
static struct shwr_header sh;
uint32_t saved_trigger;
uint32_t _databuf[DATASIZE + 2], *databuf;
uint16_t traces[SHWR_NCH_MAX][SHWR_NSAMPLES];
char *adc_trace_fn = NULL;
int adcfd[SHWR_RAW_NCH_MAX];
int failedadcfd = -1;  /* store adc fd where error occured, skip it in exit */

/* functions */

void printver(char *progname) {
  fprintf(stderr, "%s v" VERSION
#ifdef BUFALIGN
	" BUFALIGN"
#endif
#ifdef REALTIME
	" REALTIME"
#endif
	"\n", progname);
}

void printhelp(char *progname) {
  fprintf(stderr, "Usage: %s [-d <adc_trace_filename>] [-h] [-v] [-V]\n"
	  "      -d: dump trace to adc_trace_filename\n"
	  "      -v: be verbose\n"
	  "      -V: print version and exit\n"
	  "      -h: print help and exit\n", progname);
}

int openspidev(int adc) {
  int fd;
  char filename[20];

  snprintf(filename, 19, "/dev/spidev32766.%d", adc);
  fd = open(filename, O_RDWR);
  if (fd < 0) {
    fprintf(stderr, "Cannot open SPI device %d\n", adc);
    exit(EXIT_OPENSPI); }
  return fd;
}

void spi_init(int adcfd) {
  static uint8_t mode = 0;
  static uint8_t bits = 8;
  static uint32_t speed = 5000000;
  
  int ret;

  // spi mode
  ret = ioctl(adcfd, SPI_IOC_WR_MODE, &mode);
  if (ret == -1) {
    fprintf(stderr, "Cannot set SPI write mode\n");
    exit(EXIT_SPIWRMODE); }
  
  ret = ioctl(adcfd, SPI_IOC_RD_MODE, &mode);
  if (ret == -1) {
    fprintf(stderr, "Cannot set SPI read mode\n");
    exit(EXIT_SPIRDMODE); }
  
  // bits per word
  ret = ioctl(adcfd, SPI_IOC_WR_BITS_PER_WORD, &bits);
  if (ret == -1) {
    fprintf(stderr, "Cannot set bits per word\n");
    exit(EXIT_SPIBITPERW); }

  ret = ioctl(adcfd, SPI_IOC_RD_BITS_PER_WORD, &bits);
  if (ret == -1) {
    fprintf(stderr, "Cannot set bits per word\n");
    exit(EXIT_SPIBITPERW); }
  
  // max speed hz
  ret = ioctl(adcfd, SPI_IOC_WR_MAX_SPEED_HZ, &speed);
  if (ret == -1) {
    fprintf(stderr, "Cannot set max wr speed\n");
    exit(EXIT_SPIWRSPEED); }

  ret = ioctl(adcfd, SPI_IOC_RD_MAX_SPEED_HZ, &speed);
  if (ret == -1) {
    fprintf(stderr, "Cannot set max rd speed\n");
    exit(EXIT_SPIRDSPEED); }
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
    failedadcfd = adcfd;
    exit(EXIT_SPIREAD);
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
    failedadcfd = adcfd;
    exit(EXIT_SPIWRITE);
  }
  return 1;
}

void adc_settestmode(int testmode) {
  int i, fd;
  for (i = 0; i < SHWR_RAW_NCH_MAX; i++) {
    fd = adcfd[i];
    if(fd >= 0) {
      if(fd == failedadcfd) { /* skip ADC fd with errors */
	fprintf(stderr, "operation ignored on failing ADC %d\n", i);
	continue; }
      adc_write(fd, ADDR_CHS, MASK_CHS);  /* set both A and B channels */
      adc_write(fd, ADDR_TEST, testmode);
    } else
      fprintf(stderr, "SPI for ADC %d not open\n", i);
  }
}

void adc_normal() {
  int i;
  adc_settestmode(RAMPOFF);
  for( i = 0; i < SHWR_RAW_NCH_MAX; i++ ) {
    close(adcfd[i]);
    adcfd[i] = -1; }
}

/* convert raw data from databuf to traces */
void convert_databuf(uint32_t *databuf, uint16_t traces[][SHWR_NSAMPLES]) {
  int adc, i;
  uint32_t *cur_ptr, *stop_ptr;
  for (adc = 0; adc < SHWR_RAW_NCH_MAX; adc ++ ) {
    stop_ptr = databuf + (adc+1)*SHWR_NSAMPLES;
    cur_ptr = databuf + adc*SHWR_NSAMPLES + sh.shwr_buf_start;
    for( i = 0; i < SHWR_NSAMPLES; i++ ) {
      traces[2*adc][i] = *cur_ptr & 0xfff;
      traces[2*adc + 1][i] = (*cur_ptr >> 16) & 0xfff;
      if( ++cur_ptr == stop_ptr)
	cur_ptr = stop_ptr - SHWR_NSAMPLES;
    }
  }
}

void dump_trace(char *fname, uint16_t trace[][SHWR_NSAMPLES]) {
  int i, ch;
  FILE *fp;

  if(( fp = fopen(fname, "w")) == NULL ) {
    fprintf(stderr, "Cannot open file '%s' for saving trace\n", fname);
    return; }
  for( i = 0; i < SHWR_NSAMPLES; i++ ) {
    for( ch = 0; ch < SHWR_NCH_MAX; ch++ )
      fprintf(fp, "%5d", traces[ch][i]);
    fputs("\n", fp);
  }
  fclose(fp);
}

int evaluate_ramp(uint16_t trace[][SHWR_NSAMPLES]) {
  int ch, i, sum, result;
  result = 0;
  for( ch = 0; ch < SHWR_RAW_NCH_MAX; ch++ ) {
    sum = trace[2*ch][0];
    for( i = 0; i < SHWR_NSAMPLES; i++ ) {
      if(trace[2*ch][i] != trace[2*ch+1][i] ||
	 (trace[2*ch][i] + i) % SHWR_MAX_VAL != sum) {
	result |= 1 << ch;
	break; }
    }
  }
  return result;
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
    exit(EXIT_EVTDEVMEM); }

  size = roundup(256*sizeof(uint32_t), PAGESIZE);
  gl.regs_size = size;
  pt = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
	    SDE_TRIGGER_BASE);
  if(pt == MAP_FAILED) {
    fprintf(stderr, "Error mapping regs\n");
    exit(EXIT_EVTMAPTRIG); }
  gl.regs = (uint32_t *)pt;

  pt = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
	    TIME_TAGGING_BASE);
  if(pt == MAP_FAILED) {
    fprintf(stderr, "Error mapping tt_regs\n");
    exit(EXIT_EVTMAPTIME); }
  gl.tt_regs = (uint32_t *)pt;

  pt = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
	    TEST_CONTROL_BASE);
  if(pt == MAP_FAILED) {
    fprintf(stderr, "Error mapping tstctl_regs\n");
    exit(EXIT_EVTMAPTEST); }
  gl.tstctl_regs = (uint32_t *)pt;

  size = roundup(SHWR_MEM_DEPTH * SHWR_MEM_NBUF, PAGESIZE);
  gl.shwr_mem_size = size;
  for( i = 0; i < SHWR_RAW_NCH_MAX; i++ ) {
    pt = mmap(NULL,size, PROT_READ, MAP_SHARED, fd, shwr_addr[i]);
    if(pt == MAP_FAILED) {
      fprintf(stderr, "Error mapping shower buf %d\n", i);
      exit(EXIT_EVTMAPSHWR); }
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
    exit(EXIT_EVTSIGNAL);
  }
  if(sigaddset(&gl.sigset, SIG_WAKEUP) != 0){
    fprintf(stderr, "error while trying to set signals ... 2\n");
    exit(EXIT_EVTSIGNAL);
  }
  if(sigprocmask(SIG_BLOCK, &gl.sigset, NULL) != 0){
    fprintf(stderr, "error while trying to set signals ... 3 \n");
    exit(EXIT_EVTSIGNAL);
  }

  /* periodical signal generation */
  sev.sigev_notify = SIGEV_SIGNAL;
  sev.sigev_signo = SIG_WAKEUP;
  if(timer_create(CLOCK_MONOTONIC, &sev, &t_alarm) != 0){
    fprintf(stderr, "timer creation error\n");
    exit(EXIT_EVTTIMER);
  }

  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = WAITTIME;
  ts.it_value.tv_sec = 0;
  ts.it_value.tv_nsec = WAITTIME;  /*the next interruption */
  if(timer_settime(t_alarm, 0, &ts, NULL) != 0){
    exit(EXIT_EVTSETTIME);
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
 * return time for data acquisition in us
 */
long long read_evt_read(struct shwr_header* sh, uint32_t *buf) {
  uint32_t volatile *st;
  void *pt_aux;
  uint32_t aux;
  int rd, sig, i;
  int offset;
  struct timeval tval;
  long long duration;

  st = &(gl.regs[SHWR_BUF_STATUS_ADDR]);

  /*wait for the periodical signal and check if there is a
    event trigger
  */
  aux = SHWR_BUF_NFULL_MASK << SHWR_BUF_NFULL_SHIFT;
  sig = SIG_WAKEUP;
  while( ((*st) & aux) == 0 && sig == SIG_WAKEUP)
    sig = sigwaitinfo(&gl.sigset, NULL);

  if(sig == SIG_WAKEUP){
    gettimeofday(&tval, NULL);
    duration = - (tval.tv_sec * 1000000L + tval.tv_usec);
    rd = (((*st) >> SHWR_BUF_RNUM_SHIFT) & SHWR_BUF_RNUM_MASK);
    offset = rd * SHWR_NSAMPLES;
    for(i = 0; i < SHWR_RAW_NCH_MAX; i++){
      pt_aux = (void *)(gl.shwr_pt[i] + offset);
      memcpy(buf, pt_aux, sizeof(uint32_t)*SHWR_NSAMPLES);
      buf += SHWR_NSAMPLES;
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
    gettimeofday(&tval, NULL);
    duration += tval.tv_sec * 1000000L + tval.tv_usec;
    return(duration);
  }
  return(-1);
}
  
void restore_trigger(void) {
  if(gl.regs)
    gl.regs[SHWR_BUF_TRIG_MASK_ADDR] = saved_trigger;
}

void LED_trigger(){
  gl.regs[LED_CONTROL_ADDR] = 0;  //TURN OFF LEDS
  gl.regs[LED_CONTROL_ADDR] = 1;  //Do led pulse now
  usleep(100);  //if you do not wait, there is no reading
}

int main(int argc, char ** argv) {
  long long duration;
  int i, fd, opt, result;
  int verbose = 0;
  
#ifdef BUFALIGN     
  /* make databuf aligned to 8*n + 4 */
  databuf = (uint32_t*)(((((uintptr_t)_databuf + 7) >> 3) << 3) + 4);
  // check databuf vs _databuf position
  if(databuf < _databuf || databuf >= _databuf + 8
     || ((uintptr_t)databuf & 7) != 4) {
    fprintf(stderr, "databuf alignment problem: _databuf = %p, databuf = %p\n",
	    (void *)_databuf, (void*)databuf);
    exit(EXIT_BUFALIGN); }
#else
  databuf = _databuf;
#endif

  // set real-time priority
#ifdef REALTIME
  struct sched_param sched_p;
  sched_p.sched_priority = 10;
  if(sched_setscheduler(0, SCHED_FIFO, &sched_p) < 0) {
    fprintf(stderr, "Schedule setting error: %s\n", strerror(errno)); }
#endif

  while ((opt = getopt(argc, argv, "d:vVh")) != -1) {
    switch(opt) {
    case 'd':
      adc_trace_fn = optarg;
      break;
    case 'v':
      verbose = 1;
      break;
    case 'V':
      printver(argv[0]);
      exit(EXIT_NOPER);
      break;
    case 'h':
    default:
      printhelp(argv[0]);
      exit(EXIT_NOPER);
      break;
    }}

  for( i = 0; i < SHWR_RAW_NCH_MAX; i++ )
    adcfd[i] = -1;  /* make all explicitely unitialized */
  for( i = 0; i < SHWR_RAW_NCH_MAX; i++ ) {
    fd = openspidev(i);
    spi_init(fd);
    adcfd[i] = fd; }

  read_evt_init();
  atexit(read_evt_end);
  // save current trigger and set to LED
  saved_trigger = gl.regs[SHWR_BUF_TRIG_MASK_ADDR];
  gl.regs[SHWR_BUF_TRIG_MASK_ADDR] = SHWR_BUF_TRIG_LED;
  atexit(restore_trigger);
  // set fake GPS
  gl.tstctl_regs[USE_FAKE_ADDR] |= 1 << USE_FAKE_PPS_BIT;

  adc_settestmode(RAMPON);
  atexit(adc_normal);

  LED_trigger();
  duration = read_evt_read(&sh, databuf);
  convert_databuf(databuf, traces);
  if( adc_trace_fn )
    dump_trace(adc_trace_fn, traces);
  if( verbose )
    fprintf(stderr, "sent id %08x, rd %u, time %9u.%09u [s.tics], evt %1x, "
	    "duration %lld [us]\n",
	    sh.id, sh.rd, sh.ttag_shwr_seconds,
	    sh.ttag_shwr_nanosec & TTAG_NANOSEC_MASK,
	    sh.ttag_shwr_nanosec >> TTAG_EVTCTR_SHIFT,
	    duration);
  result = evaluate_ramp(traces);

  /* clean up function registered via atexit */
  return(result);
}
