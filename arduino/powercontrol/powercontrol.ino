/*
 * Control power supplies for individual UUBs
 * Petr Tobiska <tobiska@fzu.cz>
 * 2019-01-28
 *
 */

#define VERSION "2019-12-20"

/* constants for voltage reference */
#define AVCC 1
#define V256 3

#undef DEBUGTOG
#define VREF V256 /* AVCC or V256 */
#define CURRHIST 10.0  /* histeresis for current, +/- mA */
#define PONDELAY 10  /* delay between power on of successive pin, ms */

/* forward declarations */
int readDecimal(char **_ptr);

#ifndef sbi
#define sbi(sfr, bit) (_SFR_BYTE(sfr) |= _BV(bit))
#endif
#ifndef cbi
#define cbi(sfr, bit) (_SFR_BYTE(sfr) &= ~_BV(bit))
#endif
#define STR(s) _STR(s)
#define _STR(s) #s

#if VREF == AVCC
  #define ANALOG_REF ((0<<REFS1) | (1<<REFS0))  /* 01 AVCC, 10 1.1V, 11 2.56V */
#elif VREF == V256
  #define ANALOG_REF ((1<<REFS1) | (1<<REFS0))  /* 01 AVCC, 10 1.1V, 11 2.56V */
#endif

#define SAMPERIOD ((uint16_t) 80)          /* sampling period [0.5us] */

#define NCHAN 10
#define alphaN 8
const float alpha = 1.0 - 1.0/(1 << alphaN);
const float alpha1 = 1.0/(1 << alphaN);
const float inv1alpha = (float)(1 << alphaN);

/* global variables */
#define BUFSIZE 30
char buffer[BUFSIZE];

volatile uint8_t adcpin; /* currently processed ADC pin */
volatile uint32_t gtime; /* time in 0.4ms */
volatile uint32_t adcvals[NCHAN];  /* running average with expo decay */
volatile uint8_t zones[NCHAN];  /* current zone of pin */

/* current reports */
struct _creport {
  uint8_t stat;
  uint32_t time; } __attribute__ ((__packed__));
#define REPORTSIZE 100
struct _creport reportbuf[REPORTSIZE];
struct _creport *rd_ptr, *wr_ptr;
#define ZONEUP 0x80
#define ZONEDOWN 0
#define ZONEOVER 7
/* insert report to report buf */
inline void report(uint8_t pin, uint8_t dir, uint8_t zone, uint32_t mtime) {
  dir |= (zone << 4) | pin;
  wr_ptr->stat = dir;
  wr_ptr->time = mtime;
  if (++wr_ptr == reportbuf+REPORTSIZE)
    wr_ptr = reportbuf;
}

/* zones:
  pinversion: pin(4b), version(12b)
     pin: 0 - 9 ... pin
          0xa   ... all pins
  limit[i] ... current in 0.1mA
  crc      ... CRC16 xmodem checksum from fields pinversion + limit
 */
#define NZONE 3
static inline uint8_t pinver2pin(uint16_t pinver) {
  return (uint8_t)( pinver >> 12); }
static inline int pinver2version(uint16_t pinver) {
  return pinver & 0xfff; }

struct _zonerec {
  uint16_t pinversion;
  uint16_t limit [NZONE];
  uint16_t crc;
} __attribute__ ((__packed__));

/* default zone: 50mA, 250mA, 700mA
import crcmod; from struct import pack
crc = crcmod.predefined.mkCrcFun('xmodem')
"%x" % crc(pack('<HHHH', 0xa001, 500, 2500, 7500))
 */
struct _zonerec DEFZONE = {0xa001, {500, 2500, 7500}, 0xd369};
uint16_t zonecurr[NCHAN][NZONE];  /* current limits per pin */
/* converted to adc/(1-alpha) plus/minus histeresis */
uint32_t zoneadc[NCHAN][2*NZONE - 1];
uint32_t zoneadcw[2*NZONE - 1]; // work buf for zoneadc
unsigned int versions[NCHAN+1] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

/* calculate CRC16 */
uint16_t calccrc(uint8_t *ptr, int count) {
  uint16_t crc = 0;
  char i;
  while( --count >= 0 ) {
    //    crc = crc ^ (((uint16_t) *ptr++) << 8);
    crc = crc ^ (uint16_t) *ptr++ << 8;
    for( i=8; i > 0; i-- )
      if( crc & 0x8000 )
	crc = crc << 1 ^ 0x1021;
      else
	crc = crc << 1;
  }
  return crc;
}

/* ADC offset, slopes mA/ADC */
#if VREF == AVCC
float offs[NCHAN] = {-2.5, -2.3, -2.7, -2.4, -2.4,
		     -2.5, -2.4, -2.3, -2.3, -2.4};
float slopes[NCHAN] = {2.422, 2.420, 2.416, 2.423, 2.420,
		       2.418, 2.424, 2.420, 2.421, 2.415};
#elif VREF == V256
float offs[NCHAN] = {-2.8, -2.7, -2.8, -2.7, -2.7,
		     -2.8, -2.9, -2.8, -2.8, -2.9};
float slopes[NCHAN] = {1.219, 1.218, 1.219, 1.220, 1.219,
		       1.218, 1.222, 1.218, 1.219, 1.217};
#endif

static inline uint32_t curr2adc(int i, float curr) {
  return (uint32_t)((curr/slopes[i] + offs[i])*inv1alpha + 0.5); }

static inline float adc2curr(int i, uint32_t adcval) {
  return slopes[i] * ((float)adcval*alpha1 - offs[i]); }

/* convert zonecurr[i] -> zoneadc[i] */
void zonec2a(int i) {
  int j;
  float curr;
  for (j = 0; j < NZONE-1; j++) {
    curr = zonecurr[i][j] / 10.;
    zoneadcw[2*j] = curr2adc(i, curr + CURRHIST);
    zoneadcw[2*j+1] = curr2adc(i, curr - CURRHIST); }
  curr = zonecurr[i][NZONE-1] / 10.;
  zoneadcw[2*NZONE-2] = curr2adc(i, curr);
  uint8_t oldSREG = SREG;
  cli();
  memcpy(zoneadc[i], zoneadcw, sizeof(zoneadcw));
  SREG = oldSREG;
}

/* process zonerec
   set versions[] and zonecurr[] */
int addzonerec(struct _zonerec *zr) {
  uint8_t pin;
  uint16_t version;

  pin = pinver2pin(zr->pinversion);
  if (pin > NCHAN)
    return -1;
  if (calccrc((uint8_t*) zr, (1+NZONE)*sizeof(uint16_t)) != zr->crc)
    return -2;
  version = pinver2version(zr->pinversion);
  if (versions[pin] >= version)
    return -3;
  versions[pin] = version;
  if (pin == NCHAN)
    for (pin = 0; pin < NCHAN; pin++)
      memcpy(zonecurr[pin], &zr->limit, NZONE*sizeof(uint16_t));
  else
    memcpy(zonecurr[pin], &zr->limit, NZONE*sizeof(uint16_t));
  return 0;
}

/* read limits from buffer
   update versions[] and zonecurr[]
   use DEFZONE as work buffer
   TODO: calc crc and write to EEPROM */
int readLimits(char **_ptr) {
  uint8_t i, pin;
  int val;
  while(**_ptr == ' ')
    (*_ptr) ++;
  i = *(*_ptr) ++;
  if (i == '*')
    pin = NCHAN;
  else if('0' <= i && i <= '9')
    pin = i - '0';
  else
    return -1;

  for (i = 0; i < NZONE; i++ ) {
    if((val = readDecimal(_ptr)) < 0)
      return -2;
    if (i > 0 && (uint16_t)val <= DEFZONE.limit[i-1])
      return -3;
    DEFZONE.limit[i] = val; }

  if (pin == NCHAN)
    for (pin = 0; pin < NCHAN; pin++)
      memcpy(zonecurr[pin], &DEFZONE.limit, NZONE*sizeof(uint16_t));
  else
    memcpy(zonecurr[pin], &DEFZONE.limit, NZONE*sizeof(uint16_t));

  versions[pin] ++;
  /* DEFZOZNE.pinversion = (pin << 12) | (versions[pin] & 0xfff);
  calccrc + write to EEPROM */
  return pin;
}

void printLimit1pin(uint8_t pin);
void printLimits(char **_ptr) {
  uint8_t pin;

  while(**_ptr == ' ')
    (*_ptr) ++;

  if('0' <= **_ptr && **_ptr <= '9')
    pin = **_ptr - '0';
  else
    pin = NCHAN;

  if ( pin == NCHAN )
    for( pin = 0; pin < NCHAN; pin++ )
      printLimit1pin(pin);
  else
    printLimit1pin(pin);
}

void printLimit1pin(uint8_t pin) {
  int i;
  Serial.print(pin);
  Serial.write(": ");
  for (i = 0; i < NZONE; i++) {
    Serial.write(' ');
    Serial.print(zonecurr[pin][i] / 10);
    Serial.write('.');
    Serial.write('0' + zonecurr[pin][i] % 10); }
  Serial.write("\r\n");
}

/* if mapping analog ports to ADC pins necessary
   const uint8_t adcPin[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }; */
#define ADCDIDR0 0xFF
#define ADCDIDR2 0x03

/*
  digital pin mappings:
  relay: PB0, PB2, PL0, PL2, PL4, PL6, PC4, PC6, PA7, PA5
  splitter: PC0, PG2, PC2
  of them PWM: PL4: OC5B
 */
void initDigiPins() {
  cbi(TCCR5A, COM5B1);  // disable PWM
  // relay: OUTPUT LOW
  sbi(DDRB, DDB0); cbi(PORTB, PB0);  /* 0: PB0 */
  sbi(DDRB, DDB2); cbi(PORTB, PB2);  /* 1: PB2 */
  sbi(DDRL, DDL0); cbi(PORTL, PL0);  /* 2: PL0 */
  sbi(DDRL, DDL2); cbi(PORTL, PL2);  /* 3: PL2 */
  sbi(DDRL, DDL4); cbi(PORTL, PL4);  /* 4: PL4 */
  sbi(DDRL, DDL6); cbi(PORTL, PL6);  /* 5: PL6 */
  sbi(DDRC, DDC4); cbi(PORTC, PC4);  /* 6: PC4 */
  sbi(DDRC, DDC6); cbi(PORTC, PC6);  /* 7: PC6 */
  sbi(DDRA, DDA7); cbi(PORTA, PA7);  /* 8: PA7 */
  sbi(DDRA, DDA5); cbi(PORTA, PA5);  /* 9: PA5 */
  // sp0, sp1 OUTPUT LOW; sp2 OUTPUT HIGH
  sbi(DDRC, DDC0); cbi(PORTC, PC0);  /* spPin0: PC0 */
  sbi(DDRG, DDG2); cbi(PORTG, PG2);  /* spPin1: PG2 */
  sbi(DDRC, DDC2); sbi(PORTC, PC2);  /* spPin2: PC2 */
}
#define spOff() cbi(PORTC, PC2)
#define spOn() sbi(PORTC, PC2)
static inline void spMode(uint8_t mode) {
  __asm__ __volatile__ ( "sbrc %0, 0 \n\t"  // bit 0: PORTC, PC0
			 "sbi %1, %2 \n\t"
			 "sbrs %0, 0 \n\t"
			 "cbi %1, %2 \n\t"
			 "sbrc %0, 1 \n\t"  // bit 1: PORTG, PG2
			 "sbi %3, %4 \n\t"
			 "sbrs %0, 1 \n\t"
			 "cbi %3, %4 \n\t"
			 :
			 : "r" (mode),
			   "I" (_SFR_IO_ADDR(PORTC)), "M" (PC0),
			   "I" (_SFR_IO_ADDR(PORTG)), "M" (PG2)
			 );
}

void relPinLow(uint8_t pin) {
  switch(pin) {
  case 0: cbi(PORTB, PB0);  break; /* 0: PB0 */
  case 1: cbi(PORTB, PB2);  break; /* 1: PB2 */
  case 2: cbi(PORTL, PL0);  break; /* 2: PL0 */
  case 3: cbi(PORTL, PL2);  break; /* 3: PL2 */
  case 4: cbi(PORTL, PL4);  break; /* 4: PL4 */
  case 5: cbi(PORTL, PL6);  break; /* 5: PL6 */
  case 6: cbi(PORTC, PC4);  break; /* 6: PC4 */
  case 7: cbi(PORTC, PC6);  break; /* 7: PC6 */
  case 8: cbi(PORTA, PA7);  break; /* 8: PA7 */
  case 9: cbi(PORTA, PA5);  break; /* 9: PA5 */
  }
}

void relPinHigh(uint8_t pin) {
  switch(pin) {
  case 0: sbi(PORTB, PB0);  break; /* 0: PB0 */
  case 1: sbi(PORTB, PB2);  break; /* 1: PB2 */
  case 2: sbi(PORTL, PL0);  break; /* 2: PL0 */
  case 3: sbi(PORTL, PL2);  break; /* 3: PL2 */
  case 4: sbi(PORTL, PL4);  break; /* 4: PL4 */
  case 5: sbi(PORTL, PL6);  break; /* 5: PL6 */
  case 6: sbi(PORTC, PC4);  break; /* 6: PC4 */
  case 7: sbi(PORTC, PC6);  break; /* 7: PC6 */
  case 8: sbi(PORTA, PA7);  break; /* 8: PA7 */
  case 9: sbi(PORTA, PA5);  break; /* 9: PA5 */
  }
}

/* read relay status to provided buffer, 1 or 0 */
void readRelPins(uint8_t *buf) {
  buf[0] = PINB & _BV(PB0) ? 1 : 0; /* 0: PB0 */
  buf[1] = PINB & _BV(PB2) ? 1 : 0; /* 1: PB2 */
  buf[2] = PINL & _BV(PL0) ? 1 : 0; /* 2: PL0 */
  buf[3] = PINL & _BV(PL2) ? 1 : 0; /* 3: PL2 */
  buf[4] = PINL & _BV(PL4) ? 1 : 0; /* 4: PL4 */
  buf[5] = PINL & _BV(PL6) ? 1 : 0; /* 5: PL6 */
  buf[6] = PINC & _BV(PC4) ? 1 : 0; /* 6: PC4 */
  buf[7] = PINC & _BV(PC6) ? 1 : 0; /* 7: PC6 */
  buf[8] = PINA & _BV(PA7) ? 1 : 0; /* 8: PA7 */
  buf[9] = PINA & _BV(PA5) ? 1 : 0; /* 9: PA5 */
}

/*
 * ISR to cummulate ADC result
 */
ISR(ADC_vect) {
  uint8_t lval, hval, zone, oldpin = adcpin;
  uint32_t *acumvalptr, *limitptr;

#ifdef DEBUGTOG
  sbi(PORTB, PB6);
#endif
  lval = ADCL;
  hval = ADCH;
  if( --adcpin == 0xFF ) {
    adcpin = NCHAN-1;
    gtime++; }
  //  adcpin = adcPin[apin];
  ADCSRB = (ADCSRB & ~(1 << MUX5)) | (((adcpin >> 3) & 0x01) << MUX5);
  ADMUX = ANALOG_REF | (adcpin & 0x07);
  sbi(TIFR1, OCF1B);  /* clear OCR1B match flag */

  acumvalptr = (uint32_t*)adcvals + oldpin;
  /*  optimalized version for
      *acumvalptr -= *acumvalptr >> alphaN;
      *acumvalptr += (hval << 8) + lval;
      currently implemented only for alphaN = 8 */
#if alphaN == 8
  __asm__ __volatile__ (
			"sub %A0, %B0 \n\t"
			"sbc %B0, %C0 \n\t"
			"sbc %C0, %D0 \n\t"
			"sbc %D0, __zero_reg__ \n\t"
			"add %A0, %2 \n\t"
			"adc %B0, %1 \n\t"
			"adc %C0, __zero_reg__ \n\t"
			"adc %D0, __zero_reg__ \n\t"
			: "+r" (*acumvalptr)
			: "r" (hval), "r" (lval)
			: "memory"
			);
#else
  #error "Not implemented"
#endif
  
  /* check zone transition */
  zone = zones[oldpin];
  limitptr = zoneadc[oldpin] + 2*zone;
  if (*acumvalptr > *limitptr) {
    if ( ++zone == NZONE ) {
      report(oldpin, ZONEUP, ZONEOVER, gtime);
      //      digitalWrite(relPin[oldpin], LOW);     /* power off pin */
      relPinLow(oldpin);
      *acumvalptr = 0;
      zone = 0; }
    else
      report(oldpin, ZONEUP, zone, gtime);
    }
  else if (zone > 0 && *acumvalptr < *--limitptr )
    report(oldpin, ZONEDOWN, --zone, gtime);
  zones[oldpin] = zone;
#ifdef DEBUGTOG
  cbi(PORTB, PB6);
#endif
}

/* print current change report */
void printReport() {
  uint8_t stat, oldSREG;
  uint32_t mtime;

  while(1) {
    oldSREG = SREG;
    cli();  /* atomically read status & time and clear the record */
    stat = rd_ptr->stat; rd_ptr->stat = ZONEUP;
    mtime = rd_ptr->time;
    SREG = oldSREG;
    if( stat == ZONEUP )
      break;

    if (++rd_ptr == reportbuf+REPORTSIZE)
      rd_ptr = reportbuf;
    Serial.flush();
    /* <pin>[+-]<final zone> */
    Serial.write('0' + (stat & 0xf));
    Serial.write(stat & ZONEUP ? '+' : '-');
    Serial.write('0' + (stat >> 4 & 0x7));
    Serial.write(':');
    Serial.print(mtime);
    Serial.write(' ');
  }
  Serial.write("\r\nOK\r\n");
}

/*
 * switch on relays according to mask
 */
void switchOn(uint16_t mask) {
  uint8_t i;
  for(i = 0; i < NCHAN; i++) {
    if( mask & 1 ) {
      //      digitalWrite(relPin[i], HIGH);
      relPinHigh(i);
      delay(PONDELAY); }
    mask >>= 1; }
}

/*
 * switch off relays according to mask
 */
void switchOff(uint16_t mask) {
  uint8_t i;
  for(i = 0; i < NCHAN; i++) {
    if( mask & 1 ) 
      //      digitalWrite(relPin[i], LOW);
      relPinLow(i);
    mask >>= 1; }
}

/*
 * read status of relays
 */
void readRelays() {
  uint8_t i;
  readRelPins((uint8_t*)buffer);
  for( i = 0; i < NCHAN; i++)
    Serial.write('0' + buffer[i]);
  Serial.write("\r\n");
}

/*
 * read a line from serial, terminated by \r
 */
char* readline() {
  char c;
  char *ptr = buffer;
  int8_t rsize = BUFSIZE-1;

  while(1) {
    if(Serial.available() > 0) {
      c = Serial.read();
      if(c == '\r') {
        *ptr = '\0';
        return buffer; }
      if(rsize > 0) {
        *ptr++ = c;
        rsize --; }
    }
  }
}

/*
 * read octal number
 */
uint16_t readOctal(char **_ptr) {
  uint16_t res = 0;
  while(**_ptr == ' ')
    (*_ptr) ++;
  while('0' <= **_ptr && **_ptr < '8') {
    res = (res << 3) + **_ptr - '0';
    (*_ptr) ++;
  }
  return res;
}

/*
 * read decimal number
   return -1 if number not present
   return -2 if overflow
 */
int readDecimal(char **_ptr) {
  int res = 0;
  while(**_ptr == ' ')
    (*_ptr) ++;
  if('0' > **_ptr || **_ptr > '9')
    return -1;
  while('0' <= **_ptr && **_ptr <= '9') {
    res = 10*res + **_ptr - '0';
    (*_ptr) ++;
  }
  if (res < 0) // overflow
    return -2;
  return res;
}

void printError() {
  Serial.print(F("Usage: r              -- read currents in mA" "\r\n"
		 "       n <octal mask> -- switch on relays" "\r\n"
		 "       f <octal mask> -- switch off relays" "\r\n"
		 "       d              -- read relays setting" "\r\n"
		 "       m <octal mask> -- set splitter mode" "\r\n"
		 "       1/0            -- switch splitter ON/OFF" "\r\n"
		 "       z              -- print zone change report" "\r\n"
		 "       t              -- reset time" "\r\n"
		 "       l [0-9*] <limit1> ... <limit" STR(NZONE) ">" "\r\n"
		 "                      -- set current limits for port(s) [0.1mA]"
		 "\r\n"
		 "       L [0-9]?       -- return current limits [mA]" "\r\n" 
		 ));
}

void printIdent() {
  Serial.print(F("PowerControl " VERSION "\r\n"));
}

void printOK() {
  Serial.print(F("OK\r\n"));
}

void setup() {
  uint8_t i;

  Serial.begin(115200, SERIAL_8N1);
  printIdent();
#ifdef DEBUGTOG
  /* set pin12 = PB6 as OUTPUT, LOW */
  sbi(DDRB, DDB6);  cbi(PORTB, PB6);
#endif

  gtime = 0;
  // init zonecurr
  addzonerec(&DEFZONE);

  // setup relay & splitter pins
  initDigiPins();

  // clear adcvals & zoneadc
  for(i = 0; i < NCHAN; i++) {
    zones[i] = 0;
    adcvals[i] = 0.0;
    zonec2a(i);
  }

  // clear report buffer and init rd/wr_ptr
  for (rd_ptr=reportbuf; rd_ptr < reportbuf+REPORTSIZE; rd_ptr++)
    rd_ptr->stat = ZONEUP;
  rd_ptr = wr_ptr = reportbuf;

  // setup timer1 for trigger ADC start
  // OC1x disconnected
  // mode WGMn3:0: 0x4 - CTC with OCR1A
  // clock scaler CSn2:0:  0x2: 8; 0x3: 64, 0x4: 256, 0x5 1024
  // freq = f_CPU/N/(1+OCR1A)
  TCCR1A = (0<<WGM11) | (0<<WGM10);
  TCCR1B = (0<<WGM13) | (1<<WGM12) | (0<<CS12) | (1<<CS11) | (0<<CS10);
  TCCR1C = 0;
  OCR1AH = (SAMPERIOD-1) >> 8; OCR1AL = (SAMPERIOD-1) % 0x100;
  OCR1BH = 0; OCR1BL = 3;
  TIMSK1 = 0;

  // setup ADCs
  /* apin = NCHAN-1; if ADC pin remapping necessary */
  /* adcpin = adcPin[apin]; */
  adcpin = NCHAN-1;
  ADMUX = ANALOG_REF | (adcpin & 0x07);   // REFS1&0, MUXi = 0 =>
  ADCSRB = (1<<ADTS2) | (0<<ADTS1) | (1<<ADTS0)
    | (((adcpin >> 3) & 0x01) << MUX5); // TC1 overflow
  DIDR0 = ADCDIDR0;  // digital input disable
  DIDR2 = ADCDIDR2;
  ADCSRA = (1<<ADEN) | (1<<ADATE) | (1<<ADIE) |
    (1<<ADPS2) | (0<<ADPS1) | (0<<ADPS0);  /* prescaler 16: 1MHz */
}

void loop() {
  uint8_t oldSREG;
  uint16_t val;
  uint32_t adcval;
  int8_t i;
  char *ptr;
  float curr;

  val = 0;  /* default for splitter on/off */
  ptr = readline();
  /* skip optional spaces */
  while(*ptr == ' ')
    ptr++;
  switch(*ptr++) {
  case 'r':   /* read currents [mA] */
    for (i = 0; i < NCHAN; i++) {
      readRelPins((uint8_t*)buffer);  // reuse buffer
      if(buffer[i]) {
	oldSREG = SREG;
        cli();  /* atomically copy accumulated value */
        adcval = adcvals[i];
	SREG = oldSREG;
	curr = adc2curr(i, adcval);
      } else   /* report zero current if relay switched off */
	curr = 0.0;
      Serial.print(curr, 1);
      Serial.write(' '); }
    Serial.write("\r\n");
    printOK();
    break;
  case 'n':   /* switch on relays */
    val = readOctal(&ptr);
    switchOn(val);
    printOK();
    break;
  case 'f':   /* switch off relays */
    val = readOctal(&ptr);
    switchOff(val);
    printOK();
    break;
  case 'd':
    readRelays();
    printOK();
    break;
  case 'm':
    val = readOctal(&ptr);
    spMode(val);
    printOK();
    break;
  case '1':
    spOn();
    printOK();
    break;
  case '0':
    spOff();
    printOK();
    break;
  case 'z':
    printReport();
    break;
  case 't':
    oldSREG = SREG;
    cli();
    gtime = 0;
    SREG = oldSREG;
    printOK();
    break;
  case 'l':
    /* reuse i for pin */
    if((i = readLimits(&ptr)) < 0) {
      printError();
      break; }
    if(i == NCHAN)
      for(i = 0; i < NCHAN; i++)
	zonec2a(i);
    else
      zonec2a(i);
    printOK();
    break;
  case 'L':
    printLimits(&ptr);
    printOK();
    break;
  case '?':
    printIdent();
    break;
  default:
    printError();
  }
}
