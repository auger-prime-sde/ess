/*
 * Control power supplies for individual UUBs
 * Petr Tobiska <tobiska@fzu.cz>
 * 2019-01-28
 *
 */

#define VERSION "2019-11-25"

#undef QUAD
#define DEBUGTOG

#ifndef sbi
#define sbi(sfr, bit) (_SFR_BYTE(sfr) |= _BV(bit))
#endif
#ifndef cbi
#define cbi(sfr, bit) (_SFR_BYTE(sfr) &= ~_BV(bit))
#endif

#define BUFSIZE 10
char buffer[BUFSIZE];

#define ANALOG_REF ((0<<REFS1) | (1<<REFS0))  /* 01 AVCC, 10 1.1V, 11 2.56V */
#define SAMPERIOD ((uint16_t) 80)          /* sampling period, 0.5us */

#define NCHAN 10
float alpha = 0.0;    /* 0.9996 */
volatile uint8_t pin; /* currently processed ADC pin */
volatile float adcvals[NCHAN];  /* running average with expo decay */

/* ADC offset, slopes mA/ADC + quadratic correction */
#ifdef QUAD
float offs[NCHAN] = {-2.9, -2.3, -3.0, -2.9, -2.4,
		     -2.6, -2.9, -2.3, -2.4, -2.3};
float slopes[NCHAN] = {2.404, 2.420, 2.401, 2.398, 2.420,
		       2.413, 2.400, 2.420, 2.416, 2.422};
float qs[NCHAN] = {-0.000048, -0.000000, -0.000037, -0.000065, 0.000000,
		   -0.000013, -0.000063, 0.000001, -0.000012, 0.000017};
#else
float offs[NCHAN] = {-2.5, -2.3, -2.7, -2.4, -2.4,
		     -2.5, -2.4, -2.3, -2.3, -2.4};
float slopes[NCHAN] = {2.422, 2.420, 2.416, 2.423, 2.420,
		       2.418, 2.424, 2.420, 2.421, 2.415};
#endif

/* mapping relay pins */
char relPin[] = { 53 /* PB0 */, 51 /* PB2 */,
		  49 /* PL0 */, 47 /* PL2 */,
		  45 /* PL4 */, 43 /* PL6 */,
		  33 /* PC4 */, 31 /* PC6 */,
		  29 /* PA7 */, 27 /* PA5 */ };

/* splitter control pins (splitmode) */
#define spPin0 37 /* PC0 */
#define spPin1 39 /* PG2 */
/* splitter on/off pin */
#define spPin2 35 /* PC2 */

/*
 * ISR to cummulate ADC result
 */
ISR(ADC_vect) {
  uint8_t lval, hval, oldpin = pin;
  uint16_t val;

#ifdef DEBUGTOG
  sbi(PORTB, PORTB6);
#endif
  lval = ADCL;
  hval = ADCH;
  if( --pin == 0xFF ) pin = NCHAN-1;
  ADCSRB = (ADCSRB & ~(1 << MUX5)) | (((pin >> 3) & 0x01) << MUX5);
  ADMUX = ANALOG_REF | (pin & 0x07);
  sbi(TIFR1, OCF1B);  /* clear OCR1B match flag */
  sei();
  
  val = ((uint16_t)hval << 8) | (uint16_t)lval;
  //  adcvals[oldpin] = alpha * adcvals[oldpin] + val;
  adcvals[oldpin] = val;
#ifdef DEBUGTOG
  cbi(PORTB, PORTB6);
#endif
}


/*
 * switch on relays according to mask
 */
void switchOn(uint16_t mask) {
  int i;
  for(i = 0; i < NCHAN; i++) {
    if( mask & 1 ) 
      digitalWrite(relPin[i], HIGH);
    mask >>= 1; }
}

/*
 * switch off relays according to mask
 */
void switchOff(uint16_t mask) {
  int i;
  for(i = 0; i < NCHAN; i++) {
    if( mask & 1 ) 
      digitalWrite(relPin[i], LOW);
    mask >>= 1; }
}

/*
 * read status of relays
 */
void readRelays() {
  int i;
  for( i = 0; i < NCHAN; i++)
    Serial.print(digitalRead(relPin[i]));
  Serial.println();
}

/*
 * read a line from serial, terminated by \r
 */
char* readline() {
  char c;
  char *ptr = buffer;
  int rsize = BUFSIZE-1;

  while(1) {
    if(Serial.available() > 0) {
      c = Serial.read();
      if(c == '\r') {
        *ptr = '\0';
        return buffer; }
      if(rsize > 0) {
        *ptr++ = c;
        rsize --; }
    } //   else delay(2000);  // timer0 stopped
  }
}

/*
 * read octal number
 */
uint16_t readOctal(char *ptr) {
  uint16_t res = 0;
  while(*ptr == ' ')
    ptr ++;
  while('0' <= *ptr && *ptr < '8') {
    res = (res << 3) + *ptr - '0';
    ptr ++;
  }
  return res;
}

void printError() {
  Serial.print(F("Error: r              -- read currents in mA" "\r\n"
		 "       n <octal mask> -- switch on relays" "\r\n"
		 "       f <octal mask> -- switch off relays" "\r\n"
		 "       d              -- read relays setting" "\r\n"
		 "       m <octal mask> -- set splitter mode" "\r\n"
		 "       1/0            -- switch splitter ON/OFF" "\r\n"));
}

void printIdent() {
  Serial.println(F("PowerControl " VERSION));
}

void setup() {
  int i;
  
  Serial.begin(115200);
  printIdent();
#ifdef DEBUGTOG
  pinMode(12, OUTPUT);
  digitalWrite(12, LOW);
#endif

  // setup relay & clear adcvals
  for(i = 0; i < NCHAN; i++) {
    pinMode(relPin[i], OUTPUT);
    digitalWrite(relPin[i], LOW);
    adcvals[i] = 511.0; }

  // setup splitter
  pinMode(spPin0, OUTPUT);
  pinMode(spPin1, OUTPUT);
  pinMode(spPin2, OUTPUT);
  digitalWrite(spPin0, LOW);
  digitalWrite(spPin1, LOW);
  digitalWrite(spPin2, HIGH);

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
  pin = NCHAN-1;
  ADMUX = ANALOG_REF;   // REFS1&0, MUXi = 0 =>
  ADCSRB = (1<<ADTS2) | (0<<ADTS1) | (1<<ADTS0); // TC1 overflow
  DIDR0 = 0xFF;  // digital input disable
  DIDR2 = 0xFF;
  ADCSRA = (1<<ADEN) | (1<<ADATE) | (1<<ADIE) |
    (1<<ADPS2) | (0<<ADPS1) | (0<<ADPS0);  /* prescaler 16: 1MHz */
}

void loop() {
  int i;
  uint16_t val;
  char *ptr;
  float adc, ival;
  
  val = 0;  /* default for splitter on/off */
  ptr = readline();
  /* skip optional spaces */
  while(*ptr == ' ')
    ptr++;
  switch(*ptr++) {
  case 'r':   /* read currents [mA] */
    for (i = 0; i < NCHAN; i++) {
      if(digitalRead(relPin[i])) {
        cli();  /* atomically copy accumulated value */
        adc = adcvals[i];
        sei();
        adc = (1-alpha)*adc - offs[i]; /* ADC difference */
#ifdef QUAD
	ival = adc * (slopes[i] - qs[i] * adc);
#else
	ival = adc * slopes[i];
#endif
      } else
	ival = 0.0;   /* report zero current if relay switched off */
      Serial.print(ival, 1);
      Serial.print(' '); }
    Serial.println("\r\nOK");
    break;
  case 'n':   /* switch on relays */
    val = readOctal(ptr);
    switchOn(val);
    Serial.println("OK");
    break;
  case 'f':   /* switch off relays */
    val = readOctal(ptr);
    switchOff(val);
    Serial.println("OK");
    break;
  case 'd':
    readRelays();
    Serial.println("OK");
    break;
  case 'm':
    val = readOctal(ptr);
    digitalWrite(spPin0, val & 1);
    val >>= 1;
    digitalWrite(spPin1, val & 1);
    Serial.println("OK");
    break;
  case '1':
    val = 1;
  case '0':
    digitalWrite(spPin2, val);  /* val is 1 or 0 */
    Serial.println("OK");
    break;
  case '?':
    printIdent();
    break;
  default:
    printError();
  }
}
