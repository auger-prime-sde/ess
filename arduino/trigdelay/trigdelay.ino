/*
 * Delay trigger pulse
 * Petr Tobiska <tobiska@fzu.cz>
 * 2018-08-17

Wait for falling edge on pin INPUT. Wait and generate pulse on output pins.

 pins: 
 - input   PD2 (INT0) - D2
 - output  PB0 - D8
           PB3 - D11

 */

#define VERSION "20180830"

#include<avr/io.h>
#include<Arduino.h>

#define PIN_INPUT 2    // port D, bit 2
#define PIN_OUT1  8    // port B, bit 0
#define PIN_OUT2  11   // port B, bit 3
#define P_OUT _SFR_IO_ADDR(PORTB)

//volatile uint16_t wait_qus;   // 16/4 us to wait
volatile uint8_t wait_qus;   // 16/3 us to wait
#define BUFSIZE 10
char buffer[BUFSIZE];

ISR(INT0_vect) {
  uint8_t qus = wait_qus;
  if(qus > 0) {
    // busy wait
    __asm__ __volatile__ (
        "loop: subi %0,1" "\n\t" // 2 cycles
        "brne loop" : "=r" (qus) : "0" (qus) // 2 cycles
        );
  }
  // digitalWrite(PIN_OUT1, HIGH);
  // PORTD |= B01000000;
  // digitalWrite(PIN_OUT1, LOW);
  //  PORTD &= ~B01000000;
  // __asm__ __volatile__  (
  //                     " sbi %[PORT],%[BIT]    ;set data bit \n\t"
  //                     " nop \n\t nop \n\t nop \n\t nop \n\t nop \n\t nop \n\t nop \n\t"
  //                     " cbi %[PORT],%[BIT]    ;clear data bit \n\t"
  //                     : :
  //                       [PORT] "I" (P_OUT), [BIT] "I" (0));

  
  PORTB = _BV(0) | _BV(3);
  _NOP(); _NOP(); _NOP(); _NOP();
  PORTB = 0;   
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

void printError() {
  Serial.println(F("Error: d <trigger delay>\\r" "\n"
                   "       q\\r"));
}

void printIdent() {
  Serial.println(F("TrigDelay " VERSION));
}

void setup() {
  Serial.begin(115200);
  // configure outputs
  pinMode(PIN_INPUT, INPUT_PULLUP);
  pinMode(PIN_OUT1, OUTPUT);
  pinMode(PIN_OUT2, OUTPUT);
  digitalWrite(PIN_OUT1, LOW);
  digitalWrite(PIN_OUT2, LOW);

  // configure INT0
  EICRA = _BV(ISC00) | _BV(ISC01);
  EIMSK = _BV(INT0);

  // disable Timer0 overflow interrupt (for millis et al.)
  TIMSK0 &= ~TOIE0;

  wait_qus = 0;
  printIdent();
}

void loop() {
  char c, *ptr;
  uint8_t res;

  ptr = readline();
  /* skip optional spaces */
  while(*ptr == ' ')
    ptr++;
  
  switch(*ptr++) {
  case 'd':   // set trigger delay
    while(*ptr == ' ')
      ptr++;
    res = 0;
    for( c = *ptr++; ('0' <= c) && (c <= '9'); c = *ptr++)
      res = 10*res + c - '0';
    wait_qus = res;
    Serial.println("OK");
    break;
  case 'q':
    Serial.print(F("trigdelay [3/16 us]: "));
    Serial.println((int)wait_qus, 10);
    break;
  case '?':
    printIdent();
    break;
  default:
    printError();
  }
}
