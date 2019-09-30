/*
 * Trigger generator and trigger
 * Petr Tobiska <tobiska@fzu.cz>
 * 2018-08-17

Send trigger (TRIG) and optionally delayed to output

 pins: 
 - trigger   PD2 - D2
 - outputs   PB0 - D8
             PB3 - D11
 */

#define VERSION "20190709"

#include<avr/io.h>
#include<Arduino.h>

#define PIN_TRIG  2    // port D, bit 2
#define PIN_OUT1  8    // port B, bit 0
#define PIN_OUT2  11   // port B, bit 3

#define BUFSIZE 10
char buffer[BUFSIZE];
uint8_t qus = 0;   // delay between trigger and output pulses

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
    }
  }
}

void printError() {
  Serial.println(F("Error: d <trigger delay>  .. set delay" "\n"
                   "       q                  .. query delay" "\n"
		   "       t                  .. trigger" "\n"
		   "       ?                  .. print identification"));
}

void printIdent() {
  Serial.println(F("TrigDelay " VERSION));
}

void setup() {
  // configure pins
  pinMode(PIN_TRIG, OUTPUT);
  pinMode(PIN_OUT1, OUTPUT);
  pinMode(PIN_OUT2, OUTPUT);
  digitalWrite(PIN_TRIG, LOW);
  digitalWrite(PIN_OUT1, LOW);
  digitalWrite(PIN_OUT2, LOW);

  Serial.begin(115200);
  printIdent();
}

void loop() {
  char c, *ptr;
  uint8_t tmp;

  ptr = readline();
  /* skip optional spaces */
  while(*ptr == ' ')
    ptr++;
  
  switch(*ptr++) {
  case 'd':   // set trigger delay
    while(*ptr == ' ')
      ptr++;
    tmp = 0;
    for( c = *ptr++; ('0' <= c) && (c <= '9'); c = *ptr++)
      tmp = 10*tmp + c - '0';
    qus = tmp;
    Serial.println("OK");
    break;
  case 'q':
    Serial.print(F("trigdelay [3/16 us]: "));
    Serial.println((int)qus, 10);
    break;
  case 't':   // make trigger pulse
    if(qus == 0) {
      cli();
      PORTD = _BV(2);
      PORTB = _BV(0) | _BV(3);
      _NOP();
      PORTD = 0;
      PORTB = 0;
      sei(); }
    else if(qus == 1) {
      PORTD = _BV(2);
      _NOP();
      _NOP();
      PORTD = 0;
      PORTB = _BV(0) | _BV(3);
      _NOP();
      _NOP();
      PORTB = 0; }
    else {
      tmp = qus - 1;
      PORTD = _BV(2);
      _NOP();
      _NOP();
      PORTD = 0;
      // busy wait
      __asm__ __volatile__ (
			    "loop: subi %0,1" "\n\t" // 1 cycle
			    "brne loop" : "=r" (tmp) : "0" (tmp) // 2 cycles
			    );
      PORTB = _BV(0) | _BV(3);
      _NOP();
      _NOP();
      PORTB = 0; }
    Serial.println("OK");
    break;
  case '?':
    printIdent();
    break;
  default:
    printError();
  }
}
