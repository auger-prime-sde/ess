/*
 * Trigger delay & fan-out
 * Petr Tobiska <tobiska@fzu.cz>
 * 2018-08-17

Send TRIG and two (optionally delayed) OUTPUT pulses

 pins: 
 - TRIG      PB2 - D10  inverted
 - OUTPUT    PB0 - D8
             PB3 - D11
 	     
 on output pins are drivers TC 4427A ~ 40ns delay and are fan-outed to 11 SMA with 50 Ohm
 trigger - to AFG
 pulse length: 3 clocks ~ 180ns
 */

#define VERSION "20200212"

#include<avr/io.h>
#include<Arduino.h>

#define PORT      PORTB
#define DDR       DDRB
#define B_TRIG    _BV(PB2)
#define B_OUT     _BV(PB0) | _BV(PB3)

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
  DDR = B_TRIG | B_OUT;    // configure pins output
  PORT = B_TRIG;           // out low, trigger high

  Serial.begin(115200);
  printIdent();
}

void loop() {
  char c, *ptr;
  uint8_t tmp, oldSREG;

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
      oldSREG = SREG;
      cli();
      PORT = B_OUT;
      _NOP();
      _NOP();
      PORT = B_TRIG;
      SREG = oldSREG; }
    else if(qus == 1) {
      oldSREG = SREG;
      cli();
      PORT = 0;
      _NOP();
      _NOP();
      PORT = B_OUT | B_TRIG;
      _NOP();
      _NOP();
      PORT = B_TRIG;
      SREG = oldSREG; }
    else {
      oldSREG = SREG;
      cli();
      tmp = qus - 1;
      PORT = 0;
      _NOP();
      _NOP();
      PORT = B_TRIG;
      // busy wait
      __asm__ __volatile__ (
			    "loop: subi %0,1" "\n\t" // 1 cycle
			    "brne loop" : "=r" (tmp) : "0" (tmp) // 2 cycles
			    );
      PORT = B_OUT | B_TRIG;
      _NOP();
      _NOP();
      PORT = B_TRIG;
      SREG = oldSREG;}
    Serial.println("OK");
    break;
  case '?':
    printIdent();
    break;
  default:
    printError();
  }
}
