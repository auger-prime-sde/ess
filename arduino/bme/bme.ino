/*

 Reading 2 temperature/humidity/pressure sensors (BME280) together
 with real time (DS3231)
 Petr Tobiska <petr.tobiska@gmail.com>
 2017-01-15
*/

#define VERSION "2020-03-26"

#include <Wire.h>
#include <SPI.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>
#include <OneWire.h>
#include <DallasTemperature.h>

#define DS3231_ADDRESS  0x68
#define DS3231_CONTROL  0x0E
#define DS3231_STATUSREG 0x0F
#define BME_ADDRESS  0x76
// flags
#define F_MANUAL 0x01
#define F_ACTION 0x02
#define F_BME1   0x04
#define F_BME2   0x08
#define F_CLOCK  0x10  /* read and print RTC */

int flags;
int counter_max;
int counter_act;
#define TIMESTR_LEN 19
char timestr[TIMESTR_LEN+1];   // 'YYYY-mm-ddTHH:MM:SS\0'
Adafruit_BME280 bme1, bme2; // I2C

#define DSMAX 10      // max. number of DS18B20 sensors
const int pinDS = 4;  // pin where DS18B20 are attached
int nDS = 0;          // the number of detected sensors
uint8_t addrDS[DSMAX][8];   // their hw addresses
OneWire oneWireDS(pinDS);
DallasTemperature ds(&oneWireDS);

// helper functions
// static uint8_t bcd2bin (uint8_t val) { return val - 6 * (val >> 4); }
// static uint8_t bin2bcd (uint8_t val) { return val + 6 * (val / 10); }

/*
  Detect DS18B20 sensors attached
  Store their HW addresses to provided array
 */
int findDS(OneWire *ow, uint8_t (*addr)[8]) {
  int i = 0;
  while(ow->search(addr[i])) {
    if(addr[i][0] == DS18B20MODEL &&
       OneWire::crc8(addr[i], 7) == addr[i][7])
      if(++i == DSMAX)
	break; }
  return i;
}

/* print hw addresses of detected DS18B20 sensors */
void printDS(int nds, uint8_t (*addr)[8]) {
  int j, i = 0;
  uint8_t x;
 
 for (i = 0; i < nds; i++) {
   Serial.print("DS["); Serial.print(i);
    Serial.print("]: 28-");
    for(j = 6; j > 0; j--) {
      if((x = addr[i][j]) < 0x10)
	Serial.write('0');
      Serial.print(x, HEX); }
    Serial.println(); }
}
	     
static uint8_t read_i2c_register(uint8_t addr, uint8_t reg) {
  Wire.beginTransmission(addr);
  Wire.write((byte)reg);
  Wire.endTransmission();

  Wire.requestFrom(addr, (byte)1);
  return Wire.read();
}

static void write_i2c_register(uint8_t addr, uint8_t reg, uint8_t val) {
  Wire.beginTransmission(addr);
  Wire.write((byte)reg);
  Wire.write((byte)val);
  Wire.endTransmission();
}

/*
 * Read time from DS3231 and store as string to timestr as 'YYYYmmddTHHMMSS\0'
 */
void readTime() {
  unsigned char Y, m, d, H, M, S;
  char* tptr;

  // Read DS3231 registers
  Wire.beginTransmission(DS3231_ADDRESS);
  Wire.write((byte)0);  
  Wire.endTransmission();

  Wire.requestFrom(DS3231_ADDRESS, 7);
  S = Wire.read();
  M = Wire.read();
  H = Wire.read();
  Wire.read();      // skip day in week
  d = Wire.read();
  m = Wire.read();
  Y = Wire.read();

  // store in timestr
  tptr = timestr;
  // year in 21st century
  *tptr++ = '2'; *tptr++ = '0'; *tptr++ = '0'+(Y >> 4); *tptr++ = '0'+(Y & 0x0F);
  *tptr++ = '-'; *tptr++ = m & 0x10 ? '1': '0'; *tptr++ = '0'+(m & 0x0F);
  *tptr++ = '-'; *tptr++ = '0'+(d >> 4); *tptr++ = '0'+(d & 0x0F);
  // 24h clock; not checked
  *tptr++ = 'T'; *tptr++ = '0'+(H >> 4); *tptr++ = '0'+(H & 0x0F);
  *tptr++ = ':'; *tptr++ = '0'+(M >> 4); *tptr++ = '0'+(M & 0x0F);
  *tptr++ = ':'; *tptr++ = '0'+(S >> 4); *tptr++ = '0'+(S & 0x0F);
  *tptr = '\0';
}

/*
 * Set time according to timestr buffer
 * return false on error
 */
int setTime() {
  unsigned char Y, m, d, H, M, S;
  char* tptr;
  
  // parse timestr
  tptr = timestr;
  if( *tptr++ != '2' )
    return false;
  if( *tptr++ != '0' )
    return false;
  Y = (*tptr++ - '0')<<4; Y += *tptr++ - '0';
  if(*tptr++ != '-')
    return false;
  m = (*tptr++ - '0')<<4; m += *tptr++ - '0';
  if(*tptr++ != '-')
    return false;
  d = (*tptr++ - '0')<<4; d += *tptr++ - '0';
  if(*tptr++ != 'T')
    return false;
  H = (*tptr++ - '0')<<4; H += *tptr++ - '0';
  if(*tptr++ != ':')
    return false;
  M = (*tptr++ - '0')<<4; M += *tptr++ - '0';
  if(*tptr++ != ':')
    return false;
  S = (*tptr++ - '0')<<4; S += *tptr++ - '0';
  
  Wire.beginTransmission(DS3231_ADDRESS);
  Wire.write((byte)0);         // start at location 0
  Wire.write((byte)S);
  Wire.write((byte)M);
  Wire.write((byte)H);
  Wire.write((byte)0);
  Wire.write((byte)d);
  Wire.write((byte)m);
  Wire.write((byte)Y);
  Wire.endTransmission();

  uint8_t statreg = read_i2c_register(DS3231_ADDRESS, DS3231_STATUSREG);
  statreg &= ~0x80; // flip OSF bit
  write_i2c_register(DS3231_ADDRESS, DS3231_STATUSREG, statreg);
  return true;
}

void skipSpaces() {
  while(Serial.available()) {
    if(Serial.peek() == ' ' || Serial.peek() == '\r')
      Serial.read();
    else
      return; }
}

/* 
 * read unsigned int from serial terminal
 * defaults to zero
 */
unsigned int readUint() {
  unsigned int res = 0;
  char c;

  /* skip optional spaces */
  while(1) {
    while(! Serial.available())
      delay(1);
    if(Serial.peek() == ' ')
      Serial.read();
    else
      break;
  }
  /* read digits until non-digit character */
  while(1) {
    while(! Serial.available())
      delay(1);
    c = Serial.read();
    if( '0' <= c && c <= '9' )
      res = 10*res + c - '0';
    else
      return(res);
  }
}

void printOK() {
  Serial.println(F("OK"));
}

void printIdent() {
  Serial.println(F("BME " VERSION));
}

void printError() {
  Serial.println(F("Error: t YYYY-mm-ddTHH:MM:SS .. set RTC" "\n"
		   "       d                     .. print DS18B20 addresses" "\n"
                   "       r/R                   .. use/do not use RTC" "\n"
		   "       m                     .. manual measurement" "\n"
		   "       c <interval [s]       .. start cyclic measurement" "\n"
		   "       ?                     .. print identification"));
}

/*
 * perform measurement action
 */
void action() {
  int i;
  unsigned long time;

  if(nDS > 0) {
    ds.requestTemperatures();
    time = millis() + 100 /* to be safe interval */
      + ds.millisToWaitForConversion(ds.getResolution()); }

  if(flags & F_CLOCK) {
    readTime();
    Serial.print(timestr); }
  if(flags & F_BME1){
    Serial.print(" ");
    Serial.print(bme1.readTemperature());
    Serial.print(" ");
    Serial.print(bme1.readHumidity());
    Serial.print(" ");
    Serial.print(bme1.readPressure()/100.0);
  }
  if(flags & F_BME2){
    Serial.print(" ");
    Serial.print(bme2.readTemperature());
    Serial.print(" ");
    Serial.print(bme2.readHumidity());
    Serial.print(" ");
    Serial.print(bme2.readPressure()/100.0);
  }

  /* wait for conversion completed: read bit returns 1 */
  if(nDS > 0) {
    while(millis() < time && oneWireDS.read_bit() == 0)
      delay(100); }
  
  for(i = 0; i < nDS; i++) {
    Serial.print(" ");
    Serial.print(ds.getTempC(addrDS[i])); }

  Serial.println();
}

void setup() {
  // put your setup code here, to run once:
  Serial.begin(115200);
  printIdent();

  Wire.begin();
  delay(500); // some time to I2C settle down
  flags = F_MANUAL;
  if(bme1.begin(BME_ADDRESS)) {
    flags |= F_BME1;
    Serial.println(F("BME1 detected")); }
  if(bme2.begin(BME_ADDRESS+1)) {
    flags |= F_BME2;
    Serial.println(F("BME2 detected")); }
  if(!(flags & (F_BME1|F_BME2)))
    Serial.println(F("No BME detected"));
  if((nDS = findDS(&oneWireDS, addrDS)) > 0) {
    Serial.print(F("Detected "));
    Serial.print(nDS);
    Serial.println(F(" DS18B20 sensor(s)")); }
  ds.begin();
  ds.setWaitForConversion(false);
}

void loop() {
  long time = millis();
  int i;
  skipSpaces();
  if(Serial.available()) {
    switch(Serial.read()) {
    case 't':   // set time
      skipSpaces();
      for(i=0; i < TIMESTR_LEN && Serial.available(); i++)
	timestr[i] = Serial.read();
      timestr[i] = '\0';
      if(!setTime())
        Serial.println("set time ERROR");
      else
	printOK();
      break;

    case 'd':  /* print DS18B20 hw addresses */
      printDS(nDS, addrDS);
      printOK();
      break;

    case 'R':  /* do not use RTC */
      flags &= ~F_CLOCK;
      printOK();
      break;

    case 'r':  /* use RTC */
      flags |= F_CLOCK;
      printOK();
      break;

    case 'm':  /* measure */
      flags |= F_MANUAL | F_ACTION;
      break;

    case 'c':  /* start cyclic measurement */
      if(( i= readUint()) > 0 ) {
	flags &= ~(F_MANUAL | F_ACTION);
	counter_max = i;
	counter_act = 1; }
      else
	Serial.println("Error: c <interval/s>");
      break;

    case '?':
      printIdent();
      break;

    default:
      printError();
    }
    while(Serial.available())  /* skip rest of input */
      Serial.read();
  }

  if(!(flags & F_MANUAL))
    if(--counter_act == 0) {
      flags |= F_ACTION;
      counter_act = counter_max; }

  if(flags & F_ACTION) {
    action();
    flags &= ~F_ACTION;
  }
  
  if(!(flags & F_MANUAL)) {
    time += 1000 - millis();  /* reuse time */
    if(time > 0)
      delay(time); }
}
