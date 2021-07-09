/*
 * Calculate CRC32 over a file and print the result as hex
 * CRC32 defined as in U-BOOT (using zlib implementation)
 * Petr Tobiska <tobiska@fzu.cz>
 * 2021-07-09

 prerequisities: build zlib (TODO: put it into Makefile)
   wget https://zlib.net/zlib-1.2.11.tar.gz && tar zxf zlib-1.2.11.tar.gz
   cd zlib-1.2.11
   source /opt/xilinx/SDK/2015.2/settings64.sh (replace with your path Xilinx SDK)
   CC=arm-xilinx-linux-gnueabi-gcc ./configure --prefix=.
   make
   make DESTDIR=../zlib/ install
   cd ..

 optional clean up: rm zlib-1.2.11.tar.gz && rm -rf zlib-1.2.11
 (not necessary after successful build of zlib; include/lib files are in crc32/zlib)

 build crc32 binary: make (from crc32 directory)
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

#include "zlib.h"

#define BUFSIZE 4096
unsigned char buf[BUFSIZE];

int main(int argc, char **argv) {
  int fd;
  ssize_t n;
  u_int32_t crc;

  if (argc != 2) {
    fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
    exit(1); }

  if ((fd = open(argv[1], O_RDONLY)) <  0) {
    fprintf(stderr, "Cannot open file %s for reading\n", argv[1]);
    exit(2); }

  crc = crc32(0L, NULL, 0);
  while ((n = read(fd, buf, BUFSIZE)) > 0) {
    crc = crc32(crc, buf, n); }
  
  printf("%08x\n", crc);

  return 0;
}
