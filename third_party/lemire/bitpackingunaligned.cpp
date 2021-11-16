#include <exception> // for std::logic_error
#include "bitpackingunaligned.h"

using FastPForLib::byte;

typedef const byte *(*runpacker)(const byte *__restrict__ in,
                                 uint32_t *__restrict__ out);
typedef byte *(*rpacker)(const uint32_t *__restrict__ in,
                         byte *__restrict__ out);

typedef const byte *(*rbyteunpacker)(const byte *__restrict__ in,
                                     byte *__restrict__ out);
typedef byte *(*rbytepacker)(const byte *__restrict__ in,
                             byte *__restrict__ out);

byte *nullpacker(const uint32_t *__restrict__ /*in*/, byte *__restrict__ out) {
  return out;
}

byte *nullbytepacker(const byte *__restrict__ /*in*/, byte *__restrict__ out) {
  return out;
}

const byte *nullunpacker8(const byte *__restrict__ in,
                          uint32_t *__restrict__ out) {
  memset(out, 0, 8 * 4);
  return in;
}

const byte *nullbyteunpacker8(const byte *__restrict__ in,
                              byte *__restrict__ out) {
  memset(out, 0, 8);
  return in;
}

byte *__fastunalignedpackwithoutmask1_8(const uint32_t *__restrict__ in,
                                        byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 7;
  ++in;

  return reinterpret_cast<byte *>(out) + 1;
}

byte *__fastunalignedpackwithoutmask2_8(const uint32_t *__restrict__ in,
                                        byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 14;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask3_8(const uint32_t *__restrict__ in,
                                        byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 15;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 21;
  ++in;

  return reinterpret_cast<byte *>(out) + 3;
}

byte *__fastunalignedpackwithoutmask4_8(const uint32_t *__restrict__ in,
                                        byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 24;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask5_8(const uint32_t *__restrict__ in,
                                        byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 15;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 25;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (5 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;

  return reinterpret_cast<byte *>(out) + 1;
}

byte *__fastunalignedpackwithoutmask6_8(const uint32_t *__restrict__ in,
                                        byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 24;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (6 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 10;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask7_8(const uint32_t *__restrict__ in,
                                        byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 21;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (7 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 17;
  ++in;

  return reinterpret_cast<byte *>(out) + 3;
}

byte *__fastunalignedpackwithoutmask8_8(const uint32_t *__restrict__ in,
                                        byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask9_8(const uint32_t *__restrict__ in,
                                        byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (9 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 22;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (9 - 8);
  ++in;

  return reinterpret_cast<byte *>(out) + 1;
}

byte *__fastunalignedpackwithoutmask10_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (10 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (10 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask11_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (11 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (11 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 13;
  ++in;

  return reinterpret_cast<byte *>(out) + 3;
}

byte *__fastunalignedpackwithoutmask12_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (12 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (12 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask13_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (13 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (13 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (13 - 8);
  ++in;

  return reinterpret_cast<byte *>(out) + 1;
}

byte *__fastunalignedpackwithoutmask14_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (14 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (14 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (14 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask15_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 15;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (15 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (15 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (15 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++in;

  return reinterpret_cast<byte *>(out) + 3;
}

byte *__fastunalignedpackwithoutmask16_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask17_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (17 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (17 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (17 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (17 - 8);
  ++in;

  return reinterpret_cast<byte *>(out) + 1;
}

byte *__fastunalignedpackwithoutmask18_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (18 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (18 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (18 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (18 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask19_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (19 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (19 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (19 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (19 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;

  return reinterpret_cast<byte *>(out) + 3;
}

byte *__fastunalignedpackwithoutmask20_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (20 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (20 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (20 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (20 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask21_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (21 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (21 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (21 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (21 - 19);
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (21 - 8);
  ++in;

  return reinterpret_cast<byte *>(out) + 1;
}

byte *__fastunalignedpackwithoutmask22_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (22 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (22 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (22 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (22 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (22 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask23_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (23 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (23 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (23 - 19);
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (23 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  *out = ((*in)) >> (23 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;

  return reinterpret_cast<byte *>(out) + 3;
}

byte *__fastunalignedpackwithoutmask24_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (24 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (24 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (24 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (24 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask25_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (25 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (25 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++out;
  *out = ((*in)) >> (25 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (25 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (25 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (25 - 8);
  ++in;

  return reinterpret_cast<byte *>(out) + 1;
}

byte *__fastunalignedpackwithoutmask26_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (26 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (26 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (26 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  *out = ((*in)) >> (26 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (26 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (26 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask27_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (27 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (27 - 17);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (27 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (27 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++out;
  *out = ((*in)) >> (27 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (27 - 24);
  ++in;

  return reinterpret_cast<byte *>(out) + 3;
}

byte *__fastunalignedpackwithoutmask28_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (28 - 24);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (28 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (28 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (28 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (28 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  *out = ((*in)) >> (28 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask29_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (29 - 26);
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (29 - 23);
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (29 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (29 - 17);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (29 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (29 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++out;
  *out = ((*in)) >> (29 - 8);
  ++in;

  return reinterpret_cast<byte *>(out) + 1;
}

byte *__fastunalignedpackwithoutmask30_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (30 - 28);
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (30 - 26);
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (30 - 24);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (30 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (30 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (30 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (30 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask31_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (31 - 30);
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (31 - 29);
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (31 - 28);
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (31 - 27);
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (31 - 26);
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (31 - 25);
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (31 - 24);
  ++in;

  return reinterpret_cast<byte *>(out) + 3;
}

byte *__fastunalignedpackwithoutmask32_8(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

const byte *__fastunalignedunpack1_8(const byte *__restrict__ inbyte,
                                     uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) & 1;
  out++;
  *out = ((*in) >> 1) & 1;
  out++;
  *out = ((*in) >> 2) & 1;
  out++;
  *out = ((*in) >> 3) & 1;
  out++;
  *out = ((*in) >> 4) & 1;
  out++;
  *out = ((*in) >> 5) & 1;
  out++;
  *out = ((*in) >> 6) & 1;
  out++;
  *out = ((*in) >> 7) & 1;
  out++;

  return reinterpret_cast<const byte *>(in) + 1;
}

const byte *__fastunalignedunpack2_8(const byte *__restrict__ inbyte,
                                     uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 2);
  out++;
  *out = ((*in) >> 2) % (1U << 2);
  out++;
  *out = ((*in) >> 4) % (1U << 2);
  out++;
  *out = ((*in) >> 6) % (1U << 2);
  out++;
  *out = ((*in) >> 8) % (1U << 2);
  out++;
  *out = ((*in) >> 10) % (1U << 2);
  out++;
  *out = ((*in) >> 12) % (1U << 2);
  out++;
  *out = ((*in) >> 14) % (1U << 2);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack3_8(const byte *__restrict__ inbyte,
                                     uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 3);
  out++;
  *out = ((*in) >> 3) % (1U << 3);
  out++;
  *out = ((*in) >> 6) % (1U << 3);
  out++;
  *out = ((*in) >> 9) % (1U << 3);
  out++;
  *out = ((*in) >> 12) % (1U << 3);
  out++;
  *out = ((*in) >> 15) % (1U << 3);
  out++;
  *out = ((*in) >> 18) % (1U << 3);
  out++;
  *out = ((*in) >> 21) % (1U << 3);
  out++;

  return reinterpret_cast<const byte *>(in) + 3;
}

const byte *__fastunalignedunpack4_8(const byte *__restrict__ inbyte,
                                     uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  out++;
  *out = ((*in) >> 8) % (1U << 4);
  out++;
  *out = ((*in) >> 12) % (1U << 4);
  out++;
  *out = ((*in) >> 16) % (1U << 4);
  out++;
  *out = ((*in) >> 20) % (1U << 4);
  out++;
  *out = ((*in) >> 24) % (1U << 4);
  out++;
  *out = ((*in) >> 28);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack5_8(const byte *__restrict__ inbyte,
                                     uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 5);
  out++;
  *out = ((*in) >> 5) % (1U << 5);
  out++;
  *out = ((*in) >> 10) % (1U << 5);
  out++;
  *out = ((*in) >> 15) % (1U << 5);
  out++;
  *out = ((*in) >> 20) % (1U << 5);
  out++;
  *out = ((*in) >> 25) % (1U << 5);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 3)) << (5 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 5);
  out++;

  return reinterpret_cast<const byte *>(in) + 1;
}

const byte *__fastunalignedunpack6_8(const byte *__restrict__ inbyte,
                                     uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 6);
  out++;
  *out = ((*in) >> 6) % (1U << 6);
  out++;
  *out = ((*in) >> 12) % (1U << 6);
  out++;
  *out = ((*in) >> 18) % (1U << 6);
  out++;
  *out = ((*in) >> 24) % (1U << 6);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 4)) << (6 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 6);
  out++;
  *out = ((*in) >> 10) % (1U << 6);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack7_8(const byte *__restrict__ inbyte,
                                     uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 7);
  out++;
  *out = ((*in) >> 7) % (1U << 7);
  out++;
  *out = ((*in) >> 14) % (1U << 7);
  out++;
  *out = ((*in) >> 21) % (1U << 7);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 3)) << (7 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 7);
  out++;
  *out = ((*in) >> 10) % (1U << 7);
  out++;
  *out = ((*in) >> 17) % (1U << 7);
  out++;

  return reinterpret_cast<const byte *>(in) + 3;
}

const byte *__fastunalignedunpack8_8(const byte *__restrict__ inbyte,
                                     uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 8);
  out++;
  *out = ((*in) >> 8) % (1U << 8);
  out++;
  *out = ((*in) >> 16) % (1U << 8);
  out++;
  *out = ((*in) >> 24);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  out++;
  *out = ((*in) >> 8) % (1U << 8);
  out++;
  *out = ((*in) >> 16) % (1U << 8);
  out++;
  *out = ((*in) >> 24);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack9_8(const byte *__restrict__ inbyte,
                                     uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 9);
  out++;
  *out = ((*in) >> 9) % (1U << 9);
  out++;
  *out = ((*in) >> 18) % (1U << 9);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 4)) << (9 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 9);
  out++;
  *out = ((*in) >> 13) % (1U << 9);
  out++;
  *out = ((*in) >> 22) % (1U << 9);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 8)) << (9 - 8);
  out++;

  return reinterpret_cast<const byte *>(in) + 1;
}

const byte *__fastunalignedunpack10_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 10);
  out++;
  *out = ((*in) >> 10) % (1U << 10);
  out++;
  *out = ((*in) >> 20) % (1U << 10);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 8)) << (10 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 10);
  out++;
  *out = ((*in) >> 18) % (1U << 10);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 6)) << (10 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 10);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack11_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 11);
  out++;
  *out = ((*in) >> 11) % (1U << 11);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 1)) << (11 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 11);
  out++;
  *out = ((*in) >> 12) % (1U << 11);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 2)) << (11 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 11);
  out++;
  *out = ((*in) >> 13) % (1U << 11);
  out++;

  return reinterpret_cast<const byte *>(in) + 3;
}

const byte *__fastunalignedunpack12_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 12);
  out++;
  *out = ((*in) >> 12) % (1U << 12);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 4)) << (12 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 12);
  out++;
  *out = ((*in) >> 16) % (1U << 12);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 8)) << (12 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 12);
  out++;
  *out = ((*in) >> 20);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack13_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 13);
  out++;
  *out = ((*in) >> 13) % (1U << 13);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 7)) << (13 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 13);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 1)) << (13 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 13);
  out++;
  *out = ((*in) >> 14) % (1U << 13);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 8)) << (13 - 8);
  out++;

  return reinterpret_cast<const byte *>(in) + 1;
}

const byte *__fastunalignedunpack14_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 14);
  out++;
  *out = ((*in) >> 14) % (1U << 14);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 10)) << (14 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 14);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 6)) << (14 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 14);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 2)) << (14 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 14);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack15_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 15);
  out++;
  *out = ((*in) >> 15) % (1U << 15);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 13)) << (15 - 13);
  out++;
  *out = ((*in) >> 13) % (1U << 15);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 11)) << (15 - 11);
  out++;
  *out = ((*in) >> 11) % (1U << 15);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 9)) << (15 - 9);
  out++;
  *out = ((*in) >> 9) % (1U << 15);
  out++;

  return reinterpret_cast<const byte *>(in) + 3;
}

const byte *__fastunalignedunpack16_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack17_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 2)) << (17 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 17);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 4)) << (17 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 17);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 6)) << (17 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 17);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 8)) << (17 - 8);
  out++;

  return reinterpret_cast<const byte *>(in) + 1;
}

const byte *__fastunalignedunpack18_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 4)) << (18 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 18);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 8)) << (18 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 18);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 12)) << (18 - 12);
  out++;
  *out = ((*in) >> 12) % (1U << 18);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 16)) << (18 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack19_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 19);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 6)) << (19 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 19);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 12)) << (19 - 12);
  out++;
  *out = ((*in) >> 12) % (1U << 19);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 18)) << (19 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 5)) << (19 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 19);
  out++;

  return reinterpret_cast<const byte *>(in) + 3;
}

const byte *__fastunalignedunpack20_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 8)) << (20 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 20);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 16)) << (20 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 4)) << (20 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 20);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 12)) << (20 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack21_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 21);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 10)) << (21 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 21);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 20)) << (21 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 9)) << (21 - 9);
  out++;
  *out = ((*in) >> 9) % (1U << 21);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 19)) << (21 - 19);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 8)) << (21 - 8);
  out++;

  return reinterpret_cast<const byte *>(in) + 1;
}

const byte *__fastunalignedunpack22_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 12)) << (22 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 2)) << (22 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 22);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 14)) << (22 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 4)) << (22 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 22);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 16)) << (22 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack23_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 23);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 14)) << (23 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 5)) << (23 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 23);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 19)) << (23 - 19);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 10)) << (23 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  *out |= ((*in) % (1U << 1)) << (23 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 23);
  out++;

  return reinterpret_cast<const byte *>(in) + 3;
}

const byte *__fastunalignedunpack24_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 16)) << (24 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 8)) << (24 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 16)) << (24 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 8)) << (24 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack25_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 25);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 18)) << (25 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 11)) << (25 - 11);
  out++;
  *out = ((*in) >> 11);
  ++in;
  *out |= ((*in) % (1U << 4)) << (25 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 25);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 22)) << (25 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 15)) << (25 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 8)) << (25 - 8);
  out++;

  return reinterpret_cast<const byte *>(in) + 1;
}

const byte *__fastunalignedunpack26_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 26);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 20)) << (26 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 14)) << (26 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 8)) << (26 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  *out |= ((*in) % (1U << 2)) << (26 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 26);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 22)) << (26 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 16)) << (26 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack27_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 27);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 22)) << (27 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 17)) << (27 - 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 12)) << (27 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 7)) << (27 - 7);
  out++;
  *out = ((*in) >> 7);
  ++in;
  *out |= ((*in) % (1U << 2)) << (27 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 27);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 24)) << (27 - 24);
  out++;

  return reinterpret_cast<const byte *>(in) + 3;
}

const byte *__fastunalignedunpack28_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 28);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 24)) << (28 - 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 20)) << (28 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 16)) << (28 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 12)) << (28 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 8)) << (28 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  *out |= ((*in) % (1U << 4)) << (28 - 4);
  out++;
  *out = ((*in) >> 4);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack29_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 29);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 26)) << (29 - 26);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 23)) << (29 - 23);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 20)) << (29 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 17)) << (29 - 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 14)) << (29 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 11)) << (29 - 11);
  out++;
  *out = ((*in) >> 11);
  ++in;
  *out |= ((*in) % (1U << 8)) << (29 - 8);
  out++;

  return reinterpret_cast<const byte *>(in) + 1;
}

const byte *__fastunalignedunpack30_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 30);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 28)) << (30 - 28);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 26)) << (30 - 26);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 24)) << (30 - 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 22)) << (30 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 20)) << (30 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 18)) << (30 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 16)) << (30 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack31_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 31);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 30)) << (31 - 30);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 29)) << (31 - 29);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 28)) << (31 - 28);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 27)) << (31 - 27);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 26)) << (31 - 26);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 25)) << (31 - 25);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 24)) << (31 - 24);
  out++;

  return reinterpret_cast<const byte *>(in) + 3;
}

const byte *__fastunalignedunpack32_8(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

byte *__fastunalignedbytepackwithoutmask1_8(const byte *__restrict__ in,
                                            byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask2_8(const byte *__restrict__ in,
                                            byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask3_8(const byte *__restrict__ in,
                                            byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (3 - 1));
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  *out = static_cast<byte>(((*in)) >> (3 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask4_8(const byte *__restrict__ in,
                                            byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask5_8(const byte *__restrict__ in,
                                            byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 1));
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 3));
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask6_8(const byte *__restrict__ in,
                                            byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask7_8(const byte *__restrict__ in,
                                            byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 6));
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 5));
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 3));
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 1));
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask8_8(const byte *__restrict__ in,
                                            byte *__restrict__ out) {

  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;

  return out;
}

const byte *__fastunalignedbyteunpack1_8(const byte *__restrict__ in,
                                         byte *__restrict__ out) {

  *out = ((*in) >> 0) & 1;
  out++;
  *out = ((*in) >> 1) & 1;
  out++;
  *out = ((*in) >> 2) & 1;
  out++;
  *out = ((*in) >> 3) & 1;
  out++;
  *out = ((*in) >> 4) & 1;
  out++;
  *out = ((*in) >> 5) & 1;
  out++;
  *out = ((*in) >> 6) & 1;
  out++;
  *out = ((*in) >> 7) & 1;
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack2_8(const byte *__restrict__ in,
                                         byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 2);
  out++;
  *out = ((*in) >> 2) % (1U << 2);
  out++;
  *out = ((*in) >> 4) % (1U << 2);
  out++;
  *out = ((*in) >> 6) % (1U << 2);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 2);
  out++;
  *out = ((*in) >> 2) % (1U << 2);
  out++;
  *out = ((*in) >> 4) % (1U << 2);
  out++;
  *out = ((*in) >> 6) % (1U << 2);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack3_8(const byte *__restrict__ in,
                                         byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 3);
  out++;
  *out = ((*in) >> 3) % (1U << 3);
  out++;
  *out = ((*in) >> 6) % (1U << 3);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 1)) << (3 - 1));
  out++;
  *out = ((*in) >> 1) % (1U << 3);
  out++;
  *out = ((*in) >> 4) % (1U << 3);
  out++;
  *out = ((*in) >> 7) % (1U << 3);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (3 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 3);
  out++;
  *out = ((*in) >> 5) % (1U << 3);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack4_8(const byte *__restrict__ in,
                                         byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack5_8(const byte *__restrict__ in,
                                         byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 5);
  out++;
  *out = ((*in) >> 5) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (5 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 5);
  out++;
  *out = ((*in) >> 7) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (5 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 1)) << (5 - 1));
  out++;
  *out = ((*in) >> 1) % (1U << 5);
  out++;
  *out = ((*in) >> 6) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 3)) << (5 - 3));
  out++;
  *out = ((*in) >> 3) % (1U << 5);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack6_8(const byte *__restrict__ in,
                                         byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 6);
  out++;
  *out = ((*in) >> 6) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (6 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (6 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 6);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 6);
  out++;
  *out = ((*in) >> 6) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (6 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (6 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 6);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack7_8(const byte *__restrict__ in,
                                         byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 7);
  out++;
  *out = ((*in) >> 7) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 6)) << (7 - 6));
  out++;
  *out = ((*in) >> 6) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 5)) << (7 - 5));
  out++;
  *out = ((*in) >> 5) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (7 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 3)) << (7 - 3));
  out++;
  *out = ((*in) >> 3) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (7 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 1)) << (7 - 1));
  out++;
  *out = ((*in) >> 1) % (1U << 7);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack8_8(const byte *__restrict__ in,
                                         byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;

  return in;
}

const byte *fastunalignedunpack_8(const byte *__restrict__ in,
                                  uint32_t *__restrict__ out,
                                  const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullunpacker8(in, out);

  case 1:
    return __fastunalignedunpack1_8(in, out);

  case 2:
    return __fastunalignedunpack2_8(in, out);

  case 3:
    return __fastunalignedunpack3_8(in, out);

  case 4:
    return __fastunalignedunpack4_8(in, out);

  case 5:
    return __fastunalignedunpack5_8(in, out);

  case 6:
    return __fastunalignedunpack6_8(in, out);

  case 7:
    return __fastunalignedunpack7_8(in, out);

  case 8:
    return __fastunalignedunpack8_8(in, out);

  case 9:
    return __fastunalignedunpack9_8(in, out);

  case 10:
    return __fastunalignedunpack10_8(in, out);

  case 11:
    return __fastunalignedunpack11_8(in, out);

  case 12:
    return __fastunalignedunpack12_8(in, out);

  case 13:
    return __fastunalignedunpack13_8(in, out);

  case 14:
    return __fastunalignedunpack14_8(in, out);

  case 15:
    return __fastunalignedunpack15_8(in, out);

  case 16:
    return __fastunalignedunpack16_8(in, out);

  case 17:
    return __fastunalignedunpack17_8(in, out);

  case 18:
    return __fastunalignedunpack18_8(in, out);

  case 19:
    return __fastunalignedunpack19_8(in, out);

  case 20:
    return __fastunalignedunpack20_8(in, out);

  case 21:
    return __fastunalignedunpack21_8(in, out);

  case 22:
    return __fastunalignedunpack22_8(in, out);

  case 23:
    return __fastunalignedunpack23_8(in, out);

  case 24:
    return __fastunalignedunpack24_8(in, out);

  case 25:
    return __fastunalignedunpack25_8(in, out);

  case 26:
    return __fastunalignedunpack26_8(in, out);

  case 27:
    return __fastunalignedunpack27_8(in, out);

  case 28:
    return __fastunalignedunpack28_8(in, out);

  case 29:
    return __fastunalignedunpack29_8(in, out);

  case 30:
    return __fastunalignedunpack30_8(in, out);

  case 31:
    return __fastunalignedunpack31_8(in, out);

  case 32:
    return __fastunalignedunpack32_8(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

/*assumes that integers fit in the prescribed number of bits*/
byte *fastunalignedpackwithoutmask_8(const uint32_t *__restrict__ in,
                                     byte *__restrict__ out,
                                     const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullpacker(in, out);

  case 1:
    return __fastunalignedpackwithoutmask1_8(in, out);

  case 2:
    return __fastunalignedpackwithoutmask2_8(in, out);

  case 3:
    return __fastunalignedpackwithoutmask3_8(in, out);

  case 4:
    return __fastunalignedpackwithoutmask4_8(in, out);

  case 5:
    return __fastunalignedpackwithoutmask5_8(in, out);

  case 6:
    return __fastunalignedpackwithoutmask6_8(in, out);

  case 7:
    return __fastunalignedpackwithoutmask7_8(in, out);

  case 8:
    return __fastunalignedpackwithoutmask8_8(in, out);

  case 9:
    return __fastunalignedpackwithoutmask9_8(in, out);

  case 10:
    return __fastunalignedpackwithoutmask10_8(in, out);

  case 11:
    return __fastunalignedpackwithoutmask11_8(in, out);

  case 12:
    return __fastunalignedpackwithoutmask12_8(in, out);

  case 13:
    return __fastunalignedpackwithoutmask13_8(in, out);

  case 14:
    return __fastunalignedpackwithoutmask14_8(in, out);

  case 15:
    return __fastunalignedpackwithoutmask15_8(in, out);

  case 16:
    return __fastunalignedpackwithoutmask16_8(in, out);

  case 17:
    return __fastunalignedpackwithoutmask17_8(in, out);

  case 18:
    return __fastunalignedpackwithoutmask18_8(in, out);

  case 19:
    return __fastunalignedpackwithoutmask19_8(in, out);

  case 20:
    return __fastunalignedpackwithoutmask20_8(in, out);

  case 21:
    return __fastunalignedpackwithoutmask21_8(in, out);

  case 22:
    return __fastunalignedpackwithoutmask22_8(in, out);

  case 23:
    return __fastunalignedpackwithoutmask23_8(in, out);

  case 24:
    return __fastunalignedpackwithoutmask24_8(in, out);

  case 25:
    return __fastunalignedpackwithoutmask25_8(in, out);

  case 26:
    return __fastunalignedpackwithoutmask26_8(in, out);

  case 27:
    return __fastunalignedpackwithoutmask27_8(in, out);

  case 28:
    return __fastunalignedpackwithoutmask28_8(in, out);

  case 29:
    return __fastunalignedpackwithoutmask29_8(in, out);

  case 30:
    return __fastunalignedpackwithoutmask30_8(in, out);

  case 31:
    return __fastunalignedpackwithoutmask31_8(in, out);

  case 32:
    return __fastunalignedpackwithoutmask32_8(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

const byte *fastunalignedbyteunpack_8(const byte *__restrict__ in,
                                      byte *__restrict__ out,
                                      const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullbyteunpacker8(in, out);

  case 1:
    return __fastunalignedbyteunpack1_8(in, out);

  case 2:
    return __fastunalignedbyteunpack2_8(in, out);

  case 3:
    return __fastunalignedbyteunpack3_8(in, out);

  case 4:
    return __fastunalignedbyteunpack4_8(in, out);

  case 5:
    return __fastunalignedbyteunpack5_8(in, out);

  case 6:
    return __fastunalignedbyteunpack6_8(in, out);

  case 7:
    return __fastunalignedbyteunpack7_8(in, out);

  case 8:
    return __fastunalignedbyteunpack8_8(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

/*assumes that integers fit in the prescribed number of bits*/
byte *fastunalignedbytepackwithoutmask_8(const byte *__restrict__ in,
                                         byte *__restrict__ out,
                                         const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullbytepacker(in, out);

  case 1:
    return __fastunalignedbytepackwithoutmask1_8(in, out);

  case 2:
    return __fastunalignedbytepackwithoutmask2_8(in, out);

  case 3:
    return __fastunalignedbytepackwithoutmask3_8(in, out);

  case 4:
    return __fastunalignedbytepackwithoutmask4_8(in, out);

  case 5:
    return __fastunalignedbytepackwithoutmask5_8(in, out);

  case 6:
    return __fastunalignedbytepackwithoutmask6_8(in, out);

  case 7:
    return __fastunalignedbytepackwithoutmask7_8(in, out);

  case 8:
    return __fastunalignedbytepackwithoutmask8_8(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

const byte *nullunpacker16(const byte *__restrict__ in,
                           uint32_t *__restrict__ out) {
  memset(out, 0, 16 * 4);
  return in;
}

const byte *nullbyteunpacker16(const byte *__restrict__ in,
                               byte *__restrict__ out) {
  memset(out, 0, 16);
  return in;
}

byte *__fastunalignedpackwithoutmask1_16(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 15;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask2_16(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 22;
  ++in;
  *out |= ((*in)) << 24;
  ++in;
  *out |= ((*in)) << 26;
  ++in;
  *out |= ((*in)) << 28;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask3_16(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 15;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 21;
  ++in;
  *out |= ((*in)) << 24;
  ++in;
  *out |= ((*in)) << 27;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (3 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 13;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask4_16(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 24;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 24;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask5_16(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 15;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 25;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (5 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 23;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (5 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 11;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask6_16(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 24;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (6 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 22;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (6 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask7_16(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 21;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (7 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 17;
  ++in;
  *out |= ((*in)) << 24;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (7 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (7 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 9;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask8_16(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask9_16(const uint32_t *__restrict__ in,
                                         byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (9 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 22;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (9 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 17;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (9 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 21;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (9 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask10_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (10 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (10 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (10 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (10 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask11_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (11 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (11 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (11 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (11 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 15;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (11 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask12_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (12 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (12 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (12 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (12 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask13_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (13 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (13 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (13 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (13 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 15;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (13 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (13 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask14_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (14 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (14 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (14 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (14 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (14 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (14 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask15_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 15;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (15 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (15 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (15 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (15 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (15 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (15 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (15 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask16_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask17_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (17 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (17 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (17 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (17 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (17 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (17 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (17 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (17 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask18_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (18 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (18 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (18 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (18 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (18 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (18 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (18 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (18 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask19_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (19 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (19 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (19 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (19 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (19 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (19 - 17);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (19 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (19 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (19 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask20_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (20 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (20 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (20 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (20 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (20 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (20 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (20 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (20 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask21_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (21 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (21 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (21 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (21 - 19);
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (21 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (21 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (21 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (21 - 17);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (21 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (21 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask22_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (22 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (22 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (22 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (22 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (22 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (22 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (22 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (22 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (22 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (22 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask23_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (23 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (23 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (23 - 19);
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (23 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  *out = ((*in)) >> (23 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (23 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (23 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (23 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (23 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++out;
  *out = ((*in)) >> (23 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (23 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask24_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (24 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (24 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (24 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (24 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (24 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (24 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (24 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (24 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask25_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (25 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (25 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++out;
  *out = ((*in)) >> (25 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (25 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (25 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (25 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  *out = ((*in)) >> (25 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (25 - 19);
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (25 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (25 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (25 - 23);
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (25 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask26_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (26 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (26 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (26 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  *out = ((*in)) >> (26 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (26 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (26 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (26 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  *out = ((*in)) >> (26 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (26 - 24);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (26 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (26 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (26 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask27_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (27 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (27 - 17);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (27 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (27 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++out;
  *out = ((*in)) >> (27 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (27 - 24);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (27 - 19);
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (27 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (27 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++out;
  *out = ((*in)) >> (27 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (27 - 26);
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (27 - 21);
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (27 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask28_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (28 - 24);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (28 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (28 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (28 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (28 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  *out = ((*in)) >> (28 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (28 - 24);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (28 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (28 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (28 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (28 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  *out = ((*in)) >> (28 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask29_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (29 - 26);
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (29 - 23);
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (29 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (29 - 17);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (29 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (29 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++out;
  *out = ((*in)) >> (29 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  *out = ((*in)) >> (29 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++out;
  *out = ((*in)) >> (29 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (29 - 28);
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (29 - 25);
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (29 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (29 - 19);
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (29 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask30_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (30 - 28);
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (30 - 26);
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (30 - 24);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (30 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (30 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (30 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (30 - 16);
  ++in;
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (30 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (30 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (30 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  *out = ((*in)) >> (30 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  *out = ((*in)) >> (30 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++out;
  *out = ((*in)) >> (30 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++out;
  *out = ((*in)) >> (30 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

byte *__fastunalignedpackwithoutmask31_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (31 - 30);
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (31 - 29);
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (31 - 28);
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (31 - 27);
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (31 - 26);
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (31 - 25);
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (31 - 24);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (31 - 23);
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (31 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (31 - 21);
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (31 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (31 - 19);
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (31 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (31 - 17);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (31 - 16);
  ++in;

  return reinterpret_cast<byte *>(out) + 2;
}

byte *__fastunalignedpackwithoutmask32_16(const uint32_t *__restrict__ in,
                                          byte *__restrict__ outbyte) {
  uint32_t *__restrict__ out = reinterpret_cast<uint32_t *>(outbyte);

  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;

  return reinterpret_cast<byte *>(out) + 0;
}

const byte *__fastunalignedunpack1_16(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) & 1;
  out++;
  *out = ((*in) >> 1) & 1;
  out++;
  *out = ((*in) >> 2) & 1;
  out++;
  *out = ((*in) >> 3) & 1;
  out++;
  *out = ((*in) >> 4) & 1;
  out++;
  *out = ((*in) >> 5) & 1;
  out++;
  *out = ((*in) >> 6) & 1;
  out++;
  *out = ((*in) >> 7) & 1;
  out++;
  *out = ((*in) >> 8) & 1;
  out++;
  *out = ((*in) >> 9) & 1;
  out++;
  *out = ((*in) >> 10) & 1;
  out++;
  *out = ((*in) >> 11) & 1;
  out++;
  *out = ((*in) >> 12) & 1;
  out++;
  *out = ((*in) >> 13) & 1;
  out++;
  *out = ((*in) >> 14) & 1;
  out++;
  *out = ((*in) >> 15) & 1;
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack2_16(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 2);
  out++;
  *out = ((*in) >> 2) % (1U << 2);
  out++;
  *out = ((*in) >> 4) % (1U << 2);
  out++;
  *out = ((*in) >> 6) % (1U << 2);
  out++;
  *out = ((*in) >> 8) % (1U << 2);
  out++;
  *out = ((*in) >> 10) % (1U << 2);
  out++;
  *out = ((*in) >> 12) % (1U << 2);
  out++;
  *out = ((*in) >> 14) % (1U << 2);
  out++;
  *out = ((*in) >> 16) % (1U << 2);
  out++;
  *out = ((*in) >> 18) % (1U << 2);
  out++;
  *out = ((*in) >> 20) % (1U << 2);
  out++;
  *out = ((*in) >> 22) % (1U << 2);
  out++;
  *out = ((*in) >> 24) % (1U << 2);
  out++;
  *out = ((*in) >> 26) % (1U << 2);
  out++;
  *out = ((*in) >> 28) % (1U << 2);
  out++;
  *out = ((*in) >> 30);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack3_16(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 3);
  out++;
  *out = ((*in) >> 3) % (1U << 3);
  out++;
  *out = ((*in) >> 6) % (1U << 3);
  out++;
  *out = ((*in) >> 9) % (1U << 3);
  out++;
  *out = ((*in) >> 12) % (1U << 3);
  out++;
  *out = ((*in) >> 15) % (1U << 3);
  out++;
  *out = ((*in) >> 18) % (1U << 3);
  out++;
  *out = ((*in) >> 21) % (1U << 3);
  out++;
  *out = ((*in) >> 24) % (1U << 3);
  out++;
  *out = ((*in) >> 27) % (1U << 3);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 1)) << (3 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 3);
  out++;
  *out = ((*in) >> 4) % (1U << 3);
  out++;
  *out = ((*in) >> 7) % (1U << 3);
  out++;
  *out = ((*in) >> 10) % (1U << 3);
  out++;
  *out = ((*in) >> 13) % (1U << 3);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack4_16(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  out++;
  *out = ((*in) >> 8) % (1U << 4);
  out++;
  *out = ((*in) >> 12) % (1U << 4);
  out++;
  *out = ((*in) >> 16) % (1U << 4);
  out++;
  *out = ((*in) >> 20) % (1U << 4);
  out++;
  *out = ((*in) >> 24) % (1U << 4);
  out++;
  *out = ((*in) >> 28);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  out++;
  *out = ((*in) >> 8) % (1U << 4);
  out++;
  *out = ((*in) >> 12) % (1U << 4);
  out++;
  *out = ((*in) >> 16) % (1U << 4);
  out++;
  *out = ((*in) >> 20) % (1U << 4);
  out++;
  *out = ((*in) >> 24) % (1U << 4);
  out++;
  *out = ((*in) >> 28);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack5_16(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 5);
  out++;
  *out = ((*in) >> 5) % (1U << 5);
  out++;
  *out = ((*in) >> 10) % (1U << 5);
  out++;
  *out = ((*in) >> 15) % (1U << 5);
  out++;
  *out = ((*in) >> 20) % (1U << 5);
  out++;
  *out = ((*in) >> 25) % (1U << 5);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 3)) << (5 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 5);
  out++;
  *out = ((*in) >> 8) % (1U << 5);
  out++;
  *out = ((*in) >> 13) % (1U << 5);
  out++;
  *out = ((*in) >> 18) % (1U << 5);
  out++;
  *out = ((*in) >> 23) % (1U << 5);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 1)) << (5 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 5);
  out++;
  *out = ((*in) >> 6) % (1U << 5);
  out++;
  *out = ((*in) >> 11) % (1U << 5);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack6_16(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 6);
  out++;
  *out = ((*in) >> 6) % (1U << 6);
  out++;
  *out = ((*in) >> 12) % (1U << 6);
  out++;
  *out = ((*in) >> 18) % (1U << 6);
  out++;
  *out = ((*in) >> 24) % (1U << 6);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 4)) << (6 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 6);
  out++;
  *out = ((*in) >> 10) % (1U << 6);
  out++;
  *out = ((*in) >> 16) % (1U << 6);
  out++;
  *out = ((*in) >> 22) % (1U << 6);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 2)) << (6 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 6);
  out++;
  *out = ((*in) >> 8) % (1U << 6);
  out++;
  *out = ((*in) >> 14) % (1U << 6);
  out++;
  *out = ((*in) >> 20) % (1U << 6);
  out++;
  *out = ((*in) >> 26);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack7_16(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 7);
  out++;
  *out = ((*in) >> 7) % (1U << 7);
  out++;
  *out = ((*in) >> 14) % (1U << 7);
  out++;
  *out = ((*in) >> 21) % (1U << 7);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 3)) << (7 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 7);
  out++;
  *out = ((*in) >> 10) % (1U << 7);
  out++;
  *out = ((*in) >> 17) % (1U << 7);
  out++;
  *out = ((*in) >> 24) % (1U << 7);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 6)) << (7 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 7);
  out++;
  *out = ((*in) >> 13) % (1U << 7);
  out++;
  *out = ((*in) >> 20) % (1U << 7);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 2)) << (7 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 7);
  out++;
  *out = ((*in) >> 9) % (1U << 7);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack8_16(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 8);
  out++;
  *out = ((*in) >> 8) % (1U << 8);
  out++;
  *out = ((*in) >> 16) % (1U << 8);
  out++;
  *out = ((*in) >> 24);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  out++;
  *out = ((*in) >> 8) % (1U << 8);
  out++;
  *out = ((*in) >> 16) % (1U << 8);
  out++;
  *out = ((*in) >> 24);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  out++;
  *out = ((*in) >> 8) % (1U << 8);
  out++;
  *out = ((*in) >> 16) % (1U << 8);
  out++;
  *out = ((*in) >> 24);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  out++;
  *out = ((*in) >> 8) % (1U << 8);
  out++;
  *out = ((*in) >> 16) % (1U << 8);
  out++;
  *out = ((*in) >> 24);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack9_16(const byte *__restrict__ inbyte,
                                      uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 9);
  out++;
  *out = ((*in) >> 9) % (1U << 9);
  out++;
  *out = ((*in) >> 18) % (1U << 9);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 4)) << (9 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 9);
  out++;
  *out = ((*in) >> 13) % (1U << 9);
  out++;
  *out = ((*in) >> 22) % (1U << 9);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 8)) << (9 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 9);
  out++;
  *out = ((*in) >> 17) % (1U << 9);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 3)) << (9 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 9);
  out++;
  *out = ((*in) >> 12) % (1U << 9);
  out++;
  *out = ((*in) >> 21) % (1U << 9);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 7)) << (9 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 9);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack10_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 10);
  out++;
  *out = ((*in) >> 10) % (1U << 10);
  out++;
  *out = ((*in) >> 20) % (1U << 10);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 8)) << (10 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 10);
  out++;
  *out = ((*in) >> 18) % (1U << 10);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 6)) << (10 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 10);
  out++;
  *out = ((*in) >> 16) % (1U << 10);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 4)) << (10 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 10);
  out++;
  *out = ((*in) >> 14) % (1U << 10);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 2)) << (10 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 10);
  out++;
  *out = ((*in) >> 12) % (1U << 10);
  out++;
  *out = ((*in) >> 22);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack11_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 11);
  out++;
  *out = ((*in) >> 11) % (1U << 11);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 1)) << (11 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 11);
  out++;
  *out = ((*in) >> 12) % (1U << 11);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 2)) << (11 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 11);
  out++;
  *out = ((*in) >> 13) % (1U << 11);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 3)) << (11 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 11);
  out++;
  *out = ((*in) >> 14) % (1U << 11);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 4)) << (11 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 11);
  out++;
  *out = ((*in) >> 15) % (1U << 11);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 5)) << (11 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 11);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack12_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 12);
  out++;
  *out = ((*in) >> 12) % (1U << 12);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 4)) << (12 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 12);
  out++;
  *out = ((*in) >> 16) % (1U << 12);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 8)) << (12 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 12);
  out++;
  *out = ((*in) >> 20);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 12);
  out++;
  *out = ((*in) >> 12) % (1U << 12);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 4)) << (12 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 12);
  out++;
  *out = ((*in) >> 16) % (1U << 12);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 8)) << (12 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 12);
  out++;
  *out = ((*in) >> 20);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack13_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 13);
  out++;
  *out = ((*in) >> 13) % (1U << 13);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 7)) << (13 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 13);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 1)) << (13 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 13);
  out++;
  *out = ((*in) >> 14) % (1U << 13);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 8)) << (13 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 13);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 2)) << (13 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 13);
  out++;
  *out = ((*in) >> 15) % (1U << 13);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 9)) << (13 - 9);
  out++;
  *out = ((*in) >> 9) % (1U << 13);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 3)) << (13 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 13);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack14_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 14);
  out++;
  *out = ((*in) >> 14) % (1U << 14);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 10)) << (14 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 14);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 6)) << (14 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 14);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 2)) << (14 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 14);
  out++;
  *out = ((*in) >> 16) % (1U << 14);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 12)) << (14 - 12);
  out++;
  *out = ((*in) >> 12) % (1U << 14);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 8)) << (14 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 14);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 4)) << (14 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 14);
  out++;
  *out = ((*in) >> 18);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack15_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 15);
  out++;
  *out = ((*in) >> 15) % (1U << 15);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 13)) << (15 - 13);
  out++;
  *out = ((*in) >> 13) % (1U << 15);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 11)) << (15 - 11);
  out++;
  *out = ((*in) >> 11) % (1U << 15);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 9)) << (15 - 9);
  out++;
  *out = ((*in) >> 9) % (1U << 15);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 7)) << (15 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 15);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 5)) << (15 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 15);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 3)) << (15 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 15);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 1)) << (15 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 15);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack16_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack17_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 2)) << (17 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 17);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 4)) << (17 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 17);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 6)) << (17 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 17);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 8)) << (17 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 17);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 10)) << (17 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 17);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 12)) << (17 - 12);
  out++;
  *out = ((*in) >> 12) % (1U << 17);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 14)) << (17 - 14);
  out++;
  *out = ((*in) >> 14) % (1U << 17);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 16)) << (17 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack18_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 4)) << (18 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 18);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 8)) << (18 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 18);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 12)) << (18 - 12);
  out++;
  *out = ((*in) >> 12) % (1U << 18);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 16)) << (18 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 2)) << (18 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 18);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 6)) << (18 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 18);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 10)) << (18 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 18);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 14)) << (18 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack19_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 19);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 6)) << (19 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 19);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 12)) << (19 - 12);
  out++;
  *out = ((*in) >> 12) % (1U << 19);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 18)) << (19 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 5)) << (19 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 19);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 11)) << (19 - 11);
  out++;
  *out = ((*in) >> 11) % (1U << 19);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 17)) << (19 - 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 4)) << (19 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 19);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 10)) << (19 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 19);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 16)) << (19 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack20_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 8)) << (20 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 20);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 16)) << (20 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 4)) << (20 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 20);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 12)) << (20 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 8)) << (20 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 20);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 16)) << (20 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 4)) << (20 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 20);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 12)) << (20 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack21_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 21);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 10)) << (21 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 21);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 20)) << (21 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 9)) << (21 - 9);
  out++;
  *out = ((*in) >> 9) % (1U << 21);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 19)) << (21 - 19);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 8)) << (21 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 21);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 18)) << (21 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 7)) << (21 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 21);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 17)) << (21 - 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 6)) << (21 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 21);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 16)) << (21 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack22_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 12)) << (22 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 2)) << (22 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 22);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 14)) << (22 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 4)) << (22 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 22);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 16)) << (22 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 6)) << (22 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 22);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 18)) << (22 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 8)) << (22 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 22);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 20)) << (22 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 10)) << (22 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack23_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 23);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 14)) << (23 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 5)) << (23 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 23);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 19)) << (23 - 19);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 10)) << (23 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  *out |= ((*in) % (1U << 1)) << (23 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 23);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 15)) << (23 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 6)) << (23 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 23);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 20)) << (23 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 11)) << (23 - 11);
  out++;
  *out = ((*in) >> 11);
  ++in;
  *out |= ((*in) % (1U << 2)) << (23 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 23);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 16)) << (23 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack24_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 16)) << (24 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 8)) << (24 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 16)) << (24 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 8)) << (24 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 16)) << (24 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 8)) << (24 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 16)) << (24 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 8)) << (24 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack25_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 25);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 18)) << (25 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 11)) << (25 - 11);
  out++;
  *out = ((*in) >> 11);
  ++in;
  *out |= ((*in) % (1U << 4)) << (25 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 25);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 22)) << (25 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 15)) << (25 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 8)) << (25 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  *out |= ((*in) % (1U << 1)) << (25 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 25);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 19)) << (25 - 19);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 12)) << (25 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 5)) << (25 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 25);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 23)) << (25 - 23);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 16)) << (25 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack26_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 26);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 20)) << (26 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 14)) << (26 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 8)) << (26 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  *out |= ((*in) % (1U << 2)) << (26 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 26);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 22)) << (26 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 16)) << (26 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 10)) << (26 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  *out |= ((*in) % (1U << 4)) << (26 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 26);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 24)) << (26 - 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 18)) << (26 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 12)) << (26 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 6)) << (26 - 6);
  out++;
  *out = ((*in) >> 6);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack27_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 27);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 22)) << (27 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 17)) << (27 - 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 12)) << (27 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 7)) << (27 - 7);
  out++;
  *out = ((*in) >> 7);
  ++in;
  *out |= ((*in) % (1U << 2)) << (27 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 27);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 24)) << (27 - 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 19)) << (27 - 19);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 14)) << (27 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 9)) << (27 - 9);
  out++;
  *out = ((*in) >> 9);
  ++in;
  *out |= ((*in) % (1U << 4)) << (27 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 27);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 26)) << (27 - 26);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 21)) << (27 - 21);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 16)) << (27 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack28_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 28);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 24)) << (28 - 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 20)) << (28 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 16)) << (28 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 12)) << (28 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 8)) << (28 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  *out |= ((*in) % (1U << 4)) << (28 - 4);
  out++;
  *out = ((*in) >> 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 28);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 24)) << (28 - 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 20)) << (28 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 16)) << (28 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 12)) << (28 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 8)) << (28 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  *out |= ((*in) % (1U << 4)) << (28 - 4);
  out++;
  *out = ((*in) >> 4);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack29_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 29);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 26)) << (29 - 26);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 23)) << (29 - 23);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 20)) << (29 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 17)) << (29 - 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 14)) << (29 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 11)) << (29 - 11);
  out++;
  *out = ((*in) >> 11);
  ++in;
  *out |= ((*in) % (1U << 8)) << (29 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  *out |= ((*in) % (1U << 5)) << (29 - 5);
  out++;
  *out = ((*in) >> 5);
  ++in;
  *out |= ((*in) % (1U << 2)) << (29 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 29);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 28)) << (29 - 28);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 25)) << (29 - 25);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 22)) << (29 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 19)) << (29 - 19);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 16)) << (29 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack30_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 30);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 28)) << (30 - 28);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 26)) << (30 - 26);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 24)) << (30 - 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 22)) << (30 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 20)) << (30 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 18)) << (30 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 16)) << (30 - 16);
  out++;
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 14)) << (30 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 12)) << (30 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 10)) << (30 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  *out |= ((*in) % (1U << 8)) << (30 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  *out |= ((*in) % (1U << 6)) << (30 - 6);
  out++;
  *out = ((*in) >> 6);
  ++in;
  *out |= ((*in) % (1U << 4)) << (30 - 4);
  out++;
  *out = ((*in) >> 4);
  ++in;
  *out |= ((*in) % (1U << 2)) << (30 - 2);
  out++;
  *out = ((*in) >> 2);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

const byte *__fastunalignedunpack31_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0) % (1U << 31);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 30)) << (31 - 30);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 29)) << (31 - 29);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 28)) << (31 - 28);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 27)) << (31 - 27);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 26)) << (31 - 26);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 25)) << (31 - 25);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 24)) << (31 - 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 23)) << (31 - 23);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 22)) << (31 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 21)) << (31 - 21);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 20)) << (31 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 19)) << (31 - 19);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 18)) << (31 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 17)) << (31 - 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 16)) << (31 - 16);
  out++;

  return reinterpret_cast<const byte *>(in) + 2;
}

const byte *__fastunalignedunpack32_16(const byte *__restrict__ inbyte,
                                       uint32_t *__restrict__ out) {
  const uint32_t *__restrict__ in = reinterpret_cast<const uint32_t *>(inbyte);

  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;
  *out = ((*in) >> 0);
  ++in;
  out++;

  return reinterpret_cast<const byte *>(in) + 0;
}

byte *__fastunalignedbytepackwithoutmask1_16(const byte *__restrict__ in,
                                             byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask2_16(const byte *__restrict__ in,
                                             byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask3_16(const byte *__restrict__ in,
                                             byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (3 - 1));
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  *out = static_cast<byte>(((*in)) >> (3 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (3 - 1));
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  *out = static_cast<byte>(((*in)) >> (3 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask4_16(const byte *__restrict__ in,
                                             byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask5_16(const byte *__restrict__ in,
                                             byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 1));
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 3));
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 1));
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (5 - 3));
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask6_16(const byte *__restrict__ in,
                                             byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (6 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask7_16(const byte *__restrict__ in,
                                             byte *__restrict__ out) {

  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 6));
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 5));
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 3));
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 1));
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++out;
  ++in;
  *out = (*in);
  ++in;
  *out |= static_cast<byte>(((*in)) << 7);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 6));
  ++in;
  *out |= static_cast<byte>(((*in)) << 6);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 5));
  ++in;
  *out |= static_cast<byte>(((*in)) << 5);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 4));
  ++in;
  *out |= static_cast<byte>(((*in)) << 4);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 3));
  ++in;
  *out |= static_cast<byte>(((*in)) << 3);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 2));
  ++in;
  *out |= static_cast<byte>(((*in)) << 2);
  ++out;
  *out = static_cast<byte>(((*in)) >> (7 - 1));
  ++in;
  *out |= static_cast<byte>(((*in)) << 1);
  ++out;
  ++in;

  return out;
}

byte *__fastunalignedbytepackwithoutmask8_16(const byte *__restrict__ in,
                                             byte *__restrict__ out) {

  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;
  *out = (*in);
  ++out;
  ++in;

  return out;
}

const byte *__fastunalignedbyteunpack1_16(const byte *__restrict__ in,
                                          byte *__restrict__ out) {

  *out = ((*in) >> 0) & 1;
  out++;
  *out = ((*in) >> 1) & 1;
  out++;
  *out = ((*in) >> 2) & 1;
  out++;
  *out = ((*in) >> 3) & 1;
  out++;
  *out = ((*in) >> 4) & 1;
  out++;
  *out = ((*in) >> 5) & 1;
  out++;
  *out = ((*in) >> 6) & 1;
  out++;
  *out = ((*in) >> 7) & 1;
  ++in;
  out++;
  *out = ((*in) >> 0) & 1;
  out++;
  *out = ((*in) >> 1) & 1;
  out++;
  *out = ((*in) >> 2) & 1;
  out++;
  *out = ((*in) >> 3) & 1;
  out++;
  *out = ((*in) >> 4) & 1;
  out++;
  *out = ((*in) >> 5) & 1;
  out++;
  *out = ((*in) >> 6) & 1;
  out++;
  *out = ((*in) >> 7) & 1;
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack2_16(const byte *__restrict__ in,
                                          byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 2);
  out++;
  *out = ((*in) >> 2) % (1U << 2);
  out++;
  *out = ((*in) >> 4) % (1U << 2);
  out++;
  *out = ((*in) >> 6) % (1U << 2);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 2);
  out++;
  *out = ((*in) >> 2) % (1U << 2);
  out++;
  *out = ((*in) >> 4) % (1U << 2);
  out++;
  *out = ((*in) >> 6) % (1U << 2);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 2);
  out++;
  *out = ((*in) >> 2) % (1U << 2);
  out++;
  *out = ((*in) >> 4) % (1U << 2);
  out++;
  *out = ((*in) >> 6) % (1U << 2);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 2);
  out++;
  *out = ((*in) >> 2) % (1U << 2);
  out++;
  *out = ((*in) >> 4) % (1U << 2);
  out++;
  *out = ((*in) >> 6) % (1U << 2);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack3_16(const byte *__restrict__ in,
                                          byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 3);
  out++;
  *out = ((*in) >> 3) % (1U << 3);
  out++;
  *out = ((*in) >> 6) % (1U << 3);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 1)) << (3 - 1));
  out++;
  *out = ((*in) >> 1) % (1U << 3);
  out++;
  *out = ((*in) >> 4) % (1U << 3);
  out++;
  *out = ((*in) >> 7) % (1U << 3);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (3 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 3);
  out++;
  *out = ((*in) >> 5) % (1U << 3);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 3);
  out++;
  *out = ((*in) >> 3) % (1U << 3);
  out++;
  *out = ((*in) >> 6) % (1U << 3);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 1)) << (3 - 1));
  out++;
  *out = ((*in) >> 1) % (1U << 3);
  out++;
  *out = ((*in) >> 4) % (1U << 3);
  out++;
  *out = ((*in) >> 7) % (1U << 3);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (3 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 3);
  out++;
  *out = ((*in) >> 5) % (1U << 3);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack4_16(const byte *__restrict__ in,
                                          byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 4);
  out++;
  *out = ((*in) >> 4) % (1U << 4);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack5_16(const byte *__restrict__ in,
                                          byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 5);
  out++;
  *out = ((*in) >> 5) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (5 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 5);
  out++;
  *out = ((*in) >> 7) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (5 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 1)) << (5 - 1));
  out++;
  *out = ((*in) >> 1) % (1U << 5);
  out++;
  *out = ((*in) >> 6) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 3)) << (5 - 3));
  out++;
  *out = ((*in) >> 3) % (1U << 5);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 5);
  out++;
  *out = ((*in) >> 5) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (5 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 5);
  out++;
  *out = ((*in) >> 7) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (5 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 1)) << (5 - 1));
  out++;
  *out = ((*in) >> 1) % (1U << 5);
  out++;
  *out = ((*in) >> 6) % (1U << 5);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 3)) << (5 - 3));
  out++;
  *out = ((*in) >> 3) % (1U << 5);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack6_16(const byte *__restrict__ in,
                                          byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 6);
  out++;
  *out = ((*in) >> 6) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (6 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (6 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 6);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 6);
  out++;
  *out = ((*in) >> 6) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (6 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (6 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 6);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 6);
  out++;
  *out = ((*in) >> 6) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (6 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (6 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 6);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 6);
  out++;
  *out = ((*in) >> 6) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (6 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 6);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (6 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 6);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack7_16(const byte *__restrict__ in,
                                          byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 7);
  out++;
  *out = ((*in) >> 7) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 6)) << (7 - 6));
  out++;
  *out = ((*in) >> 6) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 5)) << (7 - 5));
  out++;
  *out = ((*in) >> 5) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (7 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 3)) << (7 - 3));
  out++;
  *out = ((*in) >> 3) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (7 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 1)) << (7 - 1));
  out++;
  *out = ((*in) >> 1) % (1U << 7);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 7);
  out++;
  *out = ((*in) >> 7) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 6)) << (7 - 6));
  out++;
  *out = ((*in) >> 6) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 5)) << (7 - 5));
  out++;
  *out = ((*in) >> 5) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 4)) << (7 - 4));
  out++;
  *out = ((*in) >> 4) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 3)) << (7 - 3));
  out++;
  *out = ((*in) >> 3) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 2)) << (7 - 2));
  out++;
  *out = ((*in) >> 2) % (1U << 7);
  ++in;
  *out |= static_cast<byte>(((*in) % (1U << 1)) << (7 - 1));
  out++;
  *out = ((*in) >> 1) % (1U << 7);
  ++in;
  out++;

  return in;
}

const byte *__fastunalignedbyteunpack8_16(const byte *__restrict__ in,
                                          byte *__restrict__ out) {

  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;
  *out = ((*in) >> 0) % (1U << 8);
  ++in;
  out++;

  return in;
}

const byte *fastunalignedunpack_16(const byte *__restrict__ in,
                                   uint32_t *__restrict__ out,
                                   const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullunpacker16(in, out);

  case 1:
    return __fastunalignedunpack1_16(in, out);

  case 2:
    return __fastunalignedunpack2_16(in, out);

  case 3:
    return __fastunalignedunpack3_16(in, out);

  case 4:
    return __fastunalignedunpack4_16(in, out);

  case 5:
    return __fastunalignedunpack5_16(in, out);

  case 6:
    return __fastunalignedunpack6_16(in, out);

  case 7:
    return __fastunalignedunpack7_16(in, out);

  case 8:
    return __fastunalignedunpack8_16(in, out);

  case 9:
    return __fastunalignedunpack9_16(in, out);

  case 10:
    return __fastunalignedunpack10_16(in, out);

  case 11:
    return __fastunalignedunpack11_16(in, out);

  case 12:
    return __fastunalignedunpack12_16(in, out);

  case 13:
    return __fastunalignedunpack13_16(in, out);

  case 14:
    return __fastunalignedunpack14_16(in, out);

  case 15:
    return __fastunalignedunpack15_16(in, out);

  case 16:
    return __fastunalignedunpack16_16(in, out);

  case 17:
    return __fastunalignedunpack17_16(in, out);

  case 18:
    return __fastunalignedunpack18_16(in, out);

  case 19:
    return __fastunalignedunpack19_16(in, out);

  case 20:
    return __fastunalignedunpack20_16(in, out);

  case 21:
    return __fastunalignedunpack21_16(in, out);

  case 22:
    return __fastunalignedunpack22_16(in, out);

  case 23:
    return __fastunalignedunpack23_16(in, out);

  case 24:
    return __fastunalignedunpack24_16(in, out);

  case 25:
    return __fastunalignedunpack25_16(in, out);

  case 26:
    return __fastunalignedunpack26_16(in, out);

  case 27:
    return __fastunalignedunpack27_16(in, out);

  case 28:
    return __fastunalignedunpack28_16(in, out);

  case 29:
    return __fastunalignedunpack29_16(in, out);

  case 30:
    return __fastunalignedunpack30_16(in, out);

  case 31:
    return __fastunalignedunpack31_16(in, out);

  case 32:
    return __fastunalignedunpack32_16(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

/*assumes that integers fit in the prescribed number of bits*/
byte *fastunalignedpackwithoutmask_16(const uint32_t *__restrict__ in,
                                      byte *__restrict__ out,
                                      const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullpacker(in, out);

  case 1:
    return __fastunalignedpackwithoutmask1_16(in, out);

  case 2:
    return __fastunalignedpackwithoutmask2_16(in, out);

  case 3:
    return __fastunalignedpackwithoutmask3_16(in, out);

  case 4:
    return __fastunalignedpackwithoutmask4_16(in, out);

  case 5:
    return __fastunalignedpackwithoutmask5_16(in, out);

  case 6:
    return __fastunalignedpackwithoutmask6_16(in, out);

  case 7:
    return __fastunalignedpackwithoutmask7_16(in, out);

  case 8:
    return __fastunalignedpackwithoutmask8_16(in, out);

  case 9:
    return __fastunalignedpackwithoutmask9_16(in, out);

  case 10:
    return __fastunalignedpackwithoutmask10_16(in, out);

  case 11:
    return __fastunalignedpackwithoutmask11_16(in, out);

  case 12:
    return __fastunalignedpackwithoutmask12_16(in, out);

  case 13:
    return __fastunalignedpackwithoutmask13_16(in, out);

  case 14:
    return __fastunalignedpackwithoutmask14_16(in, out);

  case 15:
    return __fastunalignedpackwithoutmask15_16(in, out);

  case 16:
    return __fastunalignedpackwithoutmask16_16(in, out);

  case 17:
    return __fastunalignedpackwithoutmask17_16(in, out);

  case 18:
    return __fastunalignedpackwithoutmask18_16(in, out);

  case 19:
    return __fastunalignedpackwithoutmask19_16(in, out);

  case 20:
    return __fastunalignedpackwithoutmask20_16(in, out);

  case 21:
    return __fastunalignedpackwithoutmask21_16(in, out);

  case 22:
    return __fastunalignedpackwithoutmask22_16(in, out);

  case 23:
    return __fastunalignedpackwithoutmask23_16(in, out);

  case 24:
    return __fastunalignedpackwithoutmask24_16(in, out);

  case 25:
    return __fastunalignedpackwithoutmask25_16(in, out);

  case 26:
    return __fastunalignedpackwithoutmask26_16(in, out);

  case 27:
    return __fastunalignedpackwithoutmask27_16(in, out);

  case 28:
    return __fastunalignedpackwithoutmask28_16(in, out);

  case 29:
    return __fastunalignedpackwithoutmask29_16(in, out);

  case 30:
    return __fastunalignedpackwithoutmask30_16(in, out);

  case 31:
    return __fastunalignedpackwithoutmask31_16(in, out);

  case 32:
    return __fastunalignedpackwithoutmask32_16(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

const byte *fastunalignedbyteunpack_16(const byte *__restrict__ in,
                                       byte *__restrict__ out,
                                       const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullbyteunpacker16(in, out);

  case 1:
    return __fastunalignedbyteunpack1_16(in, out);

  case 2:
    return __fastunalignedbyteunpack2_16(in, out);

  case 3:
    return __fastunalignedbyteunpack3_16(in, out);

  case 4:
    return __fastunalignedbyteunpack4_16(in, out);

  case 5:
    return __fastunalignedbyteunpack5_16(in, out);

  case 6:
    return __fastunalignedbyteunpack6_16(in, out);

  case 7:
    return __fastunalignedbyteunpack7_16(in, out);

  case 8:
    return __fastunalignedbyteunpack8_16(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

/*assumes that integers fit in the prescribed number of bits*/
byte *fastunalignedbytepackwithoutmask_16(const byte *__restrict__ in,
                                          byte *__restrict__ out,
                                          const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullbytepacker(in, out);

  case 1:
    return __fastunalignedbytepackwithoutmask1_16(in, out);

  case 2:
    return __fastunalignedbytepackwithoutmask2_16(in, out);

  case 3:
    return __fastunalignedbytepackwithoutmask3_16(in, out);

  case 4:
    return __fastunalignedbytepackwithoutmask4_16(in, out);

  case 5:
    return __fastunalignedbytepackwithoutmask5_16(in, out);

  case 6:
    return __fastunalignedbytepackwithoutmask6_16(in, out);

  case 7:
    return __fastunalignedbytepackwithoutmask7_16(in, out);

  case 8:
    return __fastunalignedbytepackwithoutmask8_16(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}
