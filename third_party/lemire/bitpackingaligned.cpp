#include "bitpackingaligned.h"

namespace FastPForLib {

uint32_t *nullpacker(const uint32_t *__restrict__ /*in*/,
                     uint32_t *__restrict__ out) {
  return out;
}

const uint32_t *nullunpacker8(const uint32_t *__restrict__ in,
                              uint32_t *__restrict__ out) {
  memset(out, 0, 8 * 4);
  return in;
}

uint32_t *__fastpackwithoutmask1_8(const uint32_t *__restrict__ in,
                                   uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask2_8(const uint32_t *__restrict__ in,
                                   uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask3_8(const uint32_t *__restrict__ in,
                                   uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask4_8(const uint32_t *__restrict__ in,
                                   uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask5_8(const uint32_t *__restrict__ in,
                                   uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask6_8(const uint32_t *__restrict__ in,
                                   uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask7_8(const uint32_t *__restrict__ in,
                                   uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask8_8(const uint32_t *__restrict__ in,
                                   uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask9_8(const uint32_t *__restrict__ in,
                                   uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask10_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask11_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask12_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask13_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask14_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask15_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask16_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask17_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask18_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask19_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask20_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask21_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask22_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask23_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask24_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask25_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask26_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask27_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask28_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask29_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask30_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask31_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask32_8(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

const uint32_t *__fastunpack1_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack2_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack3_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack4_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack5_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack6_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack7_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack8_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack9_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack10_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack11_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack12_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack13_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack14_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack15_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack16_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack17_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack18_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack19_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack20_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack21_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack22_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack23_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack24_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack25_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack26_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack27_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack28_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack29_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack30_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack31_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack32_8(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *fastunpack_8(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out, const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullunpacker8(in, out);

  case 1:
    return __fastunpack1_8(in, out);

  case 2:
    return __fastunpack2_8(in, out);

  case 3:
    return __fastunpack3_8(in, out);

  case 4:
    return __fastunpack4_8(in, out);

  case 5:
    return __fastunpack5_8(in, out);

  case 6:
    return __fastunpack6_8(in, out);

  case 7:
    return __fastunpack7_8(in, out);

  case 8:
    return __fastunpack8_8(in, out);

  case 9:
    return __fastunpack9_8(in, out);

  case 10:
    return __fastunpack10_8(in, out);

  case 11:
    return __fastunpack11_8(in, out);

  case 12:
    return __fastunpack12_8(in, out);

  case 13:
    return __fastunpack13_8(in, out);

  case 14:
    return __fastunpack14_8(in, out);

  case 15:
    return __fastunpack15_8(in, out);

  case 16:
    return __fastunpack16_8(in, out);

  case 17:
    return __fastunpack17_8(in, out);

  case 18:
    return __fastunpack18_8(in, out);

  case 19:
    return __fastunpack19_8(in, out);

  case 20:
    return __fastunpack20_8(in, out);

  case 21:
    return __fastunpack21_8(in, out);

  case 22:
    return __fastunpack22_8(in, out);

  case 23:
    return __fastunpack23_8(in, out);

  case 24:
    return __fastunpack24_8(in, out);

  case 25:
    return __fastunpack25_8(in, out);

  case 26:
    return __fastunpack26_8(in, out);

  case 27:
    return __fastunpack27_8(in, out);

  case 28:
    return __fastunpack28_8(in, out);

  case 29:
    return __fastunpack29_8(in, out);

  case 30:
    return __fastunpack30_8(in, out);

  case 31:
    return __fastunpack31_8(in, out);

  case 32:
    return __fastunpack32_8(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

/*assumes that integers fit in the prescribed number of bits*/
uint32_t *fastpackwithoutmask_8(const uint32_t *__restrict__ in,
                                uint32_t *__restrict__ out,
                                const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullpacker(in, out);

  case 1:
    return __fastpackwithoutmask1_8(in, out);

  case 2:
    return __fastpackwithoutmask2_8(in, out);

  case 3:
    return __fastpackwithoutmask3_8(in, out);

  case 4:
    return __fastpackwithoutmask4_8(in, out);

  case 5:
    return __fastpackwithoutmask5_8(in, out);

  case 6:
    return __fastpackwithoutmask6_8(in, out);

  case 7:
    return __fastpackwithoutmask7_8(in, out);

  case 8:
    return __fastpackwithoutmask8_8(in, out);

  case 9:
    return __fastpackwithoutmask9_8(in, out);

  case 10:
    return __fastpackwithoutmask10_8(in, out);

  case 11:
    return __fastpackwithoutmask11_8(in, out);

  case 12:
    return __fastpackwithoutmask12_8(in, out);

  case 13:
    return __fastpackwithoutmask13_8(in, out);

  case 14:
    return __fastpackwithoutmask14_8(in, out);

  case 15:
    return __fastpackwithoutmask15_8(in, out);

  case 16:
    return __fastpackwithoutmask16_8(in, out);

  case 17:
    return __fastpackwithoutmask17_8(in, out);

  case 18:
    return __fastpackwithoutmask18_8(in, out);

  case 19:
    return __fastpackwithoutmask19_8(in, out);

  case 20:
    return __fastpackwithoutmask20_8(in, out);

  case 21:
    return __fastpackwithoutmask21_8(in, out);

  case 22:
    return __fastpackwithoutmask22_8(in, out);

  case 23:
    return __fastpackwithoutmask23_8(in, out);

  case 24:
    return __fastpackwithoutmask24_8(in, out);

  case 25:
    return __fastpackwithoutmask25_8(in, out);

  case 26:
    return __fastpackwithoutmask26_8(in, out);

  case 27:
    return __fastpackwithoutmask27_8(in, out);

  case 28:
    return __fastpackwithoutmask28_8(in, out);

  case 29:
    return __fastpackwithoutmask29_8(in, out);

  case 30:
    return __fastpackwithoutmask30_8(in, out);

  case 31:
    return __fastpackwithoutmask31_8(in, out);

  case 32:
    return __fastpackwithoutmask32_8(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

const uint32_t *nullunpacker16(const uint32_t *__restrict__ in,
                               uint32_t *__restrict__ out) {
  memset(out, 0, 16 * 4);
  return in;
}

uint32_t *__fastpackwithoutmask1_16(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask2_16(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask3_16(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask4_16(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask5_16(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask6_16(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask7_16(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask8_16(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask9_16(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask10_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask11_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask12_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask13_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask14_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask15_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask16_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask17_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask18_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask19_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask20_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask21_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask22_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask23_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask24_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask25_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask26_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask27_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask28_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask29_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask30_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask31_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask32_16(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

const uint32_t *__fastunpack1_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack2_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack3_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack4_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack5_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack6_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack7_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack8_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack9_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack10_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack11_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack12_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack13_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack14_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack15_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack16_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack17_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack18_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack19_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack20_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack21_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack22_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack23_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack24_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack25_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack26_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack27_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack28_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack29_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack30_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack31_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack32_16(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *fastunpack_16(const uint32_t *__restrict__ in,
                              uint32_t *__restrict__ out, const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullunpacker16(in, out);

  case 1:
    return __fastunpack1_16(in, out);

  case 2:
    return __fastunpack2_16(in, out);

  case 3:
    return __fastunpack3_16(in, out);

  case 4:
    return __fastunpack4_16(in, out);

  case 5:
    return __fastunpack5_16(in, out);

  case 6:
    return __fastunpack6_16(in, out);

  case 7:
    return __fastunpack7_16(in, out);

  case 8:
    return __fastunpack8_16(in, out);

  case 9:
    return __fastunpack9_16(in, out);

  case 10:
    return __fastunpack10_16(in, out);

  case 11:
    return __fastunpack11_16(in, out);

  case 12:
    return __fastunpack12_16(in, out);

  case 13:
    return __fastunpack13_16(in, out);

  case 14:
    return __fastunpack14_16(in, out);

  case 15:
    return __fastunpack15_16(in, out);

  case 16:
    return __fastunpack16_16(in, out);

  case 17:
    return __fastunpack17_16(in, out);

  case 18:
    return __fastunpack18_16(in, out);

  case 19:
    return __fastunpack19_16(in, out);

  case 20:
    return __fastunpack20_16(in, out);

  case 21:
    return __fastunpack21_16(in, out);

  case 22:
    return __fastunpack22_16(in, out);

  case 23:
    return __fastunpack23_16(in, out);

  case 24:
    return __fastunpack24_16(in, out);

  case 25:
    return __fastunpack25_16(in, out);

  case 26:
    return __fastunpack26_16(in, out);

  case 27:
    return __fastunpack27_16(in, out);

  case 28:
    return __fastunpack28_16(in, out);

  case 29:
    return __fastunpack29_16(in, out);

  case 30:
    return __fastunpack30_16(in, out);

  case 31:
    return __fastunpack31_16(in, out);

  case 32:
    return __fastunpack32_16(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

/*assumes that integers fit in the prescribed number of bits*/
uint32_t *fastpackwithoutmask_16(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out,
                                 const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullpacker(in, out);

  case 1:
    return __fastpackwithoutmask1_16(in, out);

  case 2:
    return __fastpackwithoutmask2_16(in, out);

  case 3:
    return __fastpackwithoutmask3_16(in, out);

  case 4:
    return __fastpackwithoutmask4_16(in, out);

  case 5:
    return __fastpackwithoutmask5_16(in, out);

  case 6:
    return __fastpackwithoutmask6_16(in, out);

  case 7:
    return __fastpackwithoutmask7_16(in, out);

  case 8:
    return __fastpackwithoutmask8_16(in, out);

  case 9:
    return __fastpackwithoutmask9_16(in, out);

  case 10:
    return __fastpackwithoutmask10_16(in, out);

  case 11:
    return __fastpackwithoutmask11_16(in, out);

  case 12:
    return __fastpackwithoutmask12_16(in, out);

  case 13:
    return __fastpackwithoutmask13_16(in, out);

  case 14:
    return __fastpackwithoutmask14_16(in, out);

  case 15:
    return __fastpackwithoutmask15_16(in, out);

  case 16:
    return __fastpackwithoutmask16_16(in, out);

  case 17:
    return __fastpackwithoutmask17_16(in, out);

  case 18:
    return __fastpackwithoutmask18_16(in, out);

  case 19:
    return __fastpackwithoutmask19_16(in, out);

  case 20:
    return __fastpackwithoutmask20_16(in, out);

  case 21:
    return __fastpackwithoutmask21_16(in, out);

  case 22:
    return __fastpackwithoutmask22_16(in, out);

  case 23:
    return __fastpackwithoutmask23_16(in, out);

  case 24:
    return __fastpackwithoutmask24_16(in, out);

  case 25:
    return __fastpackwithoutmask25_16(in, out);

  case 26:
    return __fastpackwithoutmask26_16(in, out);

  case 27:
    return __fastpackwithoutmask27_16(in, out);

  case 28:
    return __fastpackwithoutmask28_16(in, out);

  case 29:
    return __fastpackwithoutmask29_16(in, out);

  case 30:
    return __fastpackwithoutmask30_16(in, out);

  case 31:
    return __fastpackwithoutmask31_16(in, out);

  case 32:
    return __fastpackwithoutmask32_16(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

const uint32_t *nullunpacker24(const uint32_t *__restrict__ in,
                               uint32_t *__restrict__ out) {
  memset(out, 0, 24 * 4);
  return in;
}

uint32_t *__fastpackwithoutmask1_24(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 17;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 19;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 21;
  ++in;
  *out |= ((*in)) << 22;
  ++in;
  *out |= ((*in)) << 23;
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask2_24(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask3_24(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 19;
  ++in;
  *out |= ((*in)) << 22;
  ++in;
  *out |= ((*in)) << 25;
  ++in;
  *out |= ((*in)) << 28;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (3 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 5;
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask4_24(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask5_24(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 21;
  ++in;
  *out |= ((*in)) << 26;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (5 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 19;
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask6_24(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask7_24(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 23;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (7 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 19;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (7 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask8_24(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask9_24(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (9 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (9 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 15;
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask10_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask11_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (11 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 17;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (11 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (11 - 8);
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask12_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask13_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (13 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (13 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 17;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (13 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask14_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask15_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (15 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (15 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (15 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (15 - 8);
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask16_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask17_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (17 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (17 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (17 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (17 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask18_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask19_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (19 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (19 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (19 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (19 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (19 - 8);
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask20_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask21_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (21 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (21 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (21 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (21 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (21 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask22_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask23_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (23 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (23 - 21);
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (23 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (23 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (23 - 17);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (23 - 8);
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask24_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask25_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (25 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++out;
  *out = ((*in)) >> (25 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (25 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (25 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  *out = ((*in)) >> (25 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (25 - 24);
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask26_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask27_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (27 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++out;
  *out = ((*in)) >> (27 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++out;
  *out = ((*in)) >> (27 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (27 - 23);
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (27 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (27 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  *out = ((*in)) >> (27 - 8);
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask28_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask29_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (29 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  *out = ((*in)) >> (29 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  *out = ((*in)) >> (29 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++out;
  *out = ((*in)) >> (29 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++out;
  *out = ((*in)) >> (29 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (29 - 27);
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (29 - 24);
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask30_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out + 1;
}

uint32_t *__fastpackwithoutmask31_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (31 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (31 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (31 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  *out = ((*in)) >> (31 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (31 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++out;
  *out = ((*in)) >> (31 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  *out = ((*in)) >> (31 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++out;
  *out = ((*in)) >> (31 - 8);
  ++in;

  return out + 1;
}

uint32_t *__fastpackwithoutmask32_24(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

const uint32_t *__fastunpack1_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) & 1;
  out++;
  *out = ((*in) >> 17) & 1;
  out++;
  *out = ((*in) >> 18) & 1;
  out++;
  *out = ((*in) >> 19) & 1;
  out++;
  *out = ((*in) >> 20) & 1;
  out++;
  *out = ((*in) >> 21) & 1;
  out++;
  *out = ((*in) >> 22) & 1;
  out++;
  *out = ((*in) >> 23) & 1;
  out++;

  return in + 1;
}

const uint32_t *__fastunpack2_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack3_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 3);
  out++;
  *out = ((*in) >> 19) % (1U << 3);
  out++;
  *out = ((*in) >> 22) % (1U << 3);
  out++;
  *out = ((*in) >> 25) % (1U << 3);
  out++;
  *out = ((*in) >> 28) % (1U << 3);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 2)) << (3 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 3);
  out++;
  *out = ((*in) >> 5) % (1U << 3);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack4_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack5_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 5);
  out++;
  *out = ((*in) >> 21) % (1U << 5);
  out++;
  *out = ((*in) >> 26) % (1U << 5);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 4)) << (5 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 5);
  out++;
  *out = ((*in) >> 9) % (1U << 5);
  out++;
  *out = ((*in) >> 14) % (1U << 5);
  out++;
  *out = ((*in) >> 19) % (1U << 5);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack6_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack7_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 7);
  out++;
  *out = ((*in) >> 23) % (1U << 7);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 5)) << (7 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 7);
  out++;
  *out = ((*in) >> 12) % (1U << 7);
  out++;
  *out = ((*in) >> 19) % (1U << 7);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 1)) << (7 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 7);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack8_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack9_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 9);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 2)) << (9 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 9);
  out++;
  *out = ((*in) >> 11) % (1U << 9);
  out++;
  *out = ((*in) >> 20) % (1U << 9);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 6)) << (9 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 9);
  out++;
  *out = ((*in) >> 15) % (1U << 9);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack10_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack11_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 11);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 6)) << (11 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 11);
  out++;
  *out = ((*in) >> 17) % (1U << 11);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 7)) << (11 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 11);
  out++;
  *out = ((*in) >> 18) % (1U << 11);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 8)) << (11 - 8);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack12_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack13_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 13);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 10)) << (13 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 13);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 4)) << (13 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 13);
  out++;
  *out = ((*in) >> 17) % (1U << 13);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 11)) << (13 - 11);
  out++;
  *out = ((*in) >> 11) % (1U << 13);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack14_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack15_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 15);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 14)) << (15 - 14);
  out++;
  *out = ((*in) >> 14) % (1U << 15);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 12)) << (15 - 12);
  out++;
  *out = ((*in) >> 12) % (1U << 15);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 10)) << (15 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 15);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 8)) << (15 - 8);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack16_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack17_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 1)) << (17 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 17);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 3)) << (17 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 17);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 5)) << (17 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 17);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 7)) << (17 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 17);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack18_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack19_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 3)) << (19 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 19);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 9)) << (19 - 9);
  out++;
  *out = ((*in) >> 9) % (1U << 19);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 15)) << (19 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 2)) << (19 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 19);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 8)) << (19 - 8);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack20_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack21_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 5)) << (21 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 21);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 15)) << (21 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 4)) << (21 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 21);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 14)) << (21 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 3)) << (21 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 21);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack22_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack23_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 7)) << (23 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 23);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 21)) << (23 - 21);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 12)) << (23 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 3)) << (23 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 23);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 17)) << (23 - 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 8)) << (23 - 8);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack24_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack25_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 9)) << (25 - 9);
  out++;
  *out = ((*in) >> 9);
  ++in;
  *out |= ((*in) % (1U << 2)) << (25 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 25);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 20)) << (25 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 13)) << (25 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  *out |= ((*in) % (1U << 6)) << (25 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 25);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 24)) << (25 - 24);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack26_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack27_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 11)) << (27 - 11);
  out++;
  *out = ((*in) >> 11);
  ++in;
  *out |= ((*in) % (1U << 6)) << (27 - 6);
  out++;
  *out = ((*in) >> 6);
  ++in;
  *out |= ((*in) % (1U << 1)) << (27 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 27);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 23)) << (27 - 23);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 18)) << (27 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 13)) << (27 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  *out |= ((*in) % (1U << 8)) << (27 - 8);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack28_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack29_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 13)) << (29 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  *out |= ((*in) % (1U << 10)) << (29 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  *out |= ((*in) % (1U << 7)) << (29 - 7);
  out++;
  *out = ((*in) >> 7);
  ++in;
  *out |= ((*in) % (1U << 4)) << (29 - 4);
  out++;
  *out = ((*in) >> 4);
  ++in;
  *out |= ((*in) % (1U << 1)) << (29 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 29);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 27)) << (29 - 27);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 24)) << (29 - 24);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack30_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in + 1;
}

const uint32_t *__fastunpack31_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 15)) << (31 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 14)) << (31 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 13)) << (31 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  *out |= ((*in) % (1U << 12)) << (31 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 11)) << (31 - 11);
  out++;
  *out = ((*in) >> 11);
  ++in;
  *out |= ((*in) % (1U << 10)) << (31 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  *out |= ((*in) % (1U << 9)) << (31 - 9);
  out++;
  *out = ((*in) >> 9);
  ++in;
  *out |= ((*in) % (1U << 8)) << (31 - 8);
  out++;

  return in + 1;
}

const uint32_t *__fastunpack32_24(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *fastunpack_24(const uint32_t *__restrict__ in,
                              uint32_t *__restrict__ out, const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullunpacker24(in, out);

  case 1:
    return __fastunpack1_24(in, out);

  case 2:
    return __fastunpack2_24(in, out);

  case 3:
    return __fastunpack3_24(in, out);

  case 4:
    return __fastunpack4_24(in, out);

  case 5:
    return __fastunpack5_24(in, out);

  case 6:
    return __fastunpack6_24(in, out);

  case 7:
    return __fastunpack7_24(in, out);

  case 8:
    return __fastunpack8_24(in, out);

  case 9:
    return __fastunpack9_24(in, out);

  case 10:
    return __fastunpack10_24(in, out);

  case 11:
    return __fastunpack11_24(in, out);

  case 12:
    return __fastunpack12_24(in, out);

  case 13:
    return __fastunpack13_24(in, out);

  case 14:
    return __fastunpack14_24(in, out);

  case 15:
    return __fastunpack15_24(in, out);

  case 16:
    return __fastunpack16_24(in, out);

  case 17:
    return __fastunpack17_24(in, out);

  case 18:
    return __fastunpack18_24(in, out);

  case 19:
    return __fastunpack19_24(in, out);

  case 20:
    return __fastunpack20_24(in, out);

  case 21:
    return __fastunpack21_24(in, out);

  case 22:
    return __fastunpack22_24(in, out);

  case 23:
    return __fastunpack23_24(in, out);

  case 24:
    return __fastunpack24_24(in, out);

  case 25:
    return __fastunpack25_24(in, out);

  case 26:
    return __fastunpack26_24(in, out);

  case 27:
    return __fastunpack27_24(in, out);

  case 28:
    return __fastunpack28_24(in, out);

  case 29:
    return __fastunpack29_24(in, out);

  case 30:
    return __fastunpack30_24(in, out);

  case 31:
    return __fastunpack31_24(in, out);

  case 32:
    return __fastunpack32_24(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

/*assumes that integers fit in the prescribed number of bits*/
uint32_t *fastpackwithoutmask_24(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out,
                                 const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullpacker(in, out);

  case 1:
    return __fastpackwithoutmask1_24(in, out);

  case 2:
    return __fastpackwithoutmask2_24(in, out);

  case 3:
    return __fastpackwithoutmask3_24(in, out);

  case 4:
    return __fastpackwithoutmask4_24(in, out);

  case 5:
    return __fastpackwithoutmask5_24(in, out);

  case 6:
    return __fastpackwithoutmask6_24(in, out);

  case 7:
    return __fastpackwithoutmask7_24(in, out);

  case 8:
    return __fastpackwithoutmask8_24(in, out);

  case 9:
    return __fastpackwithoutmask9_24(in, out);

  case 10:
    return __fastpackwithoutmask10_24(in, out);

  case 11:
    return __fastpackwithoutmask11_24(in, out);

  case 12:
    return __fastpackwithoutmask12_24(in, out);

  case 13:
    return __fastpackwithoutmask13_24(in, out);

  case 14:
    return __fastpackwithoutmask14_24(in, out);

  case 15:
    return __fastpackwithoutmask15_24(in, out);

  case 16:
    return __fastpackwithoutmask16_24(in, out);

  case 17:
    return __fastpackwithoutmask17_24(in, out);

  case 18:
    return __fastpackwithoutmask18_24(in, out);

  case 19:
    return __fastpackwithoutmask19_24(in, out);

  case 20:
    return __fastpackwithoutmask20_24(in, out);

  case 21:
    return __fastpackwithoutmask21_24(in, out);

  case 22:
    return __fastpackwithoutmask22_24(in, out);

  case 23:
    return __fastpackwithoutmask23_24(in, out);

  case 24:
    return __fastpackwithoutmask24_24(in, out);

  case 25:
    return __fastpackwithoutmask25_24(in, out);

  case 26:
    return __fastpackwithoutmask26_24(in, out);

  case 27:
    return __fastpackwithoutmask27_24(in, out);

  case 28:
    return __fastpackwithoutmask28_24(in, out);

  case 29:
    return __fastpackwithoutmask29_24(in, out);

  case 30:
    return __fastpackwithoutmask30_24(in, out);

  case 31:
    return __fastpackwithoutmask31_24(in, out);

  case 32:
    return __fastpackwithoutmask32_24(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

const uint32_t *nullunpacker32(const uint32_t *__restrict__ in,
                               uint32_t *__restrict__ out) {
  memset(out, 0, 32 * 4);
  return in;
}

uint32_t *__fastpackwithoutmask1_32(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 17;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 19;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 21;
  ++in;
  *out |= ((*in)) << 22;
  ++in;
  *out |= ((*in)) << 23;
  ++in;
  *out |= ((*in)) << 24;
  ++in;
  *out |= ((*in)) << 25;
  ++in;
  *out |= ((*in)) << 26;
  ++in;
  *out |= ((*in)) << 27;
  ++in;
  *out |= ((*in)) << 28;
  ++in;
  *out |= ((*in)) << 29;
  ++in;
  *out |= ((*in)) << 30;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask2_32(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask3_32(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 19;
  ++in;
  *out |= ((*in)) << 22;
  ++in;
  *out |= ((*in)) << 25;
  ++in;
  *out |= ((*in)) << 28;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (3 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 17;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 23;
  ++in;
  *out |= ((*in)) << 26;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask4_32(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask5_32(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 21;
  ++in;
  *out |= ((*in)) << 26;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (5 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 19;
  ++in;
  *out |= ((*in)) << 24;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (5 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 17;
  ++in;
  *out |= ((*in)) << 22;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask6_32(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask7_32(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 23;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (7 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 19;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (7 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 15;
  ++in;
  *out |= ((*in)) << 22;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (7 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask8_32(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask9_32(const uint32_t *__restrict__ in,
                                    uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (9 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (9 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 15;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (9 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 19;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (9 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask10_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask11_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (11 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 17;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (11 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (11 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 19;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (11 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 20;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (11 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask12_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask13_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (13 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (13 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 17;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (13 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (13 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 18;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (13 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (13 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask14_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask15_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (15 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++in;
  *out |= ((*in)) << 29;
  ++out;
  *out = ((*in)) >> (15 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (15 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (15 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (15 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (15 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 19;
  ++out;
  *out = ((*in)) >> (15 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask16_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask17_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (17 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (17 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (17 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (17 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (17 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (17 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (17 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (17 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask18_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask19_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (19 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (19 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (19 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (19 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (19 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (19 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (19 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (19 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (19 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask20_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask21_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (21 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (21 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (21 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (21 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (21 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (21 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  *out = ((*in)) >> (21 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (21 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (21 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (21 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask22_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask23_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (23 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (23 - 21);
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (23 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (23 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 26;
  ++out;
  *out = ((*in)) >> (23 - 17);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (23 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (23 - 22);
  ++in;
  *out |= ((*in)) << 22;
  ++out;
  *out = ((*in)) >> (23 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  *out = ((*in)) >> (23 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (23 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (23 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask24_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask25_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (25 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++out;
  *out = ((*in)) >> (25 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (25 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (25 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  *out = ((*in)) >> (25 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++in;
  *out |= ((*in)) << 31;
  ++out;
  *out = ((*in)) >> (25 - 24);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (25 - 17);
  ++in;
  *out |= ((*in)) << 17;
  ++out;
  *out = ((*in)) >> (25 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  *out = ((*in)) >> (25 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (25 - 21);
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (25 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (25 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask26_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask27_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (27 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++out;
  *out = ((*in)) >> (27 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++out;
  *out = ((*in)) >> (27 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 28;
  ++out;
  *out = ((*in)) >> (27 - 23);
  ++in;
  *out |= ((*in)) << 23;
  ++out;
  *out = ((*in)) >> (27 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (27 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  *out = ((*in)) >> (27 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  *out = ((*in)) >> (27 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (27 - 25);
  ++in;
  *out |= ((*in)) << 25;
  ++out;
  *out = ((*in)) >> (27 - 20);
  ++in;
  *out |= ((*in)) << 20;
  ++out;
  *out = ((*in)) >> (27 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (27 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  *out = ((*in)) >> (27 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask28_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask29_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (29 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  *out = ((*in)) >> (29 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  *out = ((*in)) >> (29 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++out;
  *out = ((*in)) >> (29 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++out;
  *out = ((*in)) >> (29 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++in;
  *out |= ((*in)) << 30;
  ++out;
  *out = ((*in)) >> (29 - 27);
  ++in;
  *out |= ((*in)) << 27;
  ++out;
  *out = ((*in)) >> (29 - 24);
  ++in;
  *out |= ((*in)) << 24;
  ++out;
  *out = ((*in)) >> (29 - 21);
  ++in;
  *out |= ((*in)) << 21;
  ++out;
  *out = ((*in)) >> (29 - 18);
  ++in;
  *out |= ((*in)) << 18;
  ++out;
  *out = ((*in)) >> (29 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (29 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (29 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++out;
  *out = ((*in)) >> (29 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++out;
  *out = ((*in)) >> (29 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask30_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

  return out;
}

uint32_t *__fastpackwithoutmask31_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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
  *out |= ((*in)) << 16;
  ++out;
  *out = ((*in)) >> (31 - 15);
  ++in;
  *out |= ((*in)) << 15;
  ++out;
  *out = ((*in)) >> (31 - 14);
  ++in;
  *out |= ((*in)) << 14;
  ++out;
  *out = ((*in)) >> (31 - 13);
  ++in;
  *out |= ((*in)) << 13;
  ++out;
  *out = ((*in)) >> (31 - 12);
  ++in;
  *out |= ((*in)) << 12;
  ++out;
  *out = ((*in)) >> (31 - 11);
  ++in;
  *out |= ((*in)) << 11;
  ++out;
  *out = ((*in)) >> (31 - 10);
  ++in;
  *out |= ((*in)) << 10;
  ++out;
  *out = ((*in)) >> (31 - 9);
  ++in;
  *out |= ((*in)) << 9;
  ++out;
  *out = ((*in)) >> (31 - 8);
  ++in;
  *out |= ((*in)) << 8;
  ++out;
  *out = ((*in)) >> (31 - 7);
  ++in;
  *out |= ((*in)) << 7;
  ++out;
  *out = ((*in)) >> (31 - 6);
  ++in;
  *out |= ((*in)) << 6;
  ++out;
  *out = ((*in)) >> (31 - 5);
  ++in;
  *out |= ((*in)) << 5;
  ++out;
  *out = ((*in)) >> (31 - 4);
  ++in;
  *out |= ((*in)) << 4;
  ++out;
  *out = ((*in)) >> (31 - 3);
  ++in;
  *out |= ((*in)) << 3;
  ++out;
  *out = ((*in)) >> (31 - 2);
  ++in;
  *out |= ((*in)) << 2;
  ++out;
  *out = ((*in)) >> (31 - 1);
  ++in;
  *out |= ((*in)) << 1;
  ++out;
  ++in;

  return out;
}

uint32_t *__fastpackwithoutmask32_32(const uint32_t *__restrict__ in,
                                     uint32_t *__restrict__ out) {

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

const uint32_t *__fastunpack1_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) & 1;
  out++;
  *out = ((*in) >> 17) & 1;
  out++;
  *out = ((*in) >> 18) & 1;
  out++;
  *out = ((*in) >> 19) & 1;
  out++;
  *out = ((*in) >> 20) & 1;
  out++;
  *out = ((*in) >> 21) & 1;
  out++;
  *out = ((*in) >> 22) & 1;
  out++;
  *out = ((*in) >> 23) & 1;
  out++;
  *out = ((*in) >> 24) & 1;
  out++;
  *out = ((*in) >> 25) & 1;
  out++;
  *out = ((*in) >> 26) & 1;
  out++;
  *out = ((*in) >> 27) & 1;
  out++;
  *out = ((*in) >> 28) & 1;
  out++;
  *out = ((*in) >> 29) & 1;
  out++;
  *out = ((*in) >> 30) & 1;
  out++;
  *out = ((*in) >> 31);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack2_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack3_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 3);
  out++;
  *out = ((*in) >> 19) % (1U << 3);
  out++;
  *out = ((*in) >> 22) % (1U << 3);
  out++;
  *out = ((*in) >> 25) % (1U << 3);
  out++;
  *out = ((*in) >> 28) % (1U << 3);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 2)) << (3 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 3);
  out++;
  *out = ((*in) >> 5) % (1U << 3);
  out++;
  *out = ((*in) >> 8) % (1U << 3);
  out++;
  *out = ((*in) >> 11) % (1U << 3);
  out++;
  *out = ((*in) >> 14) % (1U << 3);
  out++;
  *out = ((*in) >> 17) % (1U << 3);
  out++;
  *out = ((*in) >> 20) % (1U << 3);
  out++;
  *out = ((*in) >> 23) % (1U << 3);
  out++;
  *out = ((*in) >> 26) % (1U << 3);
  out++;
  *out = ((*in) >> 29);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack4_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack5_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 5);
  out++;
  *out = ((*in) >> 21) % (1U << 5);
  out++;
  *out = ((*in) >> 26) % (1U << 5);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 4)) << (5 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 5);
  out++;
  *out = ((*in) >> 9) % (1U << 5);
  out++;
  *out = ((*in) >> 14) % (1U << 5);
  out++;
  *out = ((*in) >> 19) % (1U << 5);
  out++;
  *out = ((*in) >> 24) % (1U << 5);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 2)) << (5 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 5);
  out++;
  *out = ((*in) >> 7) % (1U << 5);
  out++;
  *out = ((*in) >> 12) % (1U << 5);
  out++;
  *out = ((*in) >> 17) % (1U << 5);
  out++;
  *out = ((*in) >> 22) % (1U << 5);
  out++;
  *out = ((*in) >> 27);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack6_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack7_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 7);
  out++;
  *out = ((*in) >> 23) % (1U << 7);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 5)) << (7 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 7);
  out++;
  *out = ((*in) >> 12) % (1U << 7);
  out++;
  *out = ((*in) >> 19) % (1U << 7);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 1)) << (7 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 7);
  out++;
  *out = ((*in) >> 8) % (1U << 7);
  out++;
  *out = ((*in) >> 15) % (1U << 7);
  out++;
  *out = ((*in) >> 22) % (1U << 7);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 4)) << (7 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 7);
  out++;
  *out = ((*in) >> 11) % (1U << 7);
  out++;
  *out = ((*in) >> 18) % (1U << 7);
  out++;
  *out = ((*in) >> 25);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack8_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack9_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 9);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 2)) << (9 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 9);
  out++;
  *out = ((*in) >> 11) % (1U << 9);
  out++;
  *out = ((*in) >> 20) % (1U << 9);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 6)) << (9 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 9);
  out++;
  *out = ((*in) >> 15) % (1U << 9);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 1)) << (9 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 9);
  out++;
  *out = ((*in) >> 10) % (1U << 9);
  out++;
  *out = ((*in) >> 19) % (1U << 9);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 5)) << (9 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 9);
  out++;
  *out = ((*in) >> 14) % (1U << 9);
  out++;
  *out = ((*in) >> 23);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack10_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack11_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 11);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 6)) << (11 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 11);
  out++;
  *out = ((*in) >> 17) % (1U << 11);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 7)) << (11 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 11);
  out++;
  *out = ((*in) >> 18) % (1U << 11);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 8)) << (11 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 11);
  out++;
  *out = ((*in) >> 19) % (1U << 11);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 9)) << (11 - 9);
  out++;
  *out = ((*in) >> 9) % (1U << 11);
  out++;
  *out = ((*in) >> 20) % (1U << 11);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 10)) << (11 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 11);
  out++;
  *out = ((*in) >> 21);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack12_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack13_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 13);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 10)) << (13 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 13);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 4)) << (13 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 13);
  out++;
  *out = ((*in) >> 17) % (1U << 13);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 11)) << (13 - 11);
  out++;
  *out = ((*in) >> 11) % (1U << 13);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 5)) << (13 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 13);
  out++;
  *out = ((*in) >> 18) % (1U << 13);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 12)) << (13 - 12);
  out++;
  *out = ((*in) >> 12) % (1U << 13);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 6)) << (13 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 13);
  out++;
  *out = ((*in) >> 19);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack14_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack15_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16) % (1U << 15);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 14)) << (15 - 14);
  out++;
  *out = ((*in) >> 14) % (1U << 15);
  out++;
  *out = ((*in) >> 29);
  ++in;
  *out |= ((*in) % (1U << 12)) << (15 - 12);
  out++;
  *out = ((*in) >> 12) % (1U << 15);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 10)) << (15 - 10);
  out++;
  *out = ((*in) >> 10) % (1U << 15);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 8)) << (15 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 15);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 6)) << (15 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 15);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 4)) << (15 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 15);
  out++;
  *out = ((*in) >> 19);
  ++in;
  *out |= ((*in) % (1U << 2)) << (15 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 15);
  out++;
  *out = ((*in) >> 17);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack16_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack17_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 1)) << (17 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 17);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 3)) << (17 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 17);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 5)) << (17 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 17);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 7)) << (17 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 17);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 9)) << (17 - 9);
  out++;
  *out = ((*in) >> 9) % (1U << 17);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 11)) << (17 - 11);
  out++;
  *out = ((*in) >> 11) % (1U << 17);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 13)) << (17 - 13);
  out++;
  *out = ((*in) >> 13) % (1U << 17);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 15)) << (17 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack18_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack19_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 3)) << (19 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 19);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 9)) << (19 - 9);
  out++;
  *out = ((*in) >> 9) % (1U << 19);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 15)) << (19 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 2)) << (19 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 19);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 8)) << (19 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 19);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 14)) << (19 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 1)) << (19 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 19);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 7)) << (19 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 19);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 13)) << (19 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack20_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack21_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 5)) << (21 - 5);
  out++;
  *out = ((*in) >> 5) % (1U << 21);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 15)) << (21 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 4)) << (21 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 21);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 14)) << (21 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 3)) << (21 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 21);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 13)) << (21 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  *out |= ((*in) % (1U << 2)) << (21 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 21);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 12)) << (21 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 1)) << (21 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 21);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 11)) << (21 - 11);
  out++;
  *out = ((*in) >> 11);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack22_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack23_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 7)) << (23 - 7);
  out++;
  *out = ((*in) >> 7) % (1U << 23);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 21)) << (23 - 21);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 12)) << (23 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 3)) << (23 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 23);
  out++;
  *out = ((*in) >> 26);
  ++in;
  *out |= ((*in) % (1U << 17)) << (23 - 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 8)) << (23 - 8);
  out++;
  *out = ((*in) >> 8) % (1U << 23);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 22)) << (23 - 22);
  out++;
  *out = ((*in) >> 22);
  ++in;
  *out |= ((*in) % (1U << 13)) << (23 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  *out |= ((*in) % (1U << 4)) << (23 - 4);
  out++;
  *out = ((*in) >> 4) % (1U << 23);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 18)) << (23 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 9)) << (23 - 9);
  out++;
  *out = ((*in) >> 9);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack24_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack25_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 9)) << (25 - 9);
  out++;
  *out = ((*in) >> 9);
  ++in;
  *out |= ((*in) % (1U << 2)) << (25 - 2);
  out++;
  *out = ((*in) >> 2) % (1U << 25);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 20)) << (25 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 13)) << (25 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  *out |= ((*in) % (1U << 6)) << (25 - 6);
  out++;
  *out = ((*in) >> 6) % (1U << 25);
  out++;
  *out = ((*in) >> 31);
  ++in;
  *out |= ((*in) % (1U << 24)) << (25 - 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 17)) << (25 - 17);
  out++;
  *out = ((*in) >> 17);
  ++in;
  *out |= ((*in) % (1U << 10)) << (25 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  *out |= ((*in) % (1U << 3)) << (25 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 25);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 21)) << (25 - 21);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 14)) << (25 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 7)) << (25 - 7);
  out++;
  *out = ((*in) >> 7);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack26_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack27_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 11)) << (27 - 11);
  out++;
  *out = ((*in) >> 11);
  ++in;
  *out |= ((*in) % (1U << 6)) << (27 - 6);
  out++;
  *out = ((*in) >> 6);
  ++in;
  *out |= ((*in) % (1U << 1)) << (27 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 27);
  out++;
  *out = ((*in) >> 28);
  ++in;
  *out |= ((*in) % (1U << 23)) << (27 - 23);
  out++;
  *out = ((*in) >> 23);
  ++in;
  *out |= ((*in) % (1U << 18)) << (27 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 13)) << (27 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  *out |= ((*in) % (1U << 8)) << (27 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  *out |= ((*in) % (1U << 3)) << (27 - 3);
  out++;
  *out = ((*in) >> 3) % (1U << 27);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 25)) << (27 - 25);
  out++;
  *out = ((*in) >> 25);
  ++in;
  *out |= ((*in) % (1U << 20)) << (27 - 20);
  out++;
  *out = ((*in) >> 20);
  ++in;
  *out |= ((*in) % (1U << 15)) << (27 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 10)) << (27 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  *out |= ((*in) % (1U << 5)) << (27 - 5);
  out++;
  *out = ((*in) >> 5);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack28_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack29_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 13)) << (29 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  *out |= ((*in) % (1U << 10)) << (29 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  *out |= ((*in) % (1U << 7)) << (29 - 7);
  out++;
  *out = ((*in) >> 7);
  ++in;
  *out |= ((*in) % (1U << 4)) << (29 - 4);
  out++;
  *out = ((*in) >> 4);
  ++in;
  *out |= ((*in) % (1U << 1)) << (29 - 1);
  out++;
  *out = ((*in) >> 1) % (1U << 29);
  out++;
  *out = ((*in) >> 30);
  ++in;
  *out |= ((*in) % (1U << 27)) << (29 - 27);
  out++;
  *out = ((*in) >> 27);
  ++in;
  *out |= ((*in) % (1U << 24)) << (29 - 24);
  out++;
  *out = ((*in) >> 24);
  ++in;
  *out |= ((*in) % (1U << 21)) << (29 - 21);
  out++;
  *out = ((*in) >> 21);
  ++in;
  *out |= ((*in) % (1U << 18)) << (29 - 18);
  out++;
  *out = ((*in) >> 18);
  ++in;
  *out |= ((*in) % (1U << 15)) << (29 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 12)) << (29 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 9)) << (29 - 9);
  out++;
  *out = ((*in) >> 9);
  ++in;
  *out |= ((*in) % (1U << 6)) << (29 - 6);
  out++;
  *out = ((*in) >> 6);
  ++in;
  *out |= ((*in) % (1U << 3)) << (29 - 3);
  out++;
  *out = ((*in) >> 3);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack30_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *__fastunpack31_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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
  *out = ((*in) >> 16);
  ++in;
  *out |= ((*in) % (1U << 15)) << (31 - 15);
  out++;
  *out = ((*in) >> 15);
  ++in;
  *out |= ((*in) % (1U << 14)) << (31 - 14);
  out++;
  *out = ((*in) >> 14);
  ++in;
  *out |= ((*in) % (1U << 13)) << (31 - 13);
  out++;
  *out = ((*in) >> 13);
  ++in;
  *out |= ((*in) % (1U << 12)) << (31 - 12);
  out++;
  *out = ((*in) >> 12);
  ++in;
  *out |= ((*in) % (1U << 11)) << (31 - 11);
  out++;
  *out = ((*in) >> 11);
  ++in;
  *out |= ((*in) % (1U << 10)) << (31 - 10);
  out++;
  *out = ((*in) >> 10);
  ++in;
  *out |= ((*in) % (1U << 9)) << (31 - 9);
  out++;
  *out = ((*in) >> 9);
  ++in;
  *out |= ((*in) % (1U << 8)) << (31 - 8);
  out++;
  *out = ((*in) >> 8);
  ++in;
  *out |= ((*in) % (1U << 7)) << (31 - 7);
  out++;
  *out = ((*in) >> 7);
  ++in;
  *out |= ((*in) % (1U << 6)) << (31 - 6);
  out++;
  *out = ((*in) >> 6);
  ++in;
  *out |= ((*in) % (1U << 5)) << (31 - 5);
  out++;
  *out = ((*in) >> 5);
  ++in;
  *out |= ((*in) % (1U << 4)) << (31 - 4);
  out++;
  *out = ((*in) >> 4);
  ++in;
  *out |= ((*in) % (1U << 3)) << (31 - 3);
  out++;
  *out = ((*in) >> 3);
  ++in;
  *out |= ((*in) % (1U << 2)) << (31 - 2);
  out++;
  *out = ((*in) >> 2);
  ++in;
  *out |= ((*in) % (1U << 1)) << (31 - 1);
  out++;
  *out = ((*in) >> 1);
  ++in;
  out++;

  return in;
}

const uint32_t *__fastunpack32_32(const uint32_t *__restrict__ in,
                                  uint32_t *__restrict__ out) {

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

  return in;
}

const uint32_t *fastunpack_32(const uint32_t *__restrict__ in,
                              uint32_t *__restrict__ out, const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullunpacker32(in, out);

  case 1:
    return __fastunpack1_32(in, out);

  case 2:
    return __fastunpack2_32(in, out);

  case 3:
    return __fastunpack3_32(in, out);

  case 4:
    return __fastunpack4_32(in, out);

  case 5:
    return __fastunpack5_32(in, out);

  case 6:
    return __fastunpack6_32(in, out);

  case 7:
    return __fastunpack7_32(in, out);

  case 8:
    return __fastunpack8_32(in, out);

  case 9:
    return __fastunpack9_32(in, out);

  case 10:
    return __fastunpack10_32(in, out);

  case 11:
    return __fastunpack11_32(in, out);

  case 12:
    return __fastunpack12_32(in, out);

  case 13:
    return __fastunpack13_32(in, out);

  case 14:
    return __fastunpack14_32(in, out);

  case 15:
    return __fastunpack15_32(in, out);

  case 16:
    return __fastunpack16_32(in, out);

  case 17:
    return __fastunpack17_32(in, out);

  case 18:
    return __fastunpack18_32(in, out);

  case 19:
    return __fastunpack19_32(in, out);

  case 20:
    return __fastunpack20_32(in, out);

  case 21:
    return __fastunpack21_32(in, out);

  case 22:
    return __fastunpack22_32(in, out);

  case 23:
    return __fastunpack23_32(in, out);

  case 24:
    return __fastunpack24_32(in, out);

  case 25:
    return __fastunpack25_32(in, out);

  case 26:
    return __fastunpack26_32(in, out);

  case 27:
    return __fastunpack27_32(in, out);

  case 28:
    return __fastunpack28_32(in, out);

  case 29:
    return __fastunpack29_32(in, out);

  case 30:
    return __fastunpack30_32(in, out);

  case 31:
    return __fastunpack31_32(in, out);

  case 32:
    return __fastunpack32_32(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

/*assumes that integers fit in the prescribed number of bits*/
uint32_t *fastpackwithoutmask_32(const uint32_t *__restrict__ in,
                                 uint32_t *__restrict__ out,
                                 const uint32_t bit) {
  switch (bit) {
  case 0:
    return nullpacker(in, out);

  case 1:
    return __fastpackwithoutmask1_32(in, out);

  case 2:
    return __fastpackwithoutmask2_32(in, out);

  case 3:
    return __fastpackwithoutmask3_32(in, out);

  case 4:
    return __fastpackwithoutmask4_32(in, out);

  case 5:
    return __fastpackwithoutmask5_32(in, out);

  case 6:
    return __fastpackwithoutmask6_32(in, out);

  case 7:
    return __fastpackwithoutmask7_32(in, out);

  case 8:
    return __fastpackwithoutmask8_32(in, out);

  case 9:
    return __fastpackwithoutmask9_32(in, out);

  case 10:
    return __fastpackwithoutmask10_32(in, out);

  case 11:
    return __fastpackwithoutmask11_32(in, out);

  case 12:
    return __fastpackwithoutmask12_32(in, out);

  case 13:
    return __fastpackwithoutmask13_32(in, out);

  case 14:
    return __fastpackwithoutmask14_32(in, out);

  case 15:
    return __fastpackwithoutmask15_32(in, out);

  case 16:
    return __fastpackwithoutmask16_32(in, out);

  case 17:
    return __fastpackwithoutmask17_32(in, out);

  case 18:
    return __fastpackwithoutmask18_32(in, out);

  case 19:
    return __fastpackwithoutmask19_32(in, out);

  case 20:
    return __fastpackwithoutmask20_32(in, out);

  case 21:
    return __fastpackwithoutmask21_32(in, out);

  case 22:
    return __fastpackwithoutmask22_32(in, out);

  case 23:
    return __fastpackwithoutmask23_32(in, out);

  case 24:
    return __fastpackwithoutmask24_32(in, out);

  case 25:
    return __fastpackwithoutmask25_32(in, out);

  case 26:
    return __fastpackwithoutmask26_32(in, out);

  case 27:
    return __fastpackwithoutmask27_32(in, out);

  case 28:
    return __fastpackwithoutmask28_32(in, out);

  case 29:
    return __fastpackwithoutmask29_32(in, out);

  case 30:
    return __fastpackwithoutmask30_32(in, out);

  case 31:
    return __fastpackwithoutmask31_32(in, out);

  case 32:
    return __fastpackwithoutmask32_32(in, out);

  default:
    break;
  }
  throw std::logic_error("number of bits is unsupported");
}

} // namespace FastPFor
