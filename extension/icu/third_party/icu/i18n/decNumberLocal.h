// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/* ------------------------------------------------------------------ */
/* decNumber package local type, tuning, and macro definitions        */
/* ------------------------------------------------------------------ */
/* Copyright (c) IBM Corporation, 2000-2016.   All rights reserved.   */
/*                                                                    */
/* This software is made available under the terms of the             */
/* ICU License -- ICU 1.8.1 and later.                                */
/*                                                                    */
/* The description and User's Guide ("The decNumber C Library") for   */
/* this software is called decNumber.pdf.  This document is           */
/* available, together with arithmetic and format specifications,     */
/* testcases, and Web links, on the General Decimal Arithmetic page.  */
/*                                                                    */
/* Please send comments, suggestions, and corrections to the author:  */
/*   mfc@uk.ibm.com                                                   */
/*   Mike Cowlishaw, IBM Fellow                                       */
/*   IBM UK, PO Box 31, Birmingham Road, Warwick CV34 5JL, UK         */
/* ------------------------------------------------------------------ */
/* This header file is included by all modules in the decNumber       */
/* library, and contains local type definitions, tuning parameters,   */
/* etc.  It should not need to be used by application programs.       */
/* decNumber.h or one of decDouble (etc.) must be included first.     */
/* ------------------------------------------------------------------ */

#ifndef DECNUMBERLOC_H
#define DECNUMBERLOC_H
#define DECVERSION    "decNumber 3.61" /* Package Version [16 max.] */

#include <stdlib.h>         /* for abs                              */
#include <string.h>         /* for memset, strcpy                   */
#include "decContext.h"

  /* Conditional code flag -- set this to match hardware platform     */
  #if !defined(DECLITEND)
  #define DECLITEND 1         /* 1=little-endian, 0=big-endian        */
  #endif

  /* Conditional code flag -- set this to 1 for best performance      */
  #if !defined(DECUSE64)
  #define DECUSE64  1         /* 1=use int64s, 0=int32 & smaller only */
  #endif

  /* Conditional check flags -- set these to 0 for best performance   */
  #if !defined(DECCHECK)
  #define DECCHECK  0         /* 1 to enable robust checking          */
  #endif
  #if !defined(DECALLOC)
  #define DECALLOC  0         /* 1 to enable memory accounting        */
  #endif
  #if !defined(DECTRACE)
  #define DECTRACE  0         /* 1 to trace certain internals, etc.   */
  #endif

  /* Tuning parameter for decNumber (arbitrary precision) module      */
  #if !defined(DECBUFFER)
  #define DECBUFFER 36        /* Size basis for local buffers.  This  */
                              /* should be a common maximum precision */
                              /* rounded up to a multiple of 4; must  */
                              /* be zero or positive.                 */
  #endif

  /* ---------------------------------------------------------------- */
  /* Definitions for all modules (general-purpose)                    */
  /* ---------------------------------------------------------------- */

  /* Local names for common types -- for safety, decNumber modules do */
  /* not use int or long directly.                                    */
  #define Flag   uint8_t
  #define Byte   int8_t
  #define uByte  uint8_t
  #define Short  int16_t
  #define uShort uint16_t
  #define Int    int32_t
  #define uInt   uint32_t
  #define Unit   decNumberUnit
  #if DECUSE64
  #define Long   int64_t
  #define uLong  uint64_t
  #endif

  /* Development-use definitions                                      */
  typedef long int LI;        /* for printf arguments only            */
  #define DECNOINT  0         /* 1 to check no internal use of 'int'  */
                              /*   or stdint types                    */
  #if DECNOINT
    /* if these interfere with your C includes, do not set DECNOINT   */
    #define int     ?         /* enable to ensure that plain C 'int'  */
    #define long    ??        /* .. or 'long' types are not used      */
  #endif

  /* LONGMUL32HI -- set w=(u*v)>>32, where w, u, and v are uInts      */
  /* (that is, sets w to be the high-order word of the 64-bit result; */
  /* the low-order word is simply u*v.)                               */
  /* This version is derived from Knuth via Hacker's Delight;         */
  /* it seems to optimize better than some others tried               */
  #define LONGMUL32HI(w, u, v) {             \
    uInt u0, u1, v0, v1, w0, w1, w2, t;      \
    u0=u & 0xffff; u1=u>>16;                 \
    v0=v & 0xffff; v1=v>>16;                 \
    w0=u0*v0;                                \
    t=u1*v0 + (w0>>16);                      \
    w1=t & 0xffff; w2=t>>16;                 \
    w1=u0*v1 + w1;                           \
    (w)=u1*v1 + w2 + (w1>>16);}

  /* ROUNDUP -- round an integer up to a multiple of n                */
  #define ROUNDUP(i, n) ((((i)+(n)-1)/n)*n)
  #define ROUNDUP4(i)   (((i)+3)&~3)    /* special for n=4            */

  /* ROUNDDOWN -- round an integer down to a multiple of n            */
  #define ROUNDDOWN(i, n) (((i)/n)*n)
  #define ROUNDDOWN4(i)   ((i)&~3)      /* special for n=4            */

  /* References to multi-byte sequences under different sizes; these  */
  /* require locally declared variables, but do not violate strict    */
  /* aliasing or alignment (as did the UINTAT simple cast to uInt).   */
  /* Variables needed are uswork, uiwork, etc. [so do not use at same */
  /* level in an expression, e.g., UBTOUI(x)==UBTOUI(y) may fail].    */

  /* Return a uInt, etc., from bytes starting at a char* or uByte*    */
  #define UBTOUS(b)  (memcpy((void *)&uswork, b, 2), uswork)
  #define UBTOUI(b)  (memcpy((void *)&uiwork, b, 4), uiwork)

  /* Store a uInt, etc., into bytes starting at a char* or uByte*.    */
  /* Returns i, evaluated, for convenience; has to use uiwork because */
  /* i may be an expression.                                          */
  #define UBFROMUS(b, i)  (uswork=(i), memcpy(b, (void *)&uswork, 2), uswork)
  #define UBFROMUI(b, i)  (uiwork=(i), memcpy(b, (void *)&uiwork, 4), uiwork)

  /* X10 and X100 -- multiply integer i by 10 or 100                  */
  /* [shifts are usually faster than multiply; could be conditional]  */
  #define X10(i)  (((i)<<1)+((i)<<3))
  #define X100(i) (((i)<<2)+((i)<<5)+((i)<<6))

  /* MAXI and MINI -- general max & min (not in ANSI) for integers    */
  #define MAXI(x,y) ((x)<(y)?(y):(x))
  #define MINI(x,y) ((x)>(y)?(y):(x))

  /* Useful constants                                                 */
  #define BILLION      1000000000            /* 10**9                 */
  /* CHARMASK: 0x30303030 for ASCII/UTF8; 0xF0F0F0F0 for EBCDIC       */
  #define CHARMASK ((((((((uInt)'0')<<8)+'0')<<8)+'0')<<8)+'0')


  /* ---------------------------------------------------------------- */
  /* Definitions for arbitary-precision modules (only valid after     */
  /* decNumber.h has been included)                                   */
  /* ---------------------------------------------------------------- */

  /* Limits and constants                                             */
  #define DECNUMMAXP 999999999  /* maximum precision code can handle  */
  #define DECNUMMAXE 999999999  /* maximum adjusted exponent ditto    */
  #define DECNUMMINE -999999999 /* minimum adjusted exponent ditto    */
  #if (DECNUMMAXP != DEC_MAX_DIGITS)
    #error Maximum digits mismatch
  #endif
  #if (DECNUMMAXE != DEC_MAX_EMAX)
    #error Maximum exponent mismatch
  #endif
  #if (DECNUMMINE != DEC_MIN_EMIN)
    #error Minimum exponent mismatch
  #endif

  /* Set DECDPUNMAX -- the maximum integer that fits in DECDPUN       */
  /* digits, and D2UTABLE -- the initializer for the D2U table        */
  // #ifndef DECDPUN
    // no-op
  // #elif   DECDPUN==1
    #define DECDPUNMAX 9
    #define D2UTABLE {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,  \
                      18,19,20,21,22,23,24,25,26,27,28,29,30,31,32, \
                      33,34,35,36,37,38,39,40,41,42,43,44,45,46,47, \
                      48,49}

  /* ----- Shared data (in decNumber.c) ----- */
  /* Public lookup table used by the D2U macro (see below)            */
  #define DECMAXD2U 49
  /*extern const uByte d2utable[DECMAXD2U+1];*/

  /* ----- Macros ----- */
  /* ISZERO -- return true if decNumber dn is a zero                  */
  /* [performance-critical in some situations]                        */
  #define ISZERO(dn) decNumberIsZero(dn)     /* now just a local name */

  /* D2U -- return the number of Units needed to hold d digits        */
  /* (runtime version, with table lookaside for small d)              */
  #if defined(DECDPUN) && DECDPUN==8
    #define D2U(d) ((unsigned)((d)<=DECMAXD2U?d2utable[d]:((d)+7)>>3))
  #elif defined(DECDPUN) && DECDPUN==4
    #define D2U(d) ((unsigned)((d)<=DECMAXD2U?d2utable[d]:((d)+3)>>2))
  #else
    #define D2U(d) ((d)<=DECMAXD2U?d2utable[d]:((d)+DECDPUN-1)/DECDPUN)
  #endif
  /* SD2U -- static D2U macro (for compile-time calculation)          */
  #define SD2U(d) (((d)+DECDPUN-1)/DECDPUN)

  /* MSUDIGITS -- returns digits in msu, from digits, calculated      */
  /* using D2U                                                        */
  #define MSUDIGITS(d) ((d)-(D2U(d)-1)*DECDPUN)

  /* D2N -- return the number of decNumber structs that would be      */
  /* needed to contain that number of digits (and the initial         */
  /* decNumber struct) safely.  Note that one Unit is included in the */
  /* initial structure.  Used for allocating space that is aligned on */
  /* a decNumber struct boundary. */
  #define D2N(d) \
    ((((SD2U(d)-1)*sizeof(Unit))+sizeof(decNumber)*2-1)/sizeof(decNumber))

  /* TODIGIT -- macro to remove the leading digit from the unsigned   */
  /* integer u at column cut (counting from the right, LSD=0) and     */
  /* place it as an ASCII character into the character pointed to by  */
  /* c.  Note that cut must be <= 9, and the maximum value for u is   */
  /* 2,000,000,000 (as is needed for negative exponents of            */
  /* subnormals).  The unsigned integer pow is used as a temporary    */
  /* variable. */
  #define TODIGIT(u, cut, c, pow) UPRV_BLOCK_MACRO_BEGIN { \
    *(c)='0';                             \
    pow=DECPOWERS[cut]*2;                 \
    if ((u)>pow) {                        \
      pow*=4;                             \
      if ((u)>=pow) {(u)-=pow; *(c)+=8;}  \
      pow/=2;                             \
      if ((u)>=pow) {(u)-=pow; *(c)+=4;}  \
      pow/=2;                             \
      }                                   \
    if ((u)>=pow) {(u)-=pow; *(c)+=2;}    \
    pow/=2;                               \
    if ((u)>=pow) {(u)-=pow; *(c)+=1;}    \
    } UPRV_BLOCK_MACRO_END

  /* ---------------------------------------------------------------- */
  /* Definitions for fixed-precision modules (only valid after        */
  /* decSingle.h, decDouble.h, or decQuad.h has been included)        */
  /* ---------------------------------------------------------------- */

  /* bcdnum -- a structure describing a format-independent finite     */
  /* number, whose coefficient is a string of bcd8 uBytes             */
  typedef struct {
    uByte   *msd;             /* -> most significant digit            */
    uByte   *lsd;             /* -> least ditto                       */
    uInt     sign;            /* 0=positive, DECFLOAT_Sign=negative   */
    Int      exponent;        /* Unadjusted signed exponent (q), or   */
                              /* DECFLOAT_NaN etc. for a special      */
    } bcdnum;

  /* Test if exponent or bcdnum exponent must be a special, etc.      */
  #define EXPISSPECIAL(exp) ((exp)>=DECFLOAT_MinSp)
  #define EXPISINF(exp) (exp==DECFLOAT_Inf)
  #define EXPISNAN(exp) (exp==DECFLOAT_qNaN || exp==DECFLOAT_sNaN)
  #define NUMISSPECIAL(num) (EXPISSPECIAL((num)->exponent))

  /* Refer to a 32-bit word or byte in a decFloat (df) by big-endian  */
  /* (array) notation (the 0 word or byte contains the sign bit),     */
  /* automatically adjusting for endianness; similarly address a word */
  /* in the next-wider format (decFloatWider, or dfw)                 */
  #define DECWORDS  (DECBYTES/4)
  #define DECWWORDS (DECWBYTES/4)
  #if DECLITEND
    #define DFBYTE(df, off)   ((df)->bytes[DECBYTES-1-(off)])
    #define DFWORD(df, off)   ((df)->words[DECWORDS-1-(off)])
    #define DFWWORD(dfw, off) ((dfw)->words[DECWWORDS-1-(off)])
  #else
    #define DFBYTE(df, off)   ((df)->bytes[off])
    #define DFWORD(df, off)   ((df)->words[off])
    #define DFWWORD(dfw, off) ((dfw)->words[off])
  #endif

  /* Tests for sign or specials, directly on DECFLOATs                */
  #define DFISSIGNED(df)   (DFWORD(df, 0)&0x80000000)
  #define DFISSPECIAL(df) ((DFWORD(df, 0)&0x78000000)==0x78000000)
  #define DFISINF(df)     ((DFWORD(df, 0)&0x7c000000)==0x78000000)
  #define DFISNAN(df)     ((DFWORD(df, 0)&0x7c000000)==0x7c000000)
  #define DFISQNAN(df)    ((DFWORD(df, 0)&0x7e000000)==0x7c000000)
  #define DFISSNAN(df)    ((DFWORD(df, 0)&0x7e000000)==0x7e000000)

  /* Shared lookup tables                                             */
  extern const uInt   DECCOMBMSD[64];   /* Combination field -> MSD   */
  extern const uInt   DECCOMBFROM[48];  /* exp+msd -> Combination     */

  /* Private generic (utility) routine                                */
  #if DECCHECK || DECTRACE
    extern void decShowNum(const bcdnum *, const char *);
  #endif

// #else
//   #error decNumberLocal included more than once
#endif
