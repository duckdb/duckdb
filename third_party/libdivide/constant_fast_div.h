/*
* When dividing by a known compile time constant, the division can be replaced
* by a multiply+shift operation. GCC will do this automatically, 
* *BUT ONLY FOR DIVISION OF REGISTER-WIDTH OR NARROWER*.
*
* So on an 8-bit system, 16-bit divides will *NOT* be optimised.
*
* The macros here manually apply the multiply+shift operation for 16-bit numbers.
*
* Testing on an AtMega2560, -O3 optimizations:
*   Performance improvement of 85% to 90%+ speed up (division by non-powers of 2)
*   Zero increase in RAM usage
*   Average of 25 bytes Flash used per call site
*     Be careful calling this in a loop with aggressive loop unrolling!
*  
* Note: testing of the multiply+shift technique on 8-bit division showed a 
* slight slow down over native code on AtMega2560. So the 8 bit equivalent 
* macros have not been included
*/

#pragma once
#include "libdivide.h"
#include "u16_ldparams.h"
#include "s16_ldparams.h"

#define CAT_HELPER(a, b) a ## b
#define CONCAT(A, B) CAT_HELPER(A, B)

// GCC will optimise division by a power of 2
// So allow that.
#define S16_ISPOW2_NEG(denom) \
 (denom==-2 || \
  denom==-4 || \
  denom==-8 || \
  denom==-16 || \
  denom==-32 || \
  denom==-64 || \
  denom==-128 || \
  denom==-256 || \
  denom==-512 || \
  denom==-1024 || \
  denom==-2048 || \
  denom==-4096 || \
  denom==-8192 || \
  denom==-16384)
#define S16_ISPOW2_POS(denom) \
 (denom==2 || \
  denom==4 || \
  denom==8 || \
  denom==16 || \
  denom==32 || \
  denom==64 || \
  denom==128 || \
  denom==256 || \
  denom==512 || \
  denom==1024 || \
  denom==2048 || \
  denom==4096 || \
  denom==8192 || \
  denom==16384)
#define U16_ISPOW2(denom) (S16_ISPOW2_POS(denom) || denom==32768)
#define S16_ISPOW2(denom) (S16_ISPOW2_POS(denom) || S16_ISPOW2_NEG(denom))

// Apply the libdivide namespace if necessary
#ifdef __cplusplus
#define LIB_DIV_NAMESPACE libdivide::
#else
#define LIB_DIV_NAMESPACE
#endif

/*
* Wrapper for *unsigned* 16-bit DIVISION. The divisor must be a compile time
* constant.
* E.g. FAST_DIV16U(value, 100)
*/
#define U16_MAGIC(d) CONCAT(CONCAT(U16LD_DENOM_, d), _MAGIC)
#define U16_MORE(d) CONCAT(CONCAT(U16LD_DENOM_, d), _MORE)
#define FAST_DIV16U(a, d) (U16_ISPOW2(d) ? a/d : LIB_DIV_NAMESPACE libdivide_u16_do_raw(a, U16_MAGIC(d), U16_MORE(d)))

/*
* Wrapper for *signed* 16-bit DIVISION by a *POSITIVE* compile time constant. 
* E.g. FAST_DIV16(-value, 777)
*
* This only works for positive parmeters :-(
* A negative number results in a hypen in the macro name, which is not allowed
*/
#define S16_MAGIC(d) CONCAT(CONCAT(S16LD_DENOM_, d), _MAGIC)
#define S16_MORE(d) CONCAT(CONCAT(S16LD_DENOM_, d), _MORE)
#define FAST_DIV16(a, d) (S16_ISPOW2(d) ? a/d : LIB_DIV_NAMESPACE libdivide_s16_do_raw(a, S16_MAGIC(d), S16_MORE(d))) 

/*
* Wrapper for *signed* 16-bit DIVISION by a *NEGATIVE* compile time constant. 
* E.g. FAST_DIV16_NEG(-value, 777) // <-- It's converted to negative. Really.
*
* This only works for positive parmeters :-(
* A negative number results in a hypen in the macro name, which is not allowed
*/
#define S16_MAGIC_NEG(d) CONCAT(CONCAT(S16LD_DENOM_MINUS_, d), _MAGIC)
#define S16_MORE_NEG(d) CONCAT(CONCAT(S16LD_DENOM_MINUS_, d), _MORE)
#define FAST_DIV16_NEG(a, d) (S16_ISPOW2(d) ? a/-d : LIB_DIV_NAMESPACE libdivide_s16_do_raw(a, S16_MAGIC_NEG(d), S16_MORE_NEG(d))) 

/*
* Wrapper for *unsigned* 16-bit MODULUS. The divisor must be a compile time
* constant. 
* E.g. FAST_MOD16U(value, 6)
*/
#define FAST_MOD16U(a, d) (a - (FAST_DIV16U(a, d) * d))
