//---------------------------------------------------------------------------
//	@filename:
//		utils.h
//
//	@doc:
//		Various utilities which are not necessarily gpos specific
//
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_utils_H
#define GPOS_utils_H

#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/io/COstreamBasic.h"
#include "duckdb/optimizer/cascade/types.h"

#define GPOS_x86_64 1
#define GPOS_64BIT 1

#if (GPOS_i386 || GPOS_i686 || GPOS_x86_64) && (GPOS_32BIT)
#define GPOS_ASMFP asm volatile("movl %%ebp, %0" : "=g"(ulp));
#define GPOS_ASMSP asm volatile("movl %%esp, %0" : "=g"(ulp));

#elif (GPOS_i386 || GPOS_i686 || GPOS_x86_64) && (GPOS_64BIT)
#define GPOS_ASMFP asm volatile("movq %%rbp, %0" : "=g"(ulp));
#define GPOS_ASMSP asm volatile("movq %%rsp, %0" : "=g"(ulp));

#elif (GPOS_aarch64) && (GPOS_64BIT)
#define GPOS_ASMFP asm volatile("mov %0, fp" : "=g"(ulp));
#define GPOS_ASMSP asm volatile("mov %0, sp" : "=g"(ulp));

#elif (GPOS_sparc) && (GPOS_32BIT)
#define GPOS_ASMFP asm volatile("st %%fp, %0" : "=g"(ulp));
#define GPOS_ASMSP asm volatile("st %%sp, %0" : "=g"(ulp));

#elif (GPOS_sparc) && (GPOS_64BIT)
#define GPOS_ASMFP asm volatile("stx %%fp, %0" : "=g"(ulp));
#define GPOS_ASMSP asm volatile("stx %%sp, %0" : "=g"(ulp));

#endif

#define ALIGNED_16(x) \
	(((ULONG_PTR) x >> 1) << 1 == (ULONG_PTR) x)  // checks 16-bit alignment
#define ALIGNED_32(x) \
	(((ULONG_PTR) x >> 2) << 2 == (ULONG_PTR) x)  // checks 32-bit alignment
#define ALIGNED_64(x) \
	(((ULONG_PTR) x >> 3) << 3 == (ULONG_PTR) x)  // checks 64-bit alignment

#if GPOS_32BIT	// align to 32 bits
#define MAX_ALIGNED(x) ALIGNED_32(x)
#else  // align to 64 bits
#define MAX_ALIGNED(x) ALIGNED_64(x)
#endif

#if GPOS_32BIT	// force alignment to 32 bits
#define ALIGN_STORAGE __attribute__((aligned(4)))
#else  // force alignment to 64 bits
#define ALIGN_STORAGE __attribute__((aligned(8)))
#endif

#define GPOS_GET_FRAME_POINTER(x) \
	do                            \
	{                             \
		ULONG_PTR ulp;            \
		GPOS_ASMFP;               \
		x = ulp;                  \
	} while (0)
#define GPOS_GET_STACK_POINTER(x) \
	do                            \
	{                             \
		ULONG_PTR ulp;            \
		GPOS_ASMSP;               \
		x = ulp;                  \
	} while (0)

#define GPOS_MSEC_IN_SEC ((ULLONG) 1000)
#define GPOS_USEC_IN_MSEC ((ULLONG) 1000)
#define GPOS_USEC_IN_SEC (((ULLONG) 1000) * 1000)
#define GPOS_NSEC_IN_SEC (((ULLONG) 1000) * 1000 * 1000)

namespace gpos
{
// print wide-character string to stdout
void Print(WCHAR *wsz);

// generic memory dumper routine
IOstream &HexDump(IOstream &os, const void *pv, ULLONG size);

// generic hash function for byte strings
ULONG HashByteArray(const BYTE *, const ULONG);

// generic hash function; by address
template <class T>
inline ULONG
HashValue(const T *pt)
{
	return HashByteArray((BYTE *) pt, GPOS_SIZEOF(T));
}

// generic hash function for pointer types -- use e.g. when address is ID of object
template <class T> inline ULONG HashPtr(const T* pt)
{
	return HashByteArray((BYTE *) &pt, GPOS_SIZEOF(void *));
}

// equality function on pointers
template <class T>
inline BOOL
EqualPtr(const T *pt1, const T *pt2)
{
	return pt1 == pt2;
}

// hash function for ULONG_PTR
inline ULONG
HashULongPtr(const ULONG_PTR &key)
{
	return (ULONG) key;
}

// combine ULONG hashes
ULONG CombineHashes(ULONG, ULONG);

// equality function, which uses the equality operator of the arguments type
template <class T>
inline BOOL
Equals(const T *pt1, const T *pt2)
{
	return *pt1 == *pt2;
}

// equality function for ULONG_PTR
inline BOOL
EqualULongPtr(const ULONG_PTR &key_left, const ULONG_PTR &key_right)
{
	return key_left == key_right;
}

// yield and sleep (time in muSec)
// note that in some platforms the minimum sleep interval is 1ms
void USleep(ULONG);

// add two unsigned long long values, throw an exception if overflow occurs
ULLONG Add(ULLONG first, ULLONG second);

// multiply two unsigned long long values, throw an exception if overflow occurs
ULLONG Multiply(ULLONG first, ULLONG second);

// extern definitions for standard streams; to be used during
// startup/shutdown when outside of task framework
extern COstreamBasic oswcerr;
extern COstreamBasic oswcout;

}  // namespace gpos


#endif
