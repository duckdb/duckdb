#pragma once

#include "duckdb.hpp"

namespace duckdb {

/*
 * We require C99, hence the compiler should understand flexible array
 * members.  However, for documentation purposes we still consider it to be
 * project style to write "field[PG_FLEXIBLE_ARRAY_MEMBER]" not just "field[]".
 * When computing the size of such an object, use "offsetof(struct s, f)"
 * for portability.  Don't use "offsetof(struct s, f[0])", as this doesn't
 * work with MSVC and with C++ compilers.
 */
#ifndef PG_FLEXIBLE_ARRAY_MEMBER
#define PG_FLEXIBLE_ARRAY_MEMBER 1 /* empty */
#endif

/*
 * uintN
 *		Unsigned integer, EXACTLY N BITS IN SIZE,
 *		used for numerical computations and the
 *		frontend/backend protocol.
 */
#ifndef HAVE_UINT8
typedef unsigned char uint8;   /* == 8 bits */
typedef unsigned short uint16; /* == 16 bits */
typedef unsigned int uint32;   /* == 32 bits */
#endif                         /* not HAVE_UINT8 */

/*
 * bitsN
 *		Unit of bitwise operation, AT LEAST N BITS IN SIZE.
 */
typedef uint8 bits8;   /* >= 8 bits */
typedef uint16 bits16; /* >= 16 bits */
typedef uint32 bits32; /* >= 32 bits */

/*
 * These structs describe the header of a varlena object that may have been
 * TOASTed.  Generally, don't reference these structs directly, but use the
 * macros below.
 *
 * We use separate structs for the aligned and unaligned cases because the
 * compiler might otherwise think it could generate code that assumes
 * alignment while touching fields of a 1-byte-header varlena.
 */
typedef union {
	struct /* Normal varlena (4-byte length) */
	{
		uint32 va_header;
		char va_data[PG_FLEXIBLE_ARRAY_MEMBER];
	} va_4byte;
	struct /* Compressed-in-line format */
	{
		uint32 va_header;
		uint32 va_tcinfo;                       /* Original data size (excludes header) and
		                                         * compression method; see va_extinfo */
		char va_data[PG_FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
	} va_compressed;
} varattrib_4b;

#define SET_VARSIZE_4B(PTR, len) (((varattrib_4b *)(PTR))->va_4byte.va_header = (((uint32)(len)) << 2))

#define SET_VARSIZE(PTR, len) SET_VARSIZE_4B(PTR, len)

} // namespace duckdb
