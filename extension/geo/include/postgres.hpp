#pragma once

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

// /* PROJ library version */
// // #define POSTGIS_PROJ_VERSION 82
// #define POSTGIS_PROJ_VERSION 1

// /*
//  * intN
//  *		Signed integer, EXACTLY N BITS IN SIZE,
//  *		used for numerical computations and the
//  *		frontend/backend protocol.
//  */
// #ifndef HAVE_INT8
// typedef signed char int8;   /* == 8 bits */
// typedef signed short int16; /* == 16 bits */
// typedef signed int int32;   /* == 32 bits */
// #endif                      /* not HAVE_INT8 */

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

// /* this test relies on the specific tag values above */
// #define VARTAG_IS_EXPANDED(tag) (((tag) & ~1) == VARTAG_EXPANDED_RO)

// #define VARTAG_SIZE(tag)                                                                                               \
// 	((tag) == VARTAG_INDIRECT  ? sizeof(varatt_indirect)                                                               \
// 	 : VARTAG_IS_EXPANDED(tag) ? sizeof(varatt_expanded)                                                               \
// 	 : (tag) == VARTAG_ONDISK  ? sizeof(varatt_external)                                                               \
// 	                           : 0)

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

// typedef struct {
// 	uint8 va_header;
// 	char va_data[PG_FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
// } varattrib_1b;

// /* TOAST pointers are a subset of varattrib_1b with an identifying tag byte */
// typedef struct {
// 	uint8 va_header;                        /* Always 0x80 or 0x01 */
// 	uint8 va_tag;                           /* Type of datum */
// 	char va_data[PG_FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
// } varattrib_1b_e;

// /*
//  * Bit layouts for varlena headers on big-endian machines:
//  *
//  * 00xxxxxx 4-byte length word, aligned, uncompressed data (up to 1G)
//  * 01xxxxxx 4-byte length word, aligned, *compressed* data (up to 1G)
//  * 10000000 1-byte length word, unaligned, TOAST pointer
//  * 1xxxxxxx 1-byte length word, unaligned, uncompressed data (up to 126b)
//  *
//  * Bit layouts for varlena headers on little-endian machines:
//  *
//  * xxxxxx00 4-byte length word, aligned, uncompressed data (up to 1G)
//  * xxxxxx10 4-byte length word, aligned, *compressed* data (up to 1G)
//  * 00000001 1-byte length word, unaligned, TOAST pointer
//  * xxxxxxx1 1-byte length word, unaligned, uncompressed data (up to 126b)
//  *
//  * The "xxx" bits are the length field (which includes itself in all cases).
//  * In the big-endian case we mask to extract the length, in the little-endian
//  * case we shift.  Note that in both cases the flag bits are in the physically
//  * first byte.  Also, it is not possible for a 1-byte length word to be zero;
//  * this lets us disambiguate alignment padding bytes from the start of an
//  * unaligned datum.  (We now *require* pad bytes to be filled with zero!)
//  *
//  * In TOAST pointers the va_tag field (see varattrib_1b_e) is used to discern
//  * the specific type and length of the pointer datum.
//  */

// /*
//  * Endian-dependent macros.  These are considered internal --- use the
//  * external macros below instead of using these directly.
//  *
//  * Note: IS_1B is true for external toast records but VARSIZE_1B will return 0
//  * for such records. Hence you should usually check for IS_EXTERNAL before
//  * checking for IS_1B.
//  */

// #ifdef WORDS_BIGENDIAN

// #define VARATT_IS_4B(PTR)        ((((varattrib_1b *)(PTR))->va_header & 0x80) == 0x00)
// #define VARATT_IS_4B_U(PTR)      ((((varattrib_1b *)(PTR))->va_header & 0xC0) == 0x00)
// #define VARATT_IS_4B_C(PTR)      ((((varattrib_1b *)(PTR))->va_header & 0xC0) == 0x40)
// #define VARATT_IS_1B(PTR)        ((((varattrib_1b *)(PTR))->va_header & 0x80) == 0x80)
// #define VARATT_IS_1B_E(PTR)      ((((varattrib_1b *)(PTR))->va_header) == 0x80)
// #define VARATT_NOT_PAD_BYTE(PTR) (*((uint8 *)(PTR)) != 0)

// /* VARSIZE_4B() should only be used on known-aligned data */
// #define VARSIZE_4B(PTR)  (((varattrib_4b *)(PTR))->va_4byte.va_header & 0x3FFFFFFF)
// #define VARSIZE_1B(PTR)  (((varattrib_1b *)(PTR))->va_header & 0x7F)
// #define VARTAG_1B_E(PTR) (((varattrib_1b_e *)(PTR))->va_tag)

// #define SET_VARSIZE_4B(PTR, len)   (((varattrib_4b *)(PTR))->va_4byte.va_header = (len)&0x3FFFFFFF)
// #define SET_VARSIZE_4B_C(PTR, len) (((varattrib_4b *)(PTR))->va_4byte.va_header = ((len)&0x3FFFFFFF) | 0x40000000)
// #define SET_VARSIZE_1B(PTR, len)   (((varattrib_1b *)(PTR))->va_header = (len) | 0x80)
// #define SET_VARTAG_1B_E(PTR, tag)                                                                                      \
// 	(((varattrib_1b_e *)(PTR))->va_header = 0x80, ((varattrib_1b_e *)(PTR))->va_tag = (tag))

// #else /* !WORDS_BIGENDIAN */

// #define VARATT_IS_4B(PTR)        ((((varattrib_1b *)(PTR))->va_header & 0x01) == 0x00)
// #define VARATT_IS_4B_U(PTR)      ((((varattrib_1b *)(PTR))->va_header & 0x03) == 0x00)
// #define VARATT_IS_4B_C(PTR)      ((((varattrib_1b *)(PTR))->va_header & 0x03) == 0x02)
// #define VARATT_IS_1B(PTR)        ((((varattrib_1b *)(PTR))->va_header & 0x01) == 0x01)
// #define VARATT_IS_1B_E(PTR)      ((((varattrib_1b *)(PTR))->va_header) == 0x01)
// #define VARATT_NOT_PAD_BYTE(PTR) (*((uint8 *)(PTR)) != 0)

// /* VARSIZE_4B() should only be used on known-aligned data */
// #define VARSIZE_4B(PTR)          ((((varattrib_4b *)(PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
// #define VARSIZE_1B(PTR)          ((((varattrib_1b *)(PTR))->va_header >> 1) & 0x7F)
// #define VARTAG_1B_E(PTR)         (((varattrib_1b_e *)(PTR))->va_tag)

#define SET_VARSIZE_4B(PTR, len)   (((varattrib_4b *)(PTR))->va_4byte.va_header = (((uint32)(len)) << 2))
// #define SET_VARSIZE_4B_C(PTR, len) (((varattrib_4b *)(PTR))->va_4byte.va_header = (((uint32)(len)) << 2) | 0x02)
// #define SET_VARSIZE_1B(PTR, len)   (((varattrib_1b *)(PTR))->va_header = (((uint8)(len)) << 1) | 0x01)
// #define SET_VARTAG_1B_E(PTR, tag)                                                                                      \
// 	(((varattrib_1b_e *)(PTR))->va_header = 0x01, ((varattrib_1b_e *)(PTR))->va_tag = (tag))

// #endif /* WORDS_BIGENDIAN */

// #define VARDATA_4B(PTR)   (((varattrib_4b *)(PTR))->va_4byte.va_data)
// #define VARDATA_4B_C(PTR) (((varattrib_4b *)(PTR))->va_compressed.va_data)
// #define VARDATA_1B(PTR)   (((varattrib_1b *)(PTR))->va_data)
// #define VARDATA_1B_E(PTR) (((varattrib_1b_e *)(PTR))->va_data)

// /*
//  * Externally visible TOAST macros begin here.
//  */

// #define VARHDRSZ_EXTERNAL   offsetof(varattrib_1b_e, va_data)
// #define VARHDRSZ_COMPRESSED offsetof(varattrib_4b, va_compressed.va_data)
// #define VARHDRSZ_SHORT      offsetof(varattrib_1b, va_data)

// #define VARATT_SHORT_MAX 0x7F
// #define VARATT_CAN_MAKE_SHORT(PTR)                                                                                     \
// 	(VARATT_IS_4B_U(PTR) && (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX)
// #define VARATT_CONVERTED_SHORT_SIZE(PTR) (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT)

// /*
//  * In consumers oblivious to data alignment, call PG_DETOAST_DATUM_PACKED(),
//  * VARDATA_ANY(), VARSIZE_ANY() and VARSIZE_ANY_EXHDR().  Elsewhere, call
//  * PG_DETOAST_DATUM(), VARDATA() and VARSIZE().  Directly fetching an int16,
//  * int32 or wider field in the struct representing the datum layout requires
//  * aligned data.  memcpy() is alignment-oblivious, as are most operations on
//  * datatypes, such as text, whose layout struct contains only char fields.
//  *
//  * Code assembling a new datum should call VARDATA() and SET_VARSIZE().
//  * (Datums begin life untoasted.)
//  *
//  * Other macros here should usually be used only by tuple assembly/disassembly
//  * code and code that specifically wants to work with still-toasted Datums.
//  */
// #define VARDATA(PTR) VARDATA_4B(PTR)
// #define VARSIZE(PTR) VARSIZE_4B(PTR)

// #define VARSIZE_SHORT(PTR) VARSIZE_1B(PTR)
// #define VARDATA_SHORT(PTR) VARDATA_1B(PTR)

// #define VARTAG_EXTERNAL(PTR)  VARTAG_1B_E(PTR)
// #define VARSIZE_EXTERNAL(PTR) (VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))
// #define VARDATA_EXTERNAL(PTR) VARDATA_1B_E(PTR)

// #define VARATT_IS_COMPRESSED(PTR)            VARATT_IS_4B_C(PTR)
// #define VARATT_IS_EXTERNAL(PTR)              VARATT_IS_1B_E(PTR)
// #define VARATT_IS_EXTERNAL_ONDISK(PTR)       (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_ONDISK)
// #define VARATT_IS_EXTERNAL_INDIRECT(PTR)     (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_INDIRECT)
// #define VARATT_IS_EXTERNAL_EXPANDED_RO(PTR)  (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RO)
// #define VARATT_IS_EXTERNAL_EXPANDED_RW(PTR)  (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RW)
// #define VARATT_IS_EXTERNAL_EXPANDED(PTR)     (VARATT_IS_EXTERNAL(PTR) && VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))
// #define VARATT_IS_EXTERNAL_NON_EXPANDED(PTR) (VARATT_IS_EXTERNAL(PTR) && !VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))
// #define VARATT_IS_SHORT(PTR)                 VARATT_IS_1B(PTR)
// #define VARATT_IS_EXTENDED(PTR)              (!VARATT_IS_4B_U(PTR))

#define SET_VARSIZE(PTR, len)            SET_VARSIZE_4B(PTR, len)
// #define SET_VARSIZE_SHORT(PTR, len)      SET_VARSIZE_1B(PTR, len)
// #define SET_VARSIZE_COMPRESSED(PTR, len) SET_VARSIZE_4B_C(PTR, len)

// #define SET_VARTAG_EXTERNAL(PTR, tag) SET_VARTAG_1B_E(PTR, tag)

// #define VARSIZE_ANY(PTR)                                                                                               \
// 	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : VARSIZE_4B(PTR)))

// /* Size of a varlena data, excluding header */
// #define VARSIZE_ANY_EXHDR(PTR)                                                                                         \
// 	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) - VARHDRSZ_EXTERNAL                                                   \
// 	                     : (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) - VARHDRSZ_SHORT : VARSIZE_4B(PTR) - VARHDRSZ))

// /* caution: this will not work on an external or compressed-in-line Datum */
// /* caution: this will return a possibly unaligned pointer */
// #define VARDATA_ANY(PTR) (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))

// /* Decompressed size and compression method of a compressed-in-line Datum */
// #define VARDATA_COMPRESSED_GET_EXTSIZE(PTR) (((varattrib_4b *)(PTR))->va_compressed.va_tcinfo & VARLENA_EXTSIZE_MASK)
// #define VARDATA_COMPRESSED_GET_COMPRESS_METHOD(PTR)                                                                    \
// 	(((varattrib_4b *)(PTR))->va_compressed.va_tcinfo >> VARLENA_EXTSIZE_BITS)

// /* Same for external Datums; but note argument is a struct varatt_external */
// #define VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer)         ((toast_pointer).va_extinfo & VARLENA_EXTSIZE_MASK)
// #define VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer) ((toast_pointer).va_extinfo >> VARLENA_EXTSIZE_BITS)

// #define VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(toast_pointer, len, cm)                                           \
// 	do {                                                                                                               \
// 		Assert((cm) == TOAST_PGLZ_COMPRESSION_ID || (cm) == TOAST_LZ4_COMPRESSION_ID);                                 \
// 		((toast_pointer).va_extinfo = (len) | ((uint32)(cm) << VARLENA_EXTSIZE_BITS));                                 \
// 	} while (0)

// /*
//  * Testing whether an externally-stored value is compressed now requires
//  * comparing size stored in va_extinfo (the actual length of the external data)
//  * to rawsize (the original uncompressed datum's size).  The latter includes
//  * VARHDRSZ overhead, the former doesn't.  We never use compression unless it
//  * actually saves space, so we expect either equality or less-than.
//  */
// #define VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)                                                                   \
// 	(VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) < (toast_pointer).va_rawsize - VARHDRSZ)

} // namespace duckdb
