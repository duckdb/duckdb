/*-------------------------------------------------------------------------
 *
 * pg_crc32c.h
 *	  Routines for computing CRC-32C checksums.
 *
 * The speed of CRC-32C calculation has a big impact on performance, so we
 * jump through some hoops to get the best implementation for each
 * platform. Some CPU architectures have special instructions for speeding
 * up CRC calculations (e.g. Intel SSE 4.2), on other platforms we use the
 * Slicing-by-8 algorithm which uses lookup tables.
 *
 * The public interface consists of four macros:
 *
 * INIT_CRC32C(crc)
 *		Initialize a CRC accumulator
 *
 * COMP_CRC32C(crc, data, len)
 *		Accumulate some (more) bytes into a CRC
 *
 * FIN_CRC32C(crc)
 *		Finish a CRC calculation
 *
 * EQ_CRC32C(c1, c2)
 *		Check for equality of two CRCs.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/pg_crc32c.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CRC32C_H
#define PG_CRC32C_H

typedef uint32 pg_crc32c;

/* The INIT and EQ macros are the same for all implementations. */
#define INIT_CRC32C(crc) ((crc) = 0xFFFFFFFF)
#define EQ_CRC32C(c1, c2) ((c1) == (c2))

#if defined(USE_SSE42_CRC32C)
/* Use SSE4.2 instructions. */
#define COMP_CRC32C(crc, data, len) \
	((crc) = pg_comp_crc32c_sse42((crc), (data), (len)))
#define FIN_CRC32C(crc) ((crc) ^= 0xFFFFFFFF)

extern pg_crc32c pg_comp_crc32c_sse42(pg_crc32c crc, const void *data, size_t len);

#elif defined(USE_SSE42_CRC32C_WITH_RUNTIME_CHECK)
/*
 * Use SSE4.2 instructions, but perform a runtime check first to check that
 * they are available.
 */
#define COMP_CRC32C(crc, data, len) \
	((crc) = pg_comp_crc32c((crc), (data), (len)))
#define FIN_CRC32C(crc) ((crc) ^= 0xFFFFFFFF)

extern pg_crc32c pg_comp_crc32c_sse42(pg_crc32c crc, const void *data, size_t len);
extern pg_crc32c pg_comp_crc32c_sb8(pg_crc32c crc, const void *data, size_t len);
extern pg_crc32c (*pg_comp_crc32c) (pg_crc32c crc, const void *data, size_t len);

#else
/*
 * Use slicing-by-8 algorithm.
 *
 * On big-endian systems, the intermediate value is kept in reverse byte
 * order, to avoid byte-swapping during the calculation. FIN_CRC32C reverses
 * the bytes to the final order.
 */
#define COMP_CRC32C(crc, data, len) \
	((crc) = pg_comp_crc32c_sb8((crc), (data), (len)))
#ifdef WORDS_BIGENDIAN

#ifdef HAVE__BUILTIN_BSWAP32
#define BSWAP32(x) __builtin_bswap32(x)
#else
#define BSWAP32(x) (((x << 24) & 0xff000000) | \
					((x << 8) & 0x00ff0000) | \
					((x >> 8) & 0x0000ff00) | \
					((x >> 24) & 0x000000ff))
#endif

#define FIN_CRC32C(crc) ((crc) = BSWAP32(crc) ^ 0xFFFFFFFF)
#else
#define FIN_CRC32C(crc) ((crc) ^= 0xFFFFFFFF)
#endif

extern pg_crc32c pg_comp_crc32c_sb8(pg_crc32c crc, const void *data, size_t len);

#endif

#endif   /* PG_CRC32C_H */
