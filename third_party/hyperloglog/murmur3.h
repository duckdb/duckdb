#ifndef MURMUR3_H
#define MURMUR3_H

//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

// Note - The x86 and x64 versions do _not_ produce the same results, as the
// algorithms are optimized for their respective platforms. You can still
// compile and run any of them on any platform, but your performance with the
// non-native version will be less than optimal.

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

// Microsoft Visual Studio

#if defined(_MSC_VER)

typedef unsigned char uint8_t;
typedef unsigned long uint32_t;
typedef unsigned __int64 uint64_t;

// Other compilers

#else   // defined(_MSC_VER)

#include <cstdint>

#endif // !defined(_MSC_VER)

#define FORCE_INLINE __attribute__((always_inline))

inline uint32_t rotl32 ( uint32_t x, uint8_t r )
{
  return (x << r) | (x >> (32 - r));
}

#define ROTL32(x,y) rotl32(x,y)

#define BIG_CONSTANT(x) (x##LLU)

/* NO-OP for little-endian platforms */
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__)
# if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#   define BYTESWAP(x) (x)
# endif
/* if __BYTE_ORDER__ is not predefined (like FreeBSD), use arch */
#elif defined(__i386)  || defined(__x86_64) \
  ||  defined(__alpha) || defined(__vax)

# define BYTESWAP(x) (x)
/* use __builtin_bswap32 if available */
#elif defined(__GNUC__) || defined(__clang__)
#ifdef __has_builtin
#if __has_builtin(__builtin_bswap32)
#define BYTESWAP(x) __builtin_bswap32(x)
#endif // __has_builtin(__builtin_bswap32)
#endif // __has_builtin
#endif // defined(__GNUC__) || defined(__clang__)
/* last resort (big-endian w/o __builtin_bswap) */
#ifndef BYTESWAP
# define BYTESWAP(x)   ((((x)&0xFF)<<24) \
         |(((x)>>24)&0xFF) \
         |(((x)&0x0000FF00)<<8)    \
         |(((x)&0x00FF0000)>>8)    )
#endif

//-----------------------------------------------------------------------------
// Block read - if your platform needs to do endian-swapping or can only
// handle aligned reads, do the conversion here

#define getblock(p, i) BYTESWAP(p[i])

//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche

uint32_t fmix32( uint32_t h )
{
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;

  return h;
}

//-----------------------------------------------------------------------------

#ifdef __cplusplus
extern "C"
#else
extern
#endif
void MurmurHash3_x86_32( const void * key, int len, uint32_t seed, void * out )
{
  const auto * data = (const uint8_t*)key;
  const int nblocks = len / 4;
  int i;

  uint32_t h1 = seed;

  uint32_t c1 = 0xcc9e2d51;
  uint32_t c2 = 0x1b873593;

  //----------
  // body

  const auto * blocks = (const uint32_t *)(data + nblocks*4);

  for(i = -nblocks; i; i++)
  {
    uint32_t k1 = getblock(blocks,i);

    k1 *= c1;
    k1 = ROTL32(k1,15);
    k1 *= c2;

    h1 ^= k1;
    h1 = ROTL32(h1,13);
    h1 = h1*5+0xe6546b64;
  }

  //----------
  // tail
  {
    const auto * tail = (const uint8_t*)(data + nblocks*4);
    uint32_t k1 = 0;
    if ((len&3) == 3){
		k1 ^= tail[2] << 16;
		k1 ^= tail[1] << 8;
		k1 ^= tail[0];
		k1 *= c1;
		k1 = ROTL32(k1,15);k1 *= c2; h1 ^= k1;
	}else if ((len&3) ==2){
		k1 ^= tail[1] << 8;
		k1 ^= tail[0];
		k1 *= c1;
		k1 = ROTL32(k1,15);k1 *= c2; h1 ^= k1;
	}else if ((len&3) ==1){
		k1 ^= tail[0];
		k1 *= c1;
		k1 = ROTL32(k1,15);k1 *= c2; h1 ^= k1;
	}
  }

  //----------
  // finalization

  h1 ^= len;

  h1 = fmix32(h1);

  *(uint32_t*)out = h1;
}

#endif