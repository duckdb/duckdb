#include "linkhash.hpp"

#include "random_seed.hpp"

#include <cassert>
#include <cstring>
#include <limits.h>
#include <string.h>

namespace json {

#if (defined(__BYTE_ORDER) && defined(__LITTLE_ENDIAN) && __BYTE_ORDER == __LITTLE_ENDIAN) ||                          \
    (defined(i386) || defined(__i386__) || defined(__i486__) || defined(__i586__) || defined(__i686__) ||              \
     defined(vax) || defined(MIPSEL))
#define HASH_LITTLE_ENDIAN 1
#define HASH_BIG_ENDIAN    0
#elif (defined(__BYTE_ORDER) && defined(__BIG_ENDIAN) && __BYTE_ORDER == __BIG_ENDIAN) ||                              \
    (defined(sparc) || defined(POWERPC) || defined(mc68000) || defined(sel))
#define HASH_LITTLE_ENDIAN 0
#define HASH_BIG_ENDIAN    1
#else
#define HASH_LITTLE_ENDIAN 0
#define HASH_BIG_ENDIAN    0
#endif

#define hashsize(n) ((uint32_t)1 << (n))
#define hashmask(n) (hashsize(n) - 1)
#define rot(x, k)   (((x) << (k)) | ((x) >> (32 - (k))))

#define mix(a, b, c)                                                                                                   \
	{                                                                                                                  \
		a -= c;                                                                                                        \
		a ^= rot(c, 4);                                                                                                \
		c += b;                                                                                                        \
		b -= a;                                                                                                        \
		b ^= rot(a, 6);                                                                                                \
		a += c;                                                                                                        \
		c -= b;                                                                                                        \
		c ^= rot(b, 8);                                                                                                \
		b += a;                                                                                                        \
		a -= c;                                                                                                        \
		a ^= rot(c, 16);                                                                                               \
		c += b;                                                                                                        \
		b -= a;                                                                                                        \
		b ^= rot(a, 19);                                                                                               \
		a += c;                                                                                                        \
		c -= b;                                                                                                        \
		c ^= rot(b, 4);                                                                                                \
		b += a;                                                                                                        \
	}

#define final(a, b, c)                                                                                                 \
	{                                                                                                                  \
		c ^= b;                                                                                                        \
		c -= rot(b, 14);                                                                                               \
		a ^= c;                                                                                                        \
		a -= rot(c, 11);                                                                                               \
		b ^= a;                                                                                                        \
		b -= rot(a, 25);                                                                                               \
		c ^= b;                                                                                                        \
		c -= rot(b, 16);                                                                                               \
		a ^= c;                                                                                                        \
		a -= rot(c, 4);                                                                                                \
		b ^= a;                                                                                                        \
		b -= rot(a, 14);                                                                                               \
		c ^= b;                                                                                                        \
		c -= rot(b, 24);                                                                                               \
	}

/* hash functions */
static unsigned long lh_char_hash(const void *k);
static lh_hash_fn *char_hash_fn = lh_char_hash;

/* clang-format off */
static uint32_t hashlittle(const void *key, size_t length, uint32_t initval)
{
	uint32_t a,b,c; /* internal state */
	union
	{
		const void *ptr;
		size_t i;
	} u; /* needed for Mac Powerbook G4 */

	/* Set up the internal state */
	a = b = c = 0xdeadbeef + ((uint32_t)length) + initval;

	u.ptr = key;
	if (HASH_LITTLE_ENDIAN && ((u.i & 0x3) == 0)) {
		const uint32_t *k = (const uint32_t *)key; /* read 32-bit chunks */

		/*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
		while (length > 12)
		{
			a += k[0];
			b += k[1];
			c += k[2];
			mix(a,b,c);
			length -= 12;
			k += 3;
		}

		/*----------------------------- handle the last (probably partial) block */
		/*
		 * "k[2]&0xffffff" actually reads beyond the end of the string, but
		 * then masks off the part it's not allowed to read.  Because the
		 * string is aligned, the masked-off tail is in the same word as the
		 * rest of the string.  Every machine with memory protection I've seen
		 * does it on word boundaries, so is OK with this.  But VALGRIND will
		 * still catch it and complain.  The masking trick does make the hash
		 * noticably faster for short strings (like English words).
		 * AddressSanitizer is similarly picky about overrunning
		 * the buffer. (http://clang.llvm.org/docs/AddressSanitizer.html
		 */
#ifdef VALGRIND
#define PRECISE_MEMORY_ACCESS 1
#elif defined(__SANITIZE_ADDRESS__) /* GCC's ASAN */
#define PRECISE_MEMORY_ACCESS 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) /* Clang's ASAN */
#define PRECISE_MEMORY_ACCESS 1
#endif
#endif
#ifndef PRECISE_MEMORY_ACCESS

		switch(length)
		{
		case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
		case 11: c+=k[2]&0xffffff; b+=k[1]; a+=k[0]; break;
		case 10: c+=k[2]&0xffff; b+=k[1]; a+=k[0]; break;
		case 9 : c+=k[2]&0xff; b+=k[1]; a+=k[0]; break;
		case 8 : b+=k[1]; a+=k[0]; break;
		case 7 : b+=k[1]&0xffffff; a+=k[0]; break;
		case 6 : b+=k[1]&0xffff; a+=k[0]; break;
		case 5 : b+=k[1]&0xff; a+=k[0]; break;
		case 4 : a+=k[0]; break;
		case 3 : a+=k[0]&0xffffff; break;
		case 2 : a+=k[0]&0xffff; break;
		case 1 : a+=k[0]&0xff; break;
		case 0 : return c; /* zero length strings require no mixing */
		}

#else /* make valgrind happy */

		const uint8_t  *k8 = (const uint8_t *)k;
		switch(length)
		{
		case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
		case 11: c+=((uint32_t)k8[10])<<16;  /* fall through */
		case 10: c+=((uint32_t)k8[9])<<8;    /* fall through */
		case 9 : c+=k8[8];                   /* fall through */
		case 8 : b+=k[1]; a+=k[0]; break;
		case 7 : b+=((uint32_t)k8[6])<<16;   /* fall through */
		case 6 : b+=((uint32_t)k8[5])<<8;    /* fall through */
		case 5 : b+=k8[4];                   /* fall through */
		case 4 : a+=k[0]; break;
		case 3 : a+=((uint32_t)k8[2])<<16;   /* fall through */
		case 2 : a+=((uint32_t)k8[1])<<8;    /* fall through */
		case 1 : a+=k8[0]; break;
		case 0 : return c;
		}

#endif /* !valgrind */

	}
	else if (HASH_LITTLE_ENDIAN && ((u.i & 0x1) == 0))
	{
		const uint16_t *k = (const uint16_t *)key; /* read 16-bit chunks */
		const uint8_t  *k8;

		/*--------------- all but last block: aligned reads and different mixing */
		while (length > 12)
		{
			a += k[0] + (((uint32_t)k[1])<<16);
			b += k[2] + (((uint32_t)k[3])<<16);
			c += k[4] + (((uint32_t)k[5])<<16);
			mix(a,b,c);
			length -= 12;
			k += 6;
		}

		/*----------------------------- handle the last (probably partial) block */
		k8 = (const uint8_t *)k;
		switch(length)
		{
		case 12: c+=k[4]+(((uint32_t)k[5])<<16);
			 b+=k[2]+(((uint32_t)k[3])<<16);
			 a+=k[0]+(((uint32_t)k[1])<<16);
			 break;
		case 11: c+=((uint32_t)k8[10])<<16;     /* fall through */
		case 10: c+=k[4];
			 b+=k[2]+(((uint32_t)k[3])<<16);
			 a+=k[0]+(((uint32_t)k[1])<<16);
			 break;
		case 9 : c+=k8[8];                      /* fall through */
		case 8 : b+=k[2]+(((uint32_t)k[3])<<16);
			 a+=k[0]+(((uint32_t)k[1])<<16);
			 break;
		case 7 : b+=((uint32_t)k8[6])<<16;      /* fall through */
		case 6 : b+=k[2];
			 a+=k[0]+(((uint32_t)k[1])<<16);
			 break;
		case 5 : b+=k8[4];                      /* fall through */
		case 4 : a+=k[0]+(((uint32_t)k[1])<<16);
			 break;
		case 3 : a+=((uint32_t)k8[2])<<16;      /* fall through */
		case 2 : a+=k[0];
			 break;
		case 1 : a+=k8[0];
			 break;
		case 0 : return c;                     /* zero length requires no mixing */
		}

	}
	else
	{
		/* need to read the key one byte at a time */
		const uint8_t *k = (const uint8_t *)key;

		/*--------------- all but the last block: affect some 32 bits of (a,b,c) */
		while (length > 12)
		{
			a += k[0];
			a += ((uint32_t)k[1])<<8;
			a += ((uint32_t)k[2])<<16;
			a += ((uint32_t)k[3])<<24;
			b += k[4];
			b += ((uint32_t)k[5])<<8;
			b += ((uint32_t)k[6])<<16;
			b += ((uint32_t)k[7])<<24;
			c += k[8];
			c += ((uint32_t)k[9])<<8;
			c += ((uint32_t)k[10])<<16;
			c += ((uint32_t)k[11])<<24;
			mix(a,b,c);
			length -= 12;
			k += 12;
		}

		/*-------------------------------- last block: affect all 32 bits of (c) */
		switch(length) /* all the case statements fall through */
		{
		case 12: c+=((uint32_t)k[11])<<24; /* FALLTHRU */
		case 11: c+=((uint32_t)k[10])<<16; /* FALLTHRU */
		case 10: c+=((uint32_t)k[9])<<8; /* FALLTHRU */
		case 9 : c+=k[8]; /* FALLTHRU */
		case 8 : b+=((uint32_t)k[7])<<24; /* FALLTHRU */
		case 7 : b+=((uint32_t)k[6])<<16; /* FALLTHRU */
		case 6 : b+=((uint32_t)k[5])<<8; /* FALLTHRU */
		case 5 : b+=k[4]; /* FALLTHRU */
		case 4 : a+=((uint32_t)k[3])<<24; /* FALLTHRU */
		case 3 : a+=((uint32_t)k[2])<<16; /* FALLTHRU */
		case 2 : a+=((uint32_t)k[1])<<8; /* FALLTHRU */
		case 1 : a+=k[0];
			 break;
		case 0 : return c;
		}
	}

	final(a,b,c);
	return c;
}
/* clang-format on */

static unsigned long lh_char_hash(const void *k) {
#if defined _MSC_VER || defined __MINGW32__
#define RANDOM_SEED_TYPE LONG
#else
#define RANDOM_SEED_TYPE int
#endif
	static volatile RANDOM_SEED_TYPE random_seed = -1;

	if (random_seed == -1) {
		RANDOM_SEED_TYPE seed;
		/* we can't use -1 as it is the unitialized sentinel */
		while ((seed = json_c_get_random_seed()) == -1) {
		}
#if SIZEOF_INT == 8 && defined __GCC_HAVE_SYNC_COMPARE_AND_SWAP_8
#define USE_SYNC_COMPARE_AND_SWAP 1
#endif
#if SIZEOF_INT == 4 && defined __GCC_HAVE_SYNC_COMPARE_AND_SWAP_4
#define USE_SYNC_COMPARE_AND_SWAP 1
#endif
#if SIZEOF_INT == 2 && defined __GCC_HAVE_SYNC_COMPARE_AND_SWAP_2
#define USE_SYNC_COMPARE_AND_SWAP 1
#endif
#if defined USE_SYNC_COMPARE_AND_SWAP
		(void)__sync_val_compare_and_swap(&random_seed, -1, seed);
#elif defined _MSC_VER || defined __MINGW32__
		InterlockedCompareExchange(&random_seed, seed, -1);
#else
		//#warning "racy random seed initializtion if used by multiple threads"
		random_seed = seed; /* potentially racy */
#endif
	}

	return hashlittle((const char *)k, strlen((const char *)k), (uint32_t)random_seed);
}

void lh_table_free(struct lh_table *t) {
	struct lh_entry *c;
	if (t->free_fn) {
		for (c = t->head; c != NULL; c = c->next)
			t->free_fn(c);
	}
	free(t->table);
	free(t);
}

int lh_char_equal(const void *k1, const void *k2) {
	return (strcmp((const char *)k1, (const char *)k2) == 0);
}

struct lh_table *lh_table_new(int size, lh_entry_free_fn *free_fn, lh_hash_fn *hash_fn, lh_equal_fn *equal_fn) {
	int i;
	struct lh_table *t;

	/* Allocate space for elements to avoid divisions by zero. */
	assert(size > 0);
	t = (struct lh_table *)calloc(1, sizeof(struct lh_table));
	if (!t)
		return NULL;

	t->count = 0;
	t->size = size;
	t->table = (struct lh_entry *)calloc(size, sizeof(struct lh_entry));
	if (!t->table) {
		free(t);
		return NULL;
	}
	t->free_fn = free_fn;
	t->hash_fn = hash_fn;
	t->equal_fn = equal_fn;
	for (i = 0; i < size; i++)
		t->table[i].k = LH_EMPTY;
	return t;
}

struct lh_table *lh_kchar_table_new(int size, lh_entry_free_fn *free_fn) {
	return lh_table_new(size, free_fn, char_hash_fn, lh_char_equal);
}

struct lh_entry *lh_table_lookup_entry_w_hash(struct lh_table *t, const void *k, const unsigned long h) {
	unsigned long n = h % t->size;
	int count = 0;

	while (count < t->size) {
		if (t->table[n].k == LH_EMPTY)
			return NULL;
		if (t->table[n].k != LH_FREED && t->equal_fn(t->table[n].k, k))
			return &t->table[n];
		if ((int)++n == t->size)
			n = 0;
		count++;
	}
	return NULL;
}

int lh_table_insert_w_hash(struct lh_table *t, const void *k, const void *v, const unsigned long h,
                           const unsigned opts) {
	unsigned long n;

	if (t->count >= t->size * LH_LOAD_FACTOR) {
		/* Avoid signed integer overflow with large tables. */
		int new_size = (t->size > INT_MAX / 2) ? INT_MAX : (t->size * 2);
		if (t->size == INT_MAX || lh_table_resize(t, new_size) != 0)
			return -1;
	}

	n = h % t->size;

	while (1) {
		if (t->table[n].k == LH_EMPTY || t->table[n].k == LH_FREED)
			break;
		if ((int)++n == t->size)
			n = 0;
	}

	t->table[n].k = k;
	t->table[n].k_is_constant = (opts & JSON_C_OBJECT_ADD_CONSTANT_KEY);
	t->table[n].v = v;
	t->count++;

	if (t->head == NULL) {
		t->head = t->tail = &t->table[n];
		t->table[n].next = t->table[n].prev = NULL;
	} else {
		t->tail->next = &t->table[n];
		t->table[n].prev = t->tail;
		t->table[n].next = NULL;
		t->tail = &t->table[n];
	}

	return 0;
}

int lh_table_resize(struct lh_table *t, int new_size) {
	struct lh_table *new_t;
	struct lh_entry *ent;

	new_t = lh_table_new(new_size, NULL, t->hash_fn, t->equal_fn);
	if (new_t == NULL)
		return -1;

	for (ent = t->head; ent != NULL; ent = ent->next) {
		unsigned long h = lh_get_hash(new_t, ent->k);
		unsigned int opts = 0;
		if (ent->k_is_constant)
			opts = JSON_C_OBJECT_ADD_CONSTANT_KEY;
		if (lh_table_insert_w_hash(new_t, ent->k, ent->v, h, opts) != 0) {
			lh_table_free(new_t);
			return -1;
		}
	}
	free(t->table);
	t->table = new_t->table;
	t->size = new_size;
	t->head = new_t->head;
	t->tail = new_t->tail;
	free(new_t);

	return 0;
}

} // namespace json
