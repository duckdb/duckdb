/*-------------------------------------------------------------------------
 *
 * hsearch.h
 *	  exported definitions for utils/hash/dynahash.c; see notes therein
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/hsearch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HSEARCH_H
#define HSEARCH_H


/*
 * Hash functions must have this signature.
 */
typedef uint32 (*HashValueFunc) (const void *key, Size keysize);

/*
 * Key comparison functions must have this signature.  Comparison functions
 * return zero for match, nonzero for no match.  (The comparison function
 * definition is designed to allow memcmp() and strncmp() to be used directly
 * as key comparison functions.)
 */
typedef int (*HashCompareFunc) (const void *key1, const void *key2,
											Size keysize);

/*
 * Key copying functions must have this signature.  The return value is not
 * used.  (The definition is set up to allow memcpy() and strlcpy() to be
 * used directly.)
 */
typedef void *(*HashCopyFunc) (void *dest, const void *src, Size keysize);

/*
 * Space allocation function for a hashtable --- designed to match malloc().
 * Note: there is no free function API; can't destroy a hashtable unless you
 * use the default allocator.
 */
typedef void *(*HashAllocFunc) (Size request);

/*
 * HASHELEMENT is the private part of a hashtable entry.  The caller's data
 * follows the HASHELEMENT structure (on a MAXALIGN'd boundary).  The hash key
 * is expected to be at the start of the caller's hash entry data structure.
 */
typedef struct HASHELEMENT
{
	struct HASHELEMENT *link;	/* link to next entry in same bucket */
	uint32		hashvalue;		/* hash function result for this entry */
} HASHELEMENT;

/* Hash table header struct is an opaque type known only within dynahash.c */
typedef struct HASHHDR HASHHDR;

/* Hash table control struct is an opaque type known only within dynahash.c */
typedef struct HTAB HTAB;

/* Parameter data structure for hash_create */
/* Only those fields indicated by hash_flags need be set */
typedef struct HASHCTL
{
	long		num_partitions; /* # partitions (must be power of 2) */
	long		ssize;			/* segment size */
	long		dsize;			/* (initial) directory size */
	long		max_dsize;		/* limit to dsize if dir size is limited */
	long		ffactor;		/* fill factor */
	Size		keysize;		/* hash key length in bytes */
	Size		entrysize;		/* total user element size in bytes */
	HashValueFunc hash;			/* hash function */
	HashCompareFunc match;		/* key comparison function */
	HashCopyFunc keycopy;		/* key copying function */
	HashAllocFunc alloc;		/* memory allocator */
	MemoryContext hcxt;			/* memory context to use for allocations */
	HASHHDR    *hctl;			/* location of header in shared mem */
} HASHCTL;

/* Flags to indicate which parameters are supplied */
#define HASH_PARTITION	0x0001	/* Hashtable is used w/partitioned locking */
#define HASH_SEGMENT	0x0002	/* Set segment size */
#define HASH_DIRSIZE	0x0004	/* Set directory size (initial and max) */
#define HASH_FFACTOR	0x0008	/* Set fill factor */
#define HASH_ELEM		0x0010	/* Set keysize and entrysize */
#define HASH_BLOBS		0x0020	/* Select support functions for binary keys */
#define HASH_FUNCTION	0x0040	/* Set user defined hash function */
#define HASH_COMPARE	0x0080	/* Set user defined comparison function */
#define HASH_KEYCOPY	0x0100	/* Set user defined key-copying function */
#define HASH_ALLOC		0x0200	/* Set memory allocator */
#define HASH_CONTEXT	0x0400	/* Set memory allocation context */
#define HASH_SHARED_MEM 0x0800	/* Hashtable is in shared memory */
#define HASH_ATTACH		0x1000	/* Do not initialize hctl */
#define HASH_FIXED_SIZE 0x2000	/* Initial size is a hard limit */


/* max_dsize value to indicate expansible directory */
#define NO_MAX_DSIZE			(-1)

/* hash_search operations */
typedef enum
{
	HASH_FIND,
	HASH_ENTER,
	HASH_REMOVE,
	HASH_ENTER_NULL
} HASHACTION;

/* hash_seq status (should be considered an opaque type by callers) */
typedef struct
{
	HTAB	   *hashp;
	uint32		curBucket;		/* index of current bucket */
	HASHELEMENT *curEntry;		/* current entry in bucket */
} HASH_SEQ_STATUS;

/*
 * prototypes for functions in dynahash.c
 */
extern HTAB *hash_create(const char *tabname, long nelem,
			HASHCTL *info, int flags);
extern void hash_destroy(HTAB *hashp);
extern void hash_stats(const char *where, HTAB *hashp);
extern void *hash_search(HTAB *hashp, const void *keyPtr, HASHACTION action,
			bool *foundPtr);
extern uint32 get_hash_value(HTAB *hashp, const void *keyPtr);
extern void *hash_search_with_hash_value(HTAB *hashp, const void *keyPtr,
							uint32 hashvalue, HASHACTION action,
							bool *foundPtr);
extern bool hash_update_hash_key(HTAB *hashp, void *existingEntry,
					 const void *newKeyPtr);
extern long hash_get_num_entries(HTAB *hashp);
extern void hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp);
extern void *hash_seq_search(HASH_SEQ_STATUS *status);
extern void hash_seq_term(HASH_SEQ_STATUS *status);
extern void hash_freeze(HTAB *hashp);
extern Size hash_estimate_size(long num_entries, Size entrysize);
extern long hash_select_dirsize(long num_entries);
extern Size hash_get_shared_size(HASHCTL *info, int flags);
extern void AtEOXact_HashTables(bool isCommit);
extern void AtEOSubXact_HashTables(bool isCommit, int nestDepth);

/*
 * prototypes for functions in hashfn.c
 *
 * Note: It is deprecated for callers of hash_create to explicitly specify
 * string_hash, tag_hash, uint32_hash, or oid_hash.  Just set HASH_BLOBS or
 * not.  Use HASH_FUNCTION only when you want something other than those.
 */
extern uint32 string_hash(const void *key, Size keysize);
extern uint32 tag_hash(const void *key, Size keysize);
extern uint32 uint32_hash(const void *key, Size keysize);
extern uint32 bitmap_hash(const void *key, Size keysize);
extern int	bitmap_match(const void *key1, const void *key2, Size keysize);

#define oid_hash uint32_hash	/* Remove me eventually */

#endif   /* HSEARCH_H */
