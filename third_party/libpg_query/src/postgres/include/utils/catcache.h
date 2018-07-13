/*-------------------------------------------------------------------------
 *
 * catcache.h
 *	  Low-level catalog cache definitions.
 *
 * NOTE: every catalog cache must have a corresponding unique index on
 * the system table that it caches --- ie, the index must match the keys
 * used to do lookups in this cache.  All cache fetches are done with
 * indexscans (under normal conditions).  The index should be unique to
 * guarantee that there can only be one matching row for a key combination.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/catcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CATCACHE_H
#define CATCACHE_H

#include "access/htup.h"
#include "access/skey.h"
#include "lib/ilist.h"
#include "utils/relcache.h"

/*
 *		struct catctup:			individual tuple in the cache.
 *		struct catclist:		list of tuples matching a partial key.
 *		struct catcache:		information for managing a cache.
 *		struct catcacheheader:	information for managing all the caches.
 */

#define CATCACHE_MAXKEYS		4

typedef struct catcache
{
	int			id;				/* cache identifier --- see syscache.h */
	slist_node	cc_next;		/* list link */
	const char *cc_relname;		/* name of relation the tuples come from */
	Oid			cc_reloid;		/* OID of relation the tuples come from */
	Oid			cc_indexoid;	/* OID of index matching cache keys */
	bool		cc_relisshared; /* is relation shared across databases? */
	TupleDesc	cc_tupdesc;		/* tuple descriptor (copied from reldesc) */
	int			cc_ntup;		/* # of tuples currently in this cache */
	int			cc_nbuckets;	/* # of hash buckets in this cache */
	int			cc_nkeys;		/* # of keys (1..CATCACHE_MAXKEYS) */
	int			cc_key[CATCACHE_MAXKEYS];		/* AttrNumber of each key */
	PGFunction	cc_hashfunc[CATCACHE_MAXKEYS];	/* hash function for each key */
	ScanKeyData cc_skey[CATCACHE_MAXKEYS];		/* precomputed key info for
												 * heap scans */
	bool		cc_isname[CATCACHE_MAXKEYS];	/* flag "name" key columns */
	dlist_head	cc_lists;		/* list of CatCList structs */
#ifdef CATCACHE_STATS
	long		cc_searches;	/* total # searches against this cache */
	long		cc_hits;		/* # of matches against existing entry */
	long		cc_neg_hits;	/* # of matches against negative entry */
	long		cc_newloads;	/* # of successful loads of new entry */

	/*
	 * cc_searches - (cc_hits + cc_neg_hits + cc_newloads) is number of failed
	 * searches, each of which will result in loading a negative entry
	 */
	long		cc_invals;		/* # of entries invalidated from cache */
	long		cc_lsearches;	/* total # list-searches */
	long		cc_lhits;		/* # of matches against existing lists */
#endif
	dlist_head *cc_bucket;		/* hash buckets */
} CatCache;


typedef struct catctup
{
	int			ct_magic;		/* for identifying CatCTup entries */
#define CT_MAGIC   0x57261502
	CatCache   *my_cache;		/* link to owning catcache */

	/*
	 * Each tuple in a cache is a member of a dlist that stores the elements
	 * of its hash bucket.  We keep each dlist in LRU order to speed repeated
	 * lookups.
	 */
	dlist_node	cache_elem;		/* list member of per-bucket list */

	/*
	 * The tuple may also be a member of at most one CatCList.  (If a single
	 * catcache is list-searched with varying numbers of keys, we may have to
	 * make multiple entries for the same tuple because of this restriction.
	 * Currently, that's not expected to be common, so we accept the potential
	 * inefficiency.)
	 */
	struct catclist *c_list;	/* containing CatCList, or NULL if none */

	/*
	 * A tuple marked "dead" must not be returned by subsequent searches.
	 * However, it won't be physically deleted from the cache until its
	 * refcount goes to zero.  (If it's a member of a CatCList, the list's
	 * refcount must go to zero, too; also, remember to mark the list dead at
	 * the same time the tuple is marked.)
	 *
	 * A negative cache entry is an assertion that there is no tuple matching
	 * a particular key.  This is just as useful as a normal entry so far as
	 * avoiding catalog searches is concerned.  Management of positive and
	 * negative entries is identical.
	 */
	int			refcount;		/* number of active references */
	bool		dead;			/* dead but not yet removed? */
	bool		negative;		/* negative cache entry? */
	uint32		hash_value;		/* hash value for this tuple's keys */
	HeapTupleData tuple;		/* tuple management header */
} CatCTup;


typedef struct catclist
{
	int			cl_magic;		/* for identifying CatCList entries */
#define CL_MAGIC   0x52765103
	CatCache   *my_cache;		/* link to owning catcache */

	/*
	 * A CatCList describes the result of a partial search, ie, a search using
	 * only the first K key columns of an N-key cache.  We form the keys used
	 * into a tuple (with other attributes NULL) to represent the stored key
	 * set.  The CatCList object contains links to cache entries for all the
	 * table rows satisfying the partial key.  (Note: none of these will be
	 * negative cache entries.)
	 *
	 * A CatCList is only a member of a per-cache list; we do not currently
	 * divide them into hash buckets.
	 *
	 * A list marked "dead" must not be returned by subsequent searches.
	 * However, it won't be physically deleted from the cache until its
	 * refcount goes to zero.  (A list should be marked dead if any of its
	 * member entries are dead.)
	 *
	 * If "ordered" is true then the member tuples appear in the order of the
	 * cache's underlying index.  This will be true in normal operation, but
	 * might not be true during bootstrap or recovery operations. (namespace.c
	 * is able to save some cycles when it is true.)
	 */
	dlist_node	cache_elem;		/* list member of per-catcache list */
	int			refcount;		/* number of active references */
	bool		dead;			/* dead but not yet removed? */
	bool		ordered;		/* members listed in index order? */
	short		nkeys;			/* number of lookup keys specified */
	uint32		hash_value;		/* hash value for lookup keys */
	HeapTupleData tuple;		/* header for tuple holding keys */
	int			n_members;		/* number of member tuples */
	CatCTup    *members[FLEXIBLE_ARRAY_MEMBER]; /* members */
} CatCList;


typedef struct catcacheheader
{
	slist_head	ch_caches;		/* head of list of CatCache structs */
	int			ch_ntup;		/* # of tuples in all caches */
} CatCacheHeader;


/* this extern duplicates utils/memutils.h... */
extern PGDLLIMPORT MemoryContext CacheMemoryContext;

extern void CreateCacheMemoryContext(void);
extern void AtEOXact_CatCache(bool isCommit);

extern CatCache *InitCatCache(int id, Oid reloid, Oid indexoid,
			 int nkeys, const int *key,
			 int nbuckets);
extern void InitCatCachePhase2(CatCache *cache, bool touch_index);

extern HeapTuple SearchCatCache(CatCache *cache,
			   Datum v1, Datum v2,
			   Datum v3, Datum v4);
extern void ReleaseCatCache(HeapTuple tuple);

extern uint32 GetCatCacheHashValue(CatCache *cache,
					 Datum v1, Datum v2,
					 Datum v3, Datum v4);

extern CatCList *SearchCatCacheList(CatCache *cache, int nkeys,
				   Datum v1, Datum v2,
				   Datum v3, Datum v4);
extern void ReleaseCatCacheList(CatCList *list);

extern void ResetCatalogCaches(void);
extern void CatalogCacheFlushCatalog(Oid catId);
extern void CatalogCacheIdInvalidate(int cacheId, uint32 hashValue);
extern void PrepareToInvalidateCacheTuple(Relation relation,
							  HeapTuple tuple,
							  HeapTuple newtuple,
							  void (*function) (int, uint32, Oid));

extern void PrintCatCacheLeakWarning(HeapTuple tuple);
extern void PrintCatCacheListLeakWarning(CatCList *list);

#endif   /* CATCACHE_H */
