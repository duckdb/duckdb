/*-------------------------------------------------------------------------
 *
 * ts_cache.h
 *	  Tsearch related object caches.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tsearch/ts_cache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TS_CACHE_H
#define TS_CACHE_H

#include "utils/guc.h"


/*
 * All TS*CacheEntry structs must share this common header
 * (see InvalidateTSCacheCallBack)
 */
typedef struct TSAnyCacheEntry
{
	Oid			objId;
	bool		isvalid;
} TSAnyCacheEntry;


typedef struct TSParserCacheEntry
{
	/* prsId is the hash lookup key and MUST BE FIRST */
	Oid			prsId;			/* OID of the parser */
	bool		isvalid;

	Oid			startOid;
	Oid			tokenOid;
	Oid			endOid;
	Oid			headlineOid;
	Oid			lextypeOid;

	/*
	 * Pre-set-up fmgr call of most needed parser's methods
	 */
	FmgrInfo	prsstart;
	FmgrInfo	prstoken;
	FmgrInfo	prsend;
	FmgrInfo	prsheadline;
} TSParserCacheEntry;

typedef struct TSDictionaryCacheEntry
{
	/* dictId is the hash lookup key and MUST BE FIRST */
	Oid			dictId;
	bool		isvalid;

	/* most frequent fmgr call */
	Oid			lexizeOid;
	FmgrInfo	lexize;

	MemoryContext dictCtx;		/* memory context to store private data */
	void	   *dictData;
} TSDictionaryCacheEntry;

typedef struct
{
	int			len;
	Oid		   *dictIds;
} ListDictionary;

typedef struct
{
	/* cfgId is the hash lookup key and MUST BE FIRST */
	Oid			cfgId;
	bool		isvalid;

	Oid			prsId;

	int			lenmap;
	ListDictionary *map;
} TSConfigCacheEntry;


/*
 * GUC variable for current configuration
 */
extern char *TSCurrentConfig;


extern TSParserCacheEntry *lookup_ts_parser_cache(Oid prsId);
extern TSDictionaryCacheEntry *lookup_ts_dictionary_cache(Oid dictId);
extern TSConfigCacheEntry *lookup_ts_config_cache(Oid cfgId);

extern Oid	getTSCurrentConfig(bool emitError);
extern bool check_TSCurrentConfig(char **newval, void **extra, GucSource source);
extern void assign_TSCurrentConfig(const char *newval, void *extra);

#endif   /* TS_CACHE_H */
