/*-------------------------------------------------------------------------
 *
 * sinval.h
 *	  POSTGRES shared cache invalidation communication definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/sinval.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SINVAL_H
#define SINVAL_H

#include <signal.h>

#include "storage/relfilenode.h"

/*
 * We support several types of shared-invalidation messages:
 *	* invalidate a specific tuple in a specific catcache
 *	* invalidate all catcache entries from a given system catalog
 *	* invalidate a relcache entry for a specific logical relation
 *	* invalidate an smgr cache entry for a specific physical relation
 *	* invalidate the mapped-relation mapping for a given database
 *	* invalidate any saved snapshot that might be used to scan a given relation
 * More types could be added if needed.  The message type is identified by
 * the first "int8" field of the message struct.  Zero or positive means a
 * specific-catcache inval message (and also serves as the catcache ID field).
 * Negative values identify the other message types, as per codes below.
 *
 * Catcache inval events are initially driven by detecting tuple inserts,
 * updates and deletions in system catalogs (see CacheInvalidateHeapTuple).
 * An update can generate two inval events, one for the old tuple and one for
 * the new, but this is reduced to one event if the tuple's hash key doesn't
 * change.  Note that the inval events themselves don't actually say whether
 * the tuple is being inserted or deleted.  Also, since we transmit only a
 * hash key, there is a small risk of unnecessary invalidations due to chance
 * matches of hash keys.
 *
 * Note that some system catalogs have multiple caches on them (with different
 * indexes).  On detecting a tuple invalidation in such a catalog, separate
 * catcache inval messages must be generated for each of its caches, since
 * the hash keys will generally be different.
 *
 * Catcache, relcache, and snapshot invalidations are transactional, and so
 * are sent to other backends upon commit.  Internally to the generating
 * backend, they are also processed at CommandCounterIncrement so that later
 * commands in the same transaction see the new state.  The generating backend
 * also has to process them at abort, to flush out any cache state it's loaded
 * from no-longer-valid entries.
 *
 * smgr and relation mapping invalidations are non-transactional: they are
 * sent immediately when the underlying file change is made.
 */

typedef struct
{
	int8		id;				/* cache ID --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	uint32		hashValue;		/* hash value of key for this catcache */
} SharedInvalCatcacheMsg;

#define SHAREDINVALCATALOG_ID	(-1)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared catalog */
	Oid			catId;			/* ID of catalog whose contents are invalid */
} SharedInvalCatalogMsg;

#define SHAREDINVALRELCACHE_ID	(-2)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	Oid			relId;			/* relation ID */
} SharedInvalRelcacheMsg;

#define SHAREDINVALSMGR_ID		(-3)

typedef struct
{
	/* note: field layout chosen to pack into 16 bytes */
	int8		id;				/* type field --- must be first */
	int8		backend_hi;		/* high bits of backend ID, if temprel */
	uint16		backend_lo;		/* low bits of backend ID, if temprel */
	RelFileNode rnode;			/* spcNode, dbNode, relNode */
} SharedInvalSmgrMsg;

#define SHAREDINVALRELMAP_ID	(-4)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 for shared catalogs */
} SharedInvalRelmapMsg;

#define SHAREDINVALSNAPSHOT_ID	(-5)

typedef struct
{
	int8		id;				/* type field --- must be first */
	Oid			dbId;			/* database ID, or 0 if a shared relation */
	Oid			relId;			/* relation ID */
} SharedInvalSnapshotMsg;

typedef union
{
	int8		id;				/* type field --- must be first */
	SharedInvalCatcacheMsg cc;
	SharedInvalCatalogMsg cat;
	SharedInvalRelcacheMsg rc;
	SharedInvalSmgrMsg sm;
	SharedInvalRelmapMsg rm;
	SharedInvalSnapshotMsg sn;
} SharedInvalidationMessage;


/* Counter of messages processed; don't worry about overflow. */
extern uint64 SharedInvalidMessageCounter;

extern volatile sig_atomic_t catchupInterruptPending;

extern void SendSharedInvalidMessages(const SharedInvalidationMessage *msgs,
						  int n);
extern void ReceiveSharedInvalidMessages(
					  void (*invalFunction) (SharedInvalidationMessage *msg),
							 void (*resetFunction) (void));

/* signal handler for catchup events (PROCSIG_CATCHUP_INTERRUPT) */
extern void HandleCatchupInterrupt(void);

/*
 * enable/disable processing of catchup events directly from signal handler.
 * The enable routine first performs processing of any catchup events that
 * have occurred since the last disable.
 */
extern void ProcessCatchupInterrupt(void);

extern int xactGetCommittedInvalidationMessages(SharedInvalidationMessage **msgs,
									 bool *RelcacheInitFileInval);
extern void ProcessCommittedInvalidationMessages(SharedInvalidationMessage *msgs,
									 int nmsgs, bool RelcacheInitFileInval,
									 Oid dbid, Oid tsid);

extern void LocalExecuteInvalidationMessage(SharedInvalidationMessage *msg);

#endif   /* SINVAL_H */
