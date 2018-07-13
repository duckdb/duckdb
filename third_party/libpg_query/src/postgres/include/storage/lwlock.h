/*-------------------------------------------------------------------------
 *
 * lwlock.h
 *	  Lightweight lock manager
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/lwlock.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LWLOCK_H
#define LWLOCK_H

#include "lib/ilist.h"
#include "storage/s_lock.h"
#include "port/atomics.h"

struct PGPROC;

/*
 * It's occasionally necessary to identify a particular LWLock "by name"; e.g.
 * because we wish to report the lock to dtrace.  We could store a name or
 * other identifying information in the lock itself, but since it's common
 * to have many nearly-identical locks (e.g. one per buffer) this would end
 * up wasting significant amounts of memory.  Instead, each lwlock stores a
 * tranche ID which tells us which array it's part of.  Based on that, we can
 * figure out where the lwlock lies within the array using the data structure
 * shown below; the lock is then identified based on the tranche name and
 * computed array index.  We need the array stride because the array might not
 * be an array of lwlocks, but rather some larger data structure that includes
 * one or more lwlocks per element.
 */
typedef struct LWLockTranche
{
	const char *name;
	void	   *array_base;
	Size		array_stride;
} LWLockTranche;

/*
 * Code outside of lwlock.c should not manipulate the contents of this
 * structure directly, but we have to declare it here to allow LWLocks to be
 * incorporated into other data structures.
 */
typedef struct LWLock
{
	slock_t		mutex;			/* Protects LWLock and queue of PGPROCs */
	uint16		tranche;		/* tranche ID */

	pg_atomic_uint32 state;		/* state of exclusive/nonexclusive lockers */
#ifdef LOCK_DEBUG
	pg_atomic_uint32 nwaiters;	/* number of waiters */
#endif
	dlist_head	waiters;		/* list of waiting PGPROCs */
#ifdef LOCK_DEBUG
	struct PGPROC *owner;		/* last exclusive owner of the lock */
#endif
} LWLock;

/*
 * Prior to PostgreSQL 9.4, every lightweight lock in the system was stored
 * in a single array.  For convenience and for compatibility with past
 * releases, we still have a main array, but it's now also permissible to
 * store LWLocks elsewhere in the main shared memory segment or in a dynamic
 * shared memory segment.  In the main array, we force the array stride to
 * be a power of 2, which saves a few cycles in indexing, but more importantly
 * also ensures that individual LWLocks don't cross cache line boundaries.
 * This reduces cache contention problems, especially on AMD Opterons.
 * (Of course, we have to also ensure that the array start address is suitably
 * aligned.)
 *
 * On a 32-bit platforms a LWLock will these days fit into 16 bytes, but since
 * that didn't use to be the case and cramming more lwlocks into a cacheline
 * might be detrimental performancewise we still use 32 byte alignment
 * there. So, both on 32 and 64 bit platforms, it should fit into 32 bytes
 * unless slock_t is really big.  We allow for that just in case.
 */
#define LWLOCK_PADDED_SIZE	(sizeof(LWLock) <= 32 ? 32 : 64)

typedef union LWLockPadded
{
	LWLock		lock;
	char		pad[LWLOCK_PADDED_SIZE];
} LWLockPadded;
extern PGDLLIMPORT LWLockPadded *MainLWLockArray;

/*
 * Some commonly-used locks have predefined positions within MainLWLockArray;
 * defining macros here makes it much easier to keep track of these.  If you
 * add a lock, add it to the end to avoid renumbering the existing locks;
 * if you remove a lock, consider leaving a gap in the numbering sequence for
 * the benefit of DTrace and other external debugging scripts.
 */
/* 0 is available; was formerly BufFreelistLock */
#define ShmemIndexLock				(&MainLWLockArray[1].lock)
#define OidGenLock					(&MainLWLockArray[2].lock)
#define XidGenLock					(&MainLWLockArray[3].lock)
#define ProcArrayLock				(&MainLWLockArray[4].lock)
#define SInvalReadLock				(&MainLWLockArray[5].lock)
#define SInvalWriteLock				(&MainLWLockArray[6].lock)
#define WALBufMappingLock			(&MainLWLockArray[7].lock)
#define WALWriteLock				(&MainLWLockArray[8].lock)
#define ControlFileLock				(&MainLWLockArray[9].lock)
#define CheckpointLock				(&MainLWLockArray[10].lock)
#define CLogControlLock				(&MainLWLockArray[11].lock)
#define SubtransControlLock			(&MainLWLockArray[12].lock)
#define MultiXactGenLock			(&MainLWLockArray[13].lock)
#define MultiXactOffsetControlLock	(&MainLWLockArray[14].lock)
#define MultiXactMemberControlLock	(&MainLWLockArray[15].lock)
#define RelCacheInitLock			(&MainLWLockArray[16].lock)
#define CheckpointerCommLock		(&MainLWLockArray[17].lock)
#define TwoPhaseStateLock			(&MainLWLockArray[18].lock)
#define TablespaceCreateLock		(&MainLWLockArray[19].lock)
#define BtreeVacuumLock				(&MainLWLockArray[20].lock)
#define AddinShmemInitLock			(&MainLWLockArray[21].lock)
#define AutovacuumLock				(&MainLWLockArray[22].lock)
#define AutovacuumScheduleLock		(&MainLWLockArray[23].lock)
#define SyncScanLock				(&MainLWLockArray[24].lock)
#define RelationMappingLock			(&MainLWLockArray[25].lock)
#define AsyncCtlLock				(&MainLWLockArray[26].lock)
#define AsyncQueueLock				(&MainLWLockArray[27].lock)
#define SerializableXactHashLock	(&MainLWLockArray[28].lock)
#define SerializableFinishedListLock		(&MainLWLockArray[29].lock)
#define SerializablePredicateLockListLock	(&MainLWLockArray[30].lock)
#define OldSerXidLock				(&MainLWLockArray[31].lock)
#define SyncRepLock					(&MainLWLockArray[32].lock)
#define BackgroundWorkerLock		(&MainLWLockArray[33].lock)
#define DynamicSharedMemoryControlLock		(&MainLWLockArray[34].lock)
#define AutoFileLock				(&MainLWLockArray[35].lock)
#define ReplicationSlotAllocationLock	(&MainLWLockArray[36].lock)
#define ReplicationSlotControlLock		(&MainLWLockArray[37].lock)
#define CommitTsControlLock			(&MainLWLockArray[38].lock)
#define CommitTsLock				(&MainLWLockArray[39].lock)
#define ReplicationOriginLock		(&MainLWLockArray[40].lock)
#define MultiXactTruncationLock		(&MainLWLockArray[41].lock)
#define NUM_INDIVIDUAL_LWLOCKS		42

/*
 * It's a bit odd to declare NUM_BUFFER_PARTITIONS and NUM_LOCK_PARTITIONS
 * here, but we need them to figure out offsets within MainLWLockArray, and
 * having this file include lock.h or bufmgr.h would be backwards.
 */

/* Number of partitions of the shared buffer mapping hashtable */
#define NUM_BUFFER_PARTITIONS  128

/* Number of partitions the shared lock tables are divided into */
#define LOG2_NUM_LOCK_PARTITIONS  4
#define NUM_LOCK_PARTITIONS  (1 << LOG2_NUM_LOCK_PARTITIONS)

/* Number of partitions the shared predicate lock tables are divided into */
#define LOG2_NUM_PREDICATELOCK_PARTITIONS  4
#define NUM_PREDICATELOCK_PARTITIONS  (1 << LOG2_NUM_PREDICATELOCK_PARTITIONS)

/* Offsets for various chunks of preallocated lwlocks. */
#define BUFFER_MAPPING_LWLOCK_OFFSET	NUM_INDIVIDUAL_LWLOCKS
#define LOCK_MANAGER_LWLOCK_OFFSET		\
	(BUFFER_MAPPING_LWLOCK_OFFSET + NUM_BUFFER_PARTITIONS)
#define PREDICATELOCK_MANAGER_LWLOCK_OFFSET \
	(LOCK_MANAGER_LWLOCK_OFFSET + NUM_LOCK_PARTITIONS)
#define NUM_FIXED_LWLOCKS \
	(PREDICATELOCK_MANAGER_LWLOCK_OFFSET + NUM_PREDICATELOCK_PARTITIONS)

typedef enum LWLockMode
{
	LW_EXCLUSIVE,
	LW_SHARED,
	LW_WAIT_UNTIL_FREE			/* A special mode used in PGPROC->lwlockMode,
								 * when waiting for lock to become free. Not
								 * to be used as LWLockAcquire argument */
} LWLockMode;


#ifdef LOCK_DEBUG
extern bool Trace_lwlocks;
#endif

extern bool LWLockAcquire(LWLock *lock, LWLockMode mode);
extern bool LWLockConditionalAcquire(LWLock *lock, LWLockMode mode);
extern bool LWLockAcquireOrWait(LWLock *lock, LWLockMode mode);
extern void LWLockRelease(LWLock *lock);
extern void LWLockReleaseClearVar(LWLock *lock, uint64 *valptr, uint64 val);
extern void LWLockReleaseAll(void);
extern bool LWLockHeldByMe(LWLock *lock);

extern bool LWLockWaitForVar(LWLock *lock, uint64 *valptr, uint64 oldval, uint64 *newval);
extern void LWLockUpdateVar(LWLock *lock, uint64 *valptr, uint64 value);

extern Size LWLockShmemSize(void);
extern void CreateLWLocks(void);
extern void InitLWLockAccess(void);

/*
 * The traditional method for obtaining an lwlock for use by an extension is
 * to call RequestAddinLWLocks() during postmaster startup; this will reserve
 * space for the indicated number of locks in MainLWLockArray.  Subsequently,
 * a lock can be allocated using LWLockAssign.
 */
extern void RequestAddinLWLocks(int n);
extern LWLock *LWLockAssign(void);

/*
 * There is another, more flexible method of obtaining lwlocks. First, call
 * LWLockNewTrancheId just once to obtain a tranche ID; this allocates from
 * a shared counter.  Next, each individual process using the tranche should
 * call LWLockRegisterTranche() to associate that tranche ID with appropriate
 * metadata.  Finally, LWLockInitialize should be called just once per lwlock,
 * passing the tranche ID as an argument.
 *
 * It may seem strange that each process using the tranche must register it
 * separately, but dynamic shared memory segments aren't guaranteed to be
 * mapped at the same address in all coordinating backends, so storing the
 * registration in the main shared memory segment wouldn't work for that case.
 */
extern int	LWLockNewTrancheId(void);
extern void LWLockRegisterTranche(int tranche_id, LWLockTranche *tranche);
extern void LWLockInitialize(LWLock *lock, int tranche_id);

/*
 * Prior to PostgreSQL 9.4, we used an enum type called LWLockId to refer
 * to LWLocks.  New code should instead use LWLock *.  However, for the
 * convenience of third-party code, we include the following typedef.
 */
typedef LWLock *LWLockId;

#endif   /* LWLOCK_H */
