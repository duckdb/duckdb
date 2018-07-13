/*-------------------------------------------------------------------------
 *
 * xact.h
 *	  postgres transaction system definitions
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xact.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XACT_H
#define XACT_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/relfilenode.h"
#include "storage/sinval.h"
#include "utils/datetime.h"


/*
 * Xact isolation levels
 */
#define XACT_READ_UNCOMMITTED	0
#define XACT_READ_COMMITTED		1
#define XACT_REPEATABLE_READ	2
#define XACT_SERIALIZABLE		3

extern int	DefaultXactIsoLevel;
extern PGDLLIMPORT int XactIsoLevel;

/*
 * We implement three isolation levels internally.
 * The two stronger ones use one snapshot per database transaction;
 * the others use one snapshot per statement.
 * Serializable uses predicate locks in addition to snapshots.
 * These macros should be used to check which isolation level is selected.
 */
#define IsolationUsesXactSnapshot() (XactIsoLevel >= XACT_REPEATABLE_READ)
#define IsolationIsSerializable() (XactIsoLevel == XACT_SERIALIZABLE)

/* Xact read-only state */
extern bool DefaultXactReadOnly;
extern bool XactReadOnly;

/*
 * Xact is deferrable -- only meaningful (currently) for read only
 * SERIALIZABLE transactions
 */
extern bool DefaultXactDeferrable;
extern bool XactDeferrable;

typedef enum
{
	SYNCHRONOUS_COMMIT_OFF,		/* asynchronous commit */
	SYNCHRONOUS_COMMIT_LOCAL_FLUSH,		/* wait for local flush only */
	SYNCHRONOUS_COMMIT_REMOTE_WRITE,	/* wait for local flush and remote
										 * write */
	SYNCHRONOUS_COMMIT_REMOTE_FLUSH		/* wait for local and remote flush */
}	SyncCommitLevel;

/* Define the default setting for synchonous_commit */
#define SYNCHRONOUS_COMMIT_ON	SYNCHRONOUS_COMMIT_REMOTE_FLUSH

/* Synchronous commit level */
extern int	synchronous_commit;

/* Kluge for 2PC support */
extern bool MyXactAccessedTempRel;

/*
 *	start- and end-of-transaction callbacks for dynamically loaded modules
 */
typedef enum
{
	XACT_EVENT_COMMIT,
	XACT_EVENT_PARALLEL_COMMIT,
	XACT_EVENT_ABORT,
	XACT_EVENT_PARALLEL_ABORT,
	XACT_EVENT_PREPARE,
	XACT_EVENT_PRE_COMMIT,
	XACT_EVENT_PARALLEL_PRE_COMMIT,
	XACT_EVENT_PRE_PREPARE
} XactEvent;

typedef void (*XactCallback) (XactEvent event, void *arg);

typedef enum
{
	SUBXACT_EVENT_START_SUB,
	SUBXACT_EVENT_COMMIT_SUB,
	SUBXACT_EVENT_ABORT_SUB,
	SUBXACT_EVENT_PRE_COMMIT_SUB
} SubXactEvent;

typedef void (*SubXactCallback) (SubXactEvent event, SubTransactionId mySubid,
									SubTransactionId parentSubid, void *arg);


/* ----------------
 *		transaction-related XLOG entries
 * ----------------
 */

/*
 * XLOG allows to store some information in high 4 bits of log record xl_info
 * field. We use 3 for the opcode, and one about an optional flag variable.
 */
#define XLOG_XACT_COMMIT			0x00
#define XLOG_XACT_PREPARE			0x10
#define XLOG_XACT_ABORT				0x20
#define XLOG_XACT_COMMIT_PREPARED	0x30
#define XLOG_XACT_ABORT_PREPARED	0x40
#define XLOG_XACT_ASSIGNMENT		0x50
/* free opcode 0x60 */
/* free opcode 0x70 */

/* mask for filtering opcodes out of xl_info */
#define XLOG_XACT_OPMASK			0x70

/* does this record have a 'xinfo' field or not */
#define XLOG_XACT_HAS_INFO			0x80

/*
 * The following flags, stored in xinfo, determine which information is
 * contained in commit/abort records.
 */
#define XACT_XINFO_HAS_DBINFO			(1U << 0)
#define XACT_XINFO_HAS_SUBXACTS			(1U << 1)
#define XACT_XINFO_HAS_RELFILENODES		(1U << 2)
#define XACT_XINFO_HAS_INVALS			(1U << 3)
#define XACT_XINFO_HAS_TWOPHASE			(1U << 4)
#define XACT_XINFO_HAS_ORIGIN			(1U << 5)

/*
 * Also stored in xinfo, these indicating a variety of additional actions that
 * need to occur when emulating transaction effects during recovery.
 *
 * They are named XactCompletion... to differentiate them from
 * EOXact... routines which run at the end of the original transaction
 * completion.
 */
#define XACT_COMPLETION_UPDATE_RELCACHE_FILE	(1U << 30)
#define XACT_COMPLETION_FORCE_SYNC_COMMIT		(1U << 31)

/* Access macros for above flags */
#define XactCompletionRelcacheInitFileInval(xinfo) \
	(!!(xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE))
#define XactCompletionForceSyncCommit(xinfo) \
	(!!(xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT))

typedef struct xl_xact_assignment
{
	TransactionId xtop;			/* assigned XID's top-level XID */
	int			nsubxacts;		/* number of subtransaction XIDs */
	TransactionId xsub[FLEXIBLE_ARRAY_MEMBER];	/* assigned subxids */
} xl_xact_assignment;

#define MinSizeOfXactAssignment offsetof(xl_xact_assignment, xsub)

/*
 * Commit and abort records can contain a lot of information. But a large
 * portion of the records won't need all possible pieces of information. So we
 * only include what's needed.
 *
 * A minimal commit/abort record only consists of a xl_xact_commit/abort
 * struct. The presence of additional information is indicated by bits set in
 * 'xl_xact_xinfo->xinfo'. The presence of the xinfo field itself is signalled
 * by a set XLOG_XACT_HAS_INFO bit in the xl_info field.
 *
 * NB: All the individual data chunks should be sized to multiples of
 * sizeof(int) and only require int32 alignment. If they require bigger
 * alignment, they need to be copied upon reading.
 */

/* sub-records for commit/abort */

typedef struct xl_xact_xinfo
{
	/*
	 * Even though we right now only require 1 byte of space in xinfo we use
	 * four so following records don't have to care about alignment. Commit
	 * records can be large, so copying large portions isn't attractive.
	 */
	uint32		xinfo;
} xl_xact_xinfo;

typedef struct xl_xact_dbinfo
{
	Oid			dbId;			/* MyDatabaseId */
	Oid			tsId;			/* MyDatabaseTableSpace */
} xl_xact_dbinfo;

typedef struct xl_xact_subxacts
{
	int			nsubxacts;		/* number of subtransaction XIDs */
	TransactionId subxacts[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_subxacts;
#define MinSizeOfXactSubxacts offsetof(xl_xact_subxacts, subxacts)

typedef struct xl_xact_relfilenodes
{
	int			nrels;			/* number of subtransaction XIDs */
	RelFileNode xnodes[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_relfilenodes;
#define MinSizeOfXactRelfilenodes offsetof(xl_xact_relfilenodes, xnodes)

typedef struct xl_xact_invals
{
	int			nmsgs;			/* number of shared inval msgs */
	SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_invals;
#define MinSizeOfXactInvals offsetof(xl_xact_invals, msgs)

typedef struct xl_xact_twophase
{
	TransactionId xid;
} xl_xact_twophase;
#define MinSizeOfXactInvals offsetof(xl_xact_invals, msgs)

typedef struct xl_xact_origin
{
	XLogRecPtr	origin_lsn;
	TimestampTz origin_timestamp;
} xl_xact_origin;

typedef struct xl_xact_commit
{
	TimestampTz xact_time;		/* time of commit */

	/* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
	/* xl_xact_dbinfo follows if XINFO_HAS_DBINFO */
	/* xl_xact_subxacts follows if XINFO_HAS_SUBXACT */
	/* xl_xact_relfilenodes follows if XINFO_HAS_RELFILENODES */
	/* xl_xact_invals follows if XINFO_HAS_INVALS */
	/* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
	/* xl_xact_origin follows if XINFO_HAS_ORIGIN, stored unaligned! */
} xl_xact_commit;
#define MinSizeOfXactCommit (offsetof(xl_xact_commit, xact_time) + sizeof(TimestampTz))

typedef struct xl_xact_abort
{
	TimestampTz xact_time;		/* time of abort */

	/* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
	/* No db_info required */
	/* xl_xact_subxacts follows if HAS_SUBXACT */
	/* xl_xact_relfilenodes follows if HAS_RELFILENODES */
	/* No invalidation messages needed. */
	/* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
} xl_xact_abort;
#define MinSizeOfXactAbort sizeof(xl_xact_abort)

/*
 * Commit/Abort records in the above form are a bit verbose to parse, so
 * there's a deconstructed versions generated by ParseCommit/AbortRecord() for
 * easier consumption.
 */
typedef struct xl_xact_parsed_commit
{
	TimestampTz xact_time;

	uint32		xinfo;

	Oid			dbId;			/* MyDatabaseId */
	Oid			tsId;			/* MyDatabaseTableSpace */

	int			nsubxacts;
	TransactionId *subxacts;

	int			nrels;
	RelFileNode *xnodes;

	int			nmsgs;
	SharedInvalidationMessage *msgs;

	TransactionId twophase_xid; /* only for 2PC */

	XLogRecPtr	origin_lsn;
	TimestampTz origin_timestamp;
} xl_xact_parsed_commit;

typedef struct xl_xact_parsed_abort
{
	TimestampTz xact_time;
	uint32		xinfo;

	int			nsubxacts;
	TransactionId *subxacts;

	int			nrels;
	RelFileNode *xnodes;

	TransactionId twophase_xid; /* only for 2PC */
} xl_xact_parsed_abort;


/* ----------------
 *		extern definitions
 * ----------------
 */
extern bool IsTransactionState(void);
extern bool IsAbortedTransactionBlockState(void);
extern TransactionId GetTopTransactionId(void);
extern TransactionId GetTopTransactionIdIfAny(void);
extern TransactionId GetCurrentTransactionId(void);
extern TransactionId GetCurrentTransactionIdIfAny(void);
extern TransactionId GetStableLatestTransactionId(void);
extern SubTransactionId GetCurrentSubTransactionId(void);
extern void MarkCurrentTransactionIdLoggedIfAny(void);
extern bool SubTransactionIsActive(SubTransactionId subxid);
extern CommandId GetCurrentCommandId(bool used);
extern TimestampTz GetCurrentTransactionStartTimestamp(void);
extern TimestampTz GetCurrentStatementStartTimestamp(void);
extern TimestampTz GetCurrentTransactionStopTimestamp(void);
extern void SetCurrentStatementStartTimestamp(void);
extern int	GetCurrentTransactionNestLevel(void);
extern bool TransactionIdIsCurrentTransactionId(TransactionId xid);
extern void CommandCounterIncrement(void);
extern void ForceSyncCommit(void);
extern void StartTransactionCommand(void);
extern void CommitTransactionCommand(void);
extern void AbortCurrentTransaction(void);
extern void BeginTransactionBlock(void);
extern bool EndTransactionBlock(void);
extern bool PrepareTransactionBlock(char *gid);
extern void UserAbortTransactionBlock(void);
extern void ReleaseSavepoint(List *options);
extern void DefineSavepoint(char *name);
extern void RollbackToSavepoint(List *options);
extern void BeginInternalSubTransaction(char *name);
extern void ReleaseCurrentSubTransaction(void);
extern void RollbackAndReleaseCurrentSubTransaction(void);
extern bool IsSubTransaction(void);
extern Size EstimateTransactionStateSpace(void);
extern void SerializeTransactionState(Size maxsize, char *start_address);
extern void StartParallelWorkerTransaction(char *tstatespace);
extern void EndParallelWorkerTransaction(void);
extern bool IsTransactionBlock(void);
extern bool IsTransactionOrTransactionBlock(void);
extern char TransactionBlockStatusCode(void);
extern void AbortOutOfAnyTransaction(void);
extern void PreventTransactionChain(bool isTopLevel, const char *stmtType);
extern void RequireTransactionChain(bool isTopLevel, const char *stmtType);
extern void WarnNoTransactionChain(bool isTopLevel, const char *stmtType);
extern bool IsInTransactionChain(bool isTopLevel);
extern void RegisterXactCallback(XactCallback callback, void *arg);
extern void UnregisterXactCallback(XactCallback callback, void *arg);
extern void RegisterSubXactCallback(SubXactCallback callback, void *arg);
extern void UnregisterSubXactCallback(SubXactCallback callback, void *arg);

extern int	xactGetCommittedChildren(TransactionId **ptr);

extern XLogRecPtr XactLogCommitRecord(TimestampTz commit_time,
					int nsubxacts, TransactionId *subxacts,
					int nrels, RelFileNode *rels,
					int nmsgs, SharedInvalidationMessage *msgs,
					bool relcacheInval, bool forceSync,
					TransactionId twophase_xid);

extern XLogRecPtr XactLogAbortRecord(TimestampTz abort_time,
				   int nsubxacts, TransactionId *subxacts,
				   int nrels, RelFileNode *rels,
				   TransactionId twophase_xid);
extern void xact_redo(XLogReaderState *record);

/* xactdesc.c */
extern void xact_desc(StringInfo buf, XLogReaderState *record);
extern const char *xact_identify(uint8 info);

/* also in xactdesc.c, so they can be shared between front/backend code */
extern void ParseCommitRecord(uint8 info, xl_xact_commit *xlrec, xl_xact_parsed_commit *parsed);
extern void ParseAbortRecord(uint8 info, xl_xact_abort *xlrec, xl_xact_parsed_abort *parsed);

extern void EnterParallelMode(void);
extern void ExitParallelMode(void);
extern bool IsInParallelMode(void);

#endif   /* XACT_H */
