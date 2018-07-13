/*
 * xlog.h
 *
 * PostgreSQL transaction log manager
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlog.h
 */
#ifndef XLOG_H
#define XLOG_H

#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/fd.h"


/* Sync methods */
#define SYNC_METHOD_FSYNC		0
#define SYNC_METHOD_FDATASYNC	1
#define SYNC_METHOD_OPEN		2		/* for O_SYNC */
#define SYNC_METHOD_FSYNC_WRITETHROUGH	3
#define SYNC_METHOD_OPEN_DSYNC	4		/* for O_DSYNC */
extern int	sync_method;

extern PGDLLIMPORT TimeLineID ThisTimeLineID;	/* current TLI */

/*
 * Prior to 8.4, all activity during recovery was carried out by the startup
 * process. This local variable continues to be used in many parts of the
 * code to indicate actions taken by RecoveryManagers. Other processes that
 * potentially perform work during recovery should check RecoveryInProgress().
 * See XLogCtl notes in xlog.c.
 */
extern bool InRecovery;

/*
 * Like InRecovery, standbyState is only valid in the startup process.
 * In all other processes it will have the value STANDBY_DISABLED (so
 * InHotStandby will read as FALSE).
 *
 * In DISABLED state, we're performing crash recovery or hot standby was
 * disabled in postgresql.conf.
 *
 * In INITIALIZED state, we've run InitRecoveryTransactionEnvironment, but
 * we haven't yet processed a RUNNING_XACTS or shutdown-checkpoint WAL record
 * to initialize our master-transaction tracking system.
 *
 * When the transaction tracking is initialized, we enter the SNAPSHOT_PENDING
 * state. The tracked information might still be incomplete, so we can't allow
 * connections yet, but redo functions must update the in-memory state when
 * appropriate.
 *
 * In SNAPSHOT_READY mode, we have full knowledge of transactions that are
 * (or were) running in the master at the current WAL location. Snapshots
 * can be taken, and read-only queries can be run.
 */
typedef enum
{
	STANDBY_DISABLED,
	STANDBY_INITIALIZED,
	STANDBY_SNAPSHOT_PENDING,
	STANDBY_SNAPSHOT_READY
} HotStandbyState;

extern HotStandbyState standbyState;

#define InHotStandby (standbyState >= STANDBY_SNAPSHOT_PENDING)

/*
 * Recovery target type.
 * Only set during a Point in Time recovery, not when standby_mode = on
 */
typedef enum
{
	RECOVERY_TARGET_UNSET,
	RECOVERY_TARGET_XID,
	RECOVERY_TARGET_TIME,
	RECOVERY_TARGET_NAME,
	RECOVERY_TARGET_IMMEDIATE
} RecoveryTargetType;

extern XLogRecPtr XactLastRecEnd;
extern PGDLLIMPORT XLogRecPtr XactLastCommitEnd;

extern bool reachedConsistency;

/* these variables are GUC parameters related to XLOG */
extern int	min_wal_size;
extern int	max_wal_size;
extern int	wal_keep_segments;
extern int	XLOGbuffers;
extern int	XLogArchiveTimeout;
extern int	wal_retrieve_retry_interval;
extern char *XLogArchiveCommand;
extern bool EnableHotStandby;
extern bool fullPageWrites;
extern bool wal_log_hints;
extern bool wal_compression;
extern bool log_checkpoints;

extern int	CheckPointSegments;

/* Archive modes */
typedef enum ArchiveMode
{
	ARCHIVE_MODE_OFF = 0,		/* disabled */
	ARCHIVE_MODE_ON,			/* enabled while server is running normally */
	ARCHIVE_MODE_ALWAYS			/* enabled always (even during recovery) */
} ArchiveMode;
extern int	XLogArchiveMode;

/* WAL levels */
typedef enum WalLevel
{
	WAL_LEVEL_MINIMAL = 0,
	WAL_LEVEL_ARCHIVE,
	WAL_LEVEL_HOT_STANDBY,
	WAL_LEVEL_LOGICAL
} WalLevel;
extern int	wal_level;

/* Is WAL archiving enabled (always or only while server is running normally)? */
#define XLogArchivingActive() \
	(XLogArchiveMode > ARCHIVE_MODE_OFF && wal_level >= WAL_LEVEL_ARCHIVE)
/* Is WAL archiving enabled always (even during recovery)? */
#define XLogArchivingAlways() \
	(XLogArchiveMode == ARCHIVE_MODE_ALWAYS && wal_level >= WAL_LEVEL_ARCHIVE)
#define XLogArchiveCommandSet() (XLogArchiveCommand[0] != '\0')

/*
 * Is WAL-logging necessary for archival or log-shipping, or can we skip
 * WAL-logging if we fsync() the data before committing instead?
 */
#define XLogIsNeeded() (wal_level >= WAL_LEVEL_ARCHIVE)

/*
 * Is a full-page image needed for hint bit updates?
 *
 * Normally, we don't WAL-log hint bit updates, but if checksums are enabled,
 * we have to protect them against torn page writes.  When you only set
 * individual bits on a page, it's still consistent no matter what combination
 * of the bits make it to disk, but the checksum wouldn't match.  Also WAL-log
 * them if forced by wal_log_hints=on.
 */
#define XLogHintBitIsNeeded() (DataChecksumsEnabled() || wal_log_hints)

/* Do we need to WAL-log information required only for Hot Standby and logical replication? */
#define XLogStandbyInfoActive() (wal_level >= WAL_LEVEL_HOT_STANDBY)

/* Do we need to WAL-log information required only for logical replication? */
#define XLogLogicalInfoActive() (wal_level >= WAL_LEVEL_LOGICAL)

#ifdef WAL_DEBUG
extern bool XLOG_DEBUG;
#endif

/*
 * OR-able request flag bits for checkpoints.  The "cause" bits are used only
 * for logging purposes.  Note: the flags must be defined so that it's
 * sensible to OR together request flags arising from different requestors.
 */

/* These directly affect the behavior of CreateCheckPoint and subsidiaries */
#define CHECKPOINT_IS_SHUTDOWN	0x0001	/* Checkpoint is for shutdown */
#define CHECKPOINT_END_OF_RECOVERY	0x0002		/* Like shutdown checkpoint,
												 * but issued at end of WAL
												 * recovery */
#define CHECKPOINT_IMMEDIATE	0x0004	/* Do it without delays */
#define CHECKPOINT_FORCE		0x0008	/* Force even if no activity */
#define CHECKPOINT_FLUSH_ALL	0x0010	/* Flush all pages, including those
										 * belonging to unlogged tables */
/* These are important to RequestCheckpoint */
#define CHECKPOINT_WAIT			0x0020	/* Wait for completion */
/* These indicate the cause of a checkpoint request */
#define CHECKPOINT_CAUSE_XLOG	0x0040	/* XLOG consumption */
#define CHECKPOINT_CAUSE_TIME	0x0080	/* Elapsed time */

/* Checkpoint statistics */
typedef struct CheckpointStatsData
{
	TimestampTz ckpt_start_t;	/* start of checkpoint */
	TimestampTz ckpt_write_t;	/* start of flushing buffers */
	TimestampTz ckpt_sync_t;	/* start of fsyncs */
	TimestampTz ckpt_sync_end_t;	/* end of fsyncs */
	TimestampTz ckpt_end_t;		/* end of checkpoint */

	int			ckpt_bufs_written;		/* # of buffers written */

	int			ckpt_segs_added;	/* # of new xlog segments created */
	int			ckpt_segs_removed;		/* # of xlog segments deleted */
	int			ckpt_segs_recycled;		/* # of xlog segments recycled */

	int			ckpt_sync_rels; /* # of relations synced */
	uint64		ckpt_longest_sync;		/* Longest sync for one relation */
	uint64		ckpt_agg_sync_time;		/* The sum of all the individual sync
										 * times, which is not necessarily the
										 * same as the total elapsed time for
										 * the entire sync phase. */
} CheckpointStatsData;

extern CheckpointStatsData CheckpointStats;

struct XLogRecData;

extern XLogRecPtr XLogInsertRecord(struct XLogRecData *rdata, XLogRecPtr fpw_lsn);
extern void XLogFlush(XLogRecPtr RecPtr);
extern bool XLogBackgroundFlush(void);
extern bool XLogNeedsFlush(XLogRecPtr RecPtr);
extern int	XLogFileInit(XLogSegNo segno, bool *use_existent, bool use_lock);
extern int	XLogFileOpen(XLogSegNo segno);

extern void CheckXLogRemoved(XLogSegNo segno, TimeLineID tli);
extern XLogSegNo XLogGetLastRemovedSegno(void);
extern void XLogSetAsyncXactLSN(XLogRecPtr record);
extern void XLogSetReplicationSlotMinimumLSN(XLogRecPtr lsn);

extern void xlog_redo(XLogReaderState *record);
extern void xlog_desc(StringInfo buf, XLogReaderState *record);
extern const char *xlog_identify(uint8 info);

extern void issue_xlog_fsync(int fd, XLogSegNo segno);

extern bool RecoveryInProgress(void);
extern bool HotStandbyActive(void);
extern bool HotStandbyActiveInReplay(void);
extern bool XLogInsertAllowed(void);
extern void GetXLogReceiptTime(TimestampTz *rtime, bool *fromStream);
extern XLogRecPtr GetXLogReplayRecPtr(TimeLineID *replayTLI);
extern XLogRecPtr GetXLogInsertRecPtr(void);
extern XLogRecPtr GetXLogWriteRecPtr(void);
extern bool RecoveryIsPaused(void);
extern void SetRecoveryPause(bool recoveryPause);
extern TimestampTz GetLatestXTime(void);
extern TimestampTz GetCurrentChunkReplayStartTime(void);
extern char *XLogFileNameP(TimeLineID tli, XLogSegNo segno);

extern void UpdateControlFile(void);
extern uint64 GetSystemIdentifier(void);
extern bool DataChecksumsEnabled(void);
extern XLogRecPtr GetFakeLSNForUnloggedRel(void);
extern Size XLOGShmemSize(void);
extern void XLOGShmemInit(void);
extern void BootStrapXLOG(void);
extern void StartupXLOG(void);
extern void ShutdownXLOG(int code, Datum arg);
extern void InitXLOGAccess(void);
extern void CreateCheckPoint(int flags);
extern bool CreateRestartPoint(int flags);
extern void XLogPutNextOid(Oid nextOid);
extern XLogRecPtr XLogRestorePoint(const char *rpName);
extern void UpdateFullPageWrites(void);
extern void GetFullPageWriteInfo(XLogRecPtr *RedoRecPtr_p, bool *doPageWrites_p);
extern XLogRecPtr GetRedoRecPtr(void);
extern XLogRecPtr GetInsertRecPtr(void);
extern XLogRecPtr GetFlushRecPtr(void);
extern void GetNextXidAndEpoch(TransactionId *xid, uint32 *epoch);
extern void RemovePromoteSignalFiles(void);

extern bool CheckPromoteSignal(void);
extern void WakeupRecovery(void);
extern void SetWalWriterSleeping(bool sleeping);

extern void assign_max_wal_size(int newval, void *extra);
extern void assign_checkpoint_completion_target(double newval, void *extra);

/*
 * Starting/stopping a base backup
 */
extern XLogRecPtr do_pg_start_backup(const char *backupidstr, bool fast,
				   TimeLineID *starttli_p, char **labelfile, DIR *tblspcdir,
				   List **tablespaces, char **tblspcmapfile, bool infotbssize,
				   bool needtblspcmapfile);
extern XLogRecPtr do_pg_stop_backup(char *labelfile, bool waitforarchive,
				  TimeLineID *stoptli_p);
extern void do_pg_abort_backup(void);

/* File path names (all relative to $PGDATA) */
#define BACKUP_LABEL_FILE		"backup_label"
#define BACKUP_LABEL_OLD		"backup_label.old"

#define TABLESPACE_MAP			"tablespace_map"
#define TABLESPACE_MAP_OLD		"tablespace_map.old"

#endif   /* XLOG_H */
