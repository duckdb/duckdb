/*-------------------------------------------------------------------------
 *
 * walreceiver.h
 *	  Exports from replication/walreceiverfuncs.c.
 *
 * Portions Copyright (c) 2010-2015, PostgreSQL Global Development Group
 *
 * src/include/replication/walreceiver.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALRECEIVER_H
#define _WALRECEIVER_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "storage/latch.h"
#include "storage/spin.h"
#include "pgtime.h"

/* user-settable parameters */
extern int	wal_receiver_status_interval;
extern int	wal_receiver_timeout;
extern bool hot_standby_feedback;

/*
 * MAXCONNINFO: maximum size of a connection string.
 *
 * XXX: Should this move to pg_config_manual.h?
 */
#define MAXCONNINFO		1024

/* Can we allow the standby to accept replication connection from another standby? */
#define AllowCascadeReplication() (EnableHotStandby && max_wal_senders > 0)

/*
 * Values for WalRcv->walRcvState.
 */
typedef enum
{
	WALRCV_STOPPED,				/* stopped and mustn't start up again */
	WALRCV_STARTING,			/* launched, but the process hasn't
								 * initialized yet */
	WALRCV_STREAMING,			/* walreceiver is streaming */
	WALRCV_WAITING,				/* stopped streaming, waiting for orders */
	WALRCV_RESTARTING,			/* asked to restart streaming */
	WALRCV_STOPPING				/* requested to stop, but still running */
} WalRcvState;

/* Shared memory area for management of walreceiver process */
typedef struct
{
	/*
	 * PID of currently active walreceiver process, its current state and
	 * start time (actually, the time at which it was requested to be
	 * started).
	 */
	pid_t		pid;
	WalRcvState walRcvState;
	pg_time_t	startTime;

	/*
	 * receiveStart and receiveStartTLI indicate the first byte position and
	 * timeline that will be received. When startup process starts the
	 * walreceiver, it sets these to the point where it wants the streaming to
	 * begin.
	 */
	XLogRecPtr	receiveStart;
	TimeLineID	receiveStartTLI;

	/*
	 * receivedUpto-1 is the last byte position that has already been
	 * received, and receivedTLI is the timeline it came from.  At the first
	 * startup of walreceiver, these are set to receiveStart and
	 * receiveStartTLI. After that, walreceiver updates these whenever it
	 * flushes the received WAL to disk.
	 */
	XLogRecPtr	receivedUpto;
	TimeLineID	receivedTLI;

	/*
	 * latestChunkStart is the starting byte position of the current "batch"
	 * of received WAL.  It's actually the same as the previous value of
	 * receivedUpto before the last flush to disk.  Startup process can use
	 * this to detect whether it's keeping up or not.
	 */
	XLogRecPtr	latestChunkStart;

	/*
	 * Time of send and receive of any message received.
	 */
	TimestampTz lastMsgSendTime;
	TimestampTz lastMsgReceiptTime;

	/*
	 * Latest reported end of WAL on the sender
	 */
	XLogRecPtr	latestWalEnd;
	TimestampTz latestWalEndTime;

	/*
	 * connection string; is used for walreceiver to connect with the primary.
	 */
	char		conninfo[MAXCONNINFO];

	/*
	 * replication slot name; is also used for walreceiver to connect with the
	 * primary
	 */
	char		slotname[NAMEDATALEN];

	slock_t		mutex;			/* locks shared variables shown above */

	/*
	 * Latch used by startup process to wake up walreceiver after telling it
	 * where to start streaming (after setting receiveStart and
	 * receiveStartTLI).
	 */
	Latch		latch;
} WalRcvData;

extern WalRcvData *WalRcv;

/* libpqwalreceiver hooks */
typedef void (*walrcv_connect_type) (char *conninfo);
extern PGDLLIMPORT walrcv_connect_type walrcv_connect;

typedef void (*walrcv_identify_system_type) (TimeLineID *primary_tli);
extern PGDLLIMPORT walrcv_identify_system_type walrcv_identify_system;

typedef void (*walrcv_readtimelinehistoryfile_type) (TimeLineID tli, char **filename, char **content, int *size);
extern PGDLLIMPORT walrcv_readtimelinehistoryfile_type walrcv_readtimelinehistoryfile;

typedef bool (*walrcv_startstreaming_type) (TimeLineID tli, XLogRecPtr startpoint, char *slotname);
extern PGDLLIMPORT walrcv_startstreaming_type walrcv_startstreaming;

typedef void (*walrcv_endstreaming_type) (TimeLineID *next_tli);
extern PGDLLIMPORT walrcv_endstreaming_type walrcv_endstreaming;

typedef int (*walrcv_receive_type) (int timeout, char **buffer);
extern PGDLLIMPORT walrcv_receive_type walrcv_receive;

typedef void (*walrcv_send_type) (const char *buffer, int nbytes);
extern PGDLLIMPORT walrcv_send_type walrcv_send;

typedef void (*walrcv_disconnect_type) (void);
extern PGDLLIMPORT walrcv_disconnect_type walrcv_disconnect;

/* prototypes for functions in walreceiver.c */
extern void WalReceiverMain(void) pg_attribute_noreturn();

/* prototypes for functions in walreceiverfuncs.c */
extern Size WalRcvShmemSize(void);
extern void WalRcvShmemInit(void);
extern void ShutdownWalRcv(void);
extern bool WalRcvStreaming(void);
extern bool WalRcvRunning(void);
extern void RequestXLogStreaming(TimeLineID tli, XLogRecPtr recptr,
					 const char *conninfo, const char *slotname);
extern XLogRecPtr GetWalRcvWriteRecPtr(XLogRecPtr *latestChunkStart, TimeLineID *receiveTLI);
extern int	GetReplicationApplyDelay(void);
extern int	GetReplicationTransferLatency(void);

#endif   /* _WALRECEIVER_H */
