/*--------------------------------------------------------------------
 * bgworker.h
 *		POSTGRES pluggable background workers interface
 *
 * A background worker is a process able to run arbitrary, user-supplied code,
 * including normal transactions.
 *
 * Any external module loaded via shared_preload_libraries can register a
 * worker.  Workers can also be registered dynamically at runtime.  In either
 * case, the worker process is forked from the postmaster and runs the
 * user-supplied "main" function.  This code may connect to a database and
 * run transactions.  Workers can remain active indefinitely, but will be
 * terminated if a shutdown or crash occurs.
 *
 * If the fork() call fails in the postmaster, it will try again later.  Note
 * that the failure can only be transient (fork failure due to high load,
 * memory pressure, too many processes, etc); more permanent problems, like
 * failure to connect to a database, are detected later in the worker and dealt
 * with just by having the worker exit normally. A worker which exits with
 * a return code of 0 will never be restarted and will be removed from worker
 * list. A worker which exits with a return code of 1 will be restarted after
 * the configured restart interval (unless that interval is BGW_NEVER_RESTART).
 * The TerminateBackgroundWorker() function can be used to terminate a
 * dynamically registered background worker; the worker will be sent a SIGTERM
 * and will not be restarted after it exits.  Whenever the postmaster knows
 * that a worker will not be restarted, it unregisters the worker, freeing up
 * that worker's slot for use by a new worker.
 *
 * Note that there might be more than one worker in a database concurrently,
 * and the same module may request more than one worker running the same (or
 * different) code.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/postmaster/bgworker.h
 *--------------------------------------------------------------------
 */
#ifndef BGWORKER_H
#define BGWORKER_H

/*---------------------------------------------------------------------
 * External module API.
 *---------------------------------------------------------------------
 */

/*
 * Pass this flag to have your worker be able to connect to shared memory.
 */
#define BGWORKER_SHMEM_ACCESS						0x0001

/*
 * This flag means the bgworker requires a database connection.  The connection
 * is not established automatically; the worker must establish it later.
 * It requires that BGWORKER_SHMEM_ACCESS was passed too.
 */
#define BGWORKER_BACKEND_DATABASE_CONNECTION		0x0002


typedef void (*bgworker_main_type) (Datum main_arg);

/*
 * Points in time at which a bgworker can request to be started
 */
typedef enum
{
	BgWorkerStart_PostmasterStart,
	BgWorkerStart_ConsistentState,
	BgWorkerStart_RecoveryFinished
} BgWorkerStartTime;

#define BGW_DEFAULT_RESTART_INTERVAL	60
#define BGW_NEVER_RESTART				-1
#define BGW_MAXLEN						64
#define BGW_EXTRALEN					128

typedef struct BackgroundWorker
{
	char		bgw_name[BGW_MAXLEN];
	int			bgw_flags;
	BgWorkerStartTime bgw_start_time;
	int			bgw_restart_time;		/* in seconds, or BGW_NEVER_RESTART */
	bgworker_main_type bgw_main;
	char		bgw_library_name[BGW_MAXLEN];	/* only if bgw_main is NULL */
	char		bgw_function_name[BGW_MAXLEN];	/* only if bgw_main is NULL */
	Datum		bgw_main_arg;
	char		bgw_extra[BGW_EXTRALEN];
	pid_t		bgw_notify_pid; /* SIGUSR1 this backend on start/stop */
} BackgroundWorker;

typedef enum BgwHandleStatus
{
	BGWH_STARTED,				/* worker is running */
	BGWH_NOT_YET_STARTED,		/* worker hasn't been started yet */
	BGWH_STOPPED,				/* worker has exited */
	BGWH_POSTMASTER_DIED		/* postmaster died; worker status unclear */
} BgwHandleStatus;

struct BackgroundWorkerHandle;
typedef struct BackgroundWorkerHandle BackgroundWorkerHandle;

/* Register a new bgworker during shared_preload_libraries */
extern void RegisterBackgroundWorker(BackgroundWorker *worker);

/* Register a new bgworker from a regular backend */
extern bool RegisterDynamicBackgroundWorker(BackgroundWorker *worker,
								BackgroundWorkerHandle **handle);

/* Query the status of a bgworker */
extern BgwHandleStatus GetBackgroundWorkerPid(BackgroundWorkerHandle *handle,
					   pid_t *pidp);
extern BgwHandleStatus
WaitForBackgroundWorkerStartup(BackgroundWorkerHandle *
							   handle, pid_t *pid);
extern BgwHandleStatus
			WaitForBackgroundWorkerShutdown(BackgroundWorkerHandle *);

/* Terminate a bgworker */
extern void TerminateBackgroundWorker(BackgroundWorkerHandle *handle);

/* This is valid in a running worker */
extern PGDLLIMPORT BackgroundWorker *MyBgworkerEntry;

/*
 * Connect to the specified database, as the specified user.  Only a worker
 * that passed BGWORKER_BACKEND_DATABASE_CONNECTION during registration may
 * call this.
 *
 * If username is NULL, bootstrapping superuser is used.
 * If dbname is NULL, connection is made to no specific database;
 * only shared catalogs can be accessed.
 */
extern void BackgroundWorkerInitializeConnection(char *dbname, char *username);

/* Just like the above, but specifying database and user by OID. */
extern void BackgroundWorkerInitializeConnectionByOid(Oid dboid, Oid useroid);

/* Block/unblock signals in a background worker process */
extern void BackgroundWorkerBlockSignals(void);
extern void BackgroundWorkerUnblockSignals(void);

#endif   /* BGWORKER_H */
