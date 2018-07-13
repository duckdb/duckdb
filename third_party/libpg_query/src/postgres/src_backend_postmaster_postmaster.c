/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - ClientAuthInProgress
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * postmaster.c
 *	  This program acts as a clearing house for requests to the
 *	  POSTGRES system.  Frontend programs send a startup message
 *	  to the Postmaster and the postmaster uses the info in the
 *	  message to setup a backend process.
 *
 *	  The postmaster also manages system-wide operations such as
 *	  startup and shutdown. The postmaster itself doesn't do those
 *	  operations, mind you --- it just forks off a subprocess to do them
 *	  at the right times.  It also takes care of resetting the system
 *	  if a backend crashes.
 *
 *	  The postmaster process creates the shared memory and semaphore
 *	  pools during startup, but as a rule does not touch them itself.
 *	  In particular, it is not a member of the PGPROC array of backends
 *	  and so it cannot participate in lock-manager operations.  Keeping
 *	  the postmaster away from shared memory operations makes it simpler
 *	  and more reliable.  The postmaster is almost always able to recover
 *	  from crashes of individual backends by resetting shared memory;
 *	  if it did much with shared memory then it would be prone to crashing
 *	  along with the backends.
 *
 *	  When a request message is received, we now fork() immediately.
 *	  The child process performs authentication of the request, and
 *	  then becomes a backend if successful.  This allows the auth code
 *	  to be written in a simple single-threaded style (as opposed to the
 *	  crufty "poor man's multitasking" code that used to be needed).
 *	  More importantly, it ensures that blockages in non-multithreaded
 *	  libraries like SSL or PAM cannot cause denial of service to other
 *	  clients.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/postmaster.c
 *
 * NOTES
 *
 * Initialization:
 *		The Postmaster sets up shared memory data structures
 *		for the backends.
 *
 * Synchronization:
 *		The Postmaster shares memory with the backends but should avoid
 *		touching shared memory, so as not to become stuck if a crashing
 *		backend screws up locks or shared memory.  Likewise, the Postmaster
 *		should never block on messages from frontend clients.
 *
 * Garbage Collection:
 *		The Postmaster cleans up after backends if they have an emergency
 *		exit and/or core dump.
 *
 * Error Reporting:
 *		Use write_stderr() only for reporting "interactive" errors
 *		(essentially, bogus arguments on the command line).  Once the
 *		postmaster is launched, use ereport().
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/param.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <limits.h>

#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#ifdef USE_BONJOUR
#include <dns_sd.h>
#endif

#ifdef HAVE_PTHREAD_IS_THREADED_NP
#include <pthread.h>
#endif

#include "access/transam.h"
#include "access/xlog.h"
#include "bootstrap/bootstrap.h"
#include "catalog/pg_control.h"
#include "lib/ilist.h"
#include "libpq/auth.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pg_getopt.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/fork_process.h"
#include "postmaster/pgarch.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "replication/walsender.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/dynamic_loader.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timeout.h"

#ifdef EXEC_BACKEND
#include "storage/spin.h"
#endif


/*
 * Possible types of a backend. Beyond being the possible bkend_type values in
 * struct bkend, these are OR-able request flag bits for SignalSomeChildren()
 * and CountChildren().
 */
#define BACKEND_TYPE_NORMAL		0x0001	/* normal backend */
#define BACKEND_TYPE_AUTOVAC	0x0002	/* autovacuum worker process */
#define BACKEND_TYPE_WALSND		0x0004	/* walsender process */
#define BACKEND_TYPE_BGWORKER	0x0008	/* bgworker process */
#define BACKEND_TYPE_ALL		0x000F	/* OR of all the above */

#define BACKEND_TYPE_WORKER		(BACKEND_TYPE_AUTOVAC | BACKEND_TYPE_BGWORKER)

/*
 * List of active backends (or child processes anyway; we don't actually
 * know whether a given child has become a backend or is still in the
 * authorization phase).  This is used mainly to keep track of how many
 * children we have and send them appropriate signals when necessary.
 *
 * "Special" children such as the startup, bgwriter and autovacuum launcher
 * tasks are not in this list.  Autovacuum worker and walsender are in it.
 * Also, "dead_end" children are in it: these are children launched just for
 * the purpose of sending a friendly rejection message to a would-be client.
 * We must track them because they are attached to shared memory, but we know
 * they will never become live backends.  dead_end children are not assigned a
 * PMChildSlot.
 *
 * Background workers are in this list, too.
 */
typedef struct bkend
{
	pid_t		pid;			/* process id of backend */
	long		cancel_key;		/* cancel key for cancels for this backend */
	int			child_slot;		/* PMChildSlot for this backend, if any */

	/*
	 * Flavor of backend or auxiliary process.  Note that BACKEND_TYPE_WALSND
	 * backends initially announce themselves as BACKEND_TYPE_NORMAL, so if
	 * bkend_type is normal, you should check for a recent transition.
	 */
	int			bkend_type;
	bool		dead_end;		/* is it going to send an error and quit? */
	bool		bgworker_notify;	/* gets bgworker start/stop notifications */
	dlist_node	elem;			/* list link in BackendList */
} Backend;



#ifdef EXEC_BACKEND
static Backend *ShmemBackendArray;
#endif





/* The socket number we are listening for connections on */


/* The directory names for Unix socket(s) */


/* The TCP listen address(es) */


/*
 * ReservedBackends is the number of backends reserved for superuser use.
 * This number is taken out of the pool size given by MaxBackends so
 * number of backend slots available to non-superusers is
 * (MaxBackends - ReservedBackends).  Note what this really means is
 * "if there are <= ReservedBackends connections available, only superusers
 * can make new connections" --- pre-existing superuser connections don't
 * count against the limit.
 */


/* The socket(s) we're listening to. */
#define MAXLISTEN	64


/*
 * Set by the -o option
 */


/*
 * These globals control the behavior of the postmaster in case some
 * backend dumps core.  Normally, it kills all peers of the dead backend
 * and reinitializes shared memory.  By specifying -s or -n, we can have
 * the postmaster stop (rather than kill) peers and not reinitialize
 * shared data structures.  (Reinit is currently dead code, though.)
 */



/* still more option variables */





		/* for ps display and logging */







/* PIDs of special child processes; 0 when not running */










/* Startup process's status */
typedef enum
{
	STARTUP_NOT_RUNNING,
	STARTUP_RUNNING,
	STARTUP_SIGNALED,			/* we sent it a SIGQUIT or SIGKILL */
	STARTUP_CRASHED
} StartupStatusEnum;



/* Startup/shutdown state */
#define			NoShutdown		0
#define			SmartShutdown	1
#define			FastShutdown	2
#define			ImmediateShutdown	3



 /* T if recovering from backend crash */

/*
 * We use a simple state machine to control startup, shutdown, and
 * crash recovery (which is rather like shutdown followed by startup).
 *
 * After doing all the postmaster initialization work, we enter PM_STARTUP
 * state and the startup process is launched. The startup process begins by
 * reading the control file and other preliminary initialization steps.
 * In a normal startup, or after crash recovery, the startup process exits
 * with exit code 0 and we switch to PM_RUN state.  However, archive recovery
 * is handled specially since it takes much longer and we would like to support
 * hot standby during archive recovery.
 *
 * When the startup process is ready to start archive recovery, it signals the
 * postmaster, and we switch to PM_RECOVERY state. The background writer and
 * checkpointer are launched, while the startup process continues applying WAL.
 * If Hot Standby is enabled, then, after reaching a consistent point in WAL
 * redo, startup process signals us again, and we switch to PM_HOT_STANDBY
 * state and begin accepting connections to perform read-only queries.  When
 * archive recovery is finished, the startup process exits with exit code 0
 * and we switch to PM_RUN state.
 *
 * Normal child backends can only be launched when we are in PM_RUN or
 * PM_HOT_STANDBY state.  (We also allow launch of normal
 * child backends in PM_WAIT_BACKUP state, but only for superusers.)
 * In other states we handle connection requests by launching "dead_end"
 * child processes, which will simply send the client an error message and
 * quit.  (We track these in the BackendList so that we can know when they
 * are all gone; this is important because they're still connected to shared
 * memory, and would interfere with an attempt to destroy the shmem segment,
 * possibly leading to SHMALL failure when we try to make a new one.)
 * In PM_WAIT_DEAD_END state we are waiting for all the dead_end children
 * to drain out of the system, and therefore stop accepting connection
 * requests at all until the last existing child has quit (which hopefully
 * will not be very long).
 *
 * Notice that this state variable does not distinguish *why* we entered
 * states later than PM_RUN --- Shutdown and FatalError must be consulted
 * to find that out.  FatalError is never true in PM_RECOVERY_* or PM_RUN
 * states, nor in PM_SHUTDOWN states (because we don't enter those states
 * when trying to recover from a crash).  It can be true in PM_STARTUP state,
 * because we don't clear it until we've successfully started WAL redo.
 */
typedef enum
{
	PM_INIT,					/* postmaster starting */
	PM_STARTUP,					/* waiting for startup subprocess */
	PM_RECOVERY,				/* in archive recovery mode */
	PM_HOT_STANDBY,				/* in hot standby mode */
	PM_RUN,						/* normal "database is alive" state */
	PM_WAIT_BACKUP,				/* waiting for online backup mode to end */
	PM_WAIT_READONLY,			/* waiting for read only backends to exit */
	PM_WAIT_BACKENDS,			/* waiting for live backends to exit */
	PM_SHUTDOWN,				/* waiting for checkpointer to do shutdown
								 * ckpt */
	PM_SHUTDOWN_2,				/* waiting for archiver and walsenders to
								 * finish */
	PM_WAIT_DEAD_END,			/* waiting for dead_end children to exit */
	PM_NO_CHILDREN				/* all important children have exited */
} PMState;



/* Start time of SIGKILL timeout during immediate shutdown or child crash */
/* Zero means timeout is not running */

/* Length of said timeout */
#define SIGKILL_CHILDREN_AFTER_SECS		5

		/* T if we've reached PM_RUN */

__thread bool		ClientAuthInProgress = false;
		/* T during new-client
												 * authentication */

	/* stderr redirected for syslogger? */

/* received START_AUTOVAC_LAUNCHER signal */


/* the launcher needs to be signalled to communicate some condition */


/* set when there's a worker that needs to be started up */



/*
 * State for assigning random salts and cancel keys.
 * Also, the global MyCancelKey passes the cancel key assigned to a given
 * backend from the postmaster to that backend (via fork).
 */



#ifdef USE_BONJOUR
static DNSServiceRef bonjour_sdref = NULL;
#endif

/*
 * postmaster.c - function prototypes
 */
static void CloseServerPorts(int status, Datum arg);
static void unlink_external_pid_file(int status, Datum arg);
static void getInstallationPaths(const char *argv0);
static void checkDataDir(void);
static Port *ConnCreate(int serverFd);
static void ConnFree(Port *port);
static void reset_shared(int port);
static void SIGHUP_handler(SIGNAL_ARGS);
static void pmdie(SIGNAL_ARGS);
static void reaper(SIGNAL_ARGS);
static void sigusr1_handler(SIGNAL_ARGS);
static void startup_die(SIGNAL_ARGS);
static void dummy_handler(SIGNAL_ARGS);
static void StartupPacketTimeoutHandler(void);
static void CleanupBackend(int pid, int exitstatus);
static bool CleanupBackgroundWorker(int pid, int exitstatus);
static void HandleChildCrash(int pid, int exitstatus, const char *procname);
static void LogChildExit(int lev, const char *procname,
			 int pid, int exitstatus);
static void PostmasterStateMachine(void);
static void BackendInitialize(Port *port);
static void BackendRun(Port *port) pg_attribute_noreturn();
static void ExitPostmaster(int status) pg_attribute_noreturn();
static int	ServerLoop(void);
static int	BackendStartup(Port *port);
static int	ProcessStartupPacket(Port *port, bool SSLdone);
static void processCancelRequest(Port *port, void *pkt);
static int	initMasks(fd_set *rmask);
static void report_fork_failure_to_client(Port *port, int errnum);
static CAC_state canAcceptConnections(void);
static long PostmasterRandom(void);
static void RandomSalt(char *md5Salt);
static void signal_child(pid_t pid, int signal);
static bool SignalSomeChildren(int signal, int targets);
static void TerminateChildren(int signal);

#define SignalChildren(sig)			   SignalSomeChildren(sig, BACKEND_TYPE_ALL)

static int	CountChildren(int target);
static void maybe_start_bgworker(void);
static bool CreateOptsFile(int argc, char *argv[], char *fullprogname);
static pid_t StartChildProcess(AuxProcType type);
static void StartAutovacuumWorker(void);
static void InitPostmasterDeathWatchHandle(void);

/*
 * Archiver is allowed to start up at the current postmaster state?
 *
 * If WAL archiving is enabled always, we are allowed to start archiver
 * even during recovery.
 */
#define PgArchStartupAllowed()	\
	((XLogArchivingActive() && pmState == PM_RUN) ||	\
	 (XLogArchivingAlways() &&	\
	  (pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY)))

#ifdef EXEC_BACKEND

#ifdef WIN32
#define WNOHANG 0				/* ignored, so any integer value will do */

static pid_t waitpid(pid_t pid, int *exitstatus, int options);
static void WINAPI pgwin32_deadchild_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired);

static HANDLE win32ChildQueue;

typedef struct
{
	HANDLE		waitHandle;
	HANDLE		procHandle;
	DWORD		procId;
} win32_deadchild_waitinfo;
#endif   /* WIN32 */

static pid_t backend_forkexec(Port *port);
static pid_t internal_forkexec(int argc, char *argv[], Port *port);

/* Type for a socket that can be inherited to a client process */
#ifdef WIN32
typedef struct
{
	SOCKET		origsocket;		/* Original socket value, or PGINVALID_SOCKET
								 * if not a socket */
	WSAPROTOCOL_INFO wsainfo;
} InheritableSocket;
#else
typedef int InheritableSocket;
#endif

/*
 * Structure contains all variables passed to exec:ed backends
 */
typedef struct
{
	Port		port;
	InheritableSocket portsocket;
	char		DataDir[MAXPGPATH];
	pgsocket	ListenSocket[MAXLISTEN];
	long		MyCancelKey;
	int			MyPMChildSlot;
#ifndef WIN32
	unsigned long UsedShmemSegID;
#else
	HANDLE		UsedShmemSegID;
#endif
	void	   *UsedShmemSegAddr;
	slock_t    *ShmemLock;
	VariableCache ShmemVariableCache;
	Backend    *ShmemBackendArray;
#ifndef HAVE_SPINLOCKS
	PGSemaphore SpinlockSemaArray;
#endif
	LWLockPadded *MainLWLockArray;
	slock_t    *ProcStructLock;
	PROC_HDR   *ProcGlobal;
	PGPROC	   *AuxiliaryProcs;
	PGPROC	   *PreparedXactProcs;
	PMSignalData *PMSignalState;
	InheritableSocket pgStatSock;
	pid_t		PostmasterPid;
	TimestampTz PgStartTime;
	TimestampTz PgReloadTime;
	pg_time_t	first_syslogger_file_time;
	bool		redirection_done;
	bool		IsBinaryUpgrade;
	int			max_safe_fds;
	int			MaxBackends;
#ifdef WIN32
	HANDLE		PostmasterHandle;
	HANDLE		initial_signal_pipe;
	HANDLE		syslogPipe[2];
#else
	int			postmaster_alive_fds[2];
	int			syslogPipe[2];
#endif
	char		my_exec_path[MAXPGPATH];
	char		pkglib_path[MAXPGPATH];
	char		ExtraOptions[MAXPGPATH];
} BackendParameters;

static void read_backend_variables(char *id, Port *port);
static void restore_backend_variables(BackendParameters *param, Port *port);

#ifndef WIN32
static bool save_backend_variables(BackendParameters *param, Port *port);
#else
static bool save_backend_variables(BackendParameters *param, Port *port,
					   HANDLE childProcess, pid_t childPid);
#endif

static void ShmemBackendArrayAdd(Backend *bn);
static void ShmemBackendArrayRemove(Backend *bn);
#endif   /* EXEC_BACKEND */

#define StartupDataBase()		StartChildProcess(StartupProcess)
#define StartBackgroundWriter() StartChildProcess(BgWriterProcess)
#define StartCheckpointer()		StartChildProcess(CheckpointerProcess)
#define StartWalWriter()		StartChildProcess(WalWriterProcess)
#define StartWalReceiver()		StartChildProcess(WalReceiverProcess)

/* Macros to check exit status of a child process */
#define EXIT_STATUS_0(st)  ((st) == 0)
#define EXIT_STATUS_1(st)  (WIFEXITED(st) && WEXITSTATUS(st) == 1)
#define EXIT_STATUS_3(st)  (WIFEXITED(st) && WEXITSTATUS(st) == 3)

#ifndef WIN32
/*
 * File descriptors for pipe used to monitor if postmaster is alive.
 * First is POSTMASTER_FD_WATCH, second is POSTMASTER_FD_OWN.
 */

#else
/* Process handle of postmaster used for the same purpose on Windows */
HANDLE		PostmasterHandle;
#endif

/*
 * Postmaster main entry point
 */
#ifdef SIGXFSZ
#endif
#ifdef HAVE_INT_OPTRESET
#endif
#ifdef USE_SSL
#endif
#ifdef USE_BONJOUR
#endif
#ifdef HAVE_UNIX_SOCKETS
#endif
#ifdef WIN32
#endif
#ifdef EXEC_BACKEND
#endif
#ifdef HAVE_PTHREAD_IS_THREADED_NP
#endif


/*
 * on_proc_exit callback to close server's listen sockets
 */


/*
 * on_proc_exit callback to delete external_pid_file
 */



/*
 * Compute and check the directory paths to files that are part of the
 * installation (as deduced from the postgres executable's own location)
 */
#ifdef EXEC_BACKEND
#endif


/*
 * Validate the proposed data directory
 */
#if !defined(WIN32) && !defined(__CYGWIN__)
#endif
#if !defined(WIN32) && !defined(__CYGWIN__)
#endif

/*
 * Determine how long should we let ServerLoop sleep.
 *
 * In normal conditions we wait at most one minute, to ensure that the other
 * background tasks handled by ServerLoop get done even when no requests are
 * arriving.  However, if there are background workers waiting to be started,
 * we don't actually sleep so that they are quickly serviced.  Other exception
 * cases are as shown in the code.
 */


/*
 * Main idle loop of postmaster
 *
 * NB: Needs to be called with signals blocked
 */
#ifdef HAVE_PTHREAD_IS_THREADED_NP
#endif

/*
 * Initialise the masks for select() for the ports we are listening on.
 * Return the number of sockets to listen on.
 */



/*
 * Read a client's startup packet and do something according to it.
 *
 * Returns STATUS_OK or STATUS_ERROR, or might call ereport(FATAL) and
 * not return at all.
 *
 * (Note that ereport(FATAL) stuff is sent to the client, so only use it
 * if that's what you want.  Return STATUS_ERROR if you don't want to
 * send anything to the client, which would typically be appropriate
 * if we detect a communications failure.)
 */
#ifdef USE_SSL
#else
#endif
#ifdef USE_SSL
#endif


/*
 * The client has sent a cancel request packet, not a normal
 * start-a-new-connection packet.  Perform the necessary processing.
 * Nothing is sent back to the client.
 */
#ifndef EXEC_BACKEND
#else
#endif
#ifndef EXEC_BACKEND
#else
#endif

/*
 * canAcceptConnections --- check to see if database state allows connections.
 */



/*
 * ConnCreate -- create a local connection data structure
 *
 * Returns NULL on failure, other than out-of-memory which is fatal.
 */
#ifndef EXEC_BACKEND
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
#endif
#endif


/*
 * ConnFree -- free a local connection data structure
 */
#ifdef USE_SSL
#endif


/*
 * ClosePostmasterPorts -- close all the postmaster's open sockets
 *
 * This is called during child process startup to release file descriptors
 * that are not needed by that child process.  The postmaster still has
 * them open, of course.
 *
 * Note: we pass am_syslogger as a boolean because we don't want to set
 * the global variable yet when this is called.
 */
#ifndef WIN32
#endif
#ifndef WIN32
#else
#endif
#ifdef USE_BONJOUR
#endif


/*
 * reset_shared -- reset shared memory and semaphores
 */



/*
 * SIGHUP -- reread config files, and tell children to do same
 */
#ifdef EXEC_BACKEND
#endif


/*
 * pmdie -- signal handler for processing various postmaster signals.
 */


/*
 * Reaper -- signal handler to cleanup after a child process dies.
 */


/*
 * Scan the bgworkers list and see if the given PID (which has just stopped
 * or crashed) is in it.  Handle its shutdown if so, and return true.  If not a
 * bgworker, return false.
 *
 * This is heavily based on CleanupBackend.  One important difference is that
 * we don't know yet that the dying process is a bgworker, so we must be silent
 * until we're sure it is.
 */
#ifdef WIN32
#endif
#ifdef EXEC_BACKEND
#endif

/*
 * CleanupBackend -- cleanup after terminated backend.
 *
 * Remove all local state associated with backend.
 *
 * If you change this, see also CleanupBackgroundWorker.
 */
#ifdef WIN32
#endif
#ifdef EXEC_BACKEND
#endif

/*
 * HandleChildCrash -- cleanup after failed backend, bgwriter, checkpointer,
 * walwriter, autovacuum, or background worker.
 *
 * The objectives here are to clean up our local state about the child
 * process, and to signal all other remaining children to quickdie.
 */
#ifdef EXEC_BACKEND
#endif
#ifdef EXEC_BACKEND
#endif

/*
 * Log the death of a child process.
 */
#if defined(WIN32)
#else
#endif

/*
 * Advance the postmaster's state machine and take actions as appropriate
 *
 * This is common code for pmdie(), reaper() and sigusr1_handler(), which
 * receive the signals that might mean we need to change state.
 */



/*
 * Send a signal to a postmaster child process
 *
 * On systems that have setsid(), each child process sets itself up as a
 * process group leader.  For signals that are generally interpreted in the
 * appropriate fashion, we signal the entire process group not just the
 * direct child process.  This allows us to, for example, SIGQUIT a blocked
 * archive_recovery script, or SIGINT a script being run by a backend via
 * system().
 *
 * There is a race condition for recently-forked children: they might not
 * have executed setsid() yet.  So we signal the child directly as well as
 * the group.  We assume such a child will handle the signal before trying
 * to spawn any grandchild processes.  We also assume that signaling the
 * child twice will not cause any problems.
 */
#ifdef HAVE_SETSID
#endif

/*
 * Send a signal to the targeted children (but NOT special children;
 * dead_end children are never signaled, either).
 */


/*
 * Send a termination signal to children.  This considers all of our children
 * processes, except syslogger and dead_end backends.
 */


/*
 * BackendStartup -- start backend process
 *
 * returns: STATUS_ERROR if the fork failed, STATUS_OK otherwise.
 *
 * Note: if you change this code, also consider StartAutovacuumWorker.
 */
#ifdef EXEC_BACKEND
#else							/* !EXEC_BACKEND */
#endif   /* EXEC_BACKEND */
#ifdef EXEC_BACKEND
#endif

/*
 * Try to report backend fork() failure to client before we close the
 * connection.  Since we do not care to risk blocking the postmaster on
 * this connection, we set the connection to non-blocking and try only once.
 *
 * This is grungy special-purpose code; we cannot use backend libpq since
 * it's not up and running.
 */



/*
 * BackendInitialize -- initialize an interactive (postmaster-child)
 *				backend process, and collect the client's startup packet.
 *
 * returns: nothing.  Will not return at all if there's any failure.
 *
 * Note: this code does not depend on having any access to shared memory.
 * In the EXEC_BACKEND case, we are physically attached to shared memory
 * but have not yet set up most of our local pointers to shmem structures.
 */



/*
 * BackendRun -- set up the backend's argument list and invoke PostgresMain()
 *
 * returns:
 *		Shouldn't return at all.
 *		If PostgresMain() fails, return status.
 */



#ifdef EXEC_BACKEND

/*
 * postmaster_forkexec -- fork and exec a postmaster subprocess
 *
 * The caller must have set up the argv array already, except for argv[2]
 * which will be filled with the name of the temp variable file.
 *
 * Returns the child process PID, or -1 on fork failure (a suitable error
 * message has been logged on failure).
 *
 * All uses of this routine will dispatch to SubPostmasterMain in the
 * child process.
 */
pid_t
postmaster_forkexec(int argc, char *argv[])
{
	Port		port;

	/* This entry point passes dummy values for the Port variables */
	memset(&port, 0, sizeof(port));
	return internal_forkexec(argc, argv, &port);
}

/*
 * backend_forkexec -- fork/exec off a backend process
 *
 * Some operating systems (WIN32) don't have fork() so we have to simulate
 * it by storing parameters that need to be passed to the child and
 * then create a new child process.
 *
 * returns the pid of the fork/exec'd process, or -1 on failure
 */
static pid_t
backend_forkexec(Port *port)
{
	char	   *av[4];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkbackend";
	av[ac++] = NULL;			/* filled in by internal_forkexec */

	av[ac] = NULL;
	Assert(ac < lengthof(av));

	return internal_forkexec(ac, av, port);
}

#ifndef WIN32

/*
 * internal_forkexec non-win32 implementation
 *
 * - writes out backend variables to the parameter file
 * - fork():s, and then exec():s the child process
 */
static pid_t
internal_forkexec(int argc, char *argv[], Port *port)
{
	static unsigned long tmpBackendFileNum = 0;
	pid_t		pid;
	char		tmpfilename[MAXPGPATH];
	BackendParameters param;
	FILE	   *fp;

	if (!save_backend_variables(&param, port))
		return -1;				/* log made by save_backend_variables */

	/* Calculate name for temp file */
	snprintf(tmpfilename, MAXPGPATH, "%s/%s.backend_var.%d.%lu",
			 PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX,
			 MyProcPid, ++tmpBackendFileNum);

	/* Open file */
	fp = AllocateFile(tmpfilename, PG_BINARY_W);
	if (!fp)
	{
		/*
		 * As in OpenTemporaryFileInTablespace, try to make the temp-file
		 * directory
		 */
		mkdir(PG_TEMP_FILES_DIR, S_IRWXU);

		fp = AllocateFile(tmpfilename, PG_BINARY_W);
		if (!fp)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m",
							tmpfilename)));
			return -1;
		}
	}

	if (fwrite(&param, sizeof(param), 1, fp) != 1)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		FreeFile(fp);
		return -1;
	}

	/* Release file */
	if (FreeFile(fp))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		return -1;
	}

	/* Make sure caller set up argv properly */
	Assert(argc >= 3);
	Assert(argv[argc] == NULL);
	Assert(strncmp(argv[1], "--fork", 6) == 0);
	Assert(argv[2] == NULL);

	/* Insert temp file name after --fork argument */
	argv[2] = tmpfilename;

	/* Fire off execv in child */
	if ((pid = fork_process()) == 0)
	{
		if (execv(postgres_exec_path, argv) < 0)
		{
			ereport(LOG,
					(errmsg("could not execute server process \"%s\": %m",
							postgres_exec_path)));
			/* We're already in the child process here, can't return */
			exit(1);
		}
	}

	return pid;					/* Parent returns pid, or -1 on fork failure */
}
#else							/* WIN32 */

/*
 * internal_forkexec win32 implementation
 *
 * - starts backend using CreateProcess(), in suspended state
 * - writes out backend variables to the parameter file
 *	- during this, duplicates handles and sockets required for
 *	  inheritance into the new process
 * - resumes execution of the new process once the backend parameter
 *	 file is complete.
 */
static pid_t
internal_forkexec(int argc, char *argv[], Port *port)
{
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	int			i;
	int			j;
	char		cmdLine[MAXPGPATH * 2];
	HANDLE		paramHandle;
	BackendParameters *param;
	SECURITY_ATTRIBUTES sa;
	char		paramHandleStr[32];
	win32_deadchild_waitinfo *childinfo;

	/* Make sure caller set up argv properly */
	Assert(argc >= 3);
	Assert(argv[argc] == NULL);
	Assert(strncmp(argv[1], "--fork", 6) == 0);
	Assert(argv[2] == NULL);

	/* Set up shared memory for parameter passing */
	ZeroMemory(&sa, sizeof(sa));
	sa.nLength = sizeof(sa);
	sa.bInheritHandle = TRUE;
	paramHandle = CreateFileMapping(INVALID_HANDLE_VALUE,
									&sa,
									PAGE_READWRITE,
									0,
									sizeof(BackendParameters),
									NULL);
	if (paramHandle == INVALID_HANDLE_VALUE)
	{
		elog(LOG, "could not create backend parameter file mapping: error code %lu",
			 GetLastError());
		return -1;
	}

	param = MapViewOfFile(paramHandle, FILE_MAP_WRITE, 0, 0, sizeof(BackendParameters));
	if (!param)
	{
		elog(LOG, "could not map backend parameter memory: error code %lu",
			 GetLastError());
		CloseHandle(paramHandle);
		return -1;
	}

	/* Insert temp file name after --fork argument */
#ifdef _WIN64
	sprintf(paramHandleStr, "%llu", (LONG_PTR) paramHandle);
#else
	sprintf(paramHandleStr, "%lu", (DWORD) paramHandle);
#endif
	argv[2] = paramHandleStr;

	/* Format the cmd line */
	cmdLine[sizeof(cmdLine) - 1] = '\0';
	cmdLine[sizeof(cmdLine) - 2] = '\0';
	snprintf(cmdLine, sizeof(cmdLine) - 1, "\"%s\"", postgres_exec_path);
	i = 0;
	while (argv[++i] != NULL)
	{
		j = strlen(cmdLine);
		snprintf(cmdLine + j, sizeof(cmdLine) - 1 - j, " \"%s\"", argv[i]);
	}
	if (cmdLine[sizeof(cmdLine) - 2] != '\0')
	{
		elog(LOG, "subprocess command line too long");
		return -1;
	}

	memset(&pi, 0, sizeof(pi));
	memset(&si, 0, sizeof(si));
	si.cb = sizeof(si);

	/*
	 * Create the subprocess in a suspended state. This will be resumed later,
	 * once we have written out the parameter file.
	 */
	if (!CreateProcess(NULL, cmdLine, NULL, NULL, TRUE, CREATE_SUSPENDED,
					   NULL, NULL, &si, &pi))
	{
		elog(LOG, "CreateProcess call failed: %m (error code %lu)",
			 GetLastError());
		return -1;
	}

	if (!save_backend_variables(param, port, pi.hProcess, pi.dwProcessId))
	{
		/*
		 * log made by save_backend_variables, but we have to clean up the
		 * mess with the half-started process
		 */
		if (!TerminateProcess(pi.hProcess, 255))
			ereport(LOG,
					(errmsg_internal("could not terminate unstarted process: error code %lu",
									 GetLastError())));
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		return -1;				/* log made by save_backend_variables */
	}

	/* Drop the parameter shared memory that is now inherited to the backend */
	if (!UnmapViewOfFile(param))
		elog(LOG, "could not unmap view of backend parameter file: error code %lu",
			 GetLastError());
	if (!CloseHandle(paramHandle))
		elog(LOG, "could not close handle to backend parameter file: error code %lu",
			 GetLastError());

	/*
	 * Reserve the memory region used by our main shared memory segment before
	 * we resume the child process.
	 */
	if (!pgwin32_ReserveSharedMemoryRegion(pi.hProcess))
	{
		/*
		 * Failed to reserve the memory, so terminate the newly created
		 * process and give up.
		 */
		if (!TerminateProcess(pi.hProcess, 255))
			ereport(LOG,
					(errmsg_internal("could not terminate process that failed to reserve memory: error code %lu",
									 GetLastError())));
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		return -1;				/* logging done made by
								 * pgwin32_ReserveSharedMemoryRegion() */
	}

	/*
	 * Now that the backend variables are written out, we start the child
	 * thread so it can start initializing while we set up the rest of the
	 * parent state.
	 */
	if (ResumeThread(pi.hThread) == -1)
	{
		if (!TerminateProcess(pi.hProcess, 255))
		{
			ereport(LOG,
					(errmsg_internal("could not terminate unstartable process: error code %lu",
									 GetLastError())));
			CloseHandle(pi.hProcess);
			CloseHandle(pi.hThread);
			return -1;
		}
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		ereport(LOG,
				(errmsg_internal("could not resume thread of unstarted process: error code %lu",
								 GetLastError())));
		return -1;
	}

	/*
	 * Queue a waiter for to signal when this child dies. The wait will be
	 * handled automatically by an operating system thread pool.
	 *
	 * Note: use malloc instead of palloc, since it needs to be thread-safe.
	 * Struct will be free():d from the callback function that runs on a
	 * different thread.
	 */
	childinfo = malloc(sizeof(win32_deadchild_waitinfo));
	if (!childinfo)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	childinfo->procHandle = pi.hProcess;
	childinfo->procId = pi.dwProcessId;

	if (!RegisterWaitForSingleObject(&childinfo->waitHandle,
									 pi.hProcess,
									 pgwin32_deadchild_callback,
									 childinfo,
									 INFINITE,
								WT_EXECUTEONLYONCE | WT_EXECUTEINWAITTHREAD))
		ereport(FATAL,
				(errmsg_internal("could not register process for wait: error code %lu",
								 GetLastError())));

	/* Don't close pi.hProcess here - the wait thread needs access to it */

	CloseHandle(pi.hThread);

	return pi.dwProcessId;
}
#endif   /* WIN32 */


/*
 * SubPostmasterMain -- Get the fork/exec'd process into a state equivalent
 *			to what it would be if we'd simply forked on Unix, and then
 *			dispatch to the appropriate place.
 *
 * The first two command line arguments are expected to be "--forkFOO"
 * (where FOO indicates which postmaster child we are to become), and
 * the name of a variables file that we can read to load data that would
 * have been inherited by fork() on Unix.  Remaining arguments go to the
 * subprocess FooMain() routine.
 */
void
SubPostmasterMain(int argc, char *argv[])
{
	Port		port;

	/* In EXEC_BACKEND case we will not have inherited these settings */
	IsPostmasterEnvironment = true;
	whereToSendOutput = DestNone;

	/* Setup as postmaster child */
	InitPostmasterChild();

	/* Setup essential subsystems (to ensure elog() behaves sanely) */
	InitializeGUCOptions();

	/* Read in the variables file */
	memset(&port, 0, sizeof(Port));
	read_backend_variables(argv[2], &port);

	/*
	 * Set reference point for stack-depth checking
	 */
	set_stack_base();

	/*
	 * Set up memory area for GSS information. Mirrors the code in ConnCreate
	 * for the non-exec case.
	 */
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
	port.gss = (pg_gssinfo *) calloc(1, sizeof(pg_gssinfo));
	if (!port.gss)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
#endif

	/* Check we got appropriate args */
	if (argc < 3)
		elog(FATAL, "invalid subpostmaster invocation");

	/*
	 * If appropriate, physically re-attach to shared memory segment. We want
	 * to do this before going any further to ensure that we can attach at the
	 * same address the postmaster used.  On the other hand, if we choose not
	 * to re-attach, we may have other cleanup to do.
	 */
	if (strcmp(argv[1], "--forkbackend") == 0 ||
		strcmp(argv[1], "--forkavlauncher") == 0 ||
		strcmp(argv[1], "--forkavworker") == 0 ||
		strcmp(argv[1], "--forkboot") == 0 ||
		strncmp(argv[1], "--forkbgworker=", 15) == 0)
		PGSharedMemoryReAttach();
	else
		PGSharedMemoryNoReAttach();

	/* autovacuum needs this set before calling InitProcess */
	if (strcmp(argv[1], "--forkavlauncher") == 0)
		AutovacuumLauncherIAm();
	if (strcmp(argv[1], "--forkavworker") == 0)
		AutovacuumWorkerIAm();

	/*
	 * Start our win32 signal implementation. This has to be done after we
	 * read the backend variables, because we need to pick up the signal pipe
	 * from the parent process.
	 */
#ifdef WIN32
	pgwin32_signal_initialize();
#endif

	/* In EXEC_BACKEND case we will not have inherited these settings */
	pqinitmask();
	PG_SETMASK(&BlockSig);

	/* Read in remaining GUC variables */
	read_nondefault_variables();

	/*
	 * Reload any libraries that were preloaded by the postmaster.  Since we
	 * exec'd this process, those libraries didn't come along with us; but we
	 * should load them into all child processes to be consistent with the
	 * non-EXEC_BACKEND behavior.
	 */
	process_shared_preload_libraries();

	/* Run backend or appropriate child */
	if (strcmp(argv[1], "--forkbackend") == 0)
	{
		Assert(argc == 3);		/* shouldn't be any more args */

		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/*
		 * Need to reinitialize the SSL library in the backend, since the
		 * context structures contain function pointers and cannot be passed
		 * through the parameter file.
		 *
		 * XXX should we do this in all child processes?  For the moment it's
		 * enough to do it in backend children.
		 */
#ifdef USE_SSL
		if (EnableSSL)
			secure_initialize();
#endif

		/*
		 * Perform additional initialization and collect startup packet.
		 *
		 * We want to do this before InitProcess() for a couple of reasons: 1.
		 * so that we aren't eating up a PGPROC slot while waiting on the
		 * client. 2. so that if InitProcess() fails due to being out of
		 * PGPROC slots, we have already initialized libpq and are able to
		 * report the error to the client.
		 */
		BackendInitialize(&port);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitProcess();

		/*
		 * Attach process to shared data structures.  If testing EXEC_BACKEND
		 * on Linux, you must run this as root before starting the postmaster:
		 *
		 * echo 0 >/proc/sys/kernel/randomize_va_space
		 *
		 * This prevents a randomized stack base address that causes child
		 * shared memory to be at a different address than the parent, making
		 * it impossible to attached to shared memory.  Return the value to
		 * '1' when finished.
		 */
		CreateSharedMemoryAndSemaphores(false, 0);

		/* And run the backend */
		BackendRun(&port);		/* does not return */
	}
	if (strcmp(argv[1], "--forkboot") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitAuxiliaryProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(false, 0);

		AuxiliaryProcessMain(argc - 2, argv + 2);		/* does not return */
	}
	if (strcmp(argv[1], "--forkavlauncher") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(false, 0);

		AutoVacLauncherMain(argc - 2, argv + 2);		/* does not return */
	}
	if (strcmp(argv[1], "--forkavworker") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(false, 0);

		AutoVacWorkerMain(argc - 2, argv + 2);	/* does not return */
	}
	if (strncmp(argv[1], "--forkbgworker=", 15) == 0)
	{
		int			shmem_slot;

		/* do this as early as possible; in particular, before InitProcess() */
		IsBackgroundWorker = true;

		InitPostmasterChild();

		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(false, 0);

		shmem_slot = atoi(argv[1] + 15);
		MyBgworkerEntry = BackgroundWorkerEntry(shmem_slot);
		StartBackgroundWorker();
	}
	if (strcmp(argv[1], "--forkarch") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Do not want to attach to shared memory */

		PgArchiverMain(argc, argv);		/* does not return */
	}
	if (strcmp(argv[1], "--forkcol") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Do not want to attach to shared memory */

		PgstatCollectorMain(argc, argv);		/* does not return */
	}
	if (strcmp(argv[1], "--forklog") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(true);

		/* Do not want to attach to shared memory */

		SysLoggerMain(argc, argv);		/* does not return */
	}

	abort();					/* shouldn't get here */
}
#endif   /* EXEC_BACKEND */


/*
 * ExitPostmaster -- cleanup
 *
 * Do NOT call exit() directly --- always go through here!
 */
#ifdef HAVE_PTHREAD_IS_THREADED_NP
#endif

/*
 * sigusr1_handler - handle signal conditions from child processes
 */


/*
 * SIGTERM or SIGQUIT while processing startup packet.
 * Clean up and exit(1).
 *
 * XXX: possible future improvement: try to send a message indicating
 * why we are disconnecting.  Problem is to be sure we don't block while
 * doing so, nor mess up SSL initialization.  In practice, if the client
 * has wedged here, it probably couldn't do anything with the message anyway.
 */


/*
 * Dummy signal handler
 *
 * We use this for signals that we don't actually use in the postmaster,
 * but we do use in backends.  If we were to SIG_IGN such signals in the
 * postmaster, then a newly started backend might drop a signal that arrives
 * before it's able to reconfigure its signal processing.  (See notes in
 * tcop/postgres.c.)
 */


/*
 * Timeout while processing startup packet.
 * As for startup_die(), we clean up and exit(1).
 */



/*
 * RandomSalt
 */


/*
 * PostmasterRandom
 */


/*
 * Count up number of child processes of specified types (dead_end chidren
 * are always excluded).
 */



/*
 * StartChildProcess -- start an auxiliary process for the postmaster
 *
 * xlop determines what kind of child will be started.  All child types
 * initially go to AuxiliaryProcessMain, which will handle common setup.
 *
 * Return value of StartChildProcess is subprocess' PID, or 0 if failed
 * to start subprocess.
 */
#ifdef EXEC_BACKEND
#endif
#ifdef EXEC_BACKEND
#else							/* !EXEC_BACKEND */
#endif   /* EXEC_BACKEND */

/*
 * StartAutovacuumWorker
 *		Start an autovac worker process.
 *
 * This function is here because it enters the resulting PID into the
 * postmaster's private backends list.
 *
 * NB -- this code very roughly matches BackendStartup.
 */
#ifdef EXEC_BACKEND
#endif

/*
 * Create the opts file
 */
#define OPTS_FILE	"postmaster.opts"


/*
 * MaxLivePostmasterChildren
 *
 * This reports the number of entries needed in per-child-process arrays
 * (the PMChildFlags array, and if EXEC_BACKEND the ShmemBackendArray).
 * These arrays include regular backends, autovac workers, walsenders
 * and background workers, but not special children nor dead_end children.
 * This allows the arrays to have a fixed maximum size, to wit the same
 * too-many-children limit enforced by canAcceptConnections().  The exact value
 * isn't too critical as long as it's more than MaxBackends.
 */


/*
 * Connect background worker to a database.
 */


/*
 * Connect background worker to a database using OIDs.
 */


/*
 * Block/unblock signals in a background worker
 */




#ifdef EXEC_BACKEND
static pid_t
bgworker_forkexec(int shmem_slot)
{
	char	   *av[10];
	int			ac = 0;
	char		forkav[MAXPGPATH];

	snprintf(forkav, MAXPGPATH, "--forkbgworker=%d", shmem_slot);

	av[ac++] = "postgres";
	av[ac++] = forkav;
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif

/*
 * Start a new bgworker.
 * Starting time conditions must have been checked already.
 *
 * This code is heavily based on autovacuum.c, q.v.
 */
#ifdef EXEC_BACKEND
#else
#endif
#ifndef EXEC_BACKEND
#endif

/*
 * Does the current postmaster state require starting a worker with the
 * specified start_time?
 */


/*
 * Allocate the Backend struct for a connected background worker, but don't
 * add it to the list of backends just yet.
 *
 * Some info from the Backend is copied into the passed rw.
 */


/*
 * If the time is right, start one background worker.
 *
 * As a side effect, the bgworker control variables are set or reset whenever
 * there are more workers to start after this one, and whenever the overall
 * system state requires it.
 */
#ifdef EXEC_BACKEND
#endif

/*
 * When a backend asks to be notified about worker state changes, we
 * set a flag in its backend entry.  The background worker machinery needs
 * to know when such backends exit.
 */


#ifdef EXEC_BACKEND

/*
 * The following need to be available to the save/restore_backend_variables
 * functions.  They are marked NON_EXEC_STATIC in their home modules.
 */
extern slock_t *ShmemLock;
extern slock_t *ProcStructLock;
extern PGPROC *AuxiliaryProcs;
extern PMSignalData *PMSignalState;
extern pgsocket pgStatSock;
extern pg_time_t first_syslogger_file_time;

#ifndef WIN32
#define write_inheritable_socket(dest, src, childpid) ((*(dest) = (src)), true)
#define read_inheritable_socket(dest, src) (*(dest) = *(src))
#else
static bool write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE child);
static bool write_inheritable_socket(InheritableSocket *dest, SOCKET src,
						 pid_t childPid);
static void read_inheritable_socket(SOCKET *dest, InheritableSocket *src);
#endif


/* Save critical backend variables into the BackendParameters struct */
#ifndef WIN32
static bool
save_backend_variables(BackendParameters *param, Port *port)
#else
static bool
save_backend_variables(BackendParameters *param, Port *port,
					   HANDLE childProcess, pid_t childPid)
#endif
{
	memcpy(&param->port, port, sizeof(Port));
	if (!write_inheritable_socket(&param->portsocket, port->sock, childPid))
		return false;

	strlcpy(param->DataDir, DataDir, MAXPGPATH);

	memcpy(&param->ListenSocket, &ListenSocket, sizeof(ListenSocket));

	param->MyCancelKey = MyCancelKey;
	param->MyPMChildSlot = MyPMChildSlot;

	param->UsedShmemSegID = UsedShmemSegID;
	param->UsedShmemSegAddr = UsedShmemSegAddr;

	param->ShmemLock = ShmemLock;
	param->ShmemVariableCache = ShmemVariableCache;
	param->ShmemBackendArray = ShmemBackendArray;

#ifndef HAVE_SPINLOCKS
	param->SpinlockSemaArray = SpinlockSemaArray;
#endif
	param->MainLWLockArray = MainLWLockArray;
	param->ProcStructLock = ProcStructLock;
	param->ProcGlobal = ProcGlobal;
	param->AuxiliaryProcs = AuxiliaryProcs;
	param->PreparedXactProcs = PreparedXactProcs;
	param->PMSignalState = PMSignalState;
	if (!write_inheritable_socket(&param->pgStatSock, pgStatSock, childPid))
		return false;

	param->PostmasterPid = PostmasterPid;
	param->PgStartTime = PgStartTime;
	param->PgReloadTime = PgReloadTime;
	param->first_syslogger_file_time = first_syslogger_file_time;

	param->redirection_done = redirection_done;
	param->IsBinaryUpgrade = IsBinaryUpgrade;
	param->max_safe_fds = max_safe_fds;

	param->MaxBackends = MaxBackends;

#ifdef WIN32
	param->PostmasterHandle = PostmasterHandle;
	if (!write_duplicated_handle(&param->initial_signal_pipe,
								 pgwin32_create_signal_listener(childPid),
								 childProcess))
		return false;
#else
	memcpy(&param->postmaster_alive_fds, &postmaster_alive_fds,
		   sizeof(postmaster_alive_fds));
#endif

	memcpy(&param->syslogPipe, &syslogPipe, sizeof(syslogPipe));

	strlcpy(param->my_exec_path, my_exec_path, MAXPGPATH);

	strlcpy(param->pkglib_path, pkglib_path, MAXPGPATH);

	strlcpy(param->ExtraOptions, ExtraOptions, MAXPGPATH);

	return true;
}


#ifdef WIN32
/*
 * Duplicate a handle for usage in a child process, and write the child
 * process instance of the handle to the parameter file.
 */
static bool
write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE childProcess)
{
	HANDLE		hChild = INVALID_HANDLE_VALUE;

	if (!DuplicateHandle(GetCurrentProcess(),
						 src,
						 childProcess,
						 &hChild,
						 0,
						 TRUE,
						 DUPLICATE_CLOSE_SOURCE | DUPLICATE_SAME_ACCESS))
	{
		ereport(LOG,
				(errmsg_internal("could not duplicate handle to be written to backend parameter file: error code %lu",
								 GetLastError())));
		return false;
	}

	*dest = hChild;
	return true;
}

/*
 * Duplicate a socket for usage in a child process, and write the resulting
 * structure to the parameter file.
 * This is required because a number of LSPs (Layered Service Providers) very
 * common on Windows (antivirus, firewalls, download managers etc) break
 * straight socket inheritance.
 */
static bool
write_inheritable_socket(InheritableSocket *dest, SOCKET src, pid_t childpid)
{
	dest->origsocket = src;
	if (src != 0 && src != PGINVALID_SOCKET)
	{
		/* Actual socket */
		if (WSADuplicateSocket(src, childpid, &dest->wsainfo) != 0)
		{
			ereport(LOG,
					(errmsg("could not duplicate socket %d for use in backend: error code %d",
							(int) src, WSAGetLastError())));
			return false;
		}
	}
	return true;
}

/*
 * Read a duplicate socket structure back, and get the socket descriptor.
 */
static void
read_inheritable_socket(SOCKET *dest, InheritableSocket *src)
{
	SOCKET		s;

	if (src->origsocket == PGINVALID_SOCKET || src->origsocket == 0)
	{
		/* Not a real socket! */
		*dest = src->origsocket;
	}
	else
	{
		/* Actual socket, so create from structure */
		s = WSASocket(FROM_PROTOCOL_INFO,
					  FROM_PROTOCOL_INFO,
					  FROM_PROTOCOL_INFO,
					  &src->wsainfo,
					  0,
					  0);
		if (s == INVALID_SOCKET)
		{
			write_stderr("could not create inherited socket: error code %d\n",
						 WSAGetLastError());
			exit(1);
		}
		*dest = s;

		/*
		 * To make sure we don't get two references to the same socket, close
		 * the original one. (This would happen when inheritance actually
		 * works..
		 */
		closesocket(src->origsocket);
	}
}
#endif

static void
read_backend_variables(char *id, Port *port)
{
	BackendParameters param;

#ifndef WIN32
	/* Non-win32 implementation reads from file */
	FILE	   *fp;

	/* Open file */
	fp = AllocateFile(id, PG_BINARY_R);
	if (!fp)
	{
		write_stderr("could not open backend variables file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}

	if (fread(&param, sizeof(param), 1, fp) != 1)
	{
		write_stderr("could not read from backend variables file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}

	/* Release file */
	FreeFile(fp);
	if (unlink(id) != 0)
	{
		write_stderr("could not remove file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}
#else
	/* Win32 version uses mapped file */
	HANDLE		paramHandle;
	BackendParameters *paramp;

#ifdef _WIN64
	paramHandle = (HANDLE) _atoi64(id);
#else
	paramHandle = (HANDLE) atol(id);
#endif
	paramp = MapViewOfFile(paramHandle, FILE_MAP_READ, 0, 0, 0);
	if (!paramp)
	{
		write_stderr("could not map view of backend variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}

	memcpy(&param, paramp, sizeof(BackendParameters));

	if (!UnmapViewOfFile(paramp))
	{
		write_stderr("could not unmap view of backend variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}

	if (!CloseHandle(paramHandle))
	{
		write_stderr("could not close handle to backend parameter variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}
#endif

	restore_backend_variables(&param, port);
}

/* Restore critical backend variables from the BackendParameters struct */
static void
restore_backend_variables(BackendParameters *param, Port *port)
{
	memcpy(port, &param->port, sizeof(Port));
	read_inheritable_socket(&port->sock, &param->portsocket);

	SetDataDir(param->DataDir);

	memcpy(&ListenSocket, &param->ListenSocket, sizeof(ListenSocket));

	MyCancelKey = param->MyCancelKey;
	MyPMChildSlot = param->MyPMChildSlot;

	UsedShmemSegID = param->UsedShmemSegID;
	UsedShmemSegAddr = param->UsedShmemSegAddr;

	ShmemLock = param->ShmemLock;
	ShmemVariableCache = param->ShmemVariableCache;
	ShmemBackendArray = param->ShmemBackendArray;

#ifndef HAVE_SPINLOCKS
	SpinlockSemaArray = param->SpinlockSemaArray;
#endif
	MainLWLockArray = param->MainLWLockArray;
	ProcStructLock = param->ProcStructLock;
	ProcGlobal = param->ProcGlobal;
	AuxiliaryProcs = param->AuxiliaryProcs;
	PreparedXactProcs = param->PreparedXactProcs;
	PMSignalState = param->PMSignalState;
	read_inheritable_socket(&pgStatSock, &param->pgStatSock);

	PostmasterPid = param->PostmasterPid;
	PgStartTime = param->PgStartTime;
	PgReloadTime = param->PgReloadTime;
	first_syslogger_file_time = param->first_syslogger_file_time;

	redirection_done = param->redirection_done;
	IsBinaryUpgrade = param->IsBinaryUpgrade;
	max_safe_fds = param->max_safe_fds;

	MaxBackends = param->MaxBackends;

#ifdef WIN32
	PostmasterHandle = param->PostmasterHandle;
	pgwin32_initial_signal_pipe = param->initial_signal_pipe;
#else
	memcpy(&postmaster_alive_fds, &param->postmaster_alive_fds,
		   sizeof(postmaster_alive_fds));
#endif

	memcpy(&syslogPipe, &param->syslogPipe, sizeof(syslogPipe));

	strlcpy(my_exec_path, param->my_exec_path, MAXPGPATH);

	strlcpy(pkglib_path, param->pkglib_path, MAXPGPATH);

	strlcpy(ExtraOptions, param->ExtraOptions, MAXPGPATH);
}


Size
ShmemBackendArraySize(void)
{
	return mul_size(MaxLivePostmasterChildren(), sizeof(Backend));
}

void
ShmemBackendArrayAllocation(void)
{
	Size		size = ShmemBackendArraySize();

	ShmemBackendArray = (Backend *) ShmemAlloc(size);
	/* Mark all slots as empty */
	memset(ShmemBackendArray, 0, size);
}

static void
ShmemBackendArrayAdd(Backend *bn)
{
	/* The array slot corresponding to my PMChildSlot should be free */
	int			i = bn->child_slot - 1;

	Assert(ShmemBackendArray[i].pid == 0);
	ShmemBackendArray[i] = *bn;
}

static void
ShmemBackendArrayRemove(Backend *bn)
{
	int			i = bn->child_slot - 1;

	Assert(ShmemBackendArray[i].pid == bn->pid);
	/* Mark the slot as empty */
	ShmemBackendArray[i].pid = 0;
}
#endif   /* EXEC_BACKEND */


#ifdef WIN32

/*
 * Subset implementation of waitpid() for Windows.  We assume pid is -1
 * (that is, check all child processes) and options is WNOHANG (don't wait).
 */
static pid_t
waitpid(pid_t pid, int *exitstatus, int options)
{
	DWORD		dwd;
	ULONG_PTR	key;
	OVERLAPPED *ovl;

	/*
	 * Check if there are any dead children. If there are, return the pid of
	 * the first one that died.
	 */
	if (GetQueuedCompletionStatus(win32ChildQueue, &dwd, &key, &ovl, 0))
	{
		*exitstatus = (int) key;
		return dwd;
	}

	return -1;
}

/*
 * Note! Code below executes on a thread pool! All operations must
 * be thread safe! Note that elog() and friends must *not* be used.
 */
static void WINAPI
pgwin32_deadchild_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired)
{
	win32_deadchild_waitinfo *childinfo = (win32_deadchild_waitinfo *) lpParameter;
	DWORD		exitcode;

	if (TimerOrWaitFired)
		return;					/* timeout. Should never happen, since we use
								 * INFINITE as timeout value. */

	/*
	 * Remove handle from wait - required even though it's set to wait only
	 * once
	 */
	UnregisterWaitEx(childinfo->waitHandle, NULL);

	if (!GetExitCodeProcess(childinfo->procHandle, &exitcode))
	{
		/*
		 * Should never happen. Inform user and set a fixed exitcode.
		 */
		write_stderr("could not read exit code for process\n");
		exitcode = 255;
	}

	if (!PostQueuedCompletionStatus(win32ChildQueue, childinfo->procId, (ULONG_PTR) exitcode, NULL))
		write_stderr("could not post child completion status\n");

	/*
	 * Handle is per-process, so we close it here instead of in the
	 * originating thread
	 */
	CloseHandle(childinfo->procHandle);

	/*
	 * Free struct that was allocated before the call to
	 * RegisterWaitForSingleObject()
	 */
	free(childinfo);

	/* Queue SIGCHLD signal */
	pg_queue_signal(SIGCHLD);
}
#endif   /* WIN32 */

/*
 * Initialize one and only handle for monitoring postmaster death.
 *
 * Called once in the postmaster, so that child processes can subsequently
 * monitor if their parent is dead.
 */
#ifndef WIN32
#else
#endif   /* WIN32 */
