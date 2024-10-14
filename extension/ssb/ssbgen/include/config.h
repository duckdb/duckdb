/*
 * Sccsid:     @(#)config.h	2.1.8.2
 *
 * this file allows the compilation of DBGEN to be tailored to specific
 * architectures and operating systems. Some options are grouped
 * together to allow easier compilation on a given vendor's hardware.
 *
 * The following #defines will effect the code:
 *   TPCH              -- make will create TPCH (set in makefile)
 *   TPCR              -- make will create TPCR (set in makefile)
 *   KILL(pid)         -- how to terminate a process in a parallel load
 *   SPAWN             -- name of system call to clone an existing process
 *   SET_HANDLER(proc) -- name of routine to handle signals in parallel load
 *   WAIT(res, pid)    -- how to await the termination of a child
 *   SEPARATOR         -- character used to separate fields in flat files
 *   DBNAME            -- default name of database to be loaded
 *   STDLIB_HAS_GETOPT -- to prevent confilcts with gloabal getopt()
 *   MDY_DATE          -- generate dates as MM-DD-YY
 *   WIN32             -- support for WindowsNT
 *   SUPPORT_64BITS    -- compiler defines a 64 bit datatype
 *   DSS_HUGE          -- 64 bit data type
 *   HUGE_FORMAT       -- printf string for 64 bit data type
 *   HUGE_COUNT        -- number of objects in DSS_HUGE
 *   EOL_HANDLING      -- flat files don't need final column separator
 *
 *   OS defines
 *   ==========
 *   ATT        -- getopt() handling
 *   DIGITAL    -- changes for DigUnix 64-bit support
 *   DOS        -- disable all multi-user functionality/dependency
 *   HP         -- posix source inclusion differences
 *   IBM        -- posix source inclusion differences
 *   ICL        -- getopt() handling
 *   MVS        -- special handling of varchar format
 *   SGI        -- getopt() handling
 *   SUN        -- getopt() handling
 *   LINUX      -- getopt() handling
 *   TANDEM     -- EOL handling
 *   U2200      -- death of parent kills children automatically
 *   VMS        -- signal/fork handing differences
 *
 *   Database defines
 *   ================
 *   DB2        -- use DB2 dialect in QGEN
 *   INFORMIX   -- use Informix dialect in QGEN
 *   SQLSERVER  -- use SQLSERVER dialect in QGEN
 *   SYBASE     -- use Sybase dialect in QGEN
 *   TDAT       -- use Teradata dialect in QGEN
 */

#ifdef DOS
#define DSS_PROC 1
#define PATH_SEP '\\'
#else

#ifdef ATT
#define STDLIB_HAS_GETOPT
#ifdef SQLSERVER
#define WIN32
#else
/* the 64 bit defines are for the Metaware compiler */
#define SUPPORT_64BITS
#define DSS_HUGE long long
#define HUGE_COUNT 1
#define HUGE_FORMAT "%LLd"
#endif /* SQLSERVER or MP/RAS */
#endif /* ATT */

#ifdef DIGITAL
#define DOUBLE_CAST (double)(int)
#endif

#ifdef HP
#define _INCLUDE_POSIX_SOURCE
#define STDLIB_HAS_GETOPT
#endif /* HP */

#ifdef IBM
#define _POSIX_SOURCE
/*
 * if the C compiler is 3.1 or later, then uncomment the
 * lines for 64 bit seed generation
 */
/* #define SUPPORT_64BITS*/
/* #define DSS_HUGE long long*/
/* #define HUGE_COUNT	1 */
#define STDLIB_HAS_GETOPT
#endif /* IBM */

#ifdef ICL
#define STDLIB_HAS_GETOPT
#endif /* ICL */

#ifdef SUN
#define STDLIB_HAS_GETOPT
#endif /* SUN */

#ifdef LINUX
#define STDLIB_HAS_GETOPT
#endif /* LINUX */

#ifdef MAC
#define _POSIX_SOURCE
#define STDLIB_HAS_GETOPT
#endif /* MAC */

#ifdef SGI
#define STDLIB_HAS_GETOPT
#define SUPPORT_64BITS
#define DSS_HUGE __uint64_t
#define HUGE_COUNT 1
#endif /* SGI */

#ifdef TANDEM
#define EOL_HANDLING
#endif /* TANDEM */

#ifdef VMS
#define SPAWN vfork
#define KILL(pid) kill(SIGQUIT, pid)
#define SET_HANDLER(proc) signal(SIGQUIT, proc)
#define WAIT(res, pid) wait(res)
#define SIGS_DEFINED
#endif /* VMS */

#if (defined(WIN32) && !defined(_POSIX_))
#define pid_t int
#define SET_HANDLER(proc) signal(SIGINT, proc)
#define KILL(pid) \
     TerminateProcess(OpenProcess(PROCESS_TERMINATE, FALSE, pid), 3)
#if (defined(__WATCOMC__))
#define SPAWN() spawnv(P_NOWAIT, spawn_args[0], spawn_args)
#define WAIT(res, pid) cwait(res, pid, WAIT_CHILD)
#else
#define SPAWN() _spawnv(_P_NOWAIT, spawn_args[0], spawn_args)
#define WAIT(res, pid) _cwait(res, pid, _WAIT_CHILD)
#define getpid _getpid
#endif /* WATCOMC */
#define SIGS_DEFINED
#define PATH_SEP '\\'
#ifndef TEST_32B
#define SUPPORT_64BITS
#define DSS_HUGE __int64
#define HUGE_COUNT 1
#define HUGE_FORMAT "%I64d"
#endif /* TEST_32B */
/* need to define process termination codes to match UNIX */
/* these are copied from Linux/GNU and need to be verified as part of a rework of */
/* process handling under NT (29 Apr 98) */
#define WIFEXITED(s) ((s & 0xFF) == 0)
#define WIFSIGNALED(s) (((unsigned int)((status) - 1) & 0xFFFF) < 0xFF)
#define WIFSTOPPED(s) (((s) & 0xff) == 0x7f)
#define WTERMSIG(s) ((s) & 0x7f)
#define WSTOPSIG(s) (((s) & 0xff00) >> 8)
#endif /* WIN32 */

#ifndef SIGS_DEFINED
#define KILL(pid) kill(SIGUSR1, pid)
#define SET_HANDLER(proc) signal(SIGUSR1, proc)
#define SPAWN fork
#define WAIT(res, pid) wait(res)
#endif /* DEFAULT */

#define DSS_PROC getpid()
#endif /* DOS */

#ifndef DBNAME
#define DBNAME "dss"
#endif /* DBNAME */

#ifndef PATH_SEP
#define PATH_SEP '/'
#endif /* PATH_SEP */

#ifndef DSS_HUGE
#define DSS_HUGE long
#define HUGE_COUNT 2
#endif

#ifndef DOUBLE_CAST
#define DOUBLE_CAST (double)
#endif /* DOUBLE_CAST */
