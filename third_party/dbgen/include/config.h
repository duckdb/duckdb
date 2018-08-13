/*
* $Id: config.h,v 1.8 2007/01/04 21:29:21 jms Exp $
*
* Revision History
* ===================
* $Log: config.h,v $
* Revision 1.8  2007/01/04 21:29:21  jms
* Porting changes uncovered as part of move to VS2005. No impact on data set
*
* Revision 1.7  2006/06/29 20:46:17  jms
* 2.4.0 changes from Meikel
*
* Revision 1.6  2006/05/31 22:25:21  jms
* Rework UnifInt calls in varsub to handle lack of PROTO defn in windows
*
* Revision 1.5  2006/05/25 22:35:36  jms
* qgen porting changes for 32b/64b
*
* Revision 1.4  2006/03/09 18:54:55  jms
* porting bugs
*
* Revision 1.3  2005/03/04 19:48:39  jms
* Changes from Doug Johnson to address very large scale factors
*
* Revision 1.2  2005/01/03 20:08:58  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:46  jms
* re-establish external server
*
* Revision 1.7  2004/04/08 17:36:47  jms
* clarify config.h/makefile linkage
*
* Revision 1.6  2004/04/08 17:35:00  jms
* SUN/SOLARIS ifdef merge between machines
*
* Revision 1.5  2004/04/08 17:27:53  jms
* solaris porting fixes
*
* Revision 1.4  2003/08/12 16:45:26  jms
* linux porting changes
*
* Revision 1.3  2003/08/08 21:35:26  jms
* first integration of rng64 for o_custkey and l_partkey
*
* Revision 1.2  2003/08/07 17:58:34  jms
* Convery RNG to 64bit space as preparation for new large scale RNG
*
* Revision 1.1.1.1  2003/04/03 18:54:21  jms
* initial checkin
*
*
*/
/* 
 * this file allows the compilation of DBGEN to be tailored to specific
 * architectures and operating systems. Some options are grouped 
 * together to allow easier compilation on a given vendor's hardware.
 * 
 * The following #defines will effect the code:
 *   KILL(pid)         -- how to terminate a process in a parallel load
 *   SPAWN             -- name of system call to clone an existing process
 *   SET_HANDLER(proc) -- name of routine to handle signals in parallel load
 *   WAIT(res, pid)    -- how to await the termination of a child
 *   SEPARATOR         -- character used to separate fields in flat files
 *   STDLIB_HAS_GETOPT -- to prevent confilcts with gloabal getopt() 
 *   MDY_DATE          -- generate dates as MM-DD-YY
 *   WIN32             -- support for WindowsNT
 *   SUPPORT_64BITS    -- compiler defines a 64 bit datatype
 *   DSS_HUGE          -- 64 bit data type
 *   HUGE_FORMAT       -- printf string for 64 bit data type
 *   EOL_HANDLING      -- flat files don't need final column separator
 *
 * Certain defines must be provided in the makefile:
 *   MACHINE defines
 *   ==========
 *   ATT        -- getopt() handling
 *   DOS        -- disable all multi-user functionality/dependency
 *   HP         -- posix source inclusion differences
 *   IBM        -- posix source inclusion differences
 *   SGI        -- getopt() handling
 *   SUN        -- getopt() handling
 *   LINUX      
 *   WIN32      -- for WINDOWS
 *
 *   DATABASE defines
 *   ================
 *   DB2        -- use DB2 dialect in QGEN
 *   INFORMIX   -- use Informix dialect in QGEN
 *   SQLSERVER  -- use SQLSERVER dialect in QGEN
 *   SYBASE     -- use Sybase dialect in QGEN
 *   TDAT       -- use Teradata dialect in QGEN
 *
 *   WORKLOAD defines
 *   ================
 *   TPCH              -- make will create TPCH (set in makefile)
 */

#ifdef DOS
#define PATH_SEP	'\\'
#else


#ifdef ATT
#define STDLIB_HAS_GETOPT
#ifdef SQLSERVER
#define WIN32
#else
/* the 64 bit defines are for the Metaware compiler */
#define SUPPORT_64BITS
#define DSS_HUGE long long
#define RNG_A	6364136223846793005ull
#define RNG_C	1ull
#define HUGE_FORMAT "%LLd"
#define HUGE_DATE_FORMAT "%02LLd"
#endif /* SQLSERVER or MP/RAS */
#endif /* ATT */

#ifdef HP
#define _INCLUDE_POSIX_SOURCE
#define STDLIB_HAS_GETOPT
#define SUPPORT_64BITS
#define DSS_HUGE long
#define HUGE_COUNT 2
#define HUGE_FORMAT "%ld"
#define HUGE_DATE_FORMAT "%02lld"
#define RNG_C 1ull
#define RNG_A 6364136223846793005ull
#endif /* HP */

#ifdef IBM
#define STDLIB_HAS_GETOPT
#define SUPPORT_64BITS
#define DSS_HUGE long long
#define HUGE_FORMAT	"%lld" 
#define HUGE_DATE_FORMAT	"%02lld" 
#define RNG_A	6364136223846793005ull
#define RNG_C	1ull
#endif /* IBM */

#ifdef LINUX
#define STDLIB_HAS_GETOPT
#define SUPPORT_64BITS
#define DSS_HUGE long long int
#define HUGE_FORMAT	"%lld" 
#define HUGE_DATE_FORMAT	"%02lld" 
#define RNG_A	6364136223846793005ull
#define RNG_C	1ull
#endif /* LINUX */

#ifdef MAC
#define _POSIX_C_SOURCE 200112L
#define _POSIX_SOURCE
#define STDLIB_HAS_GETOPT
#define SUPPORT_64BITS
#define DSS_HUGE long
#define HUGE_FORMAT	"%ld"
#define HUGE_DATE_FORMAT	"%02ld"
#define RNG_A	6364136223846793005ull
#define RNG_C	1ull
#endif /* MAC */

#ifdef SUN
#define STDLIB_HAS_GETOPT
#define RNG_A	6364136223846793005ull
#define RNG_C	1ull
#define SUPPORT_64BITS
#define DSS_HUGE long long
#define HUGE_FORMAT	"%lld" 
#define HUGE_DATE_FORMAT	"%02lld" 
#endif /* SUN */

#ifdef SGI
#define STDLIB_HAS_GETOPT
#define SUPPORT_64BITS
#define DSS_HUGE __int64_t
#endif /* SGI */

#if (defined(WIN32)&&!defined(_POSIX_))
#define pid_t int
#define SET_HANDLER(proc) signal(SIGINT, proc)
#define KILL(pid) \
     TerminateProcess(OpenProcess(PROCESS_TERMINATE,FALSE,pid),3)
#if (defined (__WATCOMC__))
#define SPAWN()   spawnv(P_NOWAIT, spawn_args[0], spawn_args)
#define WAIT(res, pid) cwait(res, pid, WAIT_CHILD)
#else
#define SPAWN()   _spawnv(_P_NOWAIT, spawn_args[0], spawn_args)
#define WAIT(res, pid) _cwait(res, pid, _WAIT_CHILD)
#define getpid          _getpid
#endif /* WATCOMC */
#define SIGS_DEFINED
#define PATH_SEP	'\\'
#define SUPPORT_64BITS
#define DSS_HUGE __int64
#define RNG_A	6364136223846793005uI64
#define RNG_C	1uI64
#define HUGE_FORMAT "%I64d"
#define HUGE_DATE_FORMAT "%02I64d"
/* need to define process termination codes to match UNIX */
/* these are copied from Linux/GNU and need to be verified as part of a rework of */
/* process handling under NT (29 Apr 98) */
#define WIFEXITED(s)	((s & 0xFF) == 0)
#define WIFSIGNALED(s)	(((unsigned int)((status)-1) & 0xFFFF) < 0xFF)	
#define WIFSTOPPED(s)	(((s) & 0xff) == 0x7f)
#define WTERMSIG(s)		((s) & 0x7f)
#define WSTOPSIG(s)		(((s) & 0xff00) >> 8)
/* requried by move to Visual Studio 2005 */
#define strdup(x) _strdup(x)
#endif /* WIN32 */

#ifndef SIGS_DEFINED
#define KILL(pid) kill(SIGUSR1, pid)
#define SET_HANDLER(proc) signal(SIGUSR1, proc)
#define SPAWN   fork
#define WAIT(res, pid) wait(res)
#endif /* DEFAULT */

#endif /* DOS */

#ifndef PATH_SEP
#define PATH_SEP '/'
#endif /* PATH_SEP */

#ifndef DSS_HUGE
#error Support for a 64-bit datatype is required in this release
#endif

#ifndef DOUBLE_CAST
#define DOUBLE_CAST (double)
#endif /* DOUBLE_CAST */

