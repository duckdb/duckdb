/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors:
 * Gradient Systems
 */

#ifndef CONFIG_H
#define CONFIG_H

//#ifdef MACOS
#define SUPPORT_64BITS
#define HUGE_TYPE int64_t
#define HUGE_FORMAT "%zu"
#define HUGE_COUNT 1
#define USE_STRING_H
#define USE_STDLIB_H
#define USE_LIMITS_H
#include <limits.h>
#define MAXINT INT_MAX

//#define FLEX
//#endif /* MACOS */

#ifdef NCR
#define STDLIB_HAS_GETOPT
#define USE_STRING_H
#define USE_VALUES_H
#ifdef SQLSERVER
#define WIN32
#else
/* the 64 bit defines are for the Metaware compiler */
#define SUPPORT_64BITS
#define HUGE_TYPE long long
#define HUGE_COUNT 1
#define HUGE_FORMAT "%LLd"
#define int32_t int
#endif /* SQLSERVER or MP/RAS */
#endif /* NCR */

#ifdef AIX
#define _ALL_SOURCE
#define USE_STRING_H
#define USE_LIMITS_H
/*
 * if the C compiler is 3.1 or later, then uncomment the
 * lines for 64 bit seed generation
 */
#define SUPPORT_64BITS
#define HUGE_TYPE long long
#define HUGE_COUNT 1
#define HUGE_FORMAT "%lld"
#define STDLIB_HAS_GETOPT
#define USE_STDLIB_H
#define FLEX
#endif /* AIX */

#ifdef CYGWIN
#define USE_STRING_H
#define PATH_SEP '\\'
#define SUPPORT_64BITS
#define HUGE_TYPE __int64
#define HUGE_COUNT 1
#define HUGE_FORMAT "%I64d"
#endif /* WIN32 */

#ifdef HPUX
#define SUPPORT_64BITS
#define HUGE_TYPE long long int
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#define USE_STRING_H
#define USE_VALUES_H
#define USE_STDLIB_H
#define FLEX
#endif /* HPUX */

#ifdef INTERIX
#define USE_LIMITS_H
#define SUPPORT_64BITS
#define HUGE_TYPE long long int
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#endif /* INTERIX */

#ifdef LINUX
#define SUPPORT_64BITS
#define HUGE_TYPE int64_t
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#define USE_STRING_H
#define USE_VALUES_H
#define USE_STDLIB_H
#define FLEX
#endif /* LINUX */

#ifdef SOLARIS
#define SUPPORT_64BITS
#define HUGE_TYPE long long
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#define USE_STRING_H
#define USE_VALUES_H
#define USE_STDLIB_H
#endif /* SOLARIS */

#ifdef SOL86
#define SUPPORT_64BITS
#define HUGE_TYPE long long
#define HUGE_FORMAT "%lld"
#define HUGE_COUNT 1
#define USE_STRING_H
#define USE_VALUES_H
#define USE_STDLIB_H
#endif /* SOLARIS */

#ifdef WIN32
#define USE_STRING_H
#define USE_LIMITS_H
#define PATH_SEP '\\'
#define SUPPORT_64BITS
#define HUGE_TYPE __int64
#define HUGE_COUNT 1
#define HUGE_FORMAT "%I64d"
#endif /* WIN32 */

/* preliminary defines for 64-bit windows compile */
#ifdef WIN64
#define USE_STRING_H
#define PATH_SEP '\\'
#define SUPPORT_64BITS
#define HUGE_TYPE __int64
#define HUGE_COUNT 1
#define HUGE_FORMAT "%I64d"
#endif /* WIN32 */

#ifndef PATH_SEP
#define PATH_SEP '/'
#endif /* PATH_SEP */

#ifndef HUGE_TYPE
#error The code now requires 64b support
#endif

/***
 ** DATABASE DEFINES
 ***/
#ifdef _MYSQL
#define STR_QUOTES
#endif
#endif /* CONFIG_H */
