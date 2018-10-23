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
 * Contributors
 * - Doug Johnson, Matt Emmerton, Larry Loen, Chris Chan-Nui
 */

/******************************************************************************
 *   Description:        This file contains mappings from platform specific
 *                       data types to platform indepenent data types used
 *                       throughout EGen.
 ******************************************************************************/

#ifndef EGEN_STANDARD_TYPES_H
#define EGEN_STANDARD_TYPES_H

////////////////////
// Standard types //
////////////////////

// Define unsigned type for convenience
typedef unsigned int UINT;

// This is a template that can be used for each
// platform type.
//
// #ifdef {platform flag}
//  // Mapping for {platform} data types.
//  typedef {platform type}     INT8,  *PINT8;
//  typedef {platform type}     INT16, *PINT16;
//  typedef {platform type}     INT32, *PINT32;
//  typedef {platform type}     INT64, *PINT64;
//
//  typedef {platform type}     UINT8,  *PUINT8;
//  typedef {platform type}     UINT16, *PUINT16;
//  typedef {platform type}     UINT32, *PUINT32;
//  typedef {platform type}     UINT64, *PUINT64;
// #endif
//

#define PRId64 "lld"

#include <cstdint>

typedef int8_t INT8, *PINT8;
typedef int16_t INT16, *PINT16;
typedef int32_t INT32, *PINT32;
typedef int64_t INT64, *PINT64;

typedef uint8_t UINT8, *PUINT8;
typedef uint16_t UINT16, *PUINT16;
typedef uint32_t UINT32, *PUINT32;
typedef uint64_t UINT64, *PUINT64;

/////////////////////////////////////////////
// 64-bit integer printf format specifier  //
/////////////////////////////////////////////
// Assume everyone else is a flavor of Unix, has __unix defined,
// and the 64-bit integer printf specifier is defined in <inttypes.h> as PRId64

/////////////////////////////////////////////
// integer constant suffixes               //
/////////////////////////////////////////////
#define INT64_CONST(x) INT64_C(x)
#define UINT64_CONST(x) UINT64_C(x)

/////////////////////////////////////////////
// mutex and thread types                  //
/////////////////////////////////////////////

#include <pthread.h>

typedef pthread_t TThread;
typedef pthread_attr_t TThreadAttr;
typedef pthread_mutex_t TMutex;

//////////////////////////////////////////////
// Database dependant indicator value types //
//////////////////////////////////////////////

#if defined(DB2)
//
// Mapping for DB2 data types.
typedef UINT16 DB_INDICATOR;
//
#elif defined(MSSQL)
//
// Mapping for MSSQL data types.
typedef long DB_INDICATOR;
//
#elif defined(ORACLE)
//
// Mapping for Oracle data types.
typedef sb2 DB_INDICATOR;
//
#else
//
// Arbitrary default just so we can compile
typedef INT32 DB_INDICATOR;
#endif // ORACLE

/////////////////////////////////////////////////////////
// Identifier type for all integer primary key fields. //
// Corresponds to IDENT_T metatype in TPC-E spec.      //
/////////////////////////////////////////////////////////
typedef INT64 TIdent;

/////////////////////////////////////////////////////////
// Identifier type for all trade id primary key fields.//
// Corresponds to TRADE_T metatype in TPC-E spec.      //
/////////////////////////////////////////////////////////
typedef INT64 TTrade;

#endif // #ifndef EGEN_STANDARD_TYPES_H
