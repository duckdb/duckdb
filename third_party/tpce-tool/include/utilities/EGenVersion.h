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
 * - Sergey Vasilevskiy
 */

/******************************************************************************
 *   Description:        Versioning information for the EGen package.
 *                       Updated on every release.
 ******************************************************************************/

#ifndef EGEN_VERSION_H
#define EGEN_VERSION_H

#include "EGenStandardTypes.h"

namespace TPCE {

extern "C" {
void GetEGenVersion_C(INT32 &iMajorVersion, INT32 &iMinorVersion, INT32 &iRevisionNumber, INT32 &iBetaLevel);
void GetEGenVersionString_C(char *szOutput, size_t iOutputBufferLen);
void PrintEGenVersion_C();
void GetEGenVersionUpdateTimestamp_C(char *szOutput, size_t iOutputBufferLen);
}

// Retrieve major, minor, revision, and beta level numbers for EGen.
// For example, v3.10 beta 1 has:
//  major       3
//  minor       10
//  revision    0
//  beta level  1
// v3.10 release has:
//  major       3
//  minor       10
//  revision    0
//  beta level  0
//
void GetEGenVersion(INT32 &iMajorVersion, INT32 &iMinorVersion, INT32 &iRevisionNumber, INT32 &iBetaLevel);

// Return versioning information formated as a string
//
// Note: requires output buffer at least 64 characters long, or nothing will be
// returned.
//
void GetEGenVersionString(char *szOutput, size_t iOutputBufferLen);

// Output EGen versioning information on stdout
//
void PrintEGenVersion();

// Return the date/time when the EGen versioning information was last updated.
//
void GetEGenVersionUpdateTimestamp(char *szOutput, size_t iOutputBufferLen);

} // namespace TPCE

#endif // #ifndef EGEN_VERSION_H
