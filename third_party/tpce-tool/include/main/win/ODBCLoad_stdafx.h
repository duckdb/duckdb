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

// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#ifndef ODBCLOAD_STDAFX_H
#define ODBCLOAD_STDAFX_H

#define WIN32_LEAN_AND_MEAN // Exclude rarely-used stuff from Windows headers

#include <windows.h>

// ODBC headers
#include <sql.h>
#include <sqlext.h>
#include <odbcss.h>

#include "../utilities/EGenUtilities_stdafx.h"
#include "../TableRows.h"
#include "../EGenBaseLoader_stdafx.h"
#include "error.h"
#include "DBLoader.h"
#include "ODBCAccountPermissionLoad.h"
#include "ODBCAddressLoad.h"
#include "ODBCBrokerLoad.h"
#include "ODBCCashTransactionLoad.h"
#include "ODBCChargeLoad.h"
#include "ODBCCommissionRateLoad.h"
#include "ODBCCompanyLoad.h"
#include "ODBCCompanyCompetitorLoad.h"
#include "ODBCCustomerLoad.h"
#include "ODBCCustomerAccountLoad.h"
#include "ODBCCustomerTaxrateLoad.h"
#include "ODBCDailyMarketLoad.h"
#include "ODBCExchangeLoad.h"
#include "ODBCFinancialLoad.h"
#include "ODBCHoldingLoad.h"
#include "ODBCHoldingHistoryLoad.h"
#include "ODBCHoldingSummaryLoad.h"
#include "ODBCIndustryLoad.h"
#include "ODBCLastTradeLoad.h"
#include "ODBCNewsItemLoad.h"
#include "ODBCNewsXRefLoad.h"
#include "ODBCSectorLoad.h"
#include "ODBCSecurityLoad.h"
#include "ODBCSettlementLoad.h"
#include "ODBCStatusTypeLoad.h"
#include "ODBCTaxrateLoad.h"
#include "ODBCTradeLoad.h"
#include "ODBCTradeHistoryLoad.h"
#include "ODBCTradeRequestLoad.h"
#include "ODBCTradeTypeLoad.h"
#include "ODBCWatchItemLoad.h"
#include "ODBCWatchListLoad.h"
#include "ODBCZipCodeLoad.h"
#include "ODBCLoaderFactory.h"

#endif // #ifndef ODBCLOAD_STDAFX_H
