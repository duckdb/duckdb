#ifndef TABLE_TYPES_H
#define TABLE_TYPES_H

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
 * - Doug Johnson
 */

#include "input/DataFileTypes.h"
#include "input/ITaxRateFileRecord.h"
#include "input/TaxRateFile.h"
#include "FixedTable.h"

namespace TPCE {
// Fixed tables - data file records and table records are the same.
//
typedef ChargeDataFileRecord ChargeTableRecord_t;
typedef FixedTable<ChargeDataFile_t, ChargeTableRecord_t> ChargeTable_t;

typedef CommissionRateDataFileRecord CommissionRateTableRecord_t;
typedef FixedTable<CommissionRateDataFile_t, CommissionRateTableRecord_t> CommissionRateTable_t;

typedef IndustryDataFileRecord IndustryTableRecord_t;
typedef FixedTable<IndustryDataFile_t, IndustryTableRecord_t> IndustryTable_t;

typedef SectorDataFileRecord SectorTableRecord_t;
typedef FixedTable<SectorDataFile_t, SectorTableRecord_t> SectorTable_t;

typedef StatusTypeDataFileRecord StatusTypeTableRecord_t;
typedef FixedTable<StatusTypeDataFile_t, StatusTypeTableRecord_t> StatusTypeTable_t;

typedef TradeTypeDataFileRecord TradeTypeTableRecord_t;
typedef FixedTable<TradeTypeDataFile_t, TradeTypeTableRecord_t> TradeTypeTable_t;

typedef ITaxRateFileRecord TaxRateTableRecord_t;
typedef FixedTable<CTaxRateFile, TaxRateTableRecord_t> TaxRateTable_t;

} // namespace TPCE
#endif // TABLE_TYPES_H
