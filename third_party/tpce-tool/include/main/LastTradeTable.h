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
 * - Doug Johnson
 */

/*
 *   Class representing the LAST_TRADE table.
 */
#ifndef LAST_TRADE_TABLE_H
#define LAST_TRADE_TABLE_H

#include "EGenTables_common.h"
#include "MEESecurity.h"

#include "input/DataFileManager.h"

namespace TPCE {

class CLastTradeTable : public TableTemplate<LAST_TRADE_ROW> {
	TIdent m_iSecurityCount;
	TIdent m_iStartFromSecurity;
	const CSecurityFile &m_SecurityFile;
	CDateTime m_date;
	CMEESecurity m_MEESecurity;
	int m_iHoursOfInitialTrades;
	TIdent m_iSecurityCountForOneLoadUnit;

	/*
	 *   LAST_TRADE table row generation
	 */
	void GenerateLastTradeRow() {
		m_SecurityFile.CreateSymbol(m_iLastRowNumber, m_row.LT_S_SYMB, static_cast<int>(sizeof(m_row.LT_S_SYMB)));

		m_row.LT_DTS = m_date;

		m_MEESecurity.Init(m_iHoursOfInitialTrades * SecondsPerHour, NULL, NULL, 0);

		m_row.LT_PRICE = m_MEESecurity.CalculatePrice(m_iLastRowNumber, 0).DollarAmount();

		m_row.LT_OPEN_PRICE = m_MEESecurity.CalculatePrice(m_iLastRowNumber, 0).DollarAmount();

		// LT_VOL tracks the trading volume for the current day. Initial
		// population ends on a day boundary, so set LT_VOL to 0 for the start
		// of the next day.
		m_row.LT_VOL = 0;
	}

	/*
	 *   Reset the state for the next load unit.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           none.
	 */
	void InitNextLoadUnit() {
		//  No RNG calls in this class, so don't need to reset the RNG.

		ClearRecord(); // this is needed for EGenTest to work
	}

public:
	/*
	 *   Constructor.
	 */
	CLastTradeTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer,
	                INT32 iHoursOfInitialTrades)
	    : TableTemplate<LAST_TRADE_ROW>(), m_SecurityFile(dfm.SecurityFile()),
	      m_iHoursOfInitialTrades(iHoursOfInitialTrades) {
		m_iSecurityCount = m_SecurityFile.CalculateSecurityCount(iCustomerCount);
		m_iStartFromSecurity = m_SecurityFile.CalculateStartFromSecurity(iStartFromCustomer);

		m_iLastRowNumber = m_iStartFromSecurity;

		//  Go to the last day of initial trades.
		//
		m_date.Set(InitialTradePopulationBaseYear, InitialTradePopulationBaseMonth, InitialTradePopulationBaseDay,
		           InitialTradePopulationBaseHour, InitialTradePopulationBaseMinute, InitialTradePopulationBaseSecond,
		           InitialTradePopulationBaseFraction);
		m_date.Add(m_iHoursOfInitialTrades / HoursPerWorkDay, 0, true);

		m_iSecurityCountForOneLoadUnit = m_SecurityFile.CalculateSecurityCount(iDefaultLoadUnitSize);
	};

	bool GenerateNextRecord() {
		if (m_iLastRowNumber % m_iSecurityCountForOneLoadUnit == 0) {
			InitNextLoadUnit();
		}

		GenerateLastTradeRow();

		++m_iLastRowNumber;

		// Update state info
		m_bMoreRecords = m_iLastRowNumber < (m_iStartFromSecurity + m_iSecurityCount);

		return (MoreRecords());
	}
};

} // namespace TPCE

#endif // LAST_TRADE_TABLE_H
