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
 *   Class representing the DAILY_MARKET table.
 */
#ifndef DAILY_MARKET_TABLE_H
#define DAILY_MARKET_TABLE_H

#include "EGenTables_common.h"
#include "SecurityPriceRange.h"

#include "input/DataFileManager.h"

namespace TPCE {

const int iTradeDaysInYear = 261; // the number of trading days in a year (for DAILY_MARKET)
const int iDailyMarketYears = 5;  // number of years of history in DAILY_MARKET
const int iDailyMarketTotalRows = iDailyMarketYears * iTradeDaysInYear;

const double fDailyMarketCloseMin = fMinSecPrice;
const double fDailyMarketCloseMax = fMaxSecPrice;

const double fDailyMarketHighRelativeToClose = 1.05;
const double fDailyMarketLowRelativeToClose = 0.92;

const INT64 iDailyMarketVolumeMax = 10000;
const INT64 iDailyMarketVolumeMin = 1000;

const int iRNGSkipOneRowDailyMarket = 2; // number of RNG calls for one row

typedef struct DAILY_MARKET_GEN_ROW {
	// big array of all the history for one security
	DAILY_MARKET_ROW m_daily_market[iDailyMarketTotalRows];
} * PDAILY_MARKET_GEN_ROW;

class CDailyMarketTable : public TableTemplate<DAILY_MARKET_GEN_ROW> {
	TIdent m_iStartFromSecurity;
	TIdent m_iSecurityCount;
	const CSecurityFile &m_SecurityFile;
	CDateTime m_StartFromDate;
	int m_iDailyMarketTotalRows;
	// Number of times GenerateNextRecord() was called for the current security
	// data. Needed to decide when to generate next security's data.
	int m_iRowsGeneratedPerSecurity;
	// Stores whether there is another security(s) for which to
	// generate daily market data.
	bool m_bMoreSecurities;
	TIdent m_iSecurityCountForOneLoadUnit;

	/*
	 *   DAILY_MARKET table rows generation
	 */
	void GenerateDailyMarketRows() {
		int i;
		int iDayNo = 0; // start from the oldest date (start date)
		char szSymbol[cSYMBOL_len + 1];

		// Create symbol only once.
		//
		m_SecurityFile.CreateSymbol(m_iLastRowNumber, szSymbol, static_cast<int>(sizeof(szSymbol)));

		for (i = 0; i < m_iDailyMarketTotalRows; ++i) {
			// copy the symbol
			strncpy(m_row.m_daily_market[i].DM_S_SYMB, szSymbol, sizeof(m_row.m_daily_market[i].DM_S_SYMB));

			// generate trade date
			m_row.m_daily_market[i].DM_DATE = m_StartFromDate;
			m_row.m_daily_market[i].DM_DATE.Add(iDayNo, 0);

			// generate prices
			m_row.m_daily_market[i].DM_CLOSE =
			    m_rnd.RndDoubleIncrRange(fDailyMarketCloseMin, fDailyMarketCloseMax, 0.01);
			m_row.m_daily_market[i].DM_HIGH = m_row.m_daily_market[i].DM_CLOSE * fDailyMarketHighRelativeToClose;
			m_row.m_daily_market[i].DM_LOW = m_row.m_daily_market[i].DM_CLOSE * fDailyMarketLowRelativeToClose;

			// generate volume
			m_row.m_daily_market[i].DM_VOL = m_rnd.RndInt64Range(iDailyMarketVolumeMin, iDailyMarketVolumeMax);

			++iDayNo; // go one day forward for the next row

			if ((iDayNo % DaysPerWeek) == DaysPerWorkWeek)
				iDayNo += 2; // skip weekend
		}
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
		m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault, (RNGSEED)m_iLastRowNumber * iRNGSkipOneRowDailyMarket));

		ClearRecord(); // this is needed for EGenTest to work
	}

public:
	/*
	 *   Constructor.
	 */
	CDailyMarketTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer)
	    : TableTemplate<DAILY_MARKET_GEN_ROW>(), m_SecurityFile(dfm.SecurityFile()),
	      m_iDailyMarketTotalRows(sizeof(m_row.m_daily_market) / sizeof(m_row.m_daily_market[0])),
	      m_iRowsGeneratedPerSecurity(sizeof(m_row.m_daily_market) / sizeof(m_row.m_daily_market[0])),
	      m_bMoreSecurities(true) // initialize once
	{
		//  Set DAILY_MARKET start date to Jan 03, 2000.
		//
		m_StartFromDate.Set(iDailyMarketBaseYear, iDailyMarketBaseMonth, iDailyMarketBaseDay, iDailyMarketBaseHour,
		                    iDailyMarketBaseMinute, iDailyMarketBaseSecond, iDailyMarketBaseMsec);

		m_bMoreRecords = true; // initialize once

		m_iSecurityCount = m_SecurityFile.CalculateSecurityCount(iCustomerCount);
		m_iStartFromSecurity = m_SecurityFile.CalculateStartFromSecurity(iStartFromCustomer);

		m_iSecurityCountForOneLoadUnit = m_SecurityFile.CalculateSecurityCount(iDefaultLoadUnitSize);

		m_iLastRowNumber = m_iStartFromSecurity;
	};

	bool GenerateNextRecord() {
		++m_iRowsGeneratedPerSecurity;

		if (m_iRowsGeneratedPerSecurity >= m_iDailyMarketTotalRows) {
			if (m_bMoreSecurities) {
				// Reset RNG at Load Unit boundary, so that all data is
				// repeatable.
				//
				if (m_iLastRowNumber % m_iSecurityCountForOneLoadUnit == 0) {
					InitNextLoadUnit();
				}

				GenerateDailyMarketRows(); // generate all rows for the current
				                           // security

				++m_iLastRowNumber;

				// Update state info
				m_bMoreSecurities = m_iLastRowNumber < (m_iStartFromSecurity + m_iSecurityCount);

				m_iRowsGeneratedPerSecurity = 0;
			}
		}

		// Return false when generated the last row of the last security
		if (!m_bMoreSecurities && (m_iRowsGeneratedPerSecurity == m_iDailyMarketTotalRows - 1)) {
			m_bMoreRecords = false;
		}

		return (MoreRecords());
	}

	const DAILY_MARKET_ROW &GetRow() {
		return m_row.m_daily_market[m_iRowsGeneratedPerSecurity];
	}
};

} // namespace TPCE

#endif // DAILY_MARKET_TABLE_H
