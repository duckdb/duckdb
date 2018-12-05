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

/*
 *   Class representing the securities table.
 */
#ifndef SECURITY_TABLE_H
#define SECURITY_TABLE_H

#include <stdio.h> // for snprintf which is not part of the C++ headers
#include "EGenTables_common.h"
#include "SecurityPriceRange.h"

#include "input/DataFileManager.h"

namespace TPCE {

const INT64 iS_NUM_OUTMin = INT64_CONST(4000000);
const INT64 iS_NUM_OUTMax = INT64_CONST(9500000000);

const double fS_PEMin = 1.0;
const double fS_PEMax = 120.0;

const double fS_DIVIDNonZeroMin = 0.01;
const double fS_DIVIDMax = 10.0;

const double fS_YIELDNonZeroMin = 0.01;
const double fS_YIELDMax = 120.0;

const int iWeeksPerYear = 52;
const int iDaysPerWeek = 7;

const int iPercentCompaniesWithNonZeroDividend = 60;

const int iRNGSkipOneRowSecurity = 11; // number of RNG calls for one row

class CSecurityTable : public TableTemplate<SECURITY_ROW> {
	TIdent m_iSecurityCount;
	TIdent m_iStartFromSecurity;
	const CCompanyFile &m_CompanyFile;
	const CSecurityFile &m_SecurityFile;
	CDateTime m_date;
	int m_iCurrentDayNo;
	int m_iJan1_1900DayNo;
	int m_iJan2_2000DayNo;
	TIdent m_iSecurityCountForOneLoadUnit;

	/*
	 *   SECURITY table row generation
	 */
	void GenerateSecurityRow() {
		int iStartDayNo, iExchangeDayNo, i52HighDayNo, i52LowDayNo;

		strncpy(m_row.S_ST_ID, m_SecurityFile.GetRecord(m_iLastRowNumber).S_ST_ID_CSTR(), sizeof(m_row.S_ST_ID));
		strncpy(m_row.S_ISSUE, m_SecurityFile.GetRecord(m_iLastRowNumber).S_ISSUE_CSTR(), sizeof(m_row.S_ISSUE));

		CreateName(m_iLastRowNumber, m_row.S_NAME, static_cast<int>(sizeof(m_row.S_NAME)));

		m_SecurityFile.CreateSymbol(m_iLastRowNumber, m_row.S_SYMB, static_cast<int>(sizeof(m_row.S_SYMB)));
		strncpy(m_row.S_EX_ID, m_SecurityFile.GetRecord(m_iLastRowNumber).S_EX_ID_CSTR(), sizeof(m_row.S_EX_ID));
		m_row.S_CO_ID = m_SecurityFile.GetCompanyId(m_iLastRowNumber);
		m_row.S_NUM_OUT = m_rnd.RndInt64Range(iS_NUM_OUTMin, iS_NUM_OUTMax);

		iStartDayNo = m_rnd.RndIntRange(m_iJan1_1900DayNo, m_iJan2_2000DayNo); // generate random date
		m_row.S_START_DATE.Set(iStartDayNo);
		iExchangeDayNo = m_rnd.RndIntRange(iStartDayNo, m_iJan2_2000DayNo);
		m_row.S_EXCH_DATE.Set(iExchangeDayNo);
		m_row.S_PE = m_rnd.RndDoubleIncrRange(fS_PEMin, fS_PEMax, 0.01);
		// iExchangeDayNo contains S_EXCH_DATE date in days.

		// 52 week high  - selected from upper half of security price range
		m_row.S_52WK_HIGH =
		    (float)m_rnd.RndDoubleIncrRange(fMinSecPrice + ((fMaxSecPrice - fMinSecPrice) / 2), fMaxSecPrice, 0.01);
		i52HighDayNo = m_rnd.RndIntRange(m_iCurrentDayNo - iDaysPerWeek * iWeeksPerYear, m_iCurrentDayNo);
		m_row.S_52WK_HIGH_DATE.Set(i52HighDayNo);

		// 52 week low - selected from the minimum security price up to the 52wk
		// high
		m_row.S_52WK_LOW = (float)m_rnd.RndDoubleIncrRange(fMinSecPrice, m_row.S_52WK_HIGH, 0.01);
		i52LowDayNo = m_rnd.RndIntRange(m_iCurrentDayNo - iDaysPerWeek * iWeeksPerYear, m_iCurrentDayNo);
		m_row.S_52WK_LOW_DATE.Set(i52LowDayNo);

		if (m_rnd.RndPercent(iPercentCompaniesWithNonZeroDividend)) {
			m_row.S_YIELD = m_rnd.RndDoubleIncrRange(fS_YIELDNonZeroMin, fS_YIELDMax, 0.01);
			m_row.S_DIVIDEND = m_rnd.RndDoubleIncrRange((m_row.S_YIELD * 0.20), (m_row.S_YIELD * 0.30), 0.01);
		} else {
			m_row.S_DIVIDEND = 0.0;
			m_row.S_YIELD = 0.0;
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
		m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault, (RNGSEED)m_iLastRowNumber * iRNGSkipOneRowSecurity));

		ClearRecord(); // this is needed for EGenTest to work
	}

public:
	CSecurityTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer)
	    : TableTemplate<SECURITY_ROW>(), m_CompanyFile(dfm.CompanyFile()), m_SecurityFile(dfm.SecurityFile()) {
		m_iCurrentDayNo = CDateTime::YMDtoDayno(InitialTradePopulationBaseYear, InitialTradePopulationBaseMonth,
		                                        InitialTradePopulationBaseDay); // last initial trading date
		m_iJan1_1900DayNo = CDateTime::YMDtoDayno(1900, 1, 1); // find out number of days for Jan-1-1900
		m_iJan2_2000DayNo = CDateTime::YMDtoDayno(2000, 1, 2); // find out number of days for Jan-2-2000

		m_iSecurityCount = m_SecurityFile.CalculateSecurityCount(iCustomerCount);
		m_iStartFromSecurity = m_SecurityFile.CalculateStartFromSecurity(iStartFromCustomer);

		m_iSecurityCountForOneLoadUnit = m_SecurityFile.CalculateSecurityCount(iDefaultLoadUnitSize);

		m_iLastRowNumber = m_iStartFromSecurity;
	};

	bool GenerateNextRecord() {
		// Reset RNG at Load Unit boundary, so that all data is repeatable.
		//
		if (m_iLastRowNumber % m_iSecurityCountForOneLoadUnit == 0) {
			InitNextLoadUnit();
		}

		GenerateSecurityRow();

		++m_iLastRowNumber;

		// Update state info
		m_bMoreRecords = m_iLastRowNumber < (m_iStartFromSecurity + m_iSecurityCount);

		return (MoreRecords());
	}

	void CreateName(TIdent iIndex,     // row number
	                char *szOutput,    // output buffer
	                size_t iOutputLen) // size of the output buffer (including null)
	{
		char CompanyName[cCO_NAME_len + 1];

		m_CompanyFile.CreateName(m_SecurityFile.GetCompanyIndex(iIndex), CompanyName,
		                         static_cast<int>(sizeof(CompanyName)));
		snprintf(szOutput, iOutputLen, "%s of %s", m_SecurityFile.GetRecord(iIndex).S_ISSUE_CSTR(), CompanyName);
	}
};

} // namespace TPCE

#endif // SECURITY_TABLE_H
