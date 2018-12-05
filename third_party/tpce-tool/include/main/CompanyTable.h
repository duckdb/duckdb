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
 *   Class representing the Company table.
 */
#ifndef COMPANY_TABLE_H
#define COMPANY_TABLE_H

#include "EGenTables_common.h"

#include "input/DataFileManager.h"

namespace TPCE {

const int iCEOMult = 1000; // for generating CEO name

// Number of RNG calls to skip for one row in order
// to not use any of the random values from the previous row.
//
const int iRNGSkipOneRowCompany = 2; // one for SP rate and one for CO_OPEN_DATE

class CCompanyTable : public TableTemplate<COMPANY_ROW> {
	const CCompanyFile &m_CompanyFile;
	const CompanySPRateDataFile_t &m_CompanySPRateFile;
	CPerson m_person; // for CEO
	CDateTime m_date;
	TIdent m_iCO_AD_ID_START; // starting address id for companies
	int m_iJan1_1800_DayNo;
	int m_iJan2_2000_DayNo;
	int m_iCurrentDayNo;
	TIdent m_iCompanyCountForOneLoadUnit;
	TIdent m_iCompanyCount;
	TIdent m_iStartFromCompany;

	/*
	 *  Generate distribution for the company inside S&P Rating file.
	 *
	 *  PARAMETERS:
	 *       none.
	 *
	 *  RETURNS:
	 *       threshold in the set of S&P credit ratings.
	 */
	int GetCompanySPRateThreshold() {
		RNGSEED OldSeed;
		int iCompanySPRateThreshold;

		OldSeed = m_rnd.GetSeed();
		m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseSPRate, (RNGSEED)m_row.CO_ID));
		iCompanySPRateThreshold = m_rnd.RndIntRange(0, m_CompanySPRateFile.size() - 1);
		m_rnd.SetSeed(OldSeed);
		return (iCompanySPRateThreshold);
	}

	/*
	 *  S&P Credit Rating for the current company.
	 *  It is stored in the current record structure.
	 *
	 *  PARAMETERS:
	 *       none.
	 *
	 *  RETURNS:
	 *       none.
	 */
	void GenerateCompanySPRate(void) {
		int iThreshold = GetCompanySPRateThreshold();

		// Select the row in the input file
		strncpy(m_row.CO_SP_RATE, m_CompanySPRateFile[iThreshold].CO_SP_RATE_CSTR(), sizeof(m_row.CO_SP_RATE));
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
		m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault, (RNGSEED)m_iLastRowNumber * iRNGSkipOneRowCompany));

		ClearRecord(); // this is needed for EGenTest to work
	}

public:
	/*
	 *  Constructor.
	 *
	 *  PARAMETERS:
	 *       IN  inputFiles              - input flat files loaded in memory
	 *       IN  iCustomerCount          - number of customers to generate
	 *       IN  iStartFromCustomer      - ordinal position of the first
	 * customer in the sequence (Note: 1-based)
	 *
	 *  RETURNS:
	 *       not applicable.
	 */
	CCompanyTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer)
	    : TableTemplate<COMPANY_ROW>(), m_CompanyFile(dfm.CompanyFile()),
	      m_CompanySPRateFile(dfm.CompanySPRateDataFile()), m_person(dfm, 0, false) {
		m_iJan1_1800_DayNo = CDateTime::YMDtoDayno(1800, 1, 1); // days number for Jan 1, 1800
		m_iJan2_2000_DayNo = CDateTime::YMDtoDayno(2000, 1, 2); // days number for Jan 2, 2000
		m_iCurrentDayNo = m_date.DayNo();                       // today's days number

		m_iCompanyCountForOneLoadUnit = m_CompanyFile.CalculateCompanyCount(iDefaultLoadUnitSize);

		m_iCompanyCount = m_CompanyFile.CalculateCompanyCount(iCustomerCount);
		m_iStartFromCompany = m_CompanyFile.CalculateStartFromCompany(iStartFromCustomer);

		m_iLastRowNumber = m_iStartFromCompany;
		// Start Company addresses immediately after Exchange addresses,
		// and company addresses for prior companies
		m_iCO_AD_ID_START = dfm.ExchangeDataFile().size() + m_iStartFromCompany + iTIdentShift;
	};

	/*
	 *  Generate and store state information for the next CO_ID.
	 *  The number of rows generated is incremented. This is why
	 *  this function cannot be called more than once for a record.
	 *
	 *  PARAMETERS:
	 *       none.
	 *
	 *  RETURNS:
	 *       TRUE, if there are more company ids to generate; FALSE, otherwise.
	 */
	bool GenerateNextCO_ID() {
		++m_iLastRowNumber;
		m_bMoreRecords = m_iLastRowNumber < (m_iStartFromCompany + m_iCompanyCount);

		return (MoreRecords());
	}

	/*
	 *  Return the current CO_ID. Since it doesn't generate the next CO_ID,
	 *  this function can be called many times for the same record.
	 *
	 *  PARAMETERS:
	 *       none.
	 *
	 *  RETURNS:
	 *       CO_ID in the current record.
	 */
	TIdent GetCurrentCO_ID() {
		return (m_CompanyFile.GetCompanyId(m_iLastRowNumber));
	}

	/*
	 *   Generate all column values for the next row
	 *   and store them in the internal record structure.
	 *   Increment the number of rows generated.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           TRUE, if there are more records in the ADDRESS table; FALSE
	 * othewise.
	 */
	bool GenerateNextRecord() {
		int iFoundDayNo;

		// Reset RNG at Load Unit boundary, so that all data is repeatable.
		//
		if (m_iLastRowNumber % m_iCompanyCountForOneLoadUnit == 0) {
			InitNextLoadUnit();
		}

		m_row.CO_ID = GetCurrentCO_ID();

		strncpy(m_row.CO_ST_ID, m_CompanyFile.GetRecord(m_iLastRowNumber).CO_ST_ID_CSTR(), sizeof(m_row.CO_ST_ID));

		m_CompanyFile.CreateName(m_iLastRowNumber, m_row.CO_NAME, static_cast<int>(sizeof(m_row.CO_NAME)));

		strncpy(m_row.CO_IN_ID, m_CompanyFile.GetRecord(m_iLastRowNumber).CO_IN_ID_CSTR(), sizeof(m_row.CO_IN_ID));

		GenerateCompanySPRate();

		snprintf(m_row.CO_CEO, sizeof(m_row.CO_CEO), "%s %s", m_person.GetFirstName(iCEOMult * m_row.CO_ID).c_str(),
		         m_person.GetLastName(iCEOMult * m_row.CO_ID).c_str());

		strncpy(m_row.CO_DESC, m_CompanyFile.GetRecord(m_iLastRowNumber).CO_DESC_CSTR(), sizeof(m_row.CO_DESC));

		m_row.CO_AD_ID = ++m_iCO_AD_ID_START;

		iFoundDayNo = m_rnd.RndIntRange(m_iJan1_1800_DayNo, m_iJan2_2000_DayNo);

		m_row.CO_OPEN_DATE.Set(iFoundDayNo);

		// Update state info
		return GenerateNextCO_ID();
	}
};

} // namespace TPCE

#endif // COMPANY_TABLE_H
