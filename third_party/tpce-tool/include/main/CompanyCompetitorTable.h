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

/*
 *   Class representing the CompanyCompetitor table.
 */
#ifndef COMPANY_COMPETITOR_TABLE_H
#define COMPANY_COMPETITOR_TABLE_H

#include "EGenTables_common.h"

#include "input/DataFileManager.h"

namespace TPCE {

class CCompanyCompetitorTable : public TableTemplate<COMPANY_COMPETITOR_ROW> {
	const CCompanyCompetitorFile &m_CompanyCompetitorFile;
	TIdent m_iCompanyCompetitorCount;
	TIdent m_iStartFromCompanyCompetitor;
	TIdent m_iCompanyCompetitorCountForOneLoadUnit;

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
	CCompanyCompetitorTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer)
	    : TableTemplate<COMPANY_COMPETITOR_ROW>(), m_CompanyCompetitorFile(dfm.CompanyCompetitorFile()) {
		m_iCompanyCompetitorCount = m_CompanyCompetitorFile.CalculateCompanyCompetitorCount(iCustomerCount);
		m_iStartFromCompanyCompetitor = m_CompanyCompetitorFile.CalculateStartFromCompanyCompetitor(iStartFromCustomer);

		m_iLastRowNumber = m_iStartFromCompanyCompetitor;

		m_iCompanyCompetitorCountForOneLoadUnit =
		    m_CompanyCompetitorFile.CalculateCompanyCompetitorCount(iDefaultLoadUnitSize);
	};

	/*
	 *   Generates all column values for the next row.
	 */
	bool GenerateNextRecord() {
		if (m_iLastRowNumber % m_iCompanyCompetitorCountForOneLoadUnit == 0) {
			InitNextLoadUnit();
		}

		m_row.CP_CO_ID = m_CompanyCompetitorFile.GetCompanyId(m_iLastRowNumber);

		m_row.CP_COMP_CO_ID = m_CompanyCompetitorFile.GetCompanyCompetitorId(m_iLastRowNumber);

		strncpy(m_row.CP_IN_ID, m_CompanyCompetitorFile.GetIndustryIdCSTR(m_iLastRowNumber), sizeof(m_row.CP_IN_ID));

		++m_iLastRowNumber;

		m_bMoreRecords = m_iLastRowNumber < m_iStartFromCompanyCompetitor + m_iCompanyCompetitorCount;

		return (MoreRecords());
	}
};

} // namespace TPCE

#endif // COMPANY_COMPETITOR_TABLE_H
