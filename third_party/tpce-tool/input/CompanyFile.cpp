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

#include "input/CompanyFile.h"

#include <cstdio>
#include <cstring>

#include "utilities/MiscConsts.h"

using namespace TPCE;

/*
 *  Constructor.
 *
 *  PARAMETERS:
 *       IN  str                         - file name of the CompanyCompetitor
 * input flat file IN  iConfiguredCustomerCount    - total configured number of
 * customers in the database IN  iActiveCustomerCount        - active number of
 * customers in the database (provided for engineering purposes)
 *
 *  RETURNS:
 *       not applicable.
 */
CCompanyFile::CCompanyFile(const CompanyDataFile_t &dataFile, TIdent iConfiguredCustomerCount,
                           TIdent iActiveCustomerCount)
    : m_dataFile(&dataFile), m_iConfiguredCompanyCount(CalculateCompanyCount(iConfiguredCustomerCount)),
      m_iActiveCompanyCount(CalculateCompanyCount(iActiveCustomerCount)) {
}

/*
 *  Calculate company count for the specified number of customers.
 *  Sort of a static method. Used in parallel generation of company related
 * tables.
 *
 *  PARAMETERS:
 *       IN  iCustomerCount          - number of customers
 *
 *  RETURNS:
 *       number of company competitors.
 */
TIdent CCompanyFile::CalculateCompanyCount(TIdent iCustomerCount) const {
	return iCustomerCount / iDefaultLoadUnitSize * iOneLoadUnitCompanyCount;
}

/*
 *  Calculate the first company id (0-based) for the specified customer id.
 *
 *  PARAMETERS:
 *       IN  iStartFromCustomer      - customer id
 *
 *  RETURNS:
 *       company competitor id.
 */
TIdent CCompanyFile::CalculateStartFromCompany(TIdent iStartFromCustomer) const {
	return iStartFromCustomer / iDefaultLoadUnitSize * iOneLoadUnitCompanyCount;
}

/*
 *  Create company name with appended suffix based on the
 *  load unit number.
 *
 *  PARAMETERS:
 *       IN  iIndex      - row number in the Company Competitor file (0-based)
 *       IN  szOutput    - output buffer for company name
 *       IN  iOutputLen  - size of the output buffer
 *
 *  RETURNS:
 *       none.
 */
void CCompanyFile::CreateName(TIdent iIndex,     // row number
                              char *szOutput,    // output buffer
                              size_t iOutputLen) // size of the output buffer
    const {
	TIdent iFileIndex = iIndex % m_dataFile->size();
	TIdent iAdd = iIndex / m_dataFile->size();

	if (iAdd > 0) {
		snprintf(szOutput, iOutputLen, "%s #%" PRId64, GetRecord(iFileIndex).CO_NAME_CSTR(), iAdd);
	} else {
		strncpy(szOutput, GetRecord(iFileIndex).CO_NAME_CSTR(), iOutputLen);
	}
}

/*
 *  Return company id for the specified row.
 *  Index can exceed the size of the Company input file.
 *
 *  PARAMETERS:
 *       IN  iIndex      - row number in the Company Competitor file (0-based)
 *
 *  RETURNS:
 *       company id.
 */
TIdent CCompanyFile::GetCompanyId(TIdent iIndex) const {
	// Index wraps around every 5000 companies.
	//
	return (*m_dataFile)[(int)(iIndex % m_dataFile->size())].CO_ID() + iTIdentShift +
	       iIndex / m_dataFile->size() * m_dataFile->size();
}

/*
 *  Return the number of companies in the database for
 *  the configured number of customers.
 *
 *  PARAMETERS:
 *       none.
 *
 *  RETURNS:
 *       number of rows in the file.
 */
TIdent CCompanyFile::GetSize() const {
	return m_iConfiguredCompanyCount;
}

/*
 *  Return the number of companies in the database for
 *  the configured number of customers.
 *
 *  PARAMETERS:
 *       none.
 *
 *  RETURNS:
 *       configured company count.
 */
TIdent CCompanyFile::GetConfiguredCompanyCount() const {
	return m_iConfiguredCompanyCount;
}

/*
 *  Return the number of companies in the database for
 *  the active number of customers.
 *
 *  PARAMETERS:
 *       none.
 *
 *  RETURNS:
 *       active company count.
 */
TIdent CCompanyFile::GetActiveCompanyCount() const {
	return m_iActiveCompanyCount;
}

/*
 *  Overload GetRecord to wrap around indices that
 *  are larger than the flat file
 *
 *  PARAMETERS:
 *       IN  iIndex      - row number in the Company file (0-based)
 *
 *  RETURNS:
 *       reference to the row structure in the Company file.
 */
const CompanyDataFileRecord &CCompanyFile::GetRecord(TIdent index) const {
	return (*m_dataFile)[(int)(index % m_dataFile->size())];
}
