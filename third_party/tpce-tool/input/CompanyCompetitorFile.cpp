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

#include "input/CompanyCompetitorFile.h"
#include "utilities/MiscConsts.h"

using namespace TPCE;

/*
 *  Constructor.
 *
 *  PARAMETERS:
 *       IN  dataFile                    - CompanyCompetitorDataFile
 *       IN  iConfiguredCustomerCount    - total configured number of customers
 * in the database IN  iActiveCustomerCount        - active number of customers
 * in the database (provided for engineering purposes)
 *
 *  RETURNS:
 *       not applicable.
 */
CCompanyCompetitorFile::CCompanyCompetitorFile(const CompanyCompetitorDataFile_t &dataFile,
                                               TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
                                               UINT baseCompanyCount)
    : m_dataFile(&dataFile),
      m_iConfiguredCompanyCompetitorCount(CalculateCompanyCompetitorCount(iConfiguredCustomerCount)),
      m_iActiveCompanyCompetitorCount(CalculateCompanyCompetitorCount(iActiveCustomerCount)),
      m_iBaseCompanyCount(baseCompanyCount) {
}

/*
 *  Calculate company competitor count for the specified number of customers.
 *  Sort of a static method. Used in parallel generation of company related
 * tables.
 *
 *  PARAMETERS:
 *       IN  iCustomerCount          - number of customers
 *
 *  RETURNS:
 *       number of company competitors.
 */
TIdent CCompanyCompetitorFile::CalculateCompanyCompetitorCount(TIdent iCustomerCount) const {
	return iCustomerCount / iDefaultLoadUnitSize * iOneLoadUnitCompanyCompetitorCount;
}

/*
 *  Calculate the first company competitor id (0-based) for the specified
 * customer id.
 *
 *  PARAMETERS:
 *       IN  iStartFromCustomer      - customer id
 *
 *  RETURNS:
 *       company competitor id.
 */
TIdent CCompanyCompetitorFile::CalculateStartFromCompanyCompetitor(TIdent iStartFromCustomer) const {
	return iStartFromCustomer / iDefaultLoadUnitSize * iOneLoadUnitCompanyCompetitorCount;
}

/*
 *  Return company id for the specified row.
 *  Index can exceed the size of the Company Competitor input file.
 *
 *  PARAMETERS:
 *       IN  iIndex      - row number in the Company Competitor file (0-based)
 *
 *  RETURNS:
 *       company id.
 */
TIdent CCompanyCompetitorFile::GetCompanyId(TIdent iIndex) const {
	// Index wraps around every 15000 companies.
	//
	return (*m_dataFile)[(int)(iIndex % m_dataFile->size())].CP_CO_ID() + iTIdentShift +
	       iIndex / m_dataFile->size() * m_iBaseCompanyCount;
}

/*
 *  Return company competitor id for the specified row.
 *  Index can exceed the size of the Company Competitor input file.
 *
 *  PARAMETERS:
 *       IN  iIndex      - row number in the Company Competitor file (0-based)
 *
 *  RETURNS:
 *       company competitor id.
 */
TIdent CCompanyCompetitorFile::GetCompanyCompetitorId(TIdent iIndex) const {
	// Index wraps around every 5000 companies.
	//
	return (*m_dataFile)[(int)(iIndex % m_dataFile->size())].CP_COMP_CO_ID() + iTIdentShift +
	       iIndex / m_dataFile->size() * m_iBaseCompanyCount;
}

/*
 *  Return industry id for the specified row.
 *  Index can exceed the size of the Company Competitor input file.
 *
 *  PARAMETERS:
 *       IN  iIndex      - row number in the Company Competitor file (0-based)
 *
 *  RETURNS:
 *       industry id.
 */
const std::string &CCompanyCompetitorFile::GetIndustryId(TIdent iIndex) const {
	// Index wraps around every 5000 companies.
	//
	return (*m_dataFile)[(int)(iIndex % m_dataFile->size())].CP_IN_ID();
}

const char *CCompanyCompetitorFile::GetIndustryIdCSTR(TIdent iIndex) const {
	// Index wraps around every 5000 companies.
	//
	return (*m_dataFile)[(int)(iIndex % m_dataFile->size())].CP_IN_ID_CSTR();
}

/*
 *  Return the number of company competitors in the database for
 *  the configured number of customers.
 *
 *  PARAMETERS:
 *       none.
 *
 *  RETURNS:
 *       configured company competitor count.
 */
TIdent CCompanyCompetitorFile::GetConfiguredCompanyCompetitorCount() const {
	return m_iConfiguredCompanyCompetitorCount;
}

/*
 *  Return the number of company competitors in the database for
 *  the active number of customers.
 *
 *  PARAMETERS:
 *       none.
 *
 *  RETURNS:
 *       active company competitor count.
 */
TIdent CCompanyCompetitorFile::GetActiveCompanyCompetitorCount() const {
	return m_iActiveCompanyCompetitorCount;
}

/*
 *  Overload GetRecord to wrap around indices that
 *  are larger than the flat file
 *
 *  PARAMETERS:
 *       IN  iIndex      - row number in the Company Competitor file (0-based)
 *
 *  RETURNS:
 *       reference to the row structure in the Company Competitor file.
 */
const CompanyCompetitorDataFileRecord &CCompanyCompetitorFile::GetRecord(TIdent index) const {
	return (*m_dataFile)[(int)(index % m_dataFile->size())];
}
