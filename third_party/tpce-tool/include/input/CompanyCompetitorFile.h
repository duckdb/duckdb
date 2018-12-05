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

/******************************************************************************
 *   Description:        Implementation of the Company Competitor input file
 *                       that scales with the database size.
 ******************************************************************************/

#ifndef COMPANY_COMPETITOR_FILE_H
#define COMPANY_COMPETITOR_FILE_H

#include <string>
#include "utilities/EGenStandardTypes.h"
#include "DataFileTypes.h"

namespace TPCE {

class CCompanyCompetitorFile {
	CompanyCompetitorDataFile_t const *m_dataFile;

	// Configured and active number of companies in the database.
	// Depends on the configured and active number of customers.
	//
	TIdent m_iConfiguredCompanyCompetitorCount;
	TIdent m_iActiveCompanyCompetitorCount;

	// Number of base companies (=rows in Company.txt input file).
	//
	UINT m_iBaseCompanyCount;

public:
	/*
	 *  Constructor.
	 *
	 *  PARAMETERS:
	 *       IN  dataFile                    - CompanyCompetitorDataFile
	 *       IN  iConfiguredCustomerCount    - total configured number of
	 * customers in the database IN  iActiveCustomerCount        - active number
	 * of customers in the database (provided for engineering purposes)
	 *
	 *  RETURNS:
	 *       not applicable.
	 */
	CCompanyCompetitorFile(const CompanyCompetitorDataFile_t &dataFile, TIdent iConfiguredCustomerCount,
	                       TIdent iActiveCustomerCount, UINT baseCompanyCount);

	/*
	 *  Calculate company competitor count for the specified number of
	 * customers. Sort of a static method. Used in parallel generation of
	 * company related tables.
	 *
	 *  PARAMETERS:
	 *       IN  iCustomerCount          - number of customers
	 *
	 *  RETURNS:
	 *       number of company competitors.
	 */
	TIdent CalculateCompanyCompetitorCount(TIdent iCustomerCount) const;

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
	TIdent CalculateStartFromCompanyCompetitor(TIdent iStartFromCustomer) const;

	/*
	 *  Return company id for the specified row.
	 *  Index can exceed the size of the Company Competitor input file.
	 *
	 *  PARAMETERS:
	 *       IN  iIndex      - row number in the Company Competitor file
	 * (0-based)
	 *
	 *  RETURNS:
	 *       company id.
	 */
	TIdent GetCompanyId(TIdent iIndex) const;

	/*
	 *  Return company competitor id for the specified row.
	 *  Index can exceed the size of the Company Competitor input file.
	 *
	 *  PARAMETERS:
	 *       IN  iIndex      - row number in the Company Competitor file
	 * (0-based)
	 *
	 *  RETURNS:
	 *       company competitor id.
	 */
	TIdent GetCompanyCompetitorId(TIdent iIndex) const;

	/*
	 *  Return industry id for the specified row.
	 *  Index can exceed the size of the Company Competitor input file.
	 *
	 *  PARAMETERS:
	 *       IN  iIndex      - row number in the Company Competitor file
	 * (0-based)
	 *
	 *  RETURNS:
	 *       industry id.
	 */
	const std::string &GetIndustryId(TIdent iIndex) const;
	const char *GetIndustryIdCSTR(TIdent iIndex) const;

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
	TIdent GetConfiguredCompanyCompetitorCount() const;

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
	TIdent GetActiveCompanyCompetitorCount() const;

	/*
	 *  Overload GetRecord to wrap around indices that
	 *  are larger than the flat file
	 *
	 *  PARAMETERS:
	 *       IN  iIndex      - row number in the Company Competitor file
	 * (0-based)
	 *
	 *  RETURNS:
	 *       reference to the row structure in the Company Competitor file.
	 */
	const CompanyCompetitorDataFileRecord &GetRecord(TIdent index) const;
};

} // namespace TPCE

#endif // COMPANY_COMPETITOR_FILE_H
