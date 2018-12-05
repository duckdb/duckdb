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
 *   Description:        Implementation of the Company input file that scales
 *                       with the database size.
 ******************************************************************************/

#ifndef COMPANY_FILE_H
#define COMPANY_FILE_H

#include "utilities/EGenStandardTypes.h"
#include "DataFileTypes.h"

namespace TPCE {

class CCompanyFile {
	CompanyDataFile_t const *m_dataFile;

	// Configured and active number of companies in the database.
	// Depends on the configured and active number of customers.
	//
	TIdent m_iConfiguredCompanyCount;
	TIdent m_iActiveCompanyCount;

public:
	/*
	 *  Constructor.
	 *
	 *  PARAMETERS:
	 *       IN  str                         - file name of the
	 * CompanyCompetitor input flat file IN  iConfiguredCustomerCount    - total
	 * configured number of customers in the database IN  iActiveCustomerCount
	 * - active number of customers in the database (provided for engineering
	 * purposes)
	 *
	 *  RETURNS:
	 *       not applicable.
	 */
	CCompanyFile(const CompanyDataFile_t &dataFile, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount);

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
	TIdent CalculateCompanyCount(TIdent iCustomerCount) const;

	/*
	 *  Calculate the first company id (0-based) for the specified customer id.
	 *
	 *  PARAMETERS:
	 *       IN  iStartFromCustomer      - customer id
	 *
	 *  RETURNS:
	 *       company competitor id.
	 */
	TIdent CalculateStartFromCompany(TIdent iStartFromCustomer) const;

	/*
	 *  Create company name with appended suffix based on the
	 *  load unit number.
	 *
	 *  PARAMETERS:
	 *       IN  iIndex      - row number in the Company Competitor file
	 * (0-based) IN  szOutput    - output buffer for company name IN  iOutputLen
	 * - size of the output buffer
	 *
	 *  RETURNS:
	 *       none.
	 */
	void CreateName(TIdent iIndex,    // row number
	                char *szOutput,   // output buffer
	                size_t iOutputLen // size of the output buffer
	                ) const;

	/*
	 *  Return company id for the specified row.
	 *  Index can exceed the size of the Company input file.
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
	 *  Return the number of companies in the database for
	 *  the configured number of customers.
	 *
	 *  PARAMETERS:
	 *       none.
	 *
	 *  RETURNS:
	 *       number of rows in the file.
	 */
	TIdent GetSize() const;

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
	TIdent GetConfiguredCompanyCount() const;

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
	TIdent GetActiveCompanyCount() const;

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
	const CompanyDataFileRecord &GetRecord(TIdent index) const;
};

} // namespace TPCE

#endif // COMPANY_FILE_H
