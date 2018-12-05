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
 *   Description:        Implementation of the Security input file that scales
 *                       with the database size.
 ******************************************************************************/

#ifndef SECURITY_FILE_H
#define SECURITY_FILE_H

#include <map>
#include <cassert>

#include "utilities/EGenStandardTypes.h"
#include "DataFileTypes.h"
#include "main/ExchangeIDs.h"

namespace TPCE {

class CSecurityFile {
	SecurityDataFile_t const *m_dataFile;

	// Total number of securities in the database.
	// Depends on the total number of customers.
	//
	TIdent m_iConfiguredSecurityCount;
	TIdent m_iActiveSecurityCount;

	// Number of base companies (=rows in Company.txt input file).
	//
	UINT m_iBaseCompanyCount;

	// Used to map a symbol to it's id value. To support logical const-ness
	// these are mutable since they don't change the "real" contents of the
	// Security File.
	mutable bool m_SymbolToIdMapIsLoaded;
	mutable std::map<std::string, TIdent> m_SymbolToIdMap;
	mutable std::map<char, int> m_LowerCaseLetterToIntMap;
	char m_SUFFIX_SEPARATOR;

	void CreateSuffix(TIdent Multiplier, char *pBuf, size_t BufSize) const;
	INT64 ParseSuffix(const char *pSymbol) const;

public:
	// Constructor.
	//
	CSecurityFile(const SecurityDataFile_t &dataFile, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
	              UINT baseCompanyCount);

	// Calculate total security count for the specified number of customers.
	// Sort of a static method. Used in parallel generation of securities
	// related tables.
	//
	TIdent CalculateSecurityCount(TIdent iCustomerCount) const;

	// Calculate the first security id (0-based) for the specified customer id
	//
	TIdent CalculateStartFromSecurity(TIdent iStartFromCustomer) const;

	// Create security symbol with mod/div magic.
	//
	// This function is needed to scale unique security
	// symbols with the database size.
	//
	void CreateSymbol(TIdent iIndex,    // row number
	                  char *szOutput,   // output buffer
	                  size_t iOutputLen // size of the output buffer (including null)
	                  ) const;

	// Return company id for the specified row of the SECURITY table.
	// Index can exceed the size of the Security flat file.
	//
	TIdent GetCompanyId(TIdent iIndex) const;

	TIdent GetCompanyIndex(TIdent Index) const;

	// Return the number of securities in the database for
	// a certain number of customers.
	//
	TIdent GetSize() const;

	// Return the number of securities in the database for
	// the configured number of customers.
	//
	TIdent GetConfiguredSecurityCount() const;

	// Return the number of securities in the database for
	// the active number of customers.
	//
	TIdent GetActiveSecurityCount() const;

	// Overload GetRecord to wrap around indices that
	// are larger than the flat file
	//
	const SecurityDataFileRecord &GetRecord(TIdent index) const;

	// Load the symbol-to-id map
	// Logical const-ness - the maps and the is-loaded flag may change but the
	// "real" Security File data is unchanged.
	bool LoadSymbolToIdMap(void) const;

	TIdent GetId(char *pSymbol) const;

	TIdent GetIndex(char *pSymbol) const;

	eExchangeID GetExchangeIndex(TIdent index) const;
};

} // namespace TPCE

#endif // SECURITY_FILE_H
