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

#include "main/EGenTables_stdafx.h"

using namespace TPCE;

const UINT iUSACtryCode = 1;    // must be the same as the code in country tax rates file
const UINT iCanadaCtryCode = 2; // must be the same as the code in country tax rates file

// Minimum and maximum to use when generating address street numbers.
const int iStreetNumberMin = 100;
const int iStreetNumberMax = 25000;

// Some customers have an AD_LINE_2, some are NULL.
const int iPctCustomersWithNullAD_LINE_2 = 60;

// Of the customers that have an AD_LINE_2, some are
// an apartment, others are a suite.
const int iPctCustomersWithAptAD_LINE_2 = 75;

// Minimum and maximum to use when generating apartment numbers.
const int iApartmentNumberMin = 1;
const int iApartmentNumberMax = 1000;

// Minimum and maximum to use when generating suite numbers.
const int iSuiteNumberMin = 1;
const int iSuiteNumberMax = 500;

// Number of RNG calls to skip for one row in order
// to not use any of the random values from the previous row.
const int iRNGSkipOneRowAddress = 10; // real number in 3.5: 7

/*
 *  Constructor for the ADDRESS table class.
 *
 *  PARAMETERS:
 *       IN  inputFiles              - input flat files loaded in memory
 *       IN  iCustomerCount          - number of customers to generate
 *       IN  iStartFromCustomer      - ordinal position of the first customer in
 * the sequence (Note: 1-based) for whom to generate the addresses. Used if
 * generating customer addresses only. IN  bCustomerAddressesOnly  - if true,
 * generate only customer addresses if false, generate exchange, company, and
 * customer addresses (always start from the first customer in this case)
 */
CAddressTable::CAddressTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer,
                             bool bCustomerAddressesOnly, bool bCacheEnabled)
    : TableTemplate<ADDRESS_ROW>(), m_companies(dfm.CompanyFile()), m_Street(dfm.StreetNameDataFile()),
      m_StreetSuffix(dfm.StreetSuffixDataFile()), m_ZipCode(dfm.ZipCodeDataFile()),
      m_iStartFromCustomer(iStartFromCustomer), m_iCustomerCount(iCustomerCount),
      m_bCustomerAddressesOnly(bCustomerAddressesOnly), m_bCustomerAddress(bCustomerAddressesOnly),
      m_bCacheEnabled(bCacheEnabled), INVALID_CACHE_ENTRY(-1) {
	m_iExchangeCount = dfm.ExchangeDataFile().size();          // number of rows in Exchange
	m_iCompanyCount = m_companies.GetConfiguredCompanyCount(); // number of configured companies

	// Generate customer addresses only (used for CUSTOMER_TAXRATE)
	if (bCustomerAddressesOnly) {
		// skip exchanges and companies
		m_iLastRowNumber = m_iExchangeCount + m_iCompanyCount + iStartFromCustomer - 1;

		//  This is not really a count, but the last address row to generate.
		//
		m_iTotalAddressCount = m_iLastRowNumber + m_iCustomerCount;
	} else { // Generating not only customer, but also exchange and company
		     // addresses
		m_iLastRowNumber = iStartFromCustomer - 1;

		//  This is not really a count, but the last address row to generate.
		//
		m_iTotalAddressCount = m_iLastRowNumber + m_iCustomerCount + m_iExchangeCount + m_iCompanyCount;
	}

	m_row.AD_ID = m_iLastRowNumber + iTIdentShift; // extend to 64 bits for address id

	if (m_bCacheEnabled) {
		m_iCacheSize = (int)iDefaultLoadUnitSize;
		m_iCacheOffset = iTIdentShift + m_iExchangeCount + m_iCompanyCount + m_iStartFromCustomer;
		m_CacheZipCode = new int[m_iCacheSize];
		for (int i = 0; i < m_iCacheSize; i++) {
			m_CacheZipCode[i] = INVALID_CACHE_ENTRY;
		}
	}
}

CAddressTable::~CAddressTable() {
	if (m_bCacheEnabled) {
		delete[] m_CacheZipCode;
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
void CAddressTable::InitNextLoadUnit() {
	m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault, (RNGSEED)m_iLastRowNumber * iRNGSkipOneRowAddress));

	ClearRecord(); // this is needed for EGenTest to work

	if (m_bCacheEnabled) {
		m_iCacheOffset += iDefaultLoadUnitSize;
		for (int i = 0; i < m_iCacheSize; i++) {
			m_CacheZipCode[i] = INVALID_CACHE_ENTRY;
		}
	}
}

/*
 *   Generates the next A_ID value.
 *   It is stored in the internal record structure and also returned.
 *   The number of rows generated is incremented. This is why
 *   this function cannot be called more than once for a record.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           next address id.
 */
TIdent CAddressTable::GenerateNextAD_ID() {
	// Reset RNG at Load Unit boundary, so that all data is repeatable.
	//
	if (m_iLastRowNumber > (m_iExchangeCount + m_iCompanyCount) &&
	    ((m_iLastRowNumber - (m_iExchangeCount + m_iCompanyCount)) % iDefaultLoadUnitSize == 0)) {
		InitNextLoadUnit();
	}

	++m_iLastRowNumber;
	// Find out whether this next row is for a customer (so as to generate
	// AD_LINE_2). Exchange and Company addresses are before Customer ones.
	//
	m_bCustomerAddress = m_iLastRowNumber >= m_iExchangeCount + m_iCompanyCount;

	// update state info
	m_bMoreRecords = m_iLastRowNumber < m_iTotalAddressCount;

	m_row.AD_ID = m_iLastRowNumber + iTIdentShift;

	return m_row.AD_ID;
}

/*
 *   Returns the address id of the customer specified by the customer id.
 *
 *   PARAMETERS:
 *           IN  C_ID        - customer id (1-based)
 *
 *   RETURNS:
 *           address id.
 */
TIdent CAddressTable::GetAD_IDForCustomer(TIdent C_ID) {
	return m_iExchangeCount + m_iCompanyCount + C_ID;
}

/*
 *   Generate AD_LINE_1 and store it in the record structure.
 *   Does not increment the number of rows generated.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CAddressTable::GenerateAD_LINE_1() {
	int iStreetNum = m_rnd.RndIntRange(iStreetNumberMin, iStreetNumberMax);
	// int iStreetThreshold = m_rnd.RndIntRange(0,
	// m_Street->GetGreatestKey()-2);
	int iStreetThreshold = m_rnd.RndIntRange(0, m_Street.size() - 2);
	int iStreetSuffixThreshold = m_rnd.RndIntRange(0, m_StreetSuffix.size() - 1);

	snprintf(m_row.AD_LINE1, sizeof(m_row.AD_LINE1), "%d %s %s", iStreetNum, m_Street[iStreetThreshold].STREET_CSTR(),
	         m_StreetSuffix[iStreetSuffixThreshold].SUFFIX_CSTR());
}

/*
 *   Generate AD_LINE_2 and store it in the record structure.
 *   Does not increment the number of rows generated.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CAddressTable::GenerateAD_LINE_2() {
	if (!m_bCustomerAddress || m_rnd.RndPercent(iPctCustomersWithNullAD_LINE_2)) { // Generate second address line
		                                                                           // only for customers (not
		                                                                           // companies)
		m_row.AD_LINE2[0] = '\0';
	} else {
		if (m_rnd.RndPercent(iPctCustomersWithAptAD_LINE_2)) {
			snprintf(m_row.AD_LINE2, sizeof(m_row.AD_LINE2), "Apt. %d",
			         m_rnd.RndIntRange(iApartmentNumberMin, iApartmentNumberMax));
		} else {
			snprintf(m_row.AD_LINE2, sizeof(m_row.AD_LINE2), "Suite %d",
			         m_rnd.RndIntRange(iSuiteNumberMin, iSuiteNumberMax));
		}
	}
}

/*
 *   For a given address id returns the same Threshold used to
 *   select the town, division, zip, and country.
 *   Needed to return a specific division/country for a given address id (for
 * customer tax rates).
 *
 *   PARAMETERS:
 *           IN  ADID        - address id
 *
 *   RETURNS:
 *           none.
 */
int CAddressTable::GetTownDivisionZipCodeThreshold(TIdent ADID) {
	RNGSEED OldSeed;
	int iThreshold;

	OldSeed = m_rnd.GetSeed();
	m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseTownDivZip, (RNGSEED)ADID));
	iThreshold = m_rnd.RndIntRange(0, m_ZipCode.size() - 1);
	m_rnd.SetSeed(OldSeed);
	return (iThreshold);
}

/*
 *   Return the country code code for a given zip code.
 *
 *   PARAMETERS:
 *           IN  szZipCode       - string with a US or Canada zip code
 *
 *   RETURNS:
 *           country code.
 */
UINT CAddressTable::GetCountryCode(const char *szZipCode) {
	if (('0' <= szZipCode[0]) && (szZipCode[0] <= '9')) {
		// If the zip code starts with a number, then it's a USA code.
		return (iUSACtryCode);
	} else {
		// If the zip code does NOT start with a number, than it's a Canadian
		// code.
		return (iCanadaCtryCode);
	}
}

/*
 *   Return a certain division/country code (from the input file) for a given
 * address id. Used in the loader to properly calculate tax on a trade.
 *
 *   PARAMETERS:
 *           IN  AD_ID       - address id
 *           OUT iDivCode    - division (state/province) code
 *           OUT iCtryCode   - country (USA/CANADA) code
 *
 *   RETURNS:
 *           none.
 */
void CAddressTable::GetDivisionAndCountryCodesForAddress(TIdent AD_ID, UINT &iDivCode, UINT &iCtryCode) {
	// const TZipCodeInputRow* pZipCodeInputRow = NULL;

	// We will sometimes get AD_ID values that are outside the current
	// load unit (cached range).  We need to check for this case
	// and avoid the lookup (as we will segfault or get bogus data.)
	TIdent index = AD_ID - m_iCacheOffset;
	bool bCheckCache = (index >= 0 && index <= m_iCacheSize);

	if (m_bCacheEnabled && bCheckCache && (INVALID_CACHE_ENTRY != m_CacheZipCode[index])) {
		// Make use of the cache to get the data.
		iDivCode = m_ZipCode[m_CacheZipCode[index]].DivisionTaxKey();
		iCtryCode = GetCountryCode(m_ZipCode[m_CacheZipCode[index]].ZC_CODE_CSTR());

		// We're done, so bail out.
		return;
	}

	// The cache wasn't used so get the necessary value.
	int iThreshold = GetTownDivisionZipCodeThreshold(AD_ID);

	// If possible, cache the result in case we need it again.
	if (m_bCacheEnabled && bCheckCache) {
		m_CacheZipCode[index] = iThreshold;
	}

	// Get the data.
	iDivCode = m_ZipCode[iThreshold].DivisionTaxKey();
	iCtryCode = GetCountryCode(m_ZipCode[iThreshold].ZC_CODE_CSTR());
}

/*
 *   Generate zip code and country for the current address id
 *   and store them in the record structure.
 *   Does not increment the number of rows generated.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           none.
 */
void CAddressTable::GenerateAD_ZC_CODE_CTRY() {
	int iThreshold;

	iThreshold = GetTownDivisionZipCodeThreshold(m_row.AD_ID);
	const ZipCodeDataFileRecord &dfr = m_ZipCode[iThreshold];

	strncpy(m_row.AD_ZC_CODE, dfr.ZC_CODE_CSTR(), sizeof(m_row.AD_ZC_CODE));

	if (iUSACtryCode == GetCountryCode(dfr.ZC_CODE_CSTR())) { // US state
		strncpy(m_row.AD_CTRY, "USA", sizeof(m_row.AD_CTRY));
	} else { // Canadian province
		strncpy(m_row.AD_CTRY, "CANADA", sizeof(m_row.AD_CTRY));
	}
}

/*
 *   Generate all column values for the next row
 *   and store them in the record structure.
 *   Increment the number of rows generated.
 *
 *   PARAMETERS:
 *           none.
 *
 *   RETURNS:
 *           TRUE, if there are more records in the ADDRESS table; FALSE
 * othewise.
 */
bool CAddressTable::GenerateNextRecord() {
	GenerateNextAD_ID();
	GenerateAD_LINE_1();
	GenerateAD_LINE_2();
	GenerateAD_ZC_CODE_CTRY();

	// Return false if all the rows have been generated
	return (MoreRecords());
}
