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
 *   Contains class definition to generate Address table.
 *   Address table contains addresses for exchanges, companies, and customers.
 */
#ifndef ADDRESS_TABLE_H
#define ADDRESS_TABLE_H

#include "EGenTables_common.h"

#include "input/DataFileManager.h"

namespace TPCE {

class CAddressTable : public TableTemplate<ADDRESS_ROW> {
private:
	CDateTime m_date_time;
	const CCompanyFile &m_companies;
	const StreetNameDataFile_t &m_Street;
	const StreetSuffixDataFile_t &m_StreetSuffix;
	const ZipCodeDataFile_t &m_ZipCode;
	TIdent m_iStartFromCustomer;
	TIdent m_iCustomerCount;       // total # of customers for whom to generate addresses
	bool m_bCustomerAddressesOnly; // whether generating only customer addresses
	bool m_bCustomerAddress;       // whether the currently generated row is for a
	                               // customer
	TIdent m_iCompanyCount;        // total # of companies for which to generate addresses
	UINT m_iExchangeCount;         // total # of exchanges for which to generate
	                               // addresses
	TIdent m_iTotalAddressCount;   // total # of address rows to generate
	bool m_bCacheEnabled;
	int m_iCacheSize;
	TIdent m_iCacheOffset;
	int *m_CacheZipCode;
	const int INVALID_CACHE_ENTRY;

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
	void GenerateAD_LINE_1();
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
	void GenerateAD_LINE_2();
	/*
	 *   For a given address id returns the same Threshold used to
	 *   select the town, division, zip, and country.
	 *   Needed to return a specific division/country for a given address id
	 * (for customer tax rates).
	 *
	 *   PARAMETERS:
	 *           IN  ADID        - address id
	 *
	 *   RETURNS:
	 *           none.
	 */
	int GetTownDivisionZipCodeThreshold(TIdent ADID);
	/*
	 *   Return the country code code for a given zip code.
	 *
	 *   PARAMETERS:
	 *           IN  szZipCode       - string with a US or Canada zip code
	 *
	 *   RETURNS:
	 *           country code.
	 */
	UINT GetCountryCode(const char *pZipCode);
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
	void GenerateAD_ZC_CODE_CTRY();

public:
	/*
	 *  Constructor for the ADDRESS table class.
	 *
	 *  PARAMETERS:
	 *       IN  dfm                     - input flat files loaded in memory
	 *       IN  iCustomerCount          - number of customers to generate
	 *       IN  iStartFromCustomer      - ordinal position of the first
	 * customer in the sequence (Note: 1-based) for whom to generate the
	 * addresses. Used if generating customer addresses only. IN
	 * bCustomerAddressesOnly  - if true, generate only customer addresses if
	 * false, generate exchange, company, and customer addresses (always start
	 * from the first customer in this case) RETURNS: not applicable.
	 */
	CAddressTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer,
	              bool bCustomerAddressesOnly = false, bool bCacheEnabled = false);

	/*
	 *  Destructor for the ADDRESS table class.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *       not applicable.
	 */
	~CAddressTable();

	/*
	 *   Reset the state for the next load unit.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           none.
	 */
	void InitNextLoadUnit();

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
	TIdent GenerateNextAD_ID();

	/*
	 *   Returns the address id of the customer specified by the customer id.
	 *
	 *   PARAMETERS:
	 *           IN  C_ID        - customer id (1-based)
	 *
	 *   RETURNS:
	 *           address id.
	 */
	TIdent GetAD_IDForCustomer(TIdent C_ID); // return address id for the customer id

	/*
	 *   Return a certain division/country code (from the input file) for a
	 * given address id. Used in the loader to properly calculate tax on a
	 * trade.
	 *
	 *   PARAMETERS:
	 *           IN  AD_ID       - address id
	 *           OUT iDivCode    - division (state/province) code
	 *           OUT iCtryCode   - country (USA/CANADA) code
	 *
	 *   RETURNS:
	 *           none.
	 */
	void GetDivisionAndCountryCodesForAddress(TIdent AD_ID, UINT &iDivCode, UINT &iCtryCode);

	/*
	 *   Return division and country codes for current address.
	 *   Used in generating customer taxrates.
	 *
	 *   PARAMETERS:
	 *           OUT iDivCode    - division (state/province) code
	 *           OUT iCtryCode   - country (USA/CANADA) code
	 *
	 *   RETURNS:
	 *           none.
	 */
	void GetDivisionAndCountryCodes(UINT &iDivCode, UINT &iCtryCode) {
		GetDivisionAndCountryCodesForAddress(m_row.AD_ID, iDivCode, iCtryCode);
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
	bool GenerateNextRecord(); // generates the next table row
};

} // namespace TPCE

#endif // ADDRESS_TABLE_H
