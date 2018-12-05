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
 *   Class representing the Customer Taxrates table.
 */
#ifndef CUSTOMER_TAX_RATE_TABLE_H
#define CUSTOMER_TAX_RATE_TABLE_H

#include "EGenTables_common.h"
#include "CustomerTable.h"
#include "AddressTable.h"

#include "input/DataFileManager.h"

namespace TPCE {

const UINT iTaxRatesPerCust = 2; // number of tax rates per customer
const int iMaxDivOrCtryName = 6;

// Number of RNG calls to skip for one row in order
// to not use any of the random values from the previous row.
const int iRNGSkipOneRowCustomerTaxrate = 5; // real max count in v3.5: 2

typedef struct CUSTOMER_TAXRATE_ROWS {
	CUSTOMER_TAXRATE_ROW
	m_row[iTaxRatesPerCust]; // multiple tax rates rows per customer
} * PCUSTOMER_TAXRATE_ROWS;

class CCustomerTaxRateTable : public TableTemplate<CUSTOMER_TAXRATE_ROWS> {
	CCustomerTable m_cust;
	CAddressTable m_addr;
	const TaxRateDivisionDataFile_t &m_division_rates;
	const TaxRateCountryDataFile_t &m_country_rates;

	/*
	 *   Reset the state for the next load unit
	 */
	void InitNextLoadUnit() {
		m_rnd.SetSeed(
		    m_rnd.RndNthElement(RNGSeedTableDefault, (RNGSEED)m_cust.GetCurrentC_ID() * iRNGSkipOneRowCustomerTaxrate));

		ClearRecord(); // this is needed for EGenTest to work
	}

	// generate the tax row deterministically for a given customer and country
	// or division code
	const ITaxRateFileRecord &GetTaxRow(TIdent C_ID, UINT iCode, bool bCtry) {
		RNGSEED OldSeed;
		UINT iThreshold;
		// const vector<TTaxRateInputRow>  *pRates;

		OldSeed = m_rnd.GetSeed();
		m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseTaxRateRow, (RNGSEED)C_ID));

		if (bCtry) {
			// Return appropriate country record.
			iThreshold = (UINT)m_rnd.RndIntRange(0, m_country_rates.getBucket(iCode).size() - 1);
			m_rnd.SetSeed(OldSeed);
			return m_country_rates.getBucket(iCode)[iThreshold];
		}

		// It's not a country so return the appropriate division record.
		iThreshold = (UINT)m_rnd.RndIntRange(0, m_division_rates.getBucket(iCode).size() - 1);
		m_rnd.SetSeed(OldSeed);
		return m_division_rates.getBucket(iCode)[iThreshold];
	}

public:
	CCustomerTaxRateTable(const DataFileManager &dfm, TIdent iCustomerCount, TIdent iStartFromCustomer,
	                      bool bCacheEnabled = false)
	    : TableTemplate<CUSTOMER_TAXRATE_ROWS>(), m_cust(dfm, iCustomerCount, iStartFromCustomer),
	      m_addr(dfm, iCustomerCount, iStartFromCustomer, true, bCacheEnabled),
	      m_division_rates(dfm.TaxRateDivisionDataFile()), m_country_rates(dfm.TaxRateCountryDataFile()){};

	/*
	 *   Generates all column values for the next row.
	 */
	bool GenerateNextRecord() {
		UINT iDivCode, iCtryCode;

		if (m_cust.GetCurrentC_ID() % iDefaultLoadUnitSize == 0) {
			InitNextLoadUnit();
		}

		++m_iLastRowNumber;

		m_cust.GenerateNextC_ID();  // next customer id
		m_addr.GenerateNextAD_ID(); // next address id (to get the one for this
		                            // customer)
		m_addr.GetDivisionAndCountryCodes(iDivCode, iCtryCode);
		// Fill the country tax rate row
		m_row.m_row[0].CX_C_ID = m_cust.GetCurrentC_ID(); // fill the customer
		                                                  // id
		// Select the country rate
		strncpy(m_row.m_row[0].CX_TX_ID, GetCountryTaxRow(m_cust.GetCurrentC_ID(), iCtryCode).TX_ID_CSTR(),
		        sizeof(m_row.m_row[0].CX_TX_ID));

		// Fill the division tax rate row
		m_row.m_row[1].CX_C_ID = m_cust.GetCurrentC_ID(); // fill the customer
		                                                  // id
		// Select the division rate
		strncpy(m_row.m_row[1].CX_TX_ID, GetDivisionTaxRow(m_cust.GetCurrentC_ID(), iDivCode).TX_ID_CSTR(),
		        sizeof(m_row.m_row[0].CX_TX_ID));

		m_bMoreRecords = m_cust.MoreRecords();

		return (MoreRecords());
	}

	const CUSTOMER_TAXRATE_ROW &GetRowByIndex(UINT i) {
		if (i < iTaxRatesPerCust)
			return m_row.m_row[i];
		else
			throw std::range_error("Customer Taxrate row index out of bounds.");
	}

	UINT GetTaxRatesCount() {
		return iTaxRatesPerCust;
	} // tax rates per customer

	// generate country tax row for a given customer
	const ITaxRateFileRecord &GetCountryTaxRow(TIdent C_ID, UINT iCtryCode) {
		return GetTaxRow(C_ID, iCtryCode, true);
	}

	// generate division tax row for a given customer
	const ITaxRateFileRecord &GetDivisionTaxRow(TIdent C_ID, UINT iDivCode) {
		return GetTaxRow(C_ID, iDivCode, false);
	}
};

} // namespace TPCE

#endif // CUSTOMER_TAX_RATE_TABLE_H
