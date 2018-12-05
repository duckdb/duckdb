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
 *   Class representing the Customer Accounts table.
 */
#ifndef CUSTOMER_ACCOUNTS_AND_PERMISSIONS_TABLE_H
#define CUSTOMER_ACCOUNTS_AND_PERMISSIONS_TABLE_H

#include "EGenTables_common.h"
#include "CustomerTable.h"
#include "AddressTable.h"

#include "input/DataFileManager.h"

namespace TPCE {
const UINT iMaxCAPerms = 3; // maximum # of customers having permissions to the same account
const UINT iMinAccountsPerCustRange[3] = {1, 2, 5};
const UINT iMaxAccountsPerCustRange[3] = {4, 8, 10};
const UINT iMaxAccountsPerCust = 10; // must be the biggest number in iMaxAccountsPerCustRange array
const TIdent iStartingBrokerID = 1;

//  This is the fixed range from which person ids (like CIDs) are selected
//  for the *additional* permissions on the account that make the
//  content of ACCOUNT_PERMISSION table.
//
//  The range is fixed for any size database in order for the parallel loaders
//  to select person ids the same way and be compatible with runtime driver
//  (and to avoid database size parameter to the loader executable).
//
const TIdent iAccountPermissionIDRange = INT64_CONST(4024) * 1024 * 1024 - iDefaultStartFromCustomer;

const UINT iPercentAccountsWithPositiveInitialBalance = 80;

const double fAccountInitialPositiveBalanceMax = 9999999.99;
const double fAccountInitialNegativeBalanceMin = -9999999.99;

const UINT iPercentAccountAdditionalPermissions_0 = 60;
const UINT iPercentAccountAdditionalPermissions_1 = 38;
const UINT iPercentAccountAdditionalPermissions_2 = 2;

const UINT iPercentAccountTaxStatusNonTaxable = 20;
const UINT iPercentAccountTaxStatusTaxableAndWithhold = 50;
const UINT iPercentAccountTaxStatusTaxableAndDontWithhold = 30;

// Number of RNG calls to skip for one row in order
// to not use any of the random values from the previous row.
const UINT iRNGSkipOneRowCustomerAccount = 10; // real max count in v3.5: 7

enum eTaxStatus { eNone = -1, eNonTaxable = 0, eTaxableAndWithhold, eTaxableAndDontWithhold };

typedef struct CUSTOMER_ACCOUNT_AND_PERMISSION_ROW {
	CUSTOMER_ACCOUNT_ROW m_ca;
	ACCOUNT_PERMISSION_ROW m_perm[iMaxCAPerms + 1];
} * PCUSTOMER_ACCOUNT_AND_PERMISSION_ROW;

class CCustomerAccountsAndPermissionsTable : public TableTemplate<CUSTOMER_ACCOUNT_AND_PERMISSION_ROW> {
	const TaxableAccountNameDataFile_t &m_TaxableAccountName;
	const NonTaxableAccountNameDataFile_t &m_NonTaxableAccountName;
	TIdent m_iStartFromCustomer;
	TIdent m_iCustomerCount;
	TIdent m_iStartingCA_ID;      // first CA_ID for the current customer
	UINT m_iRowsToGenForCust;     // total # of rows to generate for a given
	                              // portfolio
	UINT m_iRowsGeneratedForCust; // rows already generated for a particular
	                              // portfolio
	CCustomerTable m_cust;
	CPerson m_person;
	UINT m_iPermsForCA;
	TIdent m_iBrokersCount;
	CAddressTable m_addr; // ADDRESS table - to calculate tax for TRADE
	UINT m_iLoadUnitSize;
	CCustomerSelection m_CustomerSelection;
	bool m_bCacheEnabled;
	int m_iCacheSizeNA;
	TIdent m_iCacheOffsetNA;
	UINT *m_CacheNA;
	int m_iCacheSizeTS;
	TIdent m_iCacheOffsetTS;
	eTaxStatus *m_CacheTS;

	/*
	 *   Generate only the Customer Account row.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           none.
	 */
	void GenerateCARow() {
		int iAcctType;

		// Generate customer account row.
		//
		GenerateNextCA_AD();

		m_row.m_ca.CA_C_ID = GetCurrentC_ID(); // get from CUSTOMER

		// Generate broker id.
		m_row.m_ca.CA_B_ID = GenerateBrokerIdForAccount(m_row.m_ca.CA_ID);

		// Generate tax status and account name.
		if ((m_row.m_ca.CA_TAX_ST = (char)GetAccountTaxStatus(m_row.m_ca.CA_ID)) ==
		    eNonTaxable) {                                                      // non-taxable account
			iAcctType = (int)m_row.m_ca.CA_ID % m_NonTaxableAccountName.size(); // select account type

			snprintf(m_row.m_ca.CA_NAME, sizeof(m_row.m_ca.CA_NAME), "%s %s %s",
			         m_person.GetFirstName(m_row.m_ca.CA_C_ID).c_str(),
			         m_person.GetLastName(m_row.m_ca.CA_C_ID).c_str(), m_NonTaxableAccountName[iAcctType].NAME_CSTR());
		} else {                                                             // taxable account
			iAcctType = (int)m_row.m_ca.CA_ID % m_TaxableAccountName.size(); // select account type

			snprintf(m_row.m_ca.CA_NAME, sizeof(m_row.m_ca.CA_NAME), "%s %s %s",
			         m_person.GetFirstName(m_row.m_ca.CA_C_ID).c_str(),
			         m_person.GetLastName(m_row.m_ca.CA_C_ID).c_str(), m_TaxableAccountName[iAcctType].NAME_CSTR());
		}

		if (m_rnd.RndPercent(iPercentAccountsWithPositiveInitialBalance)) {
			m_row.m_ca.CA_BAL = m_rnd.RndDoubleIncrRange(0.00, fAccountInitialPositiveBalanceMax, 0.01);
		} else {
			m_row.m_ca.CA_BAL = m_rnd.RndDoubleIncrRange(fAccountInitialNegativeBalanceMin, 0.00, 0.01);
		}
	}

	/*
	 *   Helper function to generate parts of an ACCOUNT_PERMISSION row.
	 *
	 *   PARAMETERS:
	 *           IN  CA_ID       - customer account id
	 *           IN  C_ID        - customer id
	 *           IN  szACL       - Access-Control-List string on the account
	 *           OUT row         - ACCOUNT_PERMISSION row structure to fill
	 *
	 *   RETURNS:
	 *             none.
	 */
	void FillAPRow(TIdent CA_ID, TIdent C_ID, const char *szACL, ACCOUNT_PERMISSION_ROW &row) {
		row.AP_CA_ID = CA_ID;
		m_cust.GetC_TAX_ID(C_ID, row.AP_TAX_ID);
		strncpy(row.AP_L_NAME, m_person.GetLastName(C_ID).c_str(), sizeof(row.AP_L_NAME));
		strncpy(row.AP_F_NAME, m_person.GetFirstName(C_ID).c_str(), sizeof(row.AP_F_NAME));
		strncpy(row.AP_ACL, szACL, sizeof(row.AP_ACL));
	}

	/*
	 *   Generate only the Account Permissions row(s).
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           none.
	 */
	void GenerateAPRows() {
		int iAdditionalPerms;
		TIdent CID_1, CID_2;

		// Generate account permissions rows.

		// Generate the owner row
		FillAPRow(m_row.m_ca.CA_ID, m_row.m_ca.CA_C_ID, "0000", m_row.m_perm[0]);

		iAdditionalPerms = GetNumPermsForCA(m_row.m_ca.CA_ID);
		switch (iAdditionalPerms) {
		case 0:
			m_iPermsForCA = 1; // 60%
			break;
		case 1:
			GetCIDsForPermissions(m_row.m_ca.CA_ID, m_row.m_ca.CA_C_ID, &CID_1, NULL);
			m_iPermsForCA = 2; // 38%
			// generate second account permission row
			FillAPRow(m_row.m_ca.CA_ID, CID_1, "0001", m_row.m_perm[1]);
			break;
		case 2:
			GetCIDsForPermissions(m_row.m_ca.CA_ID, m_row.m_ca.CA_C_ID, &CID_1, &CID_2);
			m_iPermsForCA = 3; // 2%
			// generate second account permission row
			FillAPRow(m_row.m_ca.CA_ID, CID_1, "0001", m_row.m_perm[1]);
			// generate third account permission row
			FillAPRow(m_row.m_ca.CA_ID, CID_2, "0011", m_row.m_perm[2]);
			break;
		}
	}

public:
	/*
	 *  Constructor.
	 *
	 *  PARAMETERS:
	 *       IN  dfm                     - input flat files loaded in memory
	 *       IN  iLoadUnitSize           - should always be 1000
	 *       IN  iCustomerCount          - number of customers to generate
	 *       IN  iStartFromCustomer      - ordinal position of the first
	 * customer in the sequence (Note: 1-based)
	 *
	 *  RETURNS:
	 *       not applicable.
	 */
	CCustomerAccountsAndPermissionsTable(const DataFileManager &dfm,
	                                     UINT iLoadUnitSize, // # of customers in one load unit
	                                     TIdent iCustomerCount, TIdent iStartFromCustomer, bool bCacheEnabled = false)
	    : TableTemplate<CUSTOMER_ACCOUNT_AND_PERMISSION_ROW>(), m_TaxableAccountName(dfm.TaxableAccountNameDataFile()),
	      m_NonTaxableAccountName(dfm.NonTaxableAccountNameDataFile()), m_iStartFromCustomer(iStartFromCustomer),
	      m_iCustomerCount(iCustomerCount), m_iRowsToGenForCust(0), m_iRowsGeneratedForCust(0),
	      m_cust(dfm, iCustomerCount, iStartFromCustomer), m_person(dfm, iStartFromCustomer, bCacheEnabled),
	      m_iPermsForCA(0), m_iBrokersCount(iLoadUnitSize / iBrokersDiv),
	      m_addr(dfm, iCustomerCount, iStartFromCustomer, bCacheEnabled), m_iLoadUnitSize(iLoadUnitSize),
	      m_bCacheEnabled(bCacheEnabled) {
		if (m_bCacheEnabled) {
			m_iCacheSizeNA = iDefaultLoadUnitSize;
			m_iCacheOffsetNA = iStartFromCustomer + iTIdentShift;
			m_CacheNA = new UINT[m_iCacheSizeNA];
			for (int i = 0; i < m_iCacheSizeNA; i++) {
				m_CacheNA[i] = 0;
			}

			m_iCacheSizeTS = iDefaultLoadUnitSize * iMaxAccountsPerCust;
			m_iCacheOffsetTS = ((iStartFromCustomer - 1) + iTIdentShift) * iMaxAccountsPerCust;
			m_CacheTS = new eTaxStatus[m_iCacheSizeTS];
			for (int i = 0; i < m_iCacheSizeTS; i++) {
				m_CacheTS[i] = eNone;
			}
		}
	};

	/*
	 *  Destructor.
	 */
	~CCustomerAccountsAndPermissionsTable() {
		if (m_bCacheEnabled) {
			delete[] m_CacheNA;
			delete[] m_CacheTS;
		}
	};

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
		m_rnd.SetSeed(m_rnd.RndNthElement(
		    RNGSeedTableDefault, (RNGSEED)(GetCurrentC_ID() * iMaxAccountsPerCust * iRNGSkipOneRowCustomerAccount)));

		ClearRecord(); // this is needed for EGenTest to work

		if (m_bCacheEnabled) {
			m_iCacheOffsetNA += iDefaultLoadUnitSize;
			for (int i = 0; i < m_iCacheSizeNA; i++) {
				m_CacheNA[i] = 0;
			}
			m_iCacheOffsetTS += iDefaultLoadUnitSize * iMaxAccountsPerCust;
			for (int i = 0; i < m_iCacheSizeTS; i++) {
				m_CacheTS[i] = eNone;
			}
		}
	}

	/*
	 *   Generate the number of accounts for a given customer id.
	 *
	 *   PARAMETERS:
	 *           IN  CID             - customer id
	 *           IN  iCustomerTier   - customer tier (indicating trading
	 * frequency)
	 *
	 *   RETURNS:
	 *       number of accounts
	 */
	UINT GetNumberOfAccounts(TIdent CID, eCustomerTier iCustomerTier) {
		UINT iNumAccounts = 0;

		// We will sometimes get CID values that are outside the current
		// load unit (cached range).  We need to check for this case
		// and avoid the lookup (as we will segfault or get bogus data.)
		TIdent index = CID - m_iCacheOffsetNA;
		bool bCheckCache = (index >= 0 && index < m_iCacheSizeNA);
		if (m_bCacheEnabled && bCheckCache) {
			iNumAccounts = m_CacheNA[index];
		}

		if (iNumAccounts == 0) {
			UINT iMinAccountCount;
			UINT iMod;
			UINT iInverseCID;

			iMinAccountCount = iMinAccountsPerCustRange[iCustomerTier - eCustomerTierOne];
			iMod = iMaxAccountsPerCustRange[iCustomerTier - eCustomerTierOne] - iMinAccountCount + 1;
			iInverseCID = m_CustomerSelection.GetInverseCID(CID);

			// Note: the calculations below assume load unit contains 1000
			// customers.
			//
			if (iInverseCID < 200) // Tier 1
			{
				iNumAccounts = (iInverseCID % iMod) + iMinAccountCount;
			} else {
				if (iInverseCID < 800) // Tier 2
				{
					iNumAccounts = ((iInverseCID - 200 + 1) % iMod) + iMinAccountCount;
				} else // Tier 3
				{
					iNumAccounts = ((iInverseCID - 800 + 2) % iMod) + iMinAccountCount;
				}
			}

			if (m_bCacheEnabled && bCheckCache) {
				m_CacheNA[index] = iNumAccounts;
			}
		}
		return iNumAccounts;
	}

	/*
	 *   Generate a random account for the specified customer.
	 *   The distribution is uniform across all the accounts for the customer.
	 *
	 *   PARAMETERS:
	 *           IN  RND                 - external Random Number Generator
	 *           IN  iCustomerId         - customer id
	 *           IN  iCustomerTier       - customer tier (indicating trading
	 * frequency) OUT piCustomerAccount   - customer account id OUT
	 * piAccountCount      - total number of accounts that the customer has
	 *
	 *   RETURNS:
	 *           none.
	 */
	void GenerateRandomAccountId(CRandom &RND,                // in - external RNG
	                             TIdent iCustomerId,          // in
	                             eCustomerTier iCustomerTier, // in
	                             TIdent *piCustomerAccount,   // out
	                             int *piAccountCount)         // out
	{
		TIdent iCustomerAccount;
		int iAccountCount;
		TIdent iStartingAccount;

		iAccountCount = GetNumberOfAccounts(iCustomerId, iCustomerTier);

		iStartingAccount = GetStartingCA_ID(iCustomerId);

		// Select random account for the customer
		//
		iCustomerAccount = RND.RndInt64Range(iStartingAccount, iStartingAccount + iAccountCount - 1);

		if (piCustomerAccount != NULL) {
			*piCustomerAccount = iCustomerAccount;
		}

		if (piAccountCount != NULL) {
			*piAccountCount = iAccountCount;
		}
	}

	/*
	 *   Generate a random account for the specified customer.
	 *
	 *   PARAMETERS:
	 *           IN  RND                 - external Random Number Generator
	 *           IN  iCustomerId         - customer id
	 *           IN  iCustomerTier       - customer tier (indicating trading
	 * frequency)
	 *
	 *   RETURNS:
	 *           customer account id.
	 */
	TIdent GenerateRandomAccountId(CRandom &RND, TIdent iCustomerId, eCustomerTier iCustomerTier) {
		TIdent iAccountOffset;
		INT32 iAccountCount;
		TIdent iStartingAccount;

		iAccountCount = GetNumberOfAccounts(iCustomerId, iCustomerTier);

		iStartingAccount = GetStartingCA_ID(iCustomerId);

		iAccountOffset = (TIdent)RND.RndInt64Range(0, (INT64)iAccountCount - 1);

		return (iStartingAccount + iAccountOffset);
	}

	/*
	 *   Get starting account id for a given customer id.
	 *   This is needed for the driver to know what account ids belong to a
	 * given customer.
	 *
	 *   PARAMETERS:
	 *           IN  CID         - customer id
	 *
	 *   RETURNS:
	 *           first account id of the customer.
	 */
	TIdent GetStartingCA_ID(TIdent CID) {
		// start account ids on the next boundary for the new customer
		return ((CID - 1) * iMaxAccountsPerCust + 1);
	}

	/*
	 *   Get (maximum potential) ending account id for a given customer id.
	 *   This is needed for the driver to restrict query results to active
	 * accounts.
	 *
	 *   PARAMETERS:
	 *           IN  CID         - customer id
	 *
	 *   RETURNS:
	 *           last account id of the customer.
	 */
	TIdent GetEndingCA_ID(TIdent CID) {
		return (CID + iTIdentShift) * iMaxAccountsPerCust;
	}

	/*
	 *   Generate next CA_ID and update state information.
	 *   It is stored in the internal record structure and also returned.
	 *   The number of rows generated is incremented. This is why
	 *   this function cannot be called more than once for a record.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           next account id of the customer.
	 */
	TIdent GenerateNextCA_AD() {
		if (GetCurrentC_ID() % iDefaultLoadUnitSize == 0) {
			InitNextLoadUnit();
		}

		++m_iLastRowNumber;

		if (m_iRowsGeneratedForCust == m_iRowsToGenForCust) { // select next customer id as all the rows
			                                                  // for this customer have been generated
			m_cust.GenerateNextC_ID();
			m_addr.GenerateNextAD_ID();  // next address id (to get the one for
			                             // this customer)
			m_iRowsGeneratedForCust = 0; // no row generated yet
			// total # of accounts for this customer
			m_iRowsToGenForCust =
			    GetNumberOfAccounts(m_cust.GetCurrentC_ID(), m_cust.GetC_TIER(m_cust.GetCurrentC_ID()));

			m_iStartingCA_ID = GetStartingCA_ID(m_cust.GetCurrentC_ID());
		}

		m_row.m_ca.CA_ID = m_iStartingCA_ID + m_iRowsGeneratedForCust;

		++m_iRowsGeneratedForCust;

		// store state info
		m_bMoreRecords = m_cust.MoreRecords() || m_iRowsGeneratedForCust < m_iRowsToGenForCust;

		return m_row.m_ca.CA_ID;
	}

	/*
	 *   Generate the number (0-2) of additional permission rows for a certain
	 * account. This number is needed by the driver.
	 *
	 *   PARAMETERS:
	 *           IN  CA_ID       - customer account id
	 *
	 *   RETURNS:
	 *           number of ACCOUNT_PERMISSION rows.
	 */
	int GetNumPermsForCA(TIdent CA_ID) {
		RNGSEED OldSeed;
		UINT iThreshold;
		UINT iNumberOfPermissions;

		OldSeed = m_rnd.GetSeed();

		m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseNumberOfAccountPermissions, (RNGSEED)CA_ID));

		iThreshold = m_rnd.RndGenerateIntegerPercentage();

		if (iThreshold <= iPercentAccountAdditionalPermissions_0) {
			iNumberOfPermissions = 0; // 60% of accounts have just the owner row permissions
		} else {
			if (iThreshold <= iPercentAccountAdditionalPermissions_0 + iPercentAccountAdditionalPermissions_1) {
				iNumberOfPermissions = 1; // 38% of accounts have one additional permisison row
			} else {
				iNumberOfPermissions = 2; // 2% of accounts have two additional permission rows
			}
		}

		m_rnd.SetSeed(OldSeed);
		return (iNumberOfPermissions);
	}

	/*
	 *   Generate customer ids for ACCOUNT_PERMISSION table for a given account
	 * id. Driver needs to know what those customer ids are based on the account
	 * id.
	 *
	 *   PARAMETERS:
	 *           IN  CA_ID       - customer account id
	 *           IN  Owner_CID   - customer id of the account owner
	 *           OUT CID_1       - first customer id in the ACL list of the
	 * account OUT CID_2       - second customer id in the ACL list of the
	 * account
	 *
	 *   RETURNS:
	 *           none.
	 */
	void GetCIDsForPermissions(TIdent CA_ID, TIdent Owner_CID, TIdent *CID_1, TIdent *CID_2) {
		RNGSEED OldSeed;

		if (CID_1 == NULL)
			return;

		OldSeed = m_rnd.GetSeed();
		m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseCIDForPermission1, (RNGSEED)CA_ID));

		// Select from a fixed range that doesn't depend on the number of
		// customers in the database. This allows not to specify the total
		// number of customers to EGenLoader, only how many a particular
		// instance needs to generate (may be a fraction of total). Note: this
		// is not implemented right now.
		*CID_1 = m_rnd.RndInt64RangeExclude(iDefaultStartFromCustomer,
		                                    iDefaultStartFromCustomer + iAccountPermissionIDRange, Owner_CID);

		if (CID_2 != NULL) {
			// NOTE: Reseeding the RNG here for the second CID value. The use of
			// this sequence is fuzzy because the number of RNG values consumed
			// is dependant on not only the CA_ID, but also the CID value chosen
			// above for the first permission. Using a different sequence here
			// may help prevent potential overlaps that might occur if the same
			// sequence from above were used.
			m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseCIDForPermission2, (RNGSEED)CA_ID));
			do // make sure the second id is different from the first
			{
				*CID_2 = m_rnd.RndInt64RangeExclude(iDefaultStartFromCustomer,
				                                    iDefaultStartFromCustomer + iAccountPermissionIDRange, Owner_CID);
			} while (*CID_2 == *CID_1);
		}

		m_rnd.SetSeed(OldSeed);
	}

	/*
	 *   Generate tax id for a given CA_ID.
	 *   This is needed to calculate tax on sale proceeds for the TRADE table.
	 *
	 *   PARAMETERS:
	 *           IN  iCA_ID      - customer account id
	 *
	 *   RETURNS:
	 *           tax status for the account.
	 */
	eTaxStatus GetAccountTaxStatus(TIdent iCA_ID) {
		eTaxStatus eCATaxStatus = eNone;

		// We will sometimes get CA values that are outside the current
		// load unit (cached range).  We need to check for this case
		// and avoid the lookup (as we will segfault or get bogus data.)
		TIdent index = iCA_ID - m_iCacheOffsetTS;
		bool bCheckCache = (index >= 0 && index < m_iCacheSizeTS);
		if (m_bCacheEnabled && bCheckCache) {
			eCATaxStatus = m_CacheTS[index];
		}

		if (eCATaxStatus == eNone) {
			RNGSEED OldSeed;
			UINT iThreshold;

			OldSeed = m_rnd.GetSeed();

			m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseAccountTaxStatus, (RNGSEED)iCA_ID));

			iThreshold = m_rnd.RndGenerateIntegerPercentage();
			if (iThreshold <= iPercentAccountTaxStatusNonTaxable) {
				eCATaxStatus = eNonTaxable;
			} else {
				if (iThreshold <= iPercentAccountTaxStatusNonTaxable + iPercentAccountTaxStatusTaxableAndWithhold) {
					eCATaxStatus = eTaxableAndWithhold;
				} else {
					eCATaxStatus = eTaxableAndDontWithhold;
				}
			}
			m_rnd.SetSeed(OldSeed);

			if (m_bCacheEnabled && bCheckCache) {
				m_CacheTS[index] = eCATaxStatus;
			}
		}
		return eCATaxStatus;
	}

	/*
	 *   Get the country and division address codes for the customer that
	 *   owns the current account.
	 *   These codes are used to get the tax rates and calculate tax on trades
	 *   in the TRADE table.
	 *
	 *   PARAMETERS:
	 *           OUT iDivCode        - division (state/province) code
	 *           OUT iCtryCode       - country (USA/CANADA) code
	 *
	 *   RETURNS:
	 *           none.
	 */
	void GetDivisionAndCountryCodesForCurrentAccount(UINT &iDivCode, UINT &iCtryCode) {
		m_addr.GetDivisionAndCountryCodes(iDivCode, iCtryCode);
	}

	/*
	 *   Generate a broker id for a certain account.
	 *   Used in CTradeGen for updating YTD values.
	 *
	 *   PARAMETERS:
	 *           IN  iCA_ID      - customer account id
	 *
	 *   RETURNS:
	 *           broker id that corresponds to the account.
	 */
	TIdent GenerateBrokerIdForAccount(TIdent iCA_ID) {
		//  Customer that own the account (actually, customer id minus 1)
		//
		TIdent iCustomerId = ((iCA_ID - 1) / iMaxAccountsPerCust) - iTIdentShift;

		// Set the starting broker to be the first broker for the current load
		// unit of customers.
		//
		TIdent iStartFromBroker = (iCustomerId / m_iLoadUnitSize) * m_iBrokersCount + iStartingBrokerID + iTIdentShift;

		// Note: this depends on broker ids being integer numbers from
		// contiguous range. The method of generating broker ids should be in
		// sync with the CBrokerTable.
		return m_rnd.RndNthInt64Range(RNGSeedBaseBrokerId, (RNGSEED)iCA_ID - (10 * iTIdentShift), iStartFromBroker,
		                              iStartFromBroker + m_iBrokersCount - 1);
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
		GenerateCARow();
		GenerateAPRows();

		// Return false if all the rows have been generated
		return (MoreRecords());
	}

	/*
	 *   Return CUSTOMER_ACCOUNT row from the internal structure.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           current CUSTOMER_ACCOUNT record.
	 */
	// PCUSTOMER_ACCOUNT_ROW   GetCARow() {return &m_row.m_ca;}
	const CUSTOMER_ACCOUNT_ROW &GetCARow() {
		return m_row.m_ca;
	}
	/*
	 *   Return ACCOUNT_PERMISSION row from the internal structure.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           current ACCOUNT_PERMISSION record.
	 */
	const ACCOUNT_PERMISSION_ROW &GetAPRow(UINT i) {
		if (i < m_iPermsForCA)
			return m_row.m_perm[i];
		else
			throw std::range_error("Account Permission row index out of bounds.");
	}

	/*
	 *   Return the number of ACCOUNT_PERMISSION rows for the current
	 * CUSTOMER_ACCOUNT row.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           the number of permissions for the account.
	 */
	UINT GetCAPermsCount() {
		return m_iPermsForCA;
	}

	/*
	 *   Return the customer ID for the currently generated CA_ID id.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           customer id of the account in the current CUSTOMER_ACCOUNT
	 * record.
	 */
	TIdent GetCurrentC_ID() {
		return m_cust.GetCurrentC_ID();
	}
	/*
	 *   Return the customer tier for the currently generated CA_ID id.
	 *
	 *   PARAMETERS:
	 *           none.
	 *
	 *   RETURNS:
	 *           customer tier of the customer, whose account is in the current
	 * CUSTOMER_ACCOUNT record.
	 */
	eCustomerTier GetCurrentC_TIER() {
		return m_cust.GetC_TIER(m_cust.GetCurrentC_ID());
	}
};

} // namespace TPCE

#endif // CUSTOMER_ACCOUNTS_AND_PERMISSIONS_TABLE_H
