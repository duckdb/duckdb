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
 *   Class representing the Holdings, Trades, Trade Request, Settlement, Trade
 * History, and Cash Transaction tables.
 */
#ifndef HOLDINGS_AND_TRADES_TABLE_H
#define HOLDINGS_AND_TRADES_TABLE_H

#include "EGenTables_common.h"
#include "CustomerAccountsAndPermissionsTable.h"
#include "SecurityPriceRange.h"

#include "input/DataFileManager.h"

namespace TPCE {

// Arrays for min and max bounds on the security ranges for different tier
// accounts The indices into these arrays are
//      1) the customer tier (zero based)
//      2) the number of accounts for the customer (zero based)
// Entries with 0 mean there cannot be that many accounts for a customer with
// that tier.
//
const int iMinSecuritiesPerAccountRange[3][10] = {
    {6, 4, 2, 2, 0, 0, 0, 0, 0, 0}, {0, 7, 5, 4, 3, 2, 2, 2, 0, 0}, {0, 0, 0, 0, 4, 4, 3, 3, 2, 2}};
const int iMaxSecuritiesPerAccountRange[3][10] = {{14, 16, 18, 18, 00, 00, 00, 00, 00, 00},
                                                  {00, 13, 15, 16, 17, 18, 18, 18, 00, 00},
                                                  {00, 00, 00, 00, 16, 16, 17, 17, 18, 18}};
const int iMaxSecuritiesPerAccount = 18; // maximum number of securities in a customer account

// const double fMinSecPrice = 20;
// const double fMaxSecPrice = 30;

// These are used for picking the transaction type at load time.
// NOTE that the corresponding "if" tests must be in the same order!
const int cMarketBuyLoadThreshold = 30;                            //  1% - 30%
const int cMarketSellLoadThreshold = cMarketBuyLoadThreshold + 30; // 31% - 60%
const int cLimitBuyLoadThreshold = cMarketSellLoadThreshold + 20;  // 61% - 80%
const int cLimitSellLoadThreshold = cLimitBuyLoadThreshold + 10;   // 81% - 90%
const int cStopLossLoadThreshold = cLimitSellLoadThreshold + 10;   // 91% - 100%

const int iPercentBuysOnMargin = 16;

// These are used when loading the table, and when generating runtime data.
const int cNUM_TRADE_QTY_SIZES = 4;
const int cTRADE_QTY_SIZES[cNUM_TRADE_QTY_SIZES] = {100, 200, 400, 800};

// Percentage of trades modifying holdings in Last-In-First-Out order.
//
const int iPercentTradeIsLIFO = 35;

// Number of RNG calls for one simulated trade
const int iRNGSkipOneTrade = 11; // average count for v3.5: 6.5

class CHoldingsAndTradesTable {
	CRandom m_rnd;
	CCustomerAccountsAndPermissionsTable m_CustomerAccountTable;

	TIdent m_iSecCount;         // number of securities
	UINT m_iMaxSecuritiesPerCA; // number of securities per account
	TIdent m_SecurityIds[iMaxSecuritiesPerAccount];
	bool m_bCacheEnabled;
	TIdent m_iCacheOffsetNS;
	int m_iCacheSizeNS;
	int *m_CacheNS;
	TIdent m_iCacheOffsetSFFI;
	int m_iCacheSizeSFFI;
	TIdent *m_CacheSFFI;

public:
	// Constructor.
	CHoldingsAndTradesTable(const DataFileManager &dfm,
	                        UINT iLoadUnitSize, // # of customers in one load unit
	                        TIdent iCustomerCount, TIdent iStartFromCustomer = iDefaultStartFromCustomer,
	                        bool bCacheEnabled = false)
	    : m_rnd(RNGSeedTableDefault),
	      m_CustomerAccountTable(dfm, iLoadUnitSize, iCustomerCount, iStartFromCustomer, bCacheEnabled),
	      m_bCacheEnabled(bCacheEnabled) {
		m_iSecCount = dfm.SecurityFile().GetConfiguredSecurityCount();

		// Set the max number of holdings per account to be
		// iMaxSecuritiesPerAccount
		//
		m_iMaxSecuritiesPerCA = iMaxSecuritiesPerAccount;

		if (m_bCacheEnabled) {
			m_iCacheSizeNS = iDefaultLoadUnitSize * iMaxAccountsPerCust;
			m_iCacheOffsetNS =
			    m_CustomerAccountTable.GetStartingCA_ID(iStartFromCustomer) + (iTIdentShift * iMaxAccountsPerCust);
			m_CacheNS = new int[m_iCacheSizeNS];
			for (int i = 0; i < m_iCacheSizeNS; i++) {
				m_CacheNS[i] = 0;
			}

			m_iCacheSizeSFFI = iDefaultLoadUnitSize * iMaxAccountsPerCust * iMaxSecuritiesPerAccount;
			m_iCacheOffsetSFFI =
			    m_CustomerAccountTable.GetStartingCA_ID(iStartFromCustomer) + (iTIdentShift * iMaxAccountsPerCust);
			m_CacheSFFI = new TIdent[m_iCacheSizeSFFI];
			for (int i = 0; i < m_iCacheSizeSFFI; i++) {
				m_CacheSFFI[i] = -1;
			}
		}
	};

	// Destructor
	~CHoldingsAndTradesTable() {
		if (m_bCacheEnabled) {
			delete[] m_CacheNS;
			delete[] m_CacheSFFI;
		}
	};

	/*
	 *   Reset the state for the next load unit.
	 *   Called only from the loader (CTradeGen), not the driver.
	 */
	void InitNextLoadUnit(INT64 TradesToSkip, TIdent iStartingAccountID) {
		m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault,
		                                  // there is only 1 call to this RNG per trade
		                                  (RNGSEED)TradesToSkip));
		if (m_bCacheEnabled) {
			m_iCacheOffsetNS = iStartingAccountID;
			for (int i = 0; i < m_iCacheSizeNS; i++) {
				m_CacheNS[i] = 0;
			}

			m_iCacheOffsetSFFI = iStartingAccountID;
			for (int i = 0; i < m_iCacheSizeSFFI; i++) {
				m_CacheSFFI[i] = -1;
			}
		}

		m_CustomerAccountTable.InitNextLoadUnit();
	}

	/*
	 *   Generate the number of securities for a given customer account.
	 */
	int GetNumberOfSecurities(TIdent iCA_ID, eCustomerTier iTier, int iAccountCount) {
		int iNumberOfSecurities = 0;

		// We will sometimes get CA_ID values that are outside the current
		// load unit (cached range).  We need to check for this case
		// and avoid the lookup (as we will segfault or get bogus data.)
		TIdent index = iCA_ID - m_iCacheOffsetNS;
		bool bCheckCache = (index >= 0 && index < m_iCacheSizeNS);
		if (m_bCacheEnabled && bCheckCache) {
			iNumberOfSecurities = m_CacheNS[index];
		}

		if (iNumberOfSecurities == 0) {
			RNGSEED OldSeed;
			int iMinRange, iMaxRange;

			iMinRange = iMinSecuritiesPerAccountRange[iTier - eCustomerTierOne][iAccountCount - 1];
			iMaxRange = iMaxSecuritiesPerAccountRange[iTier - eCustomerTierOne][iAccountCount - 1];

			OldSeed = m_rnd.GetSeed();
			m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedBaseNumberOfSecurities, (RNGSEED)iCA_ID));
			iNumberOfSecurities = m_rnd.RndIntRange(iMinRange, iMaxRange);
			m_rnd.SetSeed(OldSeed);

			if (m_bCacheEnabled && bCheckCache) {
				m_CacheNS[index] = iNumberOfSecurities;
			}
		}
		return iNumberOfSecurities;
	}

	/*
	 *   Get seed for the starting security ID seed for a given customer id.
	 */
	RNGSEED GetStartingSecIDSeed(TIdent iCA_ID) {
		return (m_rnd.RndNthElement(RNGSeedBaseStartingSecurityID, (RNGSEED)iCA_ID * m_iMaxSecuritiesPerCA));
	}

	/*
	 *   Convert security index within an account (1-18) into
	 *   corresponding security index within the
	 *   Security.txt input file (0-6849).
	 *
	 *   Needed to be able to get the security symbol
	 *   and other information from the input file.
	 *
	 *   RETURNS:
	 *           security index within the input file (0-based)
	 */
	TIdent GetSecurityFlatFileIndex(TIdent iCustomerAccount, UINT iSecurityAccountIndex) {
		TIdent iSecurityFlatFileIndex = -1;

		// We will sometimes get CA_ID values that are outside the current
		// load unit (cached range).  We need to check for this case
		// and avoid the lookup (as we will segfault or get bogus data.)
		TIdent index = (iCustomerAccount - m_iCacheOffsetSFFI) * iMaxSecuritiesPerAccount + iSecurityAccountIndex - 1;
		bool bCheckCache = (index >= 0 && index < m_iCacheSizeSFFI);
		if (m_bCacheEnabled && bCheckCache) {
			iSecurityFlatFileIndex = m_CacheSFFI[index];
		}

		if (iSecurityFlatFileIndex == -1) {
			RNGSEED OldSeed;
			UINT iGeneratedIndexCount = 0; // number of currently generated unique flat file indexes
			UINT i;

			OldSeed = m_rnd.GetSeed();
			m_rnd.SetSeed(GetStartingSecIDSeed(iCustomerAccount));

			iGeneratedIndexCount = 0;

			while (iGeneratedIndexCount < iSecurityAccountIndex) {
				iSecurityFlatFileIndex = m_rnd.RndInt64Range(0, m_iSecCount - 1);

				for (i = 0; i < iGeneratedIndexCount; ++i) {
					if (m_SecurityIds[i] == iSecurityFlatFileIndex)
						break;
				}

				// If a duplicate is found, overwrite it in the same location
				// so basically no changes are made.
				//
				m_SecurityIds[i] = iSecurityFlatFileIndex;

				// If no duplicate is found, increment the count of unique ids
				//
				if (i == iGeneratedIndexCount) {
					++iGeneratedIndexCount;
				}
			}

			m_rnd.SetSeed(OldSeed);

			if (m_bCacheEnabled && bCheckCache) {
				m_CacheSFFI[index] = iSecurityFlatFileIndex;
			}
		}
		return iSecurityFlatFileIndex;
	}

	/*
	 *   Generate random customer account and security to perfrom a trade on.
	 *   This function is used by both the runtime driver (CCETxnInputGenerator)
	 * and by the loader when generating initial trades (CTradeGen).
	 *
	 */
	void GenerateRandomAccountSecurity(TIdent iCustomer,                // in
	                                   eCustomerTier iTier,             // in
	                                   TIdent *piCustomerAccount,       // out
	                                   TIdent *piSecurityFlatFileIndex, // out
	                                   UINT *piSecurityAccountIndex)    // out
	{
		TIdent iCustomerAccount;
		int iAccountCount;
		int iTotalAccountSecurities;
		UINT iSecurityAccountIndex;    // index of the selected security in the
		                               // account's basket
		TIdent iSecurityFlatFileIndex; // index of the selected security in the
		                               // input flat file

		// Select random account for the customer
		//
		m_CustomerAccountTable.GenerateRandomAccountId(m_rnd, iCustomer, iTier, &iCustomerAccount, &iAccountCount);

		iTotalAccountSecurities = GetNumberOfSecurities(iCustomerAccount, iTier, iAccountCount);

		// Select random security in the account
		//
		iSecurityAccountIndex = (UINT)m_rnd.RndIntRange(1, iTotalAccountSecurities);

		iSecurityFlatFileIndex = GetSecurityFlatFileIndex(iCustomerAccount, iSecurityAccountIndex);

		// Return data
		//
		*piCustomerAccount = iCustomerAccount;
		*piSecurityFlatFileIndex = iSecurityFlatFileIndex;
		if (piSecurityAccountIndex != NULL) {
			*piSecurityAccountIndex = iSecurityAccountIndex;
		}
	}

	bool IsAbortedTrade(TIdent TradeId) {
		bool bResult = false;
		if (iAbortedTradeModFactor == TradeId % iAbortTrade) {
			bResult = true;
		}
		return bResult;
	}
};

} // namespace TPCE

#endif // HOLDINGS_AND_TRADES_TABLE_H
