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
 * - Gregory Dake, Cecil Reames, Doug Johnson, Matt Emmerton
 */

/******************************************************************************
 *   Description:        Implementation of CTxnGeneration class.
 *                       See TxnMixGenerator.h for description.
 ******************************************************************************/

#include "main/CETxnMixGenerator.h"

using namespace TPCE;

CCETxnMixGenerator::CCETxnMixGenerator(const PDriverCETxnSettings pDriverCETxnSettings, CBaseLogger *pLogger)
    : m_pDriverCETxnSettings(pDriverCETxnSettings), m_rnd(RNGSeedBaseTxnMixGenerator) // initialize with default seed
      ,
      m_pLogger(pLogger), m_iTxnArrayCurrentIndex(0), m_pTxnArray(NULL) {
	// UpdateTunables() is called from CCE constructor (Initialize)
}

CCETxnMixGenerator::CCETxnMixGenerator(const PDriverCETxnSettings pDriverCETxnSettings, RNGSEED RNGSeed,
                                       CBaseLogger *pLogger)
    : m_pDriverCETxnSettings(pDriverCETxnSettings), m_rnd(RNGSeed) // seed is provided for us
      ,
      m_pLogger(pLogger), m_iTxnArrayCurrentIndex(0), m_pTxnArray(NULL) {
	// UpdateTunables() is called from CCE constructor (Initialize)
}

RNGSEED CCETxnMixGenerator::GetRNGSeed(void) {
	return (m_rnd.GetSeed());
}

void CCETxnMixGenerator::SetRNGSeed(RNGSEED RNGSeed) {
	m_rnd.SetSeed(RNGSeed);
}

CCETxnMixGenerator::~CCETxnMixGenerator() {
	if (m_pTxnArray != NULL) {
		delete[] m_pTxnArray;
		m_pTxnArray = NULL;
	}
}

void CCETxnMixGenerator::UpdateTunables(void) {
	INT32 i;
	INT32 BrokerVolumeMixLimit;
	INT32 CustomerPositionMixLimit;
	INT32 MarketWatchMixLimit;
	INT32 SecurityDetailMixLimit;
	INT32 TradeLookupMixLimit;
	INT32 TradeOrderMixLimit;
	INT32 TradeStatusMixLimit;
	INT32 TradeUpdateMixLimit;

	// Add all the weights together
	m_CETransactionMixTotal = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.BrokerVolumeMixLevel +
	                          m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.CustomerPositionMixLevel +
	                          m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.MarketWatchMixLevel +
	                          m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.SecurityDetailMixLevel +
	                          m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeLookupMixLevel +
	                          m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeOrderMixLevel +
	                          m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeStatusMixLevel +
	                          m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeUpdateMixLevel;

	TradeStatusMixLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeStatusMixLevel;
	MarketWatchMixLimit =
	    m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.MarketWatchMixLevel + TradeStatusMixLimit;
	SecurityDetailMixLimit =
	    m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.SecurityDetailMixLevel + MarketWatchMixLimit;
	CustomerPositionMixLimit =
	    m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.CustomerPositionMixLevel + SecurityDetailMixLimit;
	TradeOrderMixLimit =
	    m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeOrderMixLevel + CustomerPositionMixLimit;
	TradeLookupMixLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeLookupMixLevel + TradeOrderMixLimit;
	TradeUpdateMixLimit =
	    m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeUpdateMixLevel + TradeLookupMixLimit;
	BrokerVolumeMixLimit =
	    m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.BrokerVolumeMixLevel + TradeUpdateMixLimit;

	// Reset the random transaction array.
	//
	if (m_pTxnArray != NULL) {
		delete[] m_pTxnArray;
		m_pTxnArray = NULL;
	}

	m_pTxnArray = new char[m_CETransactionMixTotal];

	// Initialize the array with transaction types.
	//
	for (i = 0; i < TradeStatusMixLimit; ++i) {
		m_pTxnArray[i] = TRADE_STATUS;
	}
	for (; i < MarketWatchMixLimit; ++i) {
		m_pTxnArray[i] = MARKET_WATCH;
	}
	for (; i < SecurityDetailMixLimit; ++i) {
		m_pTxnArray[i] = SECURITY_DETAIL;
	}
	for (; i < CustomerPositionMixLimit; ++i) {
		m_pTxnArray[i] = CUSTOMER_POSITION;
	}
	for (; i < TradeOrderMixLimit; ++i) {
		m_pTxnArray[i] = TRADE_ORDER;
	}
	for (; i < TradeLookupMixLimit; ++i) {
		m_pTxnArray[i] = TRADE_LOOKUP;
	}
	for (; i < TradeUpdateMixLimit; ++i) {
		m_pTxnArray[i] = TRADE_UPDATE;
	}
	for (; i < BrokerVolumeMixLimit; ++i) {
		m_pTxnArray[i] = BROKER_VOLUME;
	}

	m_iTxnArrayCurrentIndex = 0; // reset the current element index

	// Log Tunables
	m_pLogger->SendToLogger(m_pDriverCETxnSettings->TxnMixGenerator_settings);
}

int CCETxnMixGenerator::GenerateNextTxnType() {
	//  Select the next transaction type using the "card deck shuffle"
	//  algorithm (also Knuth algorithm) that guarantees a certain number
	//  of transactions of each type is returned.
	//
	//  1) Get a 32-bit random number.
	//  2) Use random number to select next transaction type from m_pTxnArray
	//  in the range [m_iTxnArrayCurrentIndex, m_CETransactionMixTotal).
	//  3) Swap the selected random element in m_pTxnArray
	//  with m_pTxnArray[m_iTxnArrayCurrentIndex].
	//  4) Increment m_iTxnArrayCurrentIndex to remove the returned
	//  transaction type from further consideration.
	//
	INT32 rnd = m_rnd.RndIntRange(m_iTxnArrayCurrentIndex, m_CETransactionMixTotal - 1);

	char iTxnType = m_pTxnArray[rnd];

	// Swap two array entries.
	//
	m_pTxnArray[rnd] = m_pTxnArray[m_iTxnArrayCurrentIndex];
	m_pTxnArray[m_iTxnArrayCurrentIndex] = iTxnType;

	m_iTxnArrayCurrentIndex = (m_iTxnArrayCurrentIndex + 1) % m_CETransactionMixTotal;

	return iTxnType;
}
