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
 * - Doug Johnson, Matt Emmerton
 */

/******************************************************************************
 *   Description:        Implementation of the CE class.
 *                       See CE.h for a description.
 ******************************************************************************/

#include "main/CE.h"

using namespace TPCE;

// Initialization that is common for all constructors.
void CCE::Initialize(PDriverCETxnSettings pTxnParamSettings) {
	m_pLogger->SendToLogger(m_DriverGlobalSettings);

	// Always configure the CE with default settings, to ensure that it is
	// set up properly.
	SetTxnTunables(&m_DriverCETxnSettings);

	// If user tunables are provided, set them now.  If they are invalid, they
	// will not be used and we will continue to use the defaults set above.
	if (pTxnParamSettings) {
		SetTxnTunables(pTxnParamSettings);
	}
}

// Automatically generate unique RNG seeds.
// The CRandom class uses an unsigned 64-bit value for the seed.
// This routine automatically generates two unique seeds. One is used for
// the TxnInput generator RNG, and the other is for the TxnMixGenerator RNG.
// The 64 bits are used as follows.
//
//  Bits    0 - 31  Caller provided unique unsigned 32-bit id.
//  Bit     32      0 for TxnInputGenerator, 1 for TxnMixGenerator
//  Bits    33 - 43 Number of days since the base time. The base time
//                  is set to be January 1 of the most recent year that is
//                  a multiple of 5. This allows enough space for the last
//                  field, and it makes the algorithm "timeless" by resetting
//                  the generated values every 5 years.
//  Bits    44 - 63 Current time of day measured in 1/10's of a second.
//
void CCE::AutoSetRNGSeeds(UINT32 UniqueId) {
	CDateTime Now;
	INT32 BaseYear;
	INT32 Tmp1, Tmp2;

	Now.GetYMD(&BaseYear, &Tmp1, &Tmp2);

	// Set the base year to be the most recent year that was a multiple of 5.
	BaseYear -= (BaseYear % 5);
	CDateTime Base(BaseYear, 1, 1); // January 1st in the BaseYear

	// Initialize the seed with the current time of day measured in 1/10's of a
	// second. This will use up to 20 bits.
	RNGSEED Seed;
	Seed = Now.MSec() / 100;

	// Now add in the number of days since the base time.
	// The number of days in the 5 year period requires 11 bits.
	// So shift up by that much to make room in the "lower" bits.
	Seed <<= 11;
	Seed += (RNGSEED)((INT64)Now.DayNo() - (INT64)Base.DayNo());

	// So far, we've used up 31 bits.
	// Save the "last" bit of the "upper" 32 for the RNG id.
	// In addition, make room for the caller's 32-bit unique id.
	// So shift a total of 33 bits.
	Seed <<= 33;

	// Now the "upper" 32-bits have been set with a value for RNG 0.
	// Add in the sponsor's unique id for the "lower" 32-bits.
	Seed += UniqueId;

	// Set the TxnMixGenerator RNG to the unique seed.
	m_TxnMixGenerator.SetRNGSeed(Seed);
	m_DriverCESettings.cur.TxnMixRNGSeed = Seed;

	// Set the RNG Id to 1 for the TxnInputGenerator.
	Seed |= UINT64_CONST(0x0000000100000000);
	m_TxnInputGenerator.SetRNGSeed(Seed);
	m_DriverCESettings.cur.TxnInputRNGSeed = Seed;
}

/*
 * Constructor - no partitioning by C_ID, automatic RNG seed generation
 * (requires unique input)
 */
CCE::CCE(CCESUTInterface *pSUT, CBaseLogger *pLogger, const DataFileManager &dfm, TIdent iConfiguredCustomerCount,
         TIdent iActiveCustomerCount, INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId,
         const PDriverCETxnSettings pDriverCETxnSettings)
    : m_DriverGlobalSettings(iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades),
      m_DriverCESettings(UniqueId, 0, 0), m_pSUT(pSUT), m_pLogger(pLogger),
      m_TxnMixGenerator(&m_DriverCETxnSettings, m_pLogger),
      m_TxnInputGenerator(dfm, iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
                          iDaysOfInitialTrades * HoursPerWorkDay, m_pLogger, &m_DriverCETxnSettings) {
	m_pLogger->SendToLogger("CE object constructed using constructor 1 (valid "
	                        "for publication: YES).");

	Initialize(pDriverCETxnSettings);
	AutoSetRNGSeeds(UniqueId);

	m_pLogger->SendToLogger(m_DriverCESettings); // log the RNG seeds
}

/*
 * Constructor - no partitioning by C_ID, RNG seeds provided
 */
CCE::CCE(CCESUTInterface *pSUT, CBaseLogger *pLogger, const DataFileManager &dfm, TIdent iConfiguredCustomerCount,
         TIdent iActiveCustomerCount, INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId,
         RNGSEED TxnMixRNGSeed, RNGSEED TxnInputRNGSeed, const PDriverCETxnSettings pDriverCETxnSettings)
    : m_DriverGlobalSettings(iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades),
      m_DriverCESettings(UniqueId, TxnMixRNGSeed, TxnInputRNGSeed), m_pSUT(pSUT), m_pLogger(pLogger),
      m_TxnMixGenerator(&m_DriverCETxnSettings, TxnMixRNGSeed, m_pLogger),
      m_TxnInputGenerator(dfm, iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
                          iDaysOfInitialTrades * HoursPerWorkDay, TxnInputRNGSeed, m_pLogger, &m_DriverCETxnSettings) {
	m_pLogger->SendToLogger("CE object constructed using constructor 2 (valid "
	                        "for publication: NO).");

	Initialize(pDriverCETxnSettings);

	m_pLogger->SendToLogger(m_DriverCESettings); // log the RNG seeds
}

/*
 * Constructor - partitioning by C_ID, automatic RNG seed generation (requires
 * unique input)
 */
CCE::CCE(CCESUTInterface *pSUT, CBaseLogger *pLogger, const DataFileManager &dfm, TIdent iConfiguredCustomerCount,
         TIdent iActiveCustomerCount, TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
         INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId,
         const PDriverCETxnSettings pDriverCETxnSettings)
    : m_DriverGlobalSettings(iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades),
      m_DriverCESettings(UniqueId, 0, 0),
      m_DriverCEPartitionSettings(iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent), m_pSUT(pSUT),
      m_pLogger(pLogger), m_TxnMixGenerator(&m_DriverCETxnSettings, m_pLogger),
      m_TxnInputGenerator(dfm, iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
                          iDaysOfInitialTrades * HoursPerWorkDay, iMyStartingCustomerId, iMyCustomerCount,
                          iPartitionPercent, m_pLogger, &m_DriverCETxnSettings) {
	m_pLogger->SendToLogger("CE object constructed using constructor 3 (valid "
	                        "for publication: YES).");

	Initialize(pDriverCETxnSettings);
	AutoSetRNGSeeds(UniqueId);

	m_pLogger->SendToLogger(m_DriverCEPartitionSettings); // log the partition settings
	m_pLogger->SendToLogger(m_DriverCESettings);          // log the RNG seeds
}

/*
 * Constructor - partitioning by C_ID, RNG seeds provided
 */
CCE::CCE(CCESUTInterface *pSUT, CBaseLogger *pLogger, const DataFileManager &dfm, TIdent iConfiguredCustomerCount,
         TIdent iActiveCustomerCount, TIdent iMyStartingCustomerId, TIdent iMyCustomerCount, INT32 iPartitionPercent,
         INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId, RNGSEED TxnMixRNGSeed,
         RNGSEED TxnInputRNGSeed, const PDriverCETxnSettings pDriverCETxnSettings)
    : m_DriverGlobalSettings(iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades),
      m_DriverCESettings(UniqueId, TxnMixRNGSeed, TxnInputRNGSeed),
      m_DriverCEPartitionSettings(iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent), m_pSUT(pSUT),
      m_pLogger(pLogger), m_TxnMixGenerator(&m_DriverCETxnSettings, TxnMixRNGSeed, m_pLogger),
      m_TxnInputGenerator(dfm, iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
                          iDaysOfInitialTrades * HoursPerWorkDay, iMyStartingCustomerId, iMyCustomerCount,
                          iPartitionPercent, TxnInputRNGSeed, m_pLogger, &m_DriverCETxnSettings) {
	m_pLogger->SendToLogger("CE object constructed using constructor 4 (valid "
	                        "for publication: NO).");

	Initialize(pDriverCETxnSettings);

	m_pLogger->SendToLogger(m_DriverCEPartitionSettings); // log the partition settings
	m_pLogger->SendToLogger(m_DriverCESettings);          // log the RNG seeds
}

CCE::~CCE(void) {
	m_pLogger->SendToLogger("CE object destroyed.");
}

RNGSEED CCE::GetTxnInputGeneratorRNGSeed(void) {
	return (m_TxnInputGenerator.GetRNGSeed());
}

RNGSEED CCE::GetTxnMixGeneratorRNGSeed(void) {
	return (m_TxnMixGenerator.GetRNGSeed());
}

void CCE::SetTxnTunables(const PDriverCETxnSettings pTxnParamSettings) {
	if (pTxnParamSettings->IsValid()) {
		// Update Tunables
		if (pTxnParamSettings != &m_DriverCETxnSettings) // only copy from a different location
		{
			m_DriverCETxnSettings = *pTxnParamSettings;
		}

		// Trigger Runtime Updates
		m_TxnMixGenerator.UpdateTunables();
		m_TxnInputGenerator.UpdateTunables();
	} else {
		m_pLogger->SendToLogger("ERROR: CCE::SetTxnTunables() failed due to invalid tunables.");
	}
}

void CCE::DoTxn(void) {
	int iTxnType = m_TxnMixGenerator.GenerateNextTxnType();

	switch (iTxnType) {
	case CCETxnMixGenerator::BROKER_VOLUME:
		m_TxnInputGenerator.GenerateBrokerVolumeInput(m_BrokerVolumeTxnInput);
		m_pSUT->BrokerVolume(&m_BrokerVolumeTxnInput);
		break;
	case CCETxnMixGenerator::CUSTOMER_POSITION:
		m_TxnInputGenerator.GenerateCustomerPositionInput(m_CustomerPositionTxnInput);
		m_pSUT->CustomerPosition(&m_CustomerPositionTxnInput);
		break;
	case CCETxnMixGenerator::MARKET_WATCH:
		m_TxnInputGenerator.GenerateMarketWatchInput(m_MarketWatchTxnInput);
		m_pSUT->MarketWatch(&m_MarketWatchTxnInput);
		break;
	case CCETxnMixGenerator::SECURITY_DETAIL:
		m_TxnInputGenerator.GenerateSecurityDetailInput(m_SecurityDetailTxnInput);
		m_pSUT->SecurityDetail(&m_SecurityDetailTxnInput);
		break;
	case CCETxnMixGenerator::TRADE_LOOKUP:
		m_TxnInputGenerator.GenerateTradeLookupInput(m_TradeLookupTxnInput);
		m_pSUT->TradeLookup(&m_TradeLookupTxnInput);
		break;
	case CCETxnMixGenerator::TRADE_ORDER:
		bool bExecutorIsAccountOwner;
		INT32 iTradeType;
		m_TxnInputGenerator.GenerateTradeOrderInput(m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner);
		m_pSUT->TradeOrder(&m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner);
		break;
	case CCETxnMixGenerator::TRADE_STATUS:
		m_TxnInputGenerator.GenerateTradeStatusInput(m_TradeStatusTxnInput);
		m_pSUT->TradeStatus(&m_TradeStatusTxnInput);
		break;
	case CCETxnMixGenerator::TRADE_UPDATE:
		m_TxnInputGenerator.GenerateTradeUpdateInput(m_TradeUpdateTxnInput);
		m_pSUT->TradeUpdate(&m_TradeUpdateTxnInput);
		break;
	default:
		cerr << "CE: Generated illegal transaction" << endl;
		exit(1);
	}
}
