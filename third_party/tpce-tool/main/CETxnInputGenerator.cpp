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
 * - Sergey Vasilevskiy, Doug Johnson, Matt Emmerton
 */

#include "main/CETxnInputGenerator.h"
#include "main/DailyMarketTable.h"
#include "main/TradeTypeIDs.h"

using namespace TPCE;

/*
 *  Constructor - no partitioning by C_ID.
 *
 *  PARAMETERS:
 *           IN  dfm                         - Data file manager
 *           IN  iConfiguredCustomerCount    - number of configured customers in
 * the database IN  iActiveCustomerCount        - number of active customers in
 * the database IN  iScaleFactor                - scale factor (number of
 * customers per 1 tpsE) of the database IN  iHoursOfInitialTrades       -
 * number of hours of the initial trades portion of the database IN  pLogger -
 * reference to parameter logging object IN  pDriverCETxnSettings        -
 * initial transaction parameter settings
 *
 *  RETURNS:
 *           not applicable.
 */
CCETxnInputGenerator::CCETxnInputGenerator(const DataFileManager &dfm, TIdent iConfiguredCustomerCount,
                                           TIdent iActiveCustomerCount, INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
                                           CBaseLogger *pLogger,
                                           const PDriverCETxnSettings pDriverCETxnSettings)
    : m_rnd(RNGSeedBaseTxnInputGenerator) // initialize with a default seed
      ,
      m_Person(dfm, 0, false), m_CustomerSelection(&m_rnd, iDefaultStartFromCustomer, iActiveCustomerCount),
      m_AccsAndPerms(dfm, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer),
      m_Holdings(dfm, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer),
      m_Brokers(dfm, iActiveCustomerCount, iDefaultStartFromCustomer), m_pCompanies(dfm.CompanyFile()),
      m_pSecurities(dfm.SecurityFile()), m_pIndustries(dfm.IndustryDataFile()), m_pSectors(dfm.SectorDataFile()),
      m_pStatusType(dfm.StatusTypeDataFile()), m_pTradeType(dfm.TradeTypeDataFile()),
      m_pDriverCETxnSettings(pDriverCETxnSettings), m_pLogger(pLogger),
      m_iConfiguredCustomerCount(iConfiguredCustomerCount), m_iActiveCustomerCount(iActiveCustomerCount),
      m_iMyStartingCustomerId(iDefaultStartFromCustomer), m_iMyCustomerCount(iActiveCustomerCount),
      m_iPartitionPercent(100), m_iScaleFactor(iScaleFactor), m_iHoursOfInitialTrades(iHoursOfInitialTrades) {
	Initialize();
}

/*
 *  Constructor - no partitioning by C_ID, RNG seed provided.
 *
 *  RNG seed is for testing/engineering work allowing repeatable transaction
 *  parameter stream. This constructor is NOT legal for a benchmark publication.
 *
 *  PARAMETERS:
 *           IN  dfm                         - Data file manager
 *           IN  iConfiguredCustomerCount    - number of configured customers in
 * the database IN  iActiveCustomerCount        - number of active customers in
 * the database IN  iScaleFactor                - scale factor (number of
 * customers per 1 tpsE) of the database IN  iHoursOfInitialTrades       -
 * number of hours of the initial trades portion of the database IN  RNGSeed -
 * initial seed for random number generator IN  pLogger                     -
 * reference to parameter logging object IN  pDriverCETxnSettings        -
 * initial transaction parameter settings
 *
 *  RETURNS:
 *           not applicable.
 */
CCETxnInputGenerator::CCETxnInputGenerator(const DataFileManager &dfm, TIdent iConfiguredCustomerCount,
                                           TIdent iActiveCustomerCount, INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
                                           RNGSEED RNGSeed, CBaseLogger *pLogger,
                                           const PDriverCETxnSettings pDriverCETxnSettings)
    : m_rnd(RNGSeed) // to be predictable
      ,
      m_Person(dfm, 0, false), m_CustomerSelection(&m_rnd, iDefaultStartFromCustomer, iActiveCustomerCount),
      m_AccsAndPerms(dfm, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer),
      m_Holdings(dfm, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer),
      m_Brokers(dfm, iActiveCustomerCount, iDefaultStartFromCustomer), m_pCompanies(dfm.CompanyFile()),
      m_pSecurities(dfm.SecurityFile()), m_pIndustries(dfm.IndustryDataFile()), m_pSectors(dfm.SectorDataFile()),
      m_pStatusType(dfm.StatusTypeDataFile()), m_pTradeType(dfm.TradeTypeDataFile()),
      m_pDriverCETxnSettings(pDriverCETxnSettings), m_pLogger(pLogger),
      m_iConfiguredCustomerCount(iConfiguredCustomerCount), m_iActiveCustomerCount(iActiveCustomerCount),
      m_iMyStartingCustomerId(iDefaultStartFromCustomer), m_iMyCustomerCount(iActiveCustomerCount),
      m_iPartitionPercent(100), m_iScaleFactor(iScaleFactor), m_iHoursOfInitialTrades(iHoursOfInitialTrades) {
	Initialize();
}

/*
 *  Constructor - partitioning by C_ID.
 *
 *  PARAMETERS:
 *           IN  dfm                         - Data file manager
 *           IN  iConfiguredCustomerCount    - number of configured customers in
 * the database IN  iActiveCustomerCount        - number of active customers in
 * the database IN  iScaleFactor                - scale factor (number of
 * customers per 1 tpsE) of the database IN  iHoursOfInitialTrades       -
 * number of hours of the initial trades portion of the database IN
 * iMyStartingCustomerId       - first customer id (1-based) of the partition
 * for this instance IN  iMyCustomerCount            - number of customers in
 * the partition for this instance IN  iPartitionPercent           - the
 * percentage of C_IDs generated within this instance's partition IN  pLogger -
 * reference to parameter logging object IN  pDriverCETxnSettings        -
 * initial transaction parameter settings
 *
 *  RETURNS:
 *           not applicable.
 */
CCETxnInputGenerator::CCETxnInputGenerator(const DataFileManager &dfm, TIdent iConfiguredCustomerCount,
                                           TIdent iActiveCustomerCount, INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
                                           TIdent iMyStartingCustomerId, TIdent iMyCustomerCount,
                                           INT32 iPartitionPercent, CBaseLogger *pLogger,
                                           const PDriverCETxnSettings pDriverCETxnSettings)
    : m_rnd(RNGSeedBaseTxnInputGenerator) // initialize with a default seed
      ,
      m_Person(dfm, 0, false), m_CustomerSelection(&m_rnd, iDefaultStartFromCustomer, iActiveCustomerCount,
                                                   iPartitionPercent, iMyStartingCustomerId, iMyCustomerCount),
      m_AccsAndPerms(dfm, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer),
      m_Holdings(dfm, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer),
      m_Brokers(dfm, iActiveCustomerCount, iDefaultStartFromCustomer), m_pCompanies(dfm.CompanyFile()),
      m_pSecurities(dfm.SecurityFile()), m_pIndustries(dfm.IndustryDataFile()), m_pSectors(dfm.SectorDataFile()),
      m_pStatusType(dfm.StatusTypeDataFile()), m_pTradeType(dfm.TradeTypeDataFile()),
      m_pDriverCETxnSettings(pDriverCETxnSettings), m_pLogger(pLogger),
      m_iConfiguredCustomerCount(iConfiguredCustomerCount), m_iActiveCustomerCount(iActiveCustomerCount),
      m_iMyStartingCustomerId(iMyStartingCustomerId), m_iMyCustomerCount(iMyCustomerCount),
      m_iPartitionPercent(iPartitionPercent), m_iScaleFactor(iScaleFactor),
      m_iHoursOfInitialTrades(iHoursOfInitialTrades) {
	Initialize();
}

/*
 *  Constructor - partitioning by C_ID, RNG seed provided.
 *
 *  RNG seed is for testing/engineering work allowing repeatable transaction
 *  parameter stream. This constructor is NOT legal for a benchmark publication.
 *
 *  PARAMETERS:
 *           IN  dfm                         - Data file manager
 *           IN  iConfiguredCustomerCount    - number of configured customers in
 * the database IN  iActiveCustomerCount        - number of active customers in
 * the database IN  iScaleFactor                - scale factor (number of
 * customers per 1 tpsE) of the database IN  iHoursOfInitialTrades       -
 * number of hours of the initial trades portion of the database IN
 * iMyStartingCustomerId       - first customer id (1-based) of the partition
 * for this instance IN  iMyCustomerCount            - number of customers in
 * the partition for this instance IN  iPartitionPercent           - the
 * percentage of C_IDs generated within this instance's partition IN  pLogger -
 * reference to parameter logging object IN  pDriverCETxnSettings        -
 * initial transaction parameter settings
 *
 *  RETURNS:
 *           not applicable.
 */
CCETxnInputGenerator::CCETxnInputGenerator(const DataFileManager &dfm, TIdent iConfiguredCustomerCount,
                                           TIdent iActiveCustomerCount, INT32 iScaleFactor, INT32 iHoursOfInitialTrades,
                                           TIdent iMyStartingCustomerId, TIdent iMyCustomerCount,
                                           INT32 iPartitionPercent, RNGSEED RNGSeed, CBaseLogger *pLogger,
                                           const PDriverCETxnSettings pDriverCETxnSettings)
    : m_rnd(RNGSeed) // to be predictable
      ,
      m_Person(dfm, 0, false), m_CustomerSelection(&m_rnd, iDefaultStartFromCustomer, iActiveCustomerCount,
                                                   iPartitionPercent, iMyStartingCustomerId, iMyCustomerCount),
      m_AccsAndPerms(dfm, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer),
      m_Holdings(dfm, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer),
      m_Brokers(dfm, iActiveCustomerCount, iDefaultStartFromCustomer), m_pCompanies(dfm.CompanyFile()),
      m_pSecurities(dfm.SecurityFile()), m_pIndustries(dfm.IndustryDataFile()), m_pSectors(dfm.SectorDataFile()),
      m_pStatusType(dfm.StatusTypeDataFile()), m_pTradeType(dfm.TradeTypeDataFile()),
      m_pDriverCETxnSettings(pDriverCETxnSettings), m_pLogger(pLogger),
      m_iConfiguredCustomerCount(iConfiguredCustomerCount), m_iActiveCustomerCount(iActiveCustomerCount),
      m_iMyStartingCustomerId(iMyStartingCustomerId), m_iMyCustomerCount(iMyCustomerCount),
      m_iPartitionPercent(iPartitionPercent), m_iScaleFactor(iScaleFactor),
      m_iHoursOfInitialTrades(iHoursOfInitialTrades) {
	Initialize();
}

/*
 *  Perform initialization common to all constructors.
 *
 *  PARAMETERS:
 *           IN  pDriverCETxnSettings        - initial transaction parameter
 * settings
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::Initialize() {
	m_iActiveCompanyCount = m_pCompanies.GetActiveCompanyCount();
	m_iActiveSecurityCount = m_pSecurities.GetActiveSecurityCount();
	m_iIndustryCount = m_pIndustries.size();
	m_iSectorCount = m_pSectors.size();
	m_iStartFromCompany = m_pCompanies.GetCompanyId(0); // from the first company

	// In order for this computation to overflow an INT64, assuming that all
	// multiplications are executed before divisions and the default value
	// for ITD is used, the active customer count must be greater than 2.1e10
	// (21 billion customers).  I hope we never get to that point.
	// NOTE: (iAbortTrade / 100) = 1.01, which is compensation for rollbacks.

	m_iMaxActivePrePopulatedTradeID = m_iHoursOfInitialTrades;
	m_iMaxActivePrePopulatedTradeID *= SecondsPerHour;
	m_iMaxActivePrePopulatedTradeID *= m_iActiveCustomerCount;
	m_iMaxActivePrePopulatedTradeID /= m_iScaleFactor;
	m_iMaxActivePrePopulatedTradeID *= iAbortTrade;
	m_iMaxActivePrePopulatedTradeID /= 100;

	//  Set the start time (time 0) to the base time
	m_StartTime.Set(InitialTradePopulationBaseYear, InitialTradePopulationBaseMonth, InitialTradePopulationBaseDay,
	                InitialTradePopulationBaseHour, InitialTradePopulationBaseMinute, InitialTradePopulationBaseSecond,
	                InitialTradePopulationBaseFraction);

	// UpdateTunables() is called from CCE constructor (Initialize)
}

/*
 *  Return internal random number generator seed.
 *
 *  PARAMETERS:
 *           none.
 *
 *  RETURNS:
 *           current random number generator seed.
 */
RNGSEED CCETxnInputGenerator::GetRNGSeed(void) {
	return (m_rnd.GetSeed());
}

/*
 *  Set internal random number generator seed.
 *
 *  PARAMETERS:
 *           IN  RNGSeed     - new random number generator seed
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::SetRNGSeed(RNGSEED RNGSeed) {
	m_rnd.SetSeed(RNGSeed);
}

/*
 *  Refresh internal information from the external transaction parameters.
 *  This function should be called anytime the external transaction
 *  parameter structure changes.
 *
 *  PARAMETERS:
 *           none.
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::UpdateTunables(void) {
	INT64 secondsOfInitialTrades = (INT64)m_iHoursOfInitialTrades * SecondsPerHour;

	m_iTradeLookupFrame2MaxTimeInMilliSeconds =
	    (INT64)(secondsOfInitialTrades - ((INT64)m_pDriverCETxnSettings->TL_settings.cur.BackOffFromEndTimeFrame2)) *
	    MsPerSecond;

	m_iTradeLookupFrame3MaxTimeInMilliSeconds =
	    (INT64)(secondsOfInitialTrades - ((INT64)m_pDriverCETxnSettings->TL_settings.cur.BackOffFromEndTimeFrame3)) *
	    MsPerSecond;

	m_iTradeLookupFrame4MaxTimeInMilliSeconds =
	    (INT64)(secondsOfInitialTrades - ((INT64)m_pDriverCETxnSettings->TL_settings.cur.BackOffFromEndTimeFrame4)) *
	    MsPerSecond;

	m_iTradeUpdateFrame2MaxTimeInMilliSeconds =
	    (INT64)(secondsOfInitialTrades - ((INT64)m_pDriverCETxnSettings->TU_settings.cur.BackOffFromEndTimeFrame2)) *
	    MsPerSecond;

	m_iTradeUpdateFrame3MaxTimeInMilliSeconds =
	    (INT64)(secondsOfInitialTrades - ((INT64)m_pDriverCETxnSettings->TU_settings.cur.BackOffFromEndTimeFrame3)) *
	    MsPerSecond;

	// Set the completion time of the last initial trade.
	// 15 minutes are added at the end of hours of initial trades for pending
	// trades.
	m_EndTime = m_StartTime;
	m_EndTime.AddWorkMs((INT64)(secondsOfInitialTrades + 15 * SecondsPerMinute) * MsPerSecond);

	// Based on 10 * Trade-Order transaction mix percentage.
	// This is currently how the mix levels are set, so use that.
	m_iTradeOrderRollbackLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeOrderMixLevel;
	m_iTradeOrderRollbackLevel = m_pDriverCETxnSettings->TO_settings.cur.rollback;

	// Log Tunables
	m_pLogger->SendToLogger(m_pDriverCETxnSettings->BV_settings);
	m_pLogger->SendToLogger(m_pDriverCETxnSettings->CP_settings);
	m_pLogger->SendToLogger(m_pDriverCETxnSettings->MW_settings);
	m_pLogger->SendToLogger(m_pDriverCETxnSettings->SD_settings);
	m_pLogger->SendToLogger(m_pDriverCETxnSettings->TL_settings);
	m_pLogger->SendToLogger(m_pDriverCETxnSettings->TO_settings);
	m_pLogger->SendToLogger(m_pDriverCETxnSettings->TU_settings);
}

/*
 *  Generate Non-Uniform customer ID.
 *
 *  PARAMETERS:
 *           OUT  iCustomerId        - generated C_ID
 *           OUT  iCustomerTier      - generated C_TIER
 *
 *  RETURNS:
 *           none.
 */
inline void CCETxnInputGenerator::GenerateNonUniformRandomCustomerId(TIdent &iCustomerId,
                                                                     eCustomerTier &iCustomerTier) {
	m_CustomerSelection.GenerateRandomCustomer(iCustomerId, iCustomerTier);
}

/*
 *  Generate customer account ID (uniformly distributed).
 *
 *  PARAMETERS:
 *           none.
 *
 *  RETURNS:
 *           CA_ID uniformly distributed across all load units.
 */
TIdent CCETxnInputGenerator::GenerateRandomCustomerAccountId() {
	TIdent iCustomerId;
	TIdent iCustomerAccountId;
	eCustomerTier iCustomerTier;

	m_CustomerSelection.GenerateRandomCustomer(iCustomerId, iCustomerTier);
	iCustomerAccountId = m_AccsAndPerms.GenerateRandomAccountId(m_rnd, iCustomerId, iCustomerTier);

	return (iCustomerAccountId);
}

/*
 *  Generate a trade id to be used in Trade-Lookup / Trade-Update Frame 1.
 *
 *  PARAMETERS:
 *           IN  AValue      - parameter to NURAND function
 *           IN  SValue      - parameter to NURAND function
 *
 *  RETURNS:
 *           T_ID, distributed non-uniformly.
 */
TTrade CCETxnInputGenerator::GenerateNonUniformTradeID(INT32 AValue, INT32 SValue) {
	TTrade TradeId;

	TradeId = m_rnd.NURnd(1, m_iMaxActivePrePopulatedTradeID, AValue, SValue);

	// Skip over trade id's that were skipped over during load time.
	if (m_Holdings.IsAbortedTrade(TradeId)) {
		TradeId++;
	}

	TradeId += iTTradeShift; // shift trade id to 64-bit value

	return (TradeId);
}

/*
 *  Generate a trade timestamp to be used in Trade-Lookup / Trade-Update.
 *
 *  PARAMETERS:
 *           OUT dts                     - returned timestamp
 *           IN  MaxTimeInMilliSeconds   - time interval (from the first initial
 * trade) in which to generate the timestamp IN  AValue                  -
 * parameter to NURAND function IN  SValue                  - parameter to
 * NURAND function
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::GenerateNonUniformTradeDTS(TPCE::TIMESTAMP_STRUCT &dts, INT64 MaxTimeInMilliSeconds,
                                                      INT32 AValue, INT32 SValue) {
	CDateTime TradeTime(InitialTradePopulationBaseYear, InitialTradePopulationBaseMonth, InitialTradePopulationBaseDay,
	                    InitialTradePopulationBaseHour, InitialTradePopulationBaseMinute,
	                    InitialTradePopulationBaseSecond,
	                    InitialTradePopulationBaseFraction); // NOTE: Interpretting Fraction as
	                                                         // milliseconds,
	                                                         // probably 0 anyway.
	INT64 TradeTimeOffset;

	// Generate random number of seconds from the base time.
	//
	TradeTimeOffset = m_rnd.NURnd(1, MaxTimeInMilliSeconds, AValue, SValue);

	// The time we have is an offset into the initial pre-populated trading
	// time. This needs to be converted into a "real" time taking into account 8
	// hour business days, etc.

	TradeTime.AddWorkMs(TradeTimeOffset);
	TradeTime.GetTimeStamp(&dts);
}

/*
 *  Generate Broker-Volume transaction input.
 *
 *  PARAMETERS:
 *           OUT TxnReq                  - input parameter structure filled in
 * for the transaction.
 *
 *  RETURNS:
 *           the number of brokers generated.
 */
void CCETxnInputGenerator::GenerateBrokerVolumeInput(TBrokerVolumeTxnInput &TxnReq) {
	INT32 iNumBrokers;
	INT32 iCount, i;
	TIdent B_ID[max_broker_list_len];
	INT32 iSectorIndex;

	// Make sure we're starting with a clean object.
	TxnReq.Clear();

	// Select the range of brokers, honoring partitioning by CID settings.
	// iBrokersStart = iStartingBrokerID;
	// iBrokersCount = m_iActiveCustomerCount / iBrokersDiv;
	iNumBrokers = m_rnd.RndIntRange(min_broker_list_len,
	                                max_broker_list_len); // 20..40 brokers
	// Small databases (<=4LUs) may contain less than the chosen number of
	// brokers. Broker names for Broker Volume are unique, so need to re-adjust
	// or be caught in an infinite loop below.
	if (iNumBrokers > m_Brokers.GetBrokerCount()) {
		iNumBrokers = (INT32)m_Brokers.GetBrokerCount(); // adjust for small databases
	}

	iCount = 0;
	do {
		// select random broker ID (from active customer range)
		B_ID[iCount] = m_Brokers.GenerateRandomBrokerId(&m_rnd);

		for (i = 0; (i < iCount) && (B_ID[i] != B_ID[iCount]); ++i) {
		};

		if (i == iCount) // make sure brokers are distinct
		{
			// put the broker name into the input parameter
			m_Brokers.GenerateBrokerName(B_ID[iCount], TxnReq.broker_list[iCount],
			                             static_cast<int>(sizeof(TxnReq.broker_list[iCount])));
			++iCount;
		}

	} while (iCount < iNumBrokers);

	// select sector name
	iSectorIndex = m_rnd.RndIntRange(0, m_iSectorCount - 1);

	strncpy(TxnReq.sector_name, m_pSectors[iSectorIndex].SC_NAME_CSTR(), sizeof(TxnReq.sector_name));
}

/*
 *  Generate Customer-Position transaction input.
 *
 *  PARAMETERS:
 *           OUT TxnReq                  - input parameter structure filled in
 * for the transaction.
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::GenerateCustomerPositionInput(TCustomerPositionTxnInput &TxnReq) {
	TIdent iCustomerId;
	eCustomerTier iCustomerTier;

	// Make sure we're starting with a clean object.
	TxnReq.Clear();

	GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier);

	if (m_rnd.RndPercent(m_pDriverCETxnSettings->CP_settings.cur.by_tax_id)) {
		// send tax id instead of customer id
		m_Person.GetTaxID(iCustomerId, TxnReq.tax_id);
	} else {
		// send customer id and not the tax id
		TxnReq.cust_id = iCustomerId;
	}

	TxnReq.get_history = m_rnd.RndPercent(m_pDriverCETxnSettings->CP_settings.cur.get_history);
	if (TxnReq.get_history) {
		TxnReq.acct_id_idx = m_rnd.RndIntRange(0, m_AccsAndPerms.GetNumberOfAccounts(iCustomerId, iCustomerTier) - 1);
	} else {
		TxnReq.acct_id_idx = -1;
	}
}

/*
 *  Generate Market-Watch transaction input.
 *
 *  PARAMETERS:
 *           OUT TxnReq                  - input parameter structure filled in
 * for the transaction.
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::GenerateMarketWatchInput(TMarketWatchTxnInput &TxnReq) {
	TIdent iCustomerId;
	eCustomerTier iCustomerTier;
	INT32 iThreshold;
	INT32 iWeek;
	INT32 iDailyMarketDay;
	CDateTime StartDate(iDailyMarketBaseYear, iDailyMarketBaseMonth, iDailyMarketBaseDay, iDailyMarketBaseHour,
	                    iDailyMarketBaseMinute, iDailyMarketBaseSecond, iDailyMarketBaseMsec);

	// Make sure we're starting with a clean object.
	TxnReq.Clear();

	iThreshold = m_rnd.RndGenerateIntegerPercentage();

	// have some distribution on what inputs to send
	if (iThreshold <= m_pDriverCETxnSettings->MW_settings.cur.by_industry) {
		// send industry name
		strncpy(TxnReq.industry_name, m_pIndustries[m_rnd.RndIntRange(0, m_iIndustryCount - 1)].IN_NAME_CSTR(),
		        sizeof(TxnReq.industry_name));

		if (iBaseCompanyCount < m_iActiveCompanyCount) {
			TxnReq.starting_co_id = m_rnd.RndInt64Range(
			    m_iStartFromCompany, m_iStartFromCompany + m_iActiveCompanyCount - (iBaseCompanyCount - 1));
			TxnReq.ending_co_id = TxnReq.starting_co_id + (iBaseCompanyCount - 1);
		} else {
			TxnReq.starting_co_id = m_iStartFromCompany;
			TxnReq.ending_co_id = m_iStartFromCompany + m_iActiveCompanyCount - 1;
		}
	} else {
		if (iThreshold <= (m_pDriverCETxnSettings->MW_settings.cur.by_industry +
		                   m_pDriverCETxnSettings->MW_settings.cur.by_watch_list)) {
			// Send customer id
			GenerateNonUniformRandomCustomerId(TxnReq.c_id, iCustomerTier);
		} else {
			// Send account id
			GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier);
			m_AccsAndPerms.GenerateRandomAccountId(m_rnd, iCustomerId, iCustomerTier, &TxnReq.acct_id, NULL);
		}
	}

	// Set start_day for both cases of the 'if'.
	//
	iWeek = (INT32)m_rnd.NURnd(0, 255, 255, 0) + 5; // A = 255, S = 0
	// Week is now between 5 and 260.
	// Select a day within the week.
	//
	iThreshold = m_rnd.RndGenerateIntegerPercentage();
	if (iThreshold > 40) {
		iDailyMarketDay = iWeek * DaysPerWeek + 4; // Friday
	} else                                         // 1..40 case
	{
		if (iThreshold <= 20) {
			iDailyMarketDay = iWeek * DaysPerWeek; // Monday
		} else {
			if (iThreshold <= 27) {
				iDailyMarketDay = iWeek * DaysPerWeek + 1; // Tuesday
			} else {
				if (iThreshold <= 33) {
					iDailyMarketDay = iWeek * DaysPerWeek + 2; // Wednesday
				} else {
					iDailyMarketDay = iWeek * DaysPerWeek + 3; // Thursday
				}
			}
		}
	}

	// Go back 256 weeks and then add our calculated day.
	//
	StartDate.Add(iDailyMarketDay, 0);

	StartDate.GetTimeStamp(&TxnReq.start_day);
}

/*
 *  Generate Security-Detail transaction input.
 *
 *  PARAMETERS:
 *           OUT TxnReq                  - input parameter structure filled in
 * for the transaction.
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::GenerateSecurityDetailInput(TSecurityDetailTxnInput &TxnReq) {
	CDateTime StartDate(iDailyMarketBaseYear, iDailyMarketBaseMonth, iDailyMarketBaseDay, iDailyMarketBaseHour,
	                    iDailyMarketBaseMinute, iDailyMarketBaseSecond, iDailyMarketBaseMsec);
	INT32 iStartDay; // day from the StartDate

	// Make sure we're starting with a clean object.
	TxnReq.Clear();

	// random symbol
	m_pSecurities.CreateSymbol(m_rnd.RndInt64Range(0, m_iActiveSecurityCount - 1), TxnReq.symbol,
	                           static_cast<int>(sizeof(TxnReq.symbol)));

	// Whether or not to access the LOB.
	TxnReq.access_lob_flag = m_rnd.RndPercent(m_pDriverCETxnSettings->SD_settings.cur.LOBAccessPercentage);

	// random number of financial rows to return
	TxnReq.max_rows_to_return = m_rnd.RndIntRange(iSecurityDetailMinRows, iSecurityDetailMaxRows);

	iStartDay = m_rnd.RndIntRange(0, iDailyMarketTotalRows - TxnReq.max_rows_to_return);

	// add the offset
	StartDate.Add(iStartDay, 0);

	StartDate.GetTimeStamp(&TxnReq.start_day);
}

/*
 *  Generate Trade-Lookup transaction input.
 *
 *  PARAMETERS:
 *           OUT TxnReq                  - input parameter structure filled in
 * for the transaction.
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::GenerateTradeLookupInput(TTradeLookupTxnInput &TxnReq) {
	INT32 iThreshold;

	// Make sure we're starting with a clean object.
	TxnReq.Clear();

	iThreshold = m_rnd.RndGenerateIntegerPercentage();

	if (iThreshold <= m_pDriverCETxnSettings->TL_settings.cur.do_frame1) {
		// Frame 1
		TxnReq.frame_to_execute = 1;
		TxnReq.max_trades = m_pDriverCETxnSettings->TL_settings.cur.MaxRowsFrame1;

		// Generate list of unique trade id's
		int ii, jj;
		bool Accepted;
		TTrade TID;

		for (ii = 0; ii < TxnReq.max_trades; ii++) {
			Accepted = false;
			while (!Accepted) {
				TID = GenerateNonUniformTradeID(TradeLookupAValueForTradeIDGenFrame1,
				                                TradeLookupSValueForTradeIDGenFrame1);
				jj = 0;
				while (jj < ii && TxnReq.trade_id[jj] != TID) {
					jj++;
				}
				if (jj == ii) {
					// We have a unique TID for this batch
					TxnReq.trade_id[ii] = TID;
					Accepted = true;
				}
			}
		}
	} else if (iThreshold <=
	           m_pDriverCETxnSettings->TL_settings.cur.do_frame1 + m_pDriverCETxnSettings->TL_settings.cur.do_frame2) {
		// Frame 2
		TxnReq.frame_to_execute = 2;
		TxnReq.acct_id = GenerateRandomCustomerAccountId();
		TxnReq.max_trades = m_pDriverCETxnSettings->TL_settings.cur.MaxRowsFrame2;

		GenerateNonUniformTradeDTS(TxnReq.start_trade_dts, m_iTradeLookupFrame2MaxTimeInMilliSeconds,
		                           TradeLookupAValueForTimeGenFrame2, TradeLookupSValueForTimeGenFrame2);

		// Set to the end of initial trades.
		m_EndTime.GetTimeStamp(&TxnReq.end_trade_dts);
	} else if (iThreshold <= m_pDriverCETxnSettings->TL_settings.cur.do_frame1 +
	                             m_pDriverCETxnSettings->TL_settings.cur.do_frame2 +
	                             m_pDriverCETxnSettings->TL_settings.cur.do_frame3) {
		// Frame 3
		TxnReq.frame_to_execute = 3;
		TxnReq.max_trades = m_pDriverCETxnSettings->TL_settings.cur.MaxRowsFrame3;

		m_pSecurities.CreateSymbol(m_rnd.NURnd(0, m_iActiveSecurityCount - 1, TradeLookupAValueForSymbolFrame3,
		                                       TradeLookupSValueForSymbolFrame3),
		                           TxnReq.symbol, static_cast<int>(sizeof(TxnReq.symbol)));

		GenerateNonUniformTradeDTS(TxnReq.start_trade_dts, m_iTradeLookupFrame3MaxTimeInMilliSeconds,
		                           TradeLookupAValueForTimeGenFrame3, TradeLookupSValueForTimeGenFrame3);

		// Set to the end of initial trades.
		m_EndTime.GetTimeStamp(&TxnReq.end_trade_dts);

		TxnReq.max_acct_id = m_AccsAndPerms.GetEndingCA_ID(m_iActiveCustomerCount);
	} else {
		// Frame 4
		TxnReq.frame_to_execute = 4;
		TxnReq.acct_id = GenerateRandomCustomerAccountId();
		GenerateNonUniformTradeDTS(TxnReq.start_trade_dts, m_iTradeLookupFrame4MaxTimeInMilliSeconds,
		                           TradeLookupAValueForTimeGenFrame4, TradeLookupSValueForTimeGenFrame4);
	}
}

/*
 *  Generate Trade-Order transaction input.
 *
 *  PARAMETERS:
 *           OUT TxnReq                  - input parameter structure filled in
 * for the transaction. OUT TradeType               - integer representation of
 * generated trade type (as eTradeTypeID enum). OUT bExecutorIsAccountOwner -
 * whether Trade-Order frame 2 should (FALSE) or shouldn't (TRUE) be called.
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::GenerateTradeOrderInput(TTradeOrderTxnInput &TxnReq, INT32 &iTradeType,
                                                   bool &bExecutorIsAccountOwner) {
	TIdent iCustomerId; // owner
	eCustomerTier iCustomerTier;
	TIdent CID_1, CID_2;
	bool bMarket;
	INT32 iAdditionalPerms;
	UINT iSymbIndex;
	TIdent iFlatFileSymbIndex;
	eTradeTypeID eTradeType;

	// Make sure we're starting with a clean object.
	TxnReq.Clear();

	// Generate random customer
	//
	GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier);

	// Generate random account id and security index
	//
	m_Holdings.GenerateRandomAccountSecurity(iCustomerId, iCustomerTier, &TxnReq.acct_id, &iFlatFileSymbIndex,
	                                         &iSymbIndex);

	// find out how many permission rows there are for this account (in addition
	// to the owner's)
	iAdditionalPerms = m_AccsAndPerms.GetNumPermsForCA(TxnReq.acct_id);
	// distribution same as in the loader for now
	if (iAdditionalPerms == 0) { // select the owner
		m_Person.GetFirstLastAndTaxID(iCustomerId, TxnReq.exec_f_name, TxnReq.exec_l_name, TxnReq.exec_tax_id);

		bExecutorIsAccountOwner = true;
	} else {
		// If there is more than one permission set on the account,
		// have some distribution on whether the executor is still
		// the account owner, or it is one of the additional permissions.
		// Here we must take into account the fact that we've excluded
		// a large portion of customers that don't have any additional
		// executors in the above code (iAdditionalPerms == 0); the
		// "exec_is_owner" percentage implicity includes such customers
		// and must be factored out here.

		int exec_is_owner =
		    (m_pDriverCETxnSettings->TO_settings.cur.exec_is_owner - iPercentAccountAdditionalPermissions_0) * 100 /
		    (100 - iPercentAccountAdditionalPermissions_0);

		if (m_rnd.RndPercent(exec_is_owner)) {
			m_Person.GetFirstLastAndTaxID(iCustomerId, TxnReq.exec_f_name, TxnReq.exec_l_name, TxnReq.exec_tax_id);

			bExecutorIsAccountOwner = true;
		} else {
			if (iAdditionalPerms == 1) {
				// select the first non-owner
				m_AccsAndPerms.GetCIDsForPermissions(TxnReq.acct_id, iCustomerId, &CID_1, NULL);

				m_Person.GetFirstLastAndTaxID(CID_1, TxnReq.exec_f_name, TxnReq.exec_l_name, TxnReq.exec_tax_id);
			} else {
				// select the second non-owner
				m_AccsAndPerms.GetCIDsForPermissions(TxnReq.acct_id, iCustomerId, &CID_1, &CID_2);
				// generate third account permission row
				m_Person.GetFirstLastAndTaxID(CID_2, TxnReq.exec_f_name, TxnReq.exec_l_name, TxnReq.exec_tax_id);
			}

			bExecutorIsAccountOwner = false;
		}
	}

	// Select either stock symbol or company from the securities flat file.
	//

	// have some distribution on the company/symbol input preference
	if (m_rnd.RndPercent(m_pDriverCETxnSettings->TO_settings.cur.security_by_symbol)) {
		// Submit the symbol
		m_pSecurities.CreateSymbol(iFlatFileSymbIndex, TxnReq.symbol, static_cast<int>(sizeof(TxnReq.symbol)));
	} else {
		// Submit the company name
		m_pCompanies.CreateName(m_pSecurities.GetCompanyIndex(iFlatFileSymbIndex), TxnReq.co_name,
		                        static_cast<int>(sizeof(TxnReq.co_name)));

		strncpy(TxnReq.issue, m_pSecurities.GetRecord(iFlatFileSymbIndex).S_ISSUE_CSTR(), sizeof(TxnReq.issue));
	}

	TxnReq.trade_qty = cTRADE_QTY_SIZES[m_rnd.RndIntRange(0, cNUM_TRADE_QTY_SIZES - 1)];
	TxnReq.requested_price = m_rnd.RndDoubleIncrRange(fMinSecPrice, fMaxSecPrice, 0.01);

	// Determine whether Market or Limit order
	bMarket = m_rnd.RndPercent(m_pDriverCETxnSettings->TO_settings.cur.market);

	// Determine whether Buy or Sell trade
	if (m_rnd.RndPercent(m_pDriverCETxnSettings->TO_settings.cur.buy_orders)) {
		if (bMarket) {
			// Market Buy
			eTradeType = eMarketBuy;
		} else {
			// Limit Buy
			eTradeType = eLimitBuy;
		}

		// Set margin or cash for Buy
		TxnReq.type_is_margin = m_rnd.RndPercent(
		    // type_is_margin is specified for all orders, but used only for
		    // buys
		    m_pDriverCETxnSettings->TO_settings.cur.type_is_margin * 100 /
		    m_pDriverCETxnSettings->TO_settings.cur.buy_orders);
	} else {
		if (bMarket) {
			// Market Sell
			eTradeType = eMarketSell;
		} else {
			// determine whether the Limit Sell is a Stop Loss
			if (m_rnd.RndPercent(m_pDriverCETxnSettings->TO_settings.cur.stop_loss)) {
				// Stop Loss
				eTradeType = eStopLoss;
			} else {
				// Limit Sell
				eTradeType = eLimitSell;
			}
		}

		TxnReq.type_is_margin = false; // all sell orders are cash
	}
	iTradeType = eTradeType;

	// Distribution of last-in-first-out flag
	TxnReq.is_lifo = m_rnd.RndPercent(m_pDriverCETxnSettings->TO_settings.cur.lifo);

	// Copy the trade type id from the flat file
	strncpy(TxnReq.trade_type_id, m_pTradeType[eTradeType].TT_ID_CSTR(), sizeof(TxnReq.trade_type_id));

	// Copy the status type id's from the flat file
	strncpy(TxnReq.st_pending_id, m_pStatusType[ePending].ST_ID_CSTR(), sizeof(TxnReq.st_pending_id));
	strncpy(TxnReq.st_submitted_id, m_pStatusType[eSubmitted].ST_ID_CSTR(), sizeof(TxnReq.st_submitted_id));

	TxnReq.roll_it_back = (m_iTradeOrderRollbackLevel >= m_rnd.RndIntRange(1, m_iTradeOrderRollbackLimit));

	// Need to address logging more comprehensively.
	// return eTradeType;
}

/*
 *  Generate Trade-Status transaction input.
 *
 *  PARAMETERS:
 *           OUT TxnReq                  - input parameter structure filled in
 * for the transaction.
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::GenerateTradeStatusInput(TTradeStatusTxnInput &TxnReq) {
	TIdent iCustomerId;
	eCustomerTier iCustomerTier;

	// Make sure we're starting with a clean object.
	TxnReq.Clear();

	// select customer id first
	GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier);

	// select random account id
	m_AccsAndPerms.GenerateRandomAccountId(m_rnd, iCustomerId, iCustomerTier, &TxnReq.acct_id, NULL);
}

/*
 *  Generate Trade-Update transaction input.
 *
 *  PARAMETERS:
 *           OUT TxnReq                  - input parameter structure filled in
 * for the transaction.
 *
 *  RETURNS:
 *           none.
 */
void CCETxnInputGenerator::GenerateTradeUpdateInput(TTradeUpdateTxnInput &TxnReq) {
	INT32 iThreshold;

	// Make sure we're starting with a clean object.
	TxnReq.Clear();

	iThreshold = m_rnd.RndGenerateIntegerPercentage();

	if (iThreshold <= m_pDriverCETxnSettings->TU_settings.cur.do_frame1) {
		// Frame 1
		TxnReq.frame_to_execute = 1;
		TxnReq.max_trades = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsFrame1;
		TxnReq.max_updates = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsToUpdateFrame1;

		// Generate list of unique trade id's
		int ii, jj;
		bool Accepted;
		TTrade TID;

		for (ii = 0; ii < TxnReq.max_trades; ii++) {
			Accepted = false;
			while (!Accepted) {
				TID = GenerateNonUniformTradeID(TradeUpdateAValueForTradeIDGenFrame1,
				                                TradeUpdateSValueForTradeIDGenFrame1);
				jj = 0;
				while (jj < ii && TxnReq.trade_id[jj] != TID) {
					jj++;
				}
				if (jj == ii) {
					// We have a unique TID for this batch
					TxnReq.trade_id[ii] = TID;
					Accepted = true;
				}
			}
		}
	} else if (iThreshold <=
	           m_pDriverCETxnSettings->TU_settings.cur.do_frame1 + m_pDriverCETxnSettings->TU_settings.cur.do_frame2) {
		// Frame 2
		TxnReq.frame_to_execute = 2;
		TxnReq.max_trades = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsFrame2;
		TxnReq.max_updates = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsToUpdateFrame2;
		TxnReq.acct_id = GenerateRandomCustomerAccountId();

		GenerateNonUniformTradeDTS(TxnReq.start_trade_dts, m_iTradeUpdateFrame2MaxTimeInMilliSeconds,
		                           TradeUpdateAValueForTimeGenFrame2, TradeUpdateSValueForTimeGenFrame2);

		// Set to the end of initial trades.
		m_EndTime.GetTimeStamp(&TxnReq.end_trade_dts);
	} else {
		// Frame 3
		TxnReq.frame_to_execute = 3;
		TxnReq.max_trades = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsFrame3;
		TxnReq.max_updates = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsToUpdateFrame3;

		m_pSecurities.CreateSymbol(m_rnd.NURnd(0, m_iActiveSecurityCount - 1, TradeUpdateAValueForSymbolFrame3,
		                                       TradeUpdateSValueForSymbolFrame3),
		                           TxnReq.symbol, static_cast<int>(sizeof(TxnReq.symbol)));

		GenerateNonUniformTradeDTS(TxnReq.start_trade_dts, m_iTradeUpdateFrame3MaxTimeInMilliSeconds,
		                           TradeUpdateAValueForTimeGenFrame3, TradeUpdateSValueForTimeGenFrame3);

		// Set to the end of initial trades.
		m_EndTime.GetTimeStamp(&TxnReq.end_trade_dts);

		TxnReq.max_acct_id = m_AccsAndPerms.GetEndingCA_ID(m_iActiveCustomerCount);
	}
}
