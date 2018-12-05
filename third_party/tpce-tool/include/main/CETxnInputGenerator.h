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
 *   Class that generates transaction input data for the Customer Emulator (CE).
 */

#ifndef CE_TXN_INPUT_GENERATOR_H
#define CE_TXN_INPUT_GENERATOR_H

#include "utilities/EGenUtilities_stdafx.h"
#include "TxnHarnessStructs.h"
#include "DriverParamSettings.h"
#include "EGenLogger.h"

#include "input/DataFileManager.h"
#include "Brokers.h"
#include "HoldingsAndTradesTable.h"

namespace TPCE {

class CCETxnInputGenerator {
	CRandom m_rnd; // used inside for parameter generation
	CPerson m_Person;
	CCustomerSelection m_CustomerSelection;
	CCustomerAccountsAndPermissionsTable m_AccsAndPerms;
	CHoldingsAndTradesTable m_Holdings;
	CBrokersTable m_Brokers;
	const CCompanyFile &m_pCompanies;
	const CSecurityFile &m_pSecurities;
	const IndustryDataFile_t &m_pIndustries;
	const SectorDataFile_t &m_pSectors;
	const StatusTypeDataFile_t &m_pStatusType;
	const TradeTypeDataFile_t &m_pTradeType;
	PDriverCETxnSettings m_pDriverCETxnSettings;
	CBaseLogger *m_pLogger;

	TIdent m_iConfiguredCustomerCount;
	TIdent m_iActiveCustomerCount;

	TIdent m_iMyStartingCustomerId;
	TIdent m_iMyCustomerCount;
	INT32 m_iPartitionPercent;

	INT32 m_iScaleFactor;
	INT32 m_iHoursOfInitialTrades;
	INT64 m_iMaxActivePrePopulatedTradeID;

	INT64 m_iTradeLookupFrame2MaxTimeInMilliSeconds;
	INT64 m_iTradeLookupFrame3MaxTimeInMilliSeconds;
	INT64 m_iTradeLookupFrame4MaxTimeInMilliSeconds;

	INT64 m_iTradeUpdateFrame2MaxTimeInMilliSeconds;
	INT64 m_iTradeUpdateFrame3MaxTimeInMilliSeconds;

	// number of securities (scaled based on active customers)
	TIdent m_iActiveSecurityCount;
	TIdent m_iActiveCompanyCount;
	// number of industries (from flat file)
	INT32 m_iIndustryCount;
	// number of sector names (from flat file)
	INT32 m_iSectorCount;
	// starting ids
	TIdent m_iStartFromCompany;
	CDateTime m_StartTime; // start time of initial trades
	CDateTime m_EndTime;   // end time of initial trades

	INT32 m_iTradeOrderRollbackLimit;
	INT32 m_iTradeOrderRollbackLevel;

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
	void Initialize();

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
	void GenerateNonUniformRandomCustomerId(TIdent &iCustomerId, eCustomerTier &iCustomerTier);

	/*
	 *  Generate customer account ID (uniformly distributed).
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           CA_ID uniformly distributed across all load units.
	 */
	TIdent GenerateRandomCustomerAccountId(void);

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
	TTrade GenerateNonUniformTradeID(INT32 AValue, INT32 SValue);

	/*
	 *  Generate a trade timestamp to be used in Trade-Lookup / Trade-Update.
	 *
	 *  PARAMETERS:
	 *           OUT dts                     - returned timestamp
	 *           IN  MaxTimeInMilliSeconds   - time interval (from the first
	 * initial trade) in which to generate the timestamp IN  AValue - parameter
	 * to NURAND function IN  SValue                  - parameter to NURAND
	 * function
	 *
	 *  RETURNS:
	 *           none.
	 */
	void GenerateNonUniformTradeDTS(TIMESTAMP_STRUCT &dts, INT64 MaxTimeInMilliSeconds, INT32 AValue, INT32 SValue);

public:
	/*
	 *  Constructor - no partitioning by C_ID.
	 *
	 *  PARAMETERS:
	 *           IN  dfm                         - Data file manager
	 *           IN  iConfiguredCustomerCount    - number of configured
	 * customers in the database IN  iActiveCustomerCount        - number of
	 * active customers in the database IN  iScaleFactor                - scale
	 * factor (number of customers per 1 tpsE) of the database IN
	 * iHoursOfInitialTrades       - number of hours of the initial trades
	 * portion of the database IN  pLogger                     - reference to
	 * parameter logging object IN  pDriverCETxnSettings        - initial
	 * transaction parameter settings
	 *
	 *  RETURNS:
	 *           not applicable.
	 */
	CCETxnInputGenerator(const DataFileManager &dfm, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
	                     INT32 iScaleFactor, INT32 iHoursOfInitialTrades, CBaseLogger *pLogger,
	                     const PDriverCETxnSettings pDriverCETxnTunables);

	/*
	 *  Constructor - no partitioning by C_ID, RNG seed provided.
	 *
	 *  RNG seed is for testing/engineering work allowing repeatable transaction
	 *  parameter stream. This constructor is NOT legal for a benchmark
	 * publication.
	 *
	 *  PARAMETERS:
	 *           IN  dfm                         - Data file manager
	 *           IN  iConfiguredCustomerCount    - number of configured
	 * customers in the database IN  iActiveCustomerCount        - number of
	 * active customers in the database IN  iScaleFactor                - scale
	 * factor (number of customers per 1 tpsE) of the database IN
	 * iHoursOfInitialTrades       - number of hours of the initial trades
	 * portion of the database IN  RNGSeed                     - initial seed
	 * for random number generator IN  pLogger                     - reference
	 * to parameter logging object IN  pDriverCETxnSettings        - initial
	 * transaction parameter settings
	 *
	 *  RETURNS:
	 *           not applicable.
	 */
	CCETxnInputGenerator(const DataFileManager &dfm, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
	                     INT32 iScaleFactor, INT32 iHoursOfInitialTrades, RNGSEED RNGSeed, CBaseLogger *pLogger,
	                     const PDriverCETxnSettings pDriverCETxnTunables);

	/*
	 *  Constructor - partitioning by C_ID.
	 *
	 *  PARAMETERS:
	 *           IN  dfm                         - Data file manager
	 *           IN  iConfiguredCustomerCount    - number of configured
	 * customers in the database IN  iActiveCustomerCount        - number of
	 * active customers in the database IN  iScaleFactor                - scale
	 * factor (number of customers per 1 tpsE) of the database IN
	 * iHoursOfInitialTrades       - number of hours of the initial trades
	 * portion of the database IN  iMyStartingCustomerId       - first customer
	 * id (1-based) of the partition for this instance IN  iMyCustomerCount -
	 * number of customers in the partition for this instance IN
	 * iPartitionPercent           - the percentage of C_IDs generated within
	 * this instance's partition IN  pLogger                     - reference to
	 * parameter logging object IN  pDriverCETxnSettings        - initial
	 * transaction parameter settings
	 *
	 *  RETURNS:
	 *           not applicable.
	 */
	CCETxnInputGenerator(const DataFileManager &dfm, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
	                     INT32 iScaleFactor, INT32 iHoursOfInitialTrades, TIdent iMyStartingCustomerId,
	                     TIdent iMyCustomerCount, INT32 iPartitionPercent, CBaseLogger *pLogger,
	                     const PDriverCETxnSettings pDriverCETxnTunables);

	/*
	 *  Constructor - partitioning by C_ID, RNG seed provided.
	 *
	 *  RNG seed is for testing/engineering work allowing repeatable transaction
	 *  parameter stream. This constructor is NOT legal for a benchmark
	 * publication.
	 *
	 *  PARAMETERS:
	 *           IN  dfm                         - Data file manager
	 *           IN  iConfiguredCustomerCount    - number of configured
	 * customers in the database IN  iActiveCustomerCount        - number of
	 * active customers in the database IN  iScaleFactor                - scale
	 * factor (number of customers per 1 tpsE) of the database IN
	 * iHoursOfInitialTrades       - number of hours of the initial trades
	 * portion of the database IN  iMyStartingCustomerId       - first customer
	 * id (1-based) of the partition for this instance IN  iMyCustomerCount -
	 * number of customers in the partition for this instance IN
	 * iPartitionPercent           - the percentage of C_IDs generated within
	 * this instance's partition IN  pLogger                     - reference to
	 * parameter logging object IN  pDriverCETxnSettings        - initial
	 * transaction parameter settings
	 *
	 *  RETURNS:
	 *           not applicable.
	 */
	CCETxnInputGenerator(const DataFileManager &dfm, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
	                     INT32 iScaleFactor, INT32 iHoursOfInitialTrades, TIdent iMyStartingCustomerId,
	                     TIdent iMyCustomerCount, INT32 iPartitionPercent, RNGSEED RNGSeed, CBaseLogger *pLogger,
	                     const PDriverCETxnSettings pDriverCETxnTunables);

	/*
	 *  Return internal random number generator seed.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           current random number generator seed.
	 */
	RNGSEED GetRNGSeed(void);

	/*
	 *  Set internal random number generator seed.
	 *
	 *  PARAMETERS:
	 *           IN  RNGSeed     - new random number generator seed
	 *
	 *  RETURNS:
	 *           none.
	 */
	void SetRNGSeed(RNGSEED RNGSeed);

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
	void UpdateTunables();

	/*
	 *  Generate Broker-Volume transaction input.
	 *
	 *  PARAMETERS:
	 *           OUT TxnReq                  - input parameter structure filled
	 * in for the transaction.
	 *
	 *  RETURNS:
	 *           the number of brokers generated.
	 */
	void GenerateBrokerVolumeInput(TBrokerVolumeTxnInput &TxnReq);

	/*
	 *  Generate Customer-Position transaction input.
	 *
	 *  PARAMETERS:
	 *           OUT TxnReq                  - input parameter structure filled
	 * in for the transaction.
	 *
	 *  RETURNS:
	 *           none.
	 */
	void GenerateCustomerPositionInput(TCustomerPositionTxnInput &TxnReq);

	/*
	 *  Generate Market-Watch transaction input.
	 *
	 *  PARAMETERS:
	 *           OUT TxnReq                  - input parameter structure filled
	 * in for the transaction.
	 *
	 *  RETURNS:
	 *           none.
	 */
	void GenerateMarketWatchInput(TMarketWatchTxnInput &TxnReq);

	/*
	 *  Generate Security-Detail transaction input.
	 *
	 *  PARAMETERS:
	 *           OUT TxnReq                  - input parameter structure filled
	 * in for the transaction.
	 *
	 *  RETURNS:
	 *           none.
	 */
	void GenerateSecurityDetailInput(TSecurityDetailTxnInput &TxnReq);

	/*
	 *  Generate Trade-Lookup transaction input.
	 *
	 *  PARAMETERS:
	 *           OUT TxnReq                  - input parameter structure filled
	 * in for the transaction.
	 *
	 *  RETURNS:
	 *           none.
	 */
	void GenerateTradeLookupInput(TTradeLookupTxnInput &TxnReq);

	/*
	 *  Generate Trade-Order transaction input.
	 *
	 *  PARAMETERS:
	 *           OUT TxnReq                  - input parameter structure filled
	 * in for the transaction. OUT TradeType               - integer
	 * representation of generated trade type (as eTradeTypeID enum). OUT
	 * bExecutorIsAccountOwner - whether Trade-Order frame 2 should (FALSE) or
	 * shouldn't (TRUE) be called.
	 *
	 *  RETURNS:
	 *           none.
	 */
	void GenerateTradeOrderInput(TTradeOrderTxnInput &TxnReq, INT32 &iTradeType, bool &bExecutorIsAccountOwner);

	/*
	 *  Generate Trade-Status transaction input.
	 *
	 *  PARAMETERS:
	 *           OUT TxnReq                  - input parameter structure filled
	 * in for the transaction.
	 *
	 *  RETURNS:
	 *           none.
	 */
	void GenerateTradeStatusInput(TTradeStatusTxnInput &TxnReq);

	/*
	 *  Generate Trade-Update transaction input.
	 *
	 *  PARAMETERS:
	 *           OUT TxnReq                  - input parameter structure filled
	 * in for the transaction.
	 *
	 *  RETURNS:
	 *           none.
	 */
	void GenerateTradeUpdateInput(TTradeUpdateTxnInput &TxnReq);
};

} // namespace TPCE

#endif // #ifndef CE_TXN_INPUT_GENERATOR_H
