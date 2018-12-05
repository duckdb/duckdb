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
 * - Doug Johnson
 */

/******************************************************************************
 *   Description:        This class provides Market Exchange Emulator
 *                       functionality. It accepts trade requests for
 *processing; generates a negative exponential delay for each trade to simulate
 *the market processing time; manages the timers for all trades being processed;
 *generates Trade-Result and Market-Feed input data to be used by a sponsor
 *provided callback interface to the SUT (see MEESUTInterface.h).
 *
 *                       The constructor for this class requires two parameters.
 *                       - TradingTimeSoFar: the number of seconds for which
 *                       trades have been run against the database. This allows
 *                       the MEE to pick up on the price curves from where it
 *                       last left off. If doing this isn't important than 0 can
 *                       passed in.
 *                       - pSUT: a pointer to an instance of a sponsor provided
 *                       subclassing of the CMEESUTInterface class.
 *
 *                       - pLogger: a pointer to an instance of CEGenLogger or a
 *                       sponsor provided subclassing of the CBaseLogger class.
 *
 *                       The MEE provides the following entry points.
 *
 *                       - SetBaseTime: used to cordinate the price curves
 *                       across multiple instances of this class. This method
 *                       should be called "at the same time" for all instances.
 *                       This call should be made just prior to starting
 *                       transactions. There is no return value.
 *
 *                       - SubmitTradeRequest: used for submitting a trade
 *                       request into the market. The return value is the number
 *                       of milliseconds before the next timer is set to expire.
 *
 *                       - GenerateTradeResult: called whenever the current
 *timer has expired (i.e. whenever the number of milliseconds returned by either
 *SubmitTradeRequest or GenerateTradeResult has elapsed). The return value is
 *                       the number of milliseconds before the next timer is set
 *                       to expire.
 *
 *                       - DisableTickerTape / EnableTickerTape: by default, the
 *                       ticker tape functionality of the MEE is enabled. It can
 *                       be disabled, or re-enabled by calls to these methods.
 *                       Disabling the ticker tape is useful at the end of a
 *                       test run to allow processing of submitted orders to
 *                       continue (Trade-Results) while not generating any
 *                       ticker tape activity (Market-Feeds).
 ******************************************************************************/

#ifndef MEE_H
#define MEE_H

#include "utilities/EGenUtilities_stdafx.h"
#include "MEETradeRequestActions.h"
#include "TxnHarnessStructs.h"
#include "MEEPriceBoard.h"
#include "MEETickerTape.h"
#include "MEETradingFloor.h"
#include "MEESUTInterface.h"
#include "BaseLogger.h"
#include "DriverParamSettings.h"

#include "input/DataFileManager.h"

namespace TPCE {

class CMEE {
private:
	CDriverMEESettings m_DriverMEESettings;

	CMEESUTInterface *m_pSUT;
	CBaseLogger *m_pLogger;
	CMEEPriceBoard m_PriceBoard;
	CMEETickerTape m_TickerTape;
	CMEETradingFloor m_TradingFloor;
	CDateTime m_BaseTime;
	CDateTime m_CurrentTime;

	CMutex m_MEELock;

	// Automatically generate unique RNG seeds
	void AutoSetRNGSeeds(UINT32 UniqueId);

public:
	static const INT32 NO_OUTSTANDING_TRADES = CMEETradingFloor::NO_OUTSTANDING_TRADES;

	// Constructor - automatic RNG seed generation
	CMEE(INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, CBaseLogger *pLogger, const DataFileManager &inputFiles,
	     UINT32 UniqueId);

	// Constructor - RNG seed provided
	CMEE(INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, CBaseLogger *pLogger, const DataFileManager &inputFiles,
	     UINT32 UniqueId, RNGSEED TickerTapeRNGSeed, RNGSEED TradingFloorRNGSeed);

	~CMEE(void);

	RNGSEED GetTickerTapeRNGSeed(void);
	RNGSEED GetTradingFloorRNGSeed(void);

	void SetBaseTime(void);

	INT32 SubmitTradeRequest(PTradeRequest pTradeRequest);
	INT32 GenerateTradeResult(void);

	bool EnableTickerTape(void);
	bool DisableTickerTape(void);
};

} // namespace TPCE

#endif // MEE_H
