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
 *   Description:        Class that handles all of the trading floor activities
 *                       for the market. This includes providing functionality
 *                       for accepting orders that need to be submitted to the
 *                       trading floor, assigning a processing delay time to
 *                       the order, and then completing the order after the
 *                       processing delay is over.
 ******************************************************************************/

#ifndef MEE_TRADING_FLOOR_H
#define MEE_TRADING_FLOOR_H

#include "utilities/EGenUtilities_stdafx.h"
#include "TxnHarnessStructs.h"
#include "TimerWheel.h"
#include "MEESUTInterface.h"
#include "MEEPriceBoard.h"
#include "MEETickerTape.h"

namespace TPCE {

class CMEETradingFloor {
private:
	CMEESUTInterface *m_pSUT;
	CMEEPriceBoard *m_pPriceBoard;
	CMEETickerTape *m_pTickerTape;

	CDateTime *m_pBaseTime;
	CDateTime *m_pCurrentTime;

	CTimerWheel<TTradeRequest, CMEETradingFloor, 5, 1> m_OrderTimers; // Size wheel for 5 seconds with 1 millisecond
	                                                                  // resolution.
	CRandom m_rnd;
	double m_OrderProcessingDelayMean;
	static const INT32 m_MaxOrderProcessingDelay = 5;

	double GenProcessingDelay(double fMean);
	void SendTradeResult(PTradeRequest pTradeRequest);

public:
	static const INT32 NO_OUTSTANDING_TRADES =
	    CTimerWheel<TTradeRequest, CMEETradingFloor, 5, 1>::NO_OUTSTANDING_TIMERS;

	// Constructor - use default RNG seed
	CMEETradingFloor(CMEESUTInterface *pSUT, CMEEPriceBoard *pPriceBoard, CMEETickerTape *pTickerTape,
	                 CDateTime *pBaseTime, CDateTime *pCurrentTime);

	// Constructor - RNG seed provided
	CMEETradingFloor(CMEESUTInterface *pSUT, CMEEPriceBoard *pPriceBoard, CMEETickerTape *pTickerTape,
	                 CDateTime *pBaseTime, CDateTime *pCurrentTime, RNGSEED RNGSeed);

	~CMEETradingFloor(void);

	INT32 SubmitTradeRequest(PTradeRequest pTradeRequest);
	INT32 GenerateTradeResult(void);

	RNGSEED GetRNGSeed(void);
	void SetRNGSeed(RNGSEED RNGSeed);
};

} // namespace TPCE

#endif // MEE_TRADING_FLOOR_H
