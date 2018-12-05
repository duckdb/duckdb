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
 *   Description:        Class that implements the ticker tape functionality of
 *                       the market. This class handles the batching up of
 *                       individual ticker entries into a batch suitable for the
 *                       Market-Feed transaction. For each "real" trade result
 *                       that gets added to the ticker, a number of artificial
 *                       entries are padded into the batch.
 ******************************************************************************/

#ifndef MEE_TICKER_TAPE_H
#define MEE_TICKER_TAPE_H

#include "utilities/EGenUtilities_stdafx.h"
#include "TxnHarnessStructs.h"
#include "TimerWheel.h"
#include "MEESUTInterface.h"
#include "MEEPriceBoard.h"

#include "input/DataFileTypes.h"

namespace TPCE {

class CMEETickerTape {
private:
	CMEESUTInterface *m_pSUT;
	CMEEPriceBoard *m_pPriceBoard;
	TMarketFeedTxnInput m_TxnInput;
	INT32 m_BatchIndex;
	INT32 m_BatchDuplicates;
	CRandom m_rnd;
	bool m_Enabled;
	const StatusTypeDataFile_t &m_StatusType;
	const TradeTypeDataFile_t &m_TradeType;

	static const int LIMIT_TRIGGER_TRADE_QTY;
	static const int RANDOM_TRADE_QTY_1;
	static const int RANDOM_TRADE_QTY_2;

	CTimerWheel<TTickerEntry, CMEETickerTape, 900, 1000> m_LimitOrderTimers; // Size wheel for 900 seconds with 1,000
	                                                                         // millisecond resolution.
	queue<PTickerEntry> m_InTheMoneyLimitOrderQ;

	CDateTime *m_pBaseTime;
	CDateTime *m_pCurrentTime;

	void AddToBatch(PTickerEntry pTickerEntry);
	void AddArtificialEntries(void);
	void AddLimitTrigger(PTickerEntry pTickerEntry);

	// Performs initialization common to all constructors.
	void Initialize(void);

public:
	// Constructor - use default RNG seed
	CMEETickerTape(CMEESUTInterface *pSUT, CMEEPriceBoard *pPriceBoard, CDateTime *pBaseTime, CDateTime *pCurrentTime,
	               const DataFileManager &inputFiles);

	// Constructor - RNG seed provided
	CMEETickerTape(CMEESUTInterface *pSUT, CMEEPriceBoard *pPriceBoard, CDateTime *pBaseTime, CDateTime *pCurrentTime,
	               RNGSEED RNGSeed, const DataFileManager &inputFiles);

	~CMEETickerTape(void);

	void AddEntry(PTickerEntry pTickerEntry);
	void PostLimitOrder(PTradeRequest pTradeRequest);
	bool DisableTicker(void);
	bool EnableTicker(void);
	eTradeTypeID ConvertTradeTypeIdToEnum(char *pTradeType);

	RNGSEED GetRNGSeed(void);
	void SetRNGSeed(RNGSEED RNGSeed);
};

} // namespace TPCE

#endif // MEE_TICKER_TAPE_H
