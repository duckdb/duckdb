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
 *   Description:        Implemenation of the MEETradingFloor class.
 *                       See MEETradingFloor.h for a description.
 ******************************************************************************/

#include "main/MEETradingFloor.h"

using namespace TPCE;

RNGSEED CMEETradingFloor::GetRNGSeed(void) {
	return (m_rnd.GetSeed());
}

void CMEETradingFloor::SetRNGSeed(RNGSEED RNGSeed) {
	m_rnd.SetSeed(RNGSeed);
}

// Constructor - use default RNG seed
CMEETradingFloor::CMEETradingFloor(CMEESUTInterface *pSUT, CMEEPriceBoard *pPriceBoard, CMEETickerTape *pTickerTape,
                                   CDateTime *pBaseTime, CDateTime *pCurrentTime)
    : m_pSUT(pSUT), m_pPriceBoard(pPriceBoard), m_pTickerTape(pTickerTape), m_pBaseTime(pBaseTime),
      m_pCurrentTime(pCurrentTime), m_rnd(RNGSeedBaseMEETradingFloor), m_OrderProcessingDelayMean(1.0) {
}

// Constructor - RNG seed provided
CMEETradingFloor::CMEETradingFloor(CMEESUTInterface *pSUT, CMEEPriceBoard *pPriceBoard, CMEETickerTape *pTickerTape,
                                   CDateTime *pBaseTime, CDateTime *pCurrentTime, RNGSEED RNGSeed)
    : m_pSUT(pSUT), m_pPriceBoard(pPriceBoard), m_pTickerTape(pTickerTape), m_pBaseTime(pBaseTime),
      m_pCurrentTime(pCurrentTime), m_rnd(RNGSeed), m_OrderProcessingDelayMean(1.0) {
}

CMEETradingFloor::~CMEETradingFloor(void) {
}

inline double CMEETradingFloor::GenProcessingDelay(double Mean) {
	double Result = RoundToNearestNsec(m_rnd.RndDoubleNegExp(Mean));

	if (Result > m_MaxOrderProcessingDelay) {
		return (m_MaxOrderProcessingDelay);
	} else {
		return (Result);
	}
}

INT32 CMEETradingFloor::SubmitTradeRequest(PTradeRequest pTradeRequest) {
	switch (pTradeRequest->eAction) {
	case eMEEProcessOrder: { // Use {...} to keep compiler from complaining that
		                     // other cases/default skip initialization of
		                     // pNewOrder.
		// This is either a market order or a limit order that has been
		// triggered, so it gets traded right away. Make a copy in storage under
		// our control.
		PTradeRequest pNewOrder = new TTradeRequest;
		*pNewOrder = *pTradeRequest;
		return (m_OrderTimers.StartTimer(GenProcessingDelay(m_OrderProcessingDelayMean), this,
		                                 &CMEETradingFloor::SendTradeResult, pNewOrder));
	} // Use {...} to keep compiler from complaining that other cases/default
	  // skip initialization of pNewOrder.
	case eMEESetLimitOrderTrigger:
		// This is a limit order
		m_pTickerTape->PostLimitOrder(pTradeRequest);
		return (m_OrderTimers.ProcessExpiredTimers());
	default:
		// Throw and exception - SHOULD NEVER GET HERE!
		return (m_OrderTimers.ProcessExpiredTimers());
	}
}

INT32 CMEETradingFloor::GenerateTradeResult(void) {
	return (m_OrderTimers.ProcessExpiredTimers());
}

void CMEETradingFloor::SendTradeResult(PTradeRequest pTradeRequest) {
	eTradeTypeID eTradeType;
	TTradeResultTxnInput TxnInput;
	TTickerEntry TickerEntry;
	double CurrentPrice = -1.0;

	eTradeType = m_pTickerTape->ConvertTradeTypeIdToEnum(pTradeRequest->trade_type_id);
	CurrentPrice = m_pPriceBoard->GetCurrentPrice(pTradeRequest->symbol).DollarAmount();

	// Populate Trade-Result inputs, and send to SUT
	TxnInput.trade_id = pTradeRequest->trade_id;

	// Make sure the Trade-Result has the right price based on the type of
	// trade.
	if ((eTradeType == eLimitBuy && pTradeRequest->price_quote < CurrentPrice) ||
	    (eTradeType == eLimitSell && pTradeRequest->price_quote > CurrentPrice)) {
		TxnInput.trade_price = pTradeRequest->price_quote;
	} else {
		TxnInput.trade_price = CurrentPrice;
	}

	m_pSUT->TradeResult(&TxnInput);

	// Populate Ticker Entry information
	strncpy(TickerEntry.symbol, pTradeRequest->symbol, sizeof(TickerEntry.symbol));
	TickerEntry.trade_qty = pTradeRequest->trade_qty;

	// Note that the Trade-Result sent out above does not always use
	// the current price. We're about to "lie" by setting the ticker entry
	// price to the current price regardless of how the Trade-Result
	// price was actually set. We do this to preserve the continuity
	// of the price curve. This is important because the ticker prices
	// are used by transactions to make decisions. It is possible for
	// the Trade-Result price to be out of sync with the price curve,
	// and putting this price into the ticker stream will alter the
	// behavior of other transactions. If you're having a guilt-attack
	// about this lie (or are just plain curious), comment out the
	// assignment using CurrentPrice and uncomment the assignment
	// using TxnInput.trade_price.
	//
	TickerEntry.price_quote = CurrentPrice;
	// TickerEntry.price_quote = TxnInput.trade_price;

	m_pTickerTape->AddEntry(&TickerEntry);

	delete pTradeRequest;
}
