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
 *   Description:        Implemenation of the MEEPriceBoard class.
 *                       See MEEPriceBoad.h for a description.
 ******************************************************************************/

#include "main/MEEPriceBoard.h"

using namespace TPCE;

CMEEPriceBoard::CMEEPriceBoard(INT32 TradingTimeSoFar, CDateTime *pBaseTime, CDateTime *pCurrentTime,
                               const DataFileManager &dfm)
    : m_fMeanInTheMoneySubmissionDelay(1.0), m_Security(), m_SecurityFile(dfm.SecurityFile()),
      m_iNumberOfSecurities(0) {
	// Number of securities is based on "active" customers, as per sub-committee
	// decision to have a scaled-down database look as much as possible as the
	// smaller database.

	// Note that this decision will ensure that some percentage of Trade-Order
	// transactions will *not* be subject to lock contention when accessing the
	// LAST_TRADE table, since they are "outside" of the active range that is
	// updated by the MEE via the Market-Feed transaction.  This may provide an
	// incentive for sponsors to over-build and under-scale at run-time, to the
	// detriment of the benchmark.  If this is the case, then we should require
	// the MEE to run using "configured" securities.

	m_iNumberOfSecurities = m_SecurityFile.GetActiveSecurityCount();
	m_Security.Init(TradingTimeSoFar, pBaseTime, pCurrentTime, m_fMeanInTheMoneySubmissionDelay);
	m_SecurityFile.LoadSymbolToIdMap();
}

CMEEPriceBoard::~CMEEPriceBoard(void) {
}

void CMEEPriceBoard::GetSymbol(TIdent SecurityIndex,
                               char *szOutput,    // output buffer
                               size_t iOutputLen) // size of the output buffer (including null));
{
	return (m_SecurityFile.CreateSymbol(SecurityIndex, szOutput, iOutputLen));
}

CMoney CMEEPriceBoard::GetMinPrice() {
	return (m_Security.GetMinPrice());
}

CMoney CMEEPriceBoard::GetMaxPrice() {
	return (m_Security.GetMaxPrice());
}

CMoney CMEEPriceBoard::GetCurrentPrice(TIdent SecurityIndex) {
	return (m_Security.GetCurrentPrice(SecurityIndex));
}

CMoney CMEEPriceBoard::GetCurrentPrice(char *pSecuritySymbol) {
	return (m_Security.GetCurrentPrice(m_SecurityFile.GetIndex(pSecuritySymbol)));
}

CMoney CMEEPriceBoard::CalculatePrice(char *pSecuritySymbol, double fTime) {
	return (m_Security.CalculatePrice(m_SecurityFile.GetIndex(pSecuritySymbol), fTime));
}

double CMEEPriceBoard::GetSubmissionTime(char *pSecuritySymbol, double fPendingTime, CMoney fLimitPrice,
                                         eTradeTypeID TradeType) {
	return (
	    m_Security.GetSubmissionTime(m_SecurityFile.GetIndex(pSecuritySymbol), fPendingTime, fLimitPrice, TradeType));
}

double CMEEPriceBoard::GetSubmissionTime(TIdent SecurityIndex, double fPendingTime, CMoney fLimitPrice,
                                         eTradeTypeID TradeType) {
	return (m_Security.GetSubmissionTime(SecurityIndex, fPendingTime, fLimitPrice, TradeType));
}

double CMEEPriceBoard::GetCompletionTime(TIdent SecurityIndex, double fSubmissionTime,
                                         CMoney *pCompletionPrice // output parameter
) {
	return (m_Security.GetCompletionTime(SecurityIndex, fSubmissionTime, pCompletionPrice));
}
