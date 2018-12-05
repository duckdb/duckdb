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
 */

#ifndef TXN_HARNESS_MARKET_FEED_H
#define TXN_HARNESS_MARKET_FEED_H

#include "TxnHarnessDBInterface.h"

namespace TPCE {

class CMarketFeed {
	CMarketFeedDBInterface *m_db;
	CSendToMarketInterface *m_pSendToMarket;

public:
	CMarketFeed(CMarketFeedDBInterface *pDB, CSendToMarketInterface *pSendToMarket)
	    : m_db(pDB), m_pSendToMarket(pSendToMarket){};

	void DoTxn(PMarketFeedTxnInput pTxnInput, PMarketFeedTxnOutput pTxnOutput) {
		// Initialization
		TMarketFeedFrame1Input Frame1Input;
		TMarketFeedFrame1Output Frame1Output;

		TXN_HARNESS_SET_STATUS_SUCCESS;

		// Copy Frame 1 Input
		Frame1Input.StatusAndTradeType = pTxnInput->StatusAndTradeType;
		for (int i = 0; i < max_feed_len; i++) {
			Frame1Input.Entries[i] = pTxnInput->Entries[i];
		}

		// Execute Frame 1
		m_db->DoMarketFeedFrame1(&Frame1Input, &Frame1Output, m_pSendToMarket);

		// Validate Frame 1 Output
		if (Frame1Output.num_updated < pTxnInput->unique_symbols) {
			TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::MFF1_ERROR1);
		}

		// Copy Frame 1 Output
		pTxnOutput->send_len = Frame1Output.send_len;
	}
};

} // namespace TPCE

#endif // TXN_HARNESS_MARKET_FEED_H
