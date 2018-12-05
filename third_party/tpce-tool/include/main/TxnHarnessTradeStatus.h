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

#ifndef TXN_HARNESS_TRADE_STATUS_H
#define TXN_HARNESS_TRADE_STATUS_H

#include "TxnHarnessDBInterface.h"

namespace TPCE {

class CTradeStatus {
	CTradeStatusDBInterface *m_db;

public:
	CTradeStatus(CTradeStatusDBInterface *pDB) : m_db(pDB) {
	}

	void DoTxn(PTradeStatusTxnInput pTxnInput, PTradeStatusTxnOutput pTxnOutput) {
		// Initialization
		TTradeStatusFrame1Output Frame1Output;

		TXN_HARNESS_SET_STATUS_SUCCESS;

		// Execute Frame 1
		m_db->DoTradeStatusFrame1(pTxnInput, &Frame1Output);

		// Validate Frame 1 Output
		if (Frame1Output.num_found != max_trade_status_len) {
			TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::TSF1_ERROR1);
		}

		// Copy Frame 1 Output
		for (int i = 0; i < Frame1Output.num_found && i < max_trade_status_len; i++) {
			strncpy(pTxnOutput->status_name[i], Frame1Output.status_name[i], sizeof(pTxnOutput->status_name[i]));
			pTxnOutput->trade_id[i] = Frame1Output.trade_id[i];
		}
	}
};

} // namespace TPCE

#endif // TXN_HARNESS_TRADE_STATUS_H
