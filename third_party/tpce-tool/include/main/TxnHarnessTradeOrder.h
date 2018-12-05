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

/*
 *   Trade Order transaction class.
 *
 */
#ifndef TXN_HARNESS_TRADE_ORDER_H
#define TXN_HARNESS_TRADE_ORDER_H

#include "TxnHarnessDBInterface.h"

namespace TPCE {

class CTradeOrder {
	CTradeOrderDBInterface *m_db;
	CSendToMarketInterface *m_pSendToMarket;

public:
	CTradeOrder(CTradeOrderDBInterface *pDB, CSendToMarketInterface *pSendToMarket)
	    : m_db(pDB), m_pSendToMarket(pSendToMarket){};

	void DoTxn(PTradeOrderTxnInput pTxnInput, PTradeOrderTxnOutput pTxnOutput) {
		// Initialization
		TTradeOrderFrame1Input Frame1Input;
		TTradeOrderFrame1Output Frame1Output;
		TTradeOrderFrame2Input Frame2Input;
		TTradeOrderFrame2Output Frame2Output;
		TTradeOrderFrame3Input Frame3Input;
		TTradeOrderFrame3Output Frame3Output;
		TTradeOrderFrame4Input Frame4Input;
		TTradeOrderFrame4Output Frame4Output;

		TTradeRequest TradeRequestForMEE; // sent to MEE

		TXN_HARNESS_SET_STATUS_SUCCESS;

		//
		// FRAME 1
		//

		// Copy Frame 1 Input
		Frame1Input.acct_id = pTxnInput->acct_id;

		// Execute Frame 1
		m_db->DoTradeOrderFrame1(&Frame1Input, &Frame1Output);

		// Validate Frame 1 Output
		if (Frame1Output.num_found != 1) {
			TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::TOF1_ERROR1);
		}

		TXN_HARNESS_EARLY_EXIT_ON_ERROR;

		//
		// FRAME 2
		//

		if (strcmp(pTxnInput->exec_l_name, Frame1Output.cust_l_name) ||
		    strcmp(pTxnInput->exec_f_name, Frame1Output.cust_f_name) ||
		    strcmp(pTxnInput->exec_tax_id, Frame1Output.tax_id)) {
			// Copy Frame 2 Input
			Frame2Input.acct_id = pTxnInput->acct_id;
			strncpy(Frame2Input.exec_f_name, pTxnInput->exec_f_name, sizeof(Frame2Input.exec_f_name));
			strncpy(Frame2Input.exec_l_name, pTxnInput->exec_l_name, sizeof(Frame2Input.exec_l_name));
			strncpy(Frame2Input.exec_tax_id, pTxnInput->exec_tax_id, sizeof(Frame2Input.exec_tax_id));

			// Execute Frame 2
			m_db->DoTradeOrderFrame2(&Frame2Input, &Frame2Output);

			// Validate Frame 2 Output
			if (Frame2Output.ap_acl[0] == '\0') {
				TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::TOF2_ERROR1);

				// Rollback
				m_db->DoTradeOrderFrame5();
				return;
			}
		}

		TXN_HARNESS_EARLY_EXIT_ON_ERROR;

		//
		// FRAME 3
		//

		// Copy Frame 3 Input
		Frame3Input.acct_id = pTxnInput->acct_id;
		Frame3Input.cust_id = Frame1Output.cust_id;
		Frame3Input.cust_tier = Frame1Output.cust_tier;
		Frame3Input.is_lifo = pTxnInput->is_lifo;
		strncpy(Frame3Input.issue, pTxnInput->issue, sizeof(Frame3Input.issue));
		strncpy(Frame3Input.st_pending_id, pTxnInput->st_pending_id, sizeof(Frame3Input.st_pending_id));
		strncpy(Frame3Input.st_submitted_id, pTxnInput->st_submitted_id, sizeof(Frame3Input.st_submitted_id));
		Frame3Input.tax_status = Frame1Output.tax_status;
		Frame3Input.trade_qty = pTxnInput->trade_qty;
		strncpy(Frame3Input.trade_type_id, pTxnInput->trade_type_id, sizeof(Frame3Input.trade_type_id));
		Frame3Input.type_is_margin = pTxnInput->type_is_margin;
		strncpy(Frame3Input.co_name, pTxnInput->co_name, sizeof(Frame3Input.co_name));
		Frame3Input.requested_price = pTxnInput->requested_price;
		strncpy(Frame3Input.symbol, pTxnInput->symbol, sizeof(Frame3Input.symbol));

		// Execute Frame 3
		m_db->DoTradeOrderFrame3(&Frame3Input, &Frame3Output);

		// Validate Frame 3 Output
		if (Frame3Output.sell_value > Frame3Output.buy_value &&
		    ((Frame3Input.tax_status == 1) || (Frame3Input.tax_status == 2)) && Frame3Output.tax_amount <= 0.00) {
			TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::TOF3_ERROR1);
		}
		if (Frame3Output.comm_rate <= 0.0000) {
			TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::TOF3_ERROR2);
		}
		if (Frame3Output.charge_amount == 0.00) {
			TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::TOF3_ERROR3);
		}

		// Copy Frame 3 Output
		pTxnOutput->buy_value = Frame3Output.buy_value; // output param
		pTxnOutput->sell_value = Frame3Output.sell_value;
		pTxnOutput->tax_amount = Frame3Output.tax_amount;

		TXN_HARNESS_EARLY_EXIT_ON_ERROR;

		//
		// FRAME 4
		//

		// Copy Frame 4 Input
		Frame4Input.acct_id = pTxnInput->acct_id;
		Frame4Input.broker_id = Frame1Output.broker_id;
		Frame4Input.charge_amount = Frame3Output.charge_amount;
		Frame4Input.comm_amount = Frame3Output.comm_rate / 100 * pTxnInput->trade_qty * Frame3Output.requested_price;
		// round up for correct precision (cents only)
		Frame4Input.comm_amount = (double)((int)(100.00 * Frame4Input.comm_amount + 0.5)) / 100.00;

		snprintf(Frame4Input.exec_name, sizeof(Frame4Input.exec_name), "%s %s", pTxnInput->exec_f_name,
		         pTxnInput->exec_l_name);
		Frame4Input.is_cash = !Frame3Input.type_is_margin;
		Frame4Input.is_lifo = pTxnInput->is_lifo;
		Frame4Input.requested_price = Frame3Output.requested_price;
		strncpy(Frame4Input.status_id, Frame3Output.status_id, sizeof(Frame4Input.status_id));
		strncpy(Frame4Input.symbol, Frame3Output.symbol, sizeof(Frame4Input.symbol));
		Frame4Input.trade_qty = pTxnInput->trade_qty;
		strncpy(Frame4Input.trade_type_id, pTxnInput->trade_type_id, sizeof(Frame4Input.trade_type_id));
		Frame4Input.type_is_market = Frame3Output.type_is_market;

		// Execute Frame 4
		m_db->DoTradeOrderFrame4(&Frame4Input, &Frame4Output);

		// Copy Frame 4 Output
		pTxnOutput->trade_id = Frame4Output.trade_id;

		TXN_HARNESS_EARLY_EXIT_ON_ERROR;

		//
		// FRAME 5/6
		//

		if (pTxnInput->roll_it_back) {
			// Execute Frame 5
			m_db->DoTradeOrderFrame5();

			TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::EXPECTED_ROLLBACK);
		} else {
			// Execute Frame 6
			m_db->DoTradeOrderFrame6();

			//
			// Send to Market Exchange Emulator
			//
			TradeRequestForMEE.price_quote = Frame4Input.requested_price;
			strncpy(TradeRequestForMEE.symbol, Frame4Input.symbol, sizeof(TradeRequestForMEE.symbol));
			TradeRequestForMEE.trade_id = Frame4Output.trade_id;
			TradeRequestForMEE.trade_qty = Frame4Input.trade_qty;
			strncpy(TradeRequestForMEE.trade_type_id, pTxnInput->trade_type_id,
			        sizeof(TradeRequestForMEE.trade_type_id));
			// TradeRequestForMEE.eTradeType = pTxnInput->eSTMTradeType;
			if (Frame4Input.type_is_market) {
				TradeRequestForMEE.eAction = eMEEProcessOrder;
			} else {
				TradeRequestForMEE.eAction = eMEESetLimitOrderTrigger;
			}

			m_pSendToMarket->SendToMarketFromHarness(TradeRequestForMEE); // maybe should check the return code here
		}
	}
};

} // namespace TPCE

#endif // TXN_HARNESS_TRADE_ORDER_H
