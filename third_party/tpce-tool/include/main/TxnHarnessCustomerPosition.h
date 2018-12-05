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
 *   Customer Position transaction class.
 *
 */
#ifndef TXN_HARNESS_CUSTOMER_POSITION_H
#define TXN_HARNESS_CUSTOMER_POSITION_H

#include "TxnHarnessDBInterface.h"

namespace TPCE {

class CCustomerPosition {
	CCustomerPositionDBInterface *m_db;

public:
	CCustomerPosition(CCustomerPositionDBInterface *pDB) : m_db(pDB){};

	void DoTxn(PCustomerPositionTxnInput pTxnInput, PCustomerPositionTxnOutput pTxnOutput) {
		// Initialization
		TCustomerPositionFrame1Input Frame1Input;
		TCustomerPositionFrame1Output Frame1Output;
		TCustomerPositionFrame2Input Frame2Input;
		TCustomerPositionFrame2Output Frame2Output;

		TXN_HARNESS_SET_STATUS_SUCCESS;

		// Copy Frame 1 Input
		Frame1Input.cust_id = pTxnInput->cust_id;
		strncpy(Frame1Input.tax_id, pTxnInput->tax_id, sizeof(Frame1Input.tax_id));

		// Execute Frame 1
		m_db->DoCustomerPositionFrame1(&Frame1Input, &Frame1Output);

		// Validate Frame 1 Output
		if ((Frame1Output.acct_len < 1) || (Frame1Output.acct_len > max_acct_len)) {
			TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::CPF1_ERROR1);
		}

		// Copy Frame 1 Output
		pTxnOutput->acct_len = Frame1Output.acct_len;
		pTxnOutput->c_ad_id = Frame1Output.c_ad_id;
		strncpy(pTxnOutput->c_area_1, Frame1Output.c_area_1, sizeof(pTxnOutput->c_area_1));
		strncpy(pTxnOutput->c_area_2, Frame1Output.c_area_2, sizeof(pTxnOutput->c_area_2));
		strncpy(pTxnOutput->c_area_3, Frame1Output.c_area_3, sizeof(pTxnOutput->c_area_3));
		strncpy(pTxnOutput->c_ctry_1, Frame1Output.c_ctry_1, sizeof(pTxnOutput->c_ctry_1));
		strncpy(pTxnOutput->c_ctry_2, Frame1Output.c_ctry_2, sizeof(pTxnOutput->c_ctry_2));
		strncpy(pTxnOutput->c_ctry_3, Frame1Output.c_ctry_3, sizeof(pTxnOutput->c_ctry_3));
		pTxnOutput->c_dob = Frame1Output.c_dob;
		strncpy(pTxnOutput->c_email_1, Frame1Output.c_email_1, sizeof(pTxnOutput->c_email_1));
		strncpy(pTxnOutput->c_email_2, Frame1Output.c_email_2, sizeof(pTxnOutput->c_email_2));
		strncpy(pTxnOutput->c_ext_1, Frame1Output.c_ext_1, sizeof(pTxnOutput->c_ext_1));
		strncpy(pTxnOutput->c_ext_2, Frame1Output.c_ext_2, sizeof(pTxnOutput->c_ext_2));
		strncpy(pTxnOutput->c_ext_3, Frame1Output.c_ext_3, sizeof(pTxnOutput->c_ext_3));
		strncpy(pTxnOutput->c_f_name, Frame1Output.c_f_name, sizeof(pTxnOutput->c_f_name));
		strncpy(pTxnOutput->c_gndr, Frame1Output.c_gndr, sizeof(pTxnOutput->c_gndr));
		strncpy(pTxnOutput->c_l_name, Frame1Output.c_l_name, sizeof(pTxnOutput->c_l_name));
		strncpy(pTxnOutput->c_local_1, Frame1Output.c_local_1, sizeof(pTxnOutput->c_local_1));
		strncpy(pTxnOutput->c_local_2, Frame1Output.c_local_2, sizeof(pTxnOutput->c_local_2));
		strncpy(pTxnOutput->c_local_3, Frame1Output.c_local_3, sizeof(pTxnOutput->c_local_3));
		strncpy(pTxnOutput->c_m_name, Frame1Output.c_m_name, sizeof(pTxnOutput->c_m_name));
		strncpy(pTxnOutput->c_st_id, Frame1Output.c_st_id, sizeof(pTxnOutput->c_st_id));
		pTxnOutput->c_tier = Frame1Output.c_tier;
		// pTxnOutput->cust_id = Frame1Output.cust_id;

		for (int i = 0; i < Frame1Output.acct_len && i < max_acct_len; i++) {
			pTxnOutput->acct_id[i] = Frame1Output.acct_id[i];
			pTxnOutput->asset_total[i] = Frame1Output.asset_total[i];
			pTxnOutput->cash_bal[i] = Frame1Output.cash_bal[i];
		}

		TXN_HARNESS_EARLY_EXIT_ON_ERROR;

		if (pTxnInput->get_history) {
			// Copy Frame 2 Input
			Frame2Input.acct_id = Frame1Output.acct_id[pTxnInput->acct_id_idx];

			// Execute Frame 2
			m_db->DoCustomerPositionFrame2(&Frame2Input, &Frame2Output);

			// Validate Frame 2 Output
			if ((Frame2Output.hist_len < min_hist_len) || (Frame2Output.hist_len > max_hist_len)) {
				TXN_HARNESS_PROPAGATE_STATUS(CBaseTxnErr::CPF2_ERROR1);
			}

			// Copy Frame 2 Output
			pTxnOutput->hist_len = Frame2Output.hist_len;
			for (int i = 0; i < Frame2Output.hist_len && i < max_hist_len; i++) {
				pTxnOutput->hist_dts[i] = Frame2Output.hist_dts[i];
				pTxnOutput->qty[i] = Frame2Output.qty[i];
				strncpy(pTxnOutput->symbol[i], Frame2Output.symbol[i], sizeof(pTxnOutput->symbol[i]));
				pTxnOutput->trade_id[i] = Frame2Output.trade_id[i];
				strncpy(pTxnOutput->trade_status[i], Frame2Output.trade_status[i], sizeof(pTxnOutput->trade_status[i]));
			}
		} else {
			// Execute Frame 3
			m_db->DoCustomerPositionFrame3();

			// Copy Frame 3 Output
			pTxnOutput->hist_len = 0;
		}
	}
};

} // namespace TPCE

#endif // TXN_HARNESS_CUSTOMER_POSITION_H
