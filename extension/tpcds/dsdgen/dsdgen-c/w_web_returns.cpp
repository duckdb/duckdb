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
 * Contributors:
 * Gradient Systems
 */
#include "config.h"
#include "porting.h"
#include <stdio.h>
#include "pricing.h"
#include "w_web_returns.h"
#include "w_web_sales.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "build_support.h"
#include "misc.h"
#include "error_msg.h"
#include "tables.h"
#include "nulls.h"
#include "tdefs.h"

struct W_WEB_RETURNS_TBL g_w_web_returns;
extern struct W_WEB_SALES_TBL g_w_web_sales;

/*
 * Routine: mk_web_returns()
 * Purpose: populate a return fact *sync'd with a sales fact*
 * Algorithm: Since the returns need to be in line with a prior sale, they are
 *built by a call from the mk_catalog_sales() routine, and then add
 *return-related information Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int mk_w_web_returns(void *row, ds_key_t index) {
	int res = 0;

	static decimal_t dMin, dMax;
	static struct W_WEB_SALES_TBL *sale;
	struct W_WEB_RETURNS_TBL *r;
	tdef *pT = getSimpleTdefsByNumber(WEB_RETURNS);

	if (row == NULL)
		r = &g_w_web_returns;
	else
		r = (W_WEB_RETURNS_TBL *)row;

	if (!InitConstants::mk_w_web_returns_init) {
		strtodec(&dMin, "1.00");
		strtodec(&dMax, "100000.00");
		InitConstants::mk_w_web_returns_init = 1;
	}

	nullSet(&pT->kNullBitMap, WR_NULLS);

	/*
	 * Some of the information in the return is taken from the original sale
	 * which has been regenerated
	 */
	sale = &g_w_web_sales;
	r->wr_item_sk = sale->ws_item_sk;
	r->wr_order_number = sale->ws_order_number;
	memcpy((void *)&r->wr_pricing, (void *)&sale->ws_pricing, sizeof(ds_pricing_t));
	r->wr_web_page_sk = sale->ws_web_page_sk;

	/*
	 * the rest of the columns are generated for this specific return
	 */
	/* the items cannot be returned until they are shipped; offset is handled in
	 * mk_join, based on sales date */
	r->wr_returned_date_sk = mk_join(WR_RETURNED_DATE_SK, DATET, sale->ws_ship_date_sk);
	r->wr_returned_time_sk = mk_join(WR_RETURNED_TIME_SK, TIME, 1);

	/* most items are returned by the people they were shipped to, but some are
	 * returned by other folks
	 */
	r->wr_refunded_customer_sk = mk_join(WR_REFUNDED_CUSTOMER_SK, CUSTOMER, 1);
	r->wr_refunded_cdemo_sk = mk_join(WR_REFUNDED_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);
	r->wr_refunded_hdemo_sk = mk_join(WR_REFUNDED_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);
	r->wr_refunded_addr_sk = mk_join(WR_REFUNDED_ADDR_SK, CUSTOMER_ADDRESS, 1);
	if (genrand_integer(NULL, DIST_UNIFORM, 0, 99, 0, WR_RETURNING_CUSTOMER_SK) < WS_GIFT_PCT) {
		r->wr_refunded_customer_sk = sale->ws_ship_customer_sk;
		r->wr_refunded_cdemo_sk = sale->ws_ship_cdemo_sk;
		r->wr_refunded_hdemo_sk = sale->ws_ship_hdemo_sk;
		r->wr_refunded_addr_sk = sale->ws_ship_addr_sk;
	}
	r->wr_returning_customer_sk = r->wr_refunded_customer_sk;
	r->wr_returning_cdemo_sk = r->wr_refunded_cdemo_sk;
	r->wr_returning_hdemo_sk = r->wr_refunded_hdemo_sk;
	r->wr_returning_addr_sk = r->wr_refunded_addr_sk;

	r->wr_reason_sk = mk_join(WR_REASON_SK, REASON, 1);
	genrand_integer(&r->wr_pricing.quantity, DIST_UNIFORM, 1, sale->ws_pricing.quantity, 0, WR_PRICING);
	set_pricing(WR_PRICING, &r->wr_pricing);

	return (res);
}
