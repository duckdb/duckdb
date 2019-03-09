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
#include "w_web_sales.h"
#include "w_web_returns.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "build_support.h"
#include "misc.h"
#include "tables.h"
#include "constants.h"
#include "nulls.h"
#include "tdefs.h"
#include "scaling.h"
#include "permute.h"
#include "scd.h"
#include "parallel.h"

#include "append_info.h"

struct W_WEB_SALES_TBL g_w_web_sales;
ds_key_t skipDays(int nTable, ds_key_t *pRemainder);

static ds_key_t kNewDateIndex = 0;
static ds_key_t jDate;
static int nItemIndex = 0;

/*
 * the validation process requires generating a single lineitem
 * so the main mk_xxx routine has been split into a master record portion
 * and a detail/lineitem portion.
 */
static void mk_master(void *info_arr, ds_key_t index) {
	static decimal_t dMin, dMax;
	int nGiftPct;
	struct W_WEB_SALES_TBL *r;
	static int bInit = 0, nItemCount;

	r = &g_w_web_sales;

	if (!bInit) {
		strtodec(&dMin, "1.00");
		strtodec(&dMax, "100000.00");
		jDate = skipDays(WEB_SALES, &kNewDateIndex);
		nItemCount = (int)getIDCount(ITEM);
		bInit = 1;
	}

	/***
	 * some attributes reamin the same for each lineitem in an order; others are
	 * different for each lineitem. Since the number of lineitems per order is
	 * static, we can use a modulo to determine when to change the semi-static
	 * values
	 */
	while (index > kNewDateIndex) /* need to move to a new date */
	{
		jDate += 1;
		kNewDateIndex += dateScaling(WEB_SALES, jDate);
	}

	r->ws_sold_date_sk = mk_join(WS_SOLD_DATE_SK, DATET, 1);
	r->ws_sold_time_sk = mk_join(WS_SOLD_TIME_SK, TIME, 1);
	r->ws_bill_customer_sk = mk_join(WS_BILL_CUSTOMER_SK, CUSTOMER, 1);
	r->ws_bill_cdemo_sk = mk_join(WS_BILL_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);
	r->ws_bill_hdemo_sk = mk_join(WS_BILL_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);
	r->ws_bill_addr_sk = mk_join(WS_BILL_ADDR_SK, CUSTOMER_ADDRESS, 1);

	/* most orders are for the ordering customers, some are not */
	genrand_integer(&nGiftPct, DIST_UNIFORM, 0, 99, 0, WS_SHIP_CUSTOMER_SK);
	if (nGiftPct > WS_GIFT_PCT) {
		r->ws_ship_customer_sk = mk_join(WS_SHIP_CUSTOMER_SK, CUSTOMER, 2);
		r->ws_ship_cdemo_sk = mk_join(WS_SHIP_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2);
		r->ws_ship_hdemo_sk = mk_join(WS_SHIP_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2);
		r->ws_ship_addr_sk = mk_join(WS_SHIP_ADDR_SK, CUSTOMER_ADDRESS, 2);
	} else {
		r->ws_ship_customer_sk = r->ws_bill_customer_sk;
		r->ws_ship_cdemo_sk = r->ws_bill_cdemo_sk;
		r->ws_ship_hdemo_sk = r->ws_bill_hdemo_sk;
		r->ws_ship_addr_sk = r->ws_bill_addr_sk;
	}

	r->ws_order_number = index;
	genrand_integer(&nItemIndex, DIST_UNIFORM, 1, nItemCount, 0, WS_ITEM_SK);

	return;
}

static void mk_detail(void *info_arr, int bPrint) {
	static int *pItemPermutation, nItemCount, bInit = 0;
	struct W_WEB_SALES_TBL *r;
	int nShipLag, nTemp;
	struct W_WEB_RETURNS_TBL w_web_returns;
	tdef *pT = getSimpleTdefsByNumber(WEB_SALES);

	if (!bInit) {
		jDate = skipDays(WEB_SALES, &kNewDateIndex);
		pItemPermutation = makePermutation(NULL, nItemCount = (int)getIDCount(ITEM), WS_PERMUTATION);

		bInit = 1;
	}

	r = &g_w_web_sales;

	nullSet(&pT->kNullBitMap, WS_NULLS);

	/* orders are shipped some number of days after they are ordered,
	 * and not all lineitems ship at the same time
	 */
	genrand_integer(&nShipLag, DIST_UNIFORM, WS_MIN_SHIP_DELAY, WS_MAX_SHIP_DELAY, 0, WS_SHIP_DATE_SK);
	r->ws_ship_date_sk = r->ws_sold_date_sk + nShipLag;

	if (++nItemIndex > nItemCount)
		nItemIndex = 1;
	r->ws_item_sk = matchSCDSK(getPermutationEntry(pItemPermutation, nItemIndex), r->ws_sold_date_sk, ITEM);

	/* the web page needs to be valid for the sale date */
	r->ws_web_page_sk = mk_join(WS_WEB_PAGE_SK, WEB_PAGE, r->ws_sold_date_sk);
	r->ws_web_site_sk = mk_join(WS_WEB_SITE_SK, WEB_SITE, r->ws_sold_date_sk);

	r->ws_ship_mode_sk = mk_join(WS_SHIP_MODE_SK, SHIP_MODE, 1);
	r->ws_warehouse_sk = mk_join(WS_WAREHOUSE_SK, WAREHOUSE, 1);
	r->ws_promo_sk = mk_join(WS_PROMO_SK, PROMOTION, 1);
	set_pricing(WS_PRICING, &r->ws_pricing);

	/**
	 * having gone to the trouble to make the sale, now let's see if it gets
	 * returned
	 */
	genrand_integer(&nTemp, DIST_UNIFORM, 0, 99, 0, WR_IS_RETURNED);
	if (nTemp < WR_RETURN_PCT) {
		mk_w_web_returns(&w_web_returns, 1);

		struct W_WEB_RETURNS_TBL *rr = &w_web_returns;

		void *info = append_info_get(info_arr, WEB_RETURNS);
		append_row_start(info);

		append_key(info, rr->wr_returned_date_sk);
		append_key(info, rr->wr_returned_time_sk);
		append_key(info, rr->wr_item_sk);
		append_key(info, rr->wr_refunded_customer_sk);
		append_key(info, rr->wr_refunded_cdemo_sk);
		append_key(info, rr->wr_refunded_hdemo_sk);
		append_key(info, rr->wr_refunded_addr_sk);
		append_key(info, rr->wr_returning_customer_sk);
		append_key(info, rr->wr_returning_cdemo_sk);
		append_key(info, rr->wr_returning_hdemo_sk);
		append_key(info, rr->wr_returning_addr_sk);
		append_key(info, rr->wr_web_page_sk);
		append_key(info, rr->wr_reason_sk);
		append_key(info, rr->wr_order_number);
		append_integer(info, rr->wr_pricing.quantity);
		append_decimal(info, &rr->wr_pricing.net_paid);
		append_decimal(info, &rr->wr_pricing.ext_tax);
		append_decimal(info, &rr->wr_pricing.net_paid_inc_tax);
		append_decimal(info, &rr->wr_pricing.fee);
		append_decimal(info, &rr->wr_pricing.ext_ship_cost);
		append_decimal(info, &rr->wr_pricing.refunded_cash);
		append_decimal(info, &rr->wr_pricing.reversed_charge);
		append_decimal(info, &rr->wr_pricing.store_credit);
		append_decimal(info, &rr->wr_pricing.net_loss);

		append_row_end(info);
	}

	void *info = append_info_get(info_arr, WEB_SALES);
	append_row_start(info);

	append_key(info, r->ws_sold_date_sk);
	append_key(info, r->ws_sold_time_sk);
	append_key(info, r->ws_ship_date_sk);
	append_key(info, r->ws_item_sk);
	append_key(info, r->ws_bill_customer_sk);
	append_key(info, r->ws_bill_cdemo_sk);
	append_key(info, r->ws_bill_hdemo_sk);
	append_key(info, r->ws_bill_addr_sk);
	append_key(info, r->ws_ship_customer_sk);
	append_key(info, r->ws_ship_cdemo_sk);
	append_key(info, r->ws_ship_hdemo_sk);
	append_key(info, r->ws_ship_addr_sk);
	append_key(info, r->ws_web_page_sk);
	append_key(info, r->ws_web_site_sk);
	append_key(info, r->ws_ship_mode_sk);
	append_key(info, r->ws_warehouse_sk);
	append_key(info, r->ws_promo_sk);
	append_key(info, r->ws_order_number);
	append_integer(info, r->ws_pricing.quantity);
	append_decimal(info, &r->ws_pricing.wholesale_cost);
	append_decimal(info, &r->ws_pricing.list_price);
	append_decimal(info, &r->ws_pricing.sales_price);
	append_decimal(info, &r->ws_pricing.ext_discount_amt);
	append_decimal(info, &r->ws_pricing.ext_sales_price);
	append_decimal(info, &r->ws_pricing.ext_wholesale_cost);
	append_decimal(info, &r->ws_pricing.ext_list_price);
	append_decimal(info, &r->ws_pricing.ext_tax);
	append_decimal(info, &r->ws_pricing.coupon_amt);
	append_decimal(info, &r->ws_pricing.ext_ship_cost);
	append_decimal(info, &r->ws_pricing.net_paid);
	append_decimal(info, &r->ws_pricing.net_paid_inc_tax);
	append_decimal(info, &r->ws_pricing.net_paid_inc_ship);
	append_decimal(info, &r->ws_pricing.net_paid_inc_ship_tax);
	append_decimal(info, &r->ws_pricing.net_profit);

	append_row_end(info);

	return;
}

/*
 * mk_web_sales
 */
int mk_w_web_sales(void *info_arr, ds_key_t index) {
	int nLineitems, i;

	/* build the static portion of an order */
	mk_master(info_arr, index);

	/* set the number of lineitems and build them */
	genrand_integer(&nLineitems, DIST_UNIFORM, 8, 16, 9, WS_ORDER_NUMBER);
	for (i = 1; i <= nLineitems; i++) {
		mk_detail(info_arr, 1);
	}

	/**
	 * and finally return 1 since we have already printed the rows
	 */
	return 0;
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int vld_web_sales(int nTable, ds_key_t kRow, int *Permutation) {
	int nLineitem, nMaxLineitem, i;

	row_skip(nTable, kRow - 1);
	row_skip(WEB_RETURNS, (kRow - 1));
	jDate = skipDays(WEB_SALES, &kNewDateIndex);
	mk_master(NULL, kRow);
	genrand_integer(&nMaxLineitem, DIST_UNIFORM, 8, 16, 9, WS_ORDER_NUMBER);
	genrand_integer(&nLineitem, DIST_UNIFORM, 1, nMaxLineitem, 0, WS_PRICING_QUANTITY);
	for (i = 1; i < nLineitem; i++) {
		mk_detail(NULL, 0);
	}
	mk_detail(NULL, 1);

	return (0);
}
