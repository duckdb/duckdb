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
#include "w_store_sales.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "constants.h"
#include "decimal.h"
#include "genrand.h"
#include "nulls.h"
#include "parallel.h"
#include "permute.h"
#include "porting.h"
#include "scaling.h"
#include "scd.h"
#include "tables.h"
#include "tdefs.h"
#include "w_store_returns.h"

#ifdef JMS
extern rng_t Streams[];
#endif

struct W_STORE_SALES_TBL g_w_store_sales;
ds_key_t skipDays(int nTable, ds_key_t *pRemainder);
static int *pItemPermutation, nItemCount, nItemIndex;
static ds_key_t jDate, kNewDateIndex;

/*
 * mk_store_sales
 */
static void mk_master(void *info_arr, ds_key_t index) {
	struct W_STORE_SALES_TBL *r;
	static decimal_t dMin, dMax;
	static int nMaxItemCount;
	static ds_key_t kNewDateIndex = 0;

	r = &g_w_store_sales;

	if (!InitConstants::mk_master_store_sales_init) {
		strtodec(&dMin, "1.00");
		strtodec(&dMax, "100000.00");
		nMaxItemCount = 20;
		jDate = skipDays(STORE_SALES, &kNewDateIndex);
		pItemPermutation = makePermutation(NULL, nItemCount = (int)getIDCount(ITEM), SS_PERMUTATION);

		InitConstants::mk_master_store_sales_init = 1;
	}

	while (index > kNewDateIndex) /* need to move to a new date */
	{
		jDate += 1;
		kNewDateIndex += dateScaling(STORE_SALES, jDate);
	}
	r->ss_sold_store_sk = mk_join(SS_SOLD_STORE_SK, STORE, 1);
	r->ss_sold_time_sk = mk_join(SS_SOLD_TIME_SK, TIME, 1);
	r->ss_sold_date_sk = mk_join(SS_SOLD_DATE_SK, DATET, 1);
	r->ss_sold_customer_sk = mk_join(SS_SOLD_CUSTOMER_SK, CUSTOMER, 1);
	r->ss_sold_cdemo_sk = mk_join(SS_SOLD_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);
	r->ss_sold_hdemo_sk = mk_join(SS_SOLD_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);
	r->ss_sold_addr_sk = mk_join(SS_SOLD_ADDR_SK, CUSTOMER_ADDRESS, 1);
	r->ss_ticket_number = index;
	genrand_integer(&nItemIndex, DIST_UNIFORM, 1, nItemCount, 0, SS_SOLD_ITEM_SK);

	return;
}

static void mk_detail(void *info_arr, int bPrint) {
	int nTemp;
	struct W_STORE_SALES_TBL *r;
	tdef *pT = getSimpleTdefsByNumber(STORE_SALES);

	r = &g_w_store_sales;

	nullSet(&pT->kNullBitMap, SS_NULLS);
	/*
	 * items need to be unique within an order
	 * use a sequence within the permutation
	 */
	if (++nItemIndex > nItemCount)
		nItemIndex = 1;
	r->ss_sold_item_sk = matchSCDSK(getPermutationEntry(pItemPermutation, nItemIndex), r->ss_sold_date_sk, ITEM);
	r->ss_sold_promo_sk = mk_join(SS_SOLD_PROMO_SK, PROMOTION, 1);
	set_pricing(SS_PRICING, &r->ss_pricing);

	/**
	 * having gone to the trouble to make the sale, now let's see if it gets
	 * returned
	 */
	genrand_integer(&nTemp, DIST_UNIFORM, 0, 99, 0, SR_IS_RETURNED);
	if (nTemp < SR_RETURN_PCT) {
		struct W_STORE_RETURNS_TBL w_web_returns;
		struct W_STORE_RETURNS_TBL *rr = &w_web_returns;
		mk_w_store_returns(rr, 1);

		void *info = append_info_get(info_arr, STORE_RETURNS);
		append_row_start(info);

		append_key(info, rr->sr_returned_date_sk, SR_RETURNED_DATE_SK);
		append_key(info, rr->sr_returned_time_sk, SR_RETURNED_TIME_SK);
		append_key(info, rr->sr_item_sk, SR_ITEM_SK);
		append_key(info, rr->sr_customer_sk, SR_CUSTOMER_SK);
		append_key(info, rr->sr_cdemo_sk, SR_CDEMO_SK);
		append_key(info, rr->sr_hdemo_sk, SR_HDEMO_SK);
		append_key(info, rr->sr_addr_sk, SR_ADDR_SK);
		append_key(info, rr->sr_store_sk, SR_STORE_SK);
		append_key(info, rr->sr_reason_sk, SR_REASON_SK);
		append_key(info, rr->sr_ticket_number, SR_TICKET_NUMBER);
		append_integer(info, rr->sr_pricing.quantity, SR_PRICING_QUANTITY);
		append_decimal(info, &rr->sr_pricing.net_paid, SR_PRICING_NET_PAID);
		append_decimal(info, &rr->sr_pricing.ext_tax, SR_PRICING_EXT_TAX);
		append_decimal(info, &rr->sr_pricing.net_paid_inc_tax, SR_PRICING_NET_PAID_INC_TAX);
		append_decimal(info, &rr->sr_pricing.fee, SR_PRICING_FEE);
		append_decimal(info, &rr->sr_pricing.ext_ship_cost, SR_PRICING_EXT_SHIP_COST);
		append_decimal(info, &rr->sr_pricing.refunded_cash, SR_PRICING_REFUNDED_CASH);
		append_decimal(info, &rr->sr_pricing.reversed_charge, SR_PRICING_REVERSED_CHARGE);
		append_decimal(info, &rr->sr_pricing.store_credit, SR_PRICING_STORE_CREDIT);
		append_decimal(info, &rr->sr_pricing.net_loss, SR_PRICING_NET_LOSS);
		append_row_end(info);
	}

	void *info = append_info_get(info_arr, STORE_SALES);
	append_row_start(info);

	append_key(info, r->ss_sold_date_sk, SS_SOLD_DATE_SK);
	append_key(info, r->ss_sold_time_sk, SS_SOLD_TIME_SK);
	append_key(info, r->ss_sold_item_sk, SS_SOLD_ITEM_SK);
	append_key(info, r->ss_sold_customer_sk, SS_SOLD_CUSTOMER_SK);
	append_key(info, r->ss_sold_cdemo_sk, SS_SOLD_CDEMO_SK);
	append_key(info, r->ss_sold_hdemo_sk, SS_SOLD_HDEMO_SK);
	append_key(info, r->ss_sold_addr_sk, SS_SOLD_ADDR_SK);
	append_key(info, r->ss_sold_store_sk, SS_SOLD_STORE_SK);
	append_key(info, r->ss_sold_promo_sk, SS_SOLD_PROMO_SK);
	append_key(info, r->ss_ticket_number, SS_TICKET_NUMBER);
	append_integer(info, r->ss_pricing.quantity, SS_PRICING_QUANTITY);
	append_decimal(info, &r->ss_pricing.wholesale_cost, SS_PRICING_WHOLESALE_COST);
	append_decimal(info, &r->ss_pricing.list_price, SS_PRICING_LIST_PRICE);
	append_decimal(info, &r->ss_pricing.sales_price, SS_PRICING_SALES_PRICE);
	append_decimal(info, &r->ss_pricing.coupon_amt, SS_PRICING_COUPON_AMT);
	append_decimal(info, &r->ss_pricing.ext_sales_price, SS_PRICING_EXT_SALES_PRICE);
	append_decimal(info, &r->ss_pricing.ext_wholesale_cost, SS_PRICING_EXT_WHOLESALE_COST);
	append_decimal(info, &r->ss_pricing.ext_list_price, SS_PRICING_EXT_LIST_PRICE);
	append_decimal(info, &r->ss_pricing.ext_tax, SS_PRICING_EXT_TAX);
	append_decimal(info, &r->ss_pricing.coupon_amt, SS_PRICING_COUPON_AMT);
	append_decimal(info, &r->ss_pricing.net_paid, SS_PRICING_NET_PAID);
	append_decimal(info, &r->ss_pricing.net_paid_inc_tax, SS_PRICING_NET_PAID_INC_TAX);
	append_decimal(info, &r->ss_pricing.net_profit, SS_PRICING_NET_PROFIT);

	append_row_end(info);

	return;
}

/*
 * mk_store_sales
 */
int mk_w_store_sales(void *info_arr, ds_key_t index) {
	int nLineitems, i;

	/* build the static portion of an order */
	mk_master(info_arr, index);

	/* set the number of lineitems and build them */
	genrand_integer(&nLineitems, DIST_UNIFORM, 8, 16, 0, SS_TICKET_NUMBER);
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
int vld_w_store_sales(int nTable, ds_key_t kRow, int *Permutation) {
	int nLineitem, nMaxLineitem, i;

	row_skip(nTable, kRow - 1);
	row_skip(STORE_RETURNS, kRow - 1);
	jDate = skipDays(STORE_SALES, &kNewDateIndex);
	mk_master(NULL, kRow);
	genrand_integer(&nMaxLineitem, DIST_UNIFORM, 8, 16, 9, SS_TICKET_NUMBER);
	genrand_integer(&nLineitem, DIST_UNIFORM, 1, nMaxLineitem, 0, SS_PRICING_QUANTITY);
	for (i = 1; i < nLineitem; i++) {
		mk_detail(NULL, 0);
	}
	mk_detail(NULL, 1);

	return (0);
}
