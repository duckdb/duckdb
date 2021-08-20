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
#include "w_catalog_sales.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "constants.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "nulls.h"
#include "init.h"
#include "parallel.h"
#include "params.h"
#include "permute.h"
#include "porting.h"
#include "scaling.h"
#include "scd.h"
#include "tables.h"
#include "tdefs.h"
#include "w_catalog_returns.h"

#include <stdio.h>

struct W_CATALOG_SALES_TBL g_w_catalog_sales;
ds_key_t skipDays(int nTable, ds_key_t *pRemainder);

static ds_key_t kNewDateIndex = 0;
static ds_key_t jDate;
static int nTicketItemBase = 1;
static int *pItemPermutation;
static int nItemCount;

/*
 * the validation process requires generating a single lineitem
 * so the main mk_xxx routine has been split into a master record portion
 * and a detail/lineitem portion.
 */
static void mk_master(void *info_arr, ds_key_t index) {
	static decimal_t dZero, dHundred, dOne, dOneHalf;
	int nGiftPct;
	struct W_CATALOG_SALES_TBL *r;

	r = &g_w_catalog_sales;

	if (!InitConstants::mk_master_catalog_sales_init) {
		strtodec(&dZero, "0.00");
		strtodec(&dHundred, "100.00");
		strtodec(&dOne, "1.00");
		strtodec(&dOneHalf, "0.50");
		jDate = skipDays(CATALOG_SALES, &kNewDateIndex);
		pItemPermutation = makePermutation(NULL, (nItemCount = (int)getIDCount(ITEM)), CS_PERMUTE);

		InitConstants::mk_master_catalog_sales_init = 1;
	}

	while (index > kNewDateIndex) /* need to move to a new date */
	{
		jDate += 1;
		kNewDateIndex += dateScaling(CATALOG_SALES, jDate);
	}

	/***
	 * some attributes remain the same for each lineitem in an order; others are
	 * different for each lineitem.
	 *
	 * Parallel generation causes another problem, since the values that get
	 * seeded may come from a prior row. If we are seeding at the start of a
	 * parallel chunk, hunt backwards in the RNG stream to find the most recent
	 * values that were used to set the values of the orderline-invariant
	 * columns
	 */

	r->cs_sold_date_sk = jDate;
	r->cs_sold_time_sk = mk_join(CS_SOLD_TIME_SK, TIME, r->cs_call_center_sk);
	r->cs_call_center_sk =
	    (r->cs_sold_date_sk == -1) ? -1 : mk_join(CS_CALL_CENTER_SK, CALL_CENTER, r->cs_sold_date_sk);

	r->cs_bill_customer_sk = mk_join(CS_BILL_CUSTOMER_SK, CUSTOMER, 1);
	r->cs_bill_cdemo_sk = mk_join(CS_BILL_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);
	r->cs_bill_hdemo_sk = mk_join(CS_BILL_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);
	r->cs_bill_addr_sk = mk_join(CS_BILL_ADDR_SK, CUSTOMER_ADDRESS, 1);

	/* most orders are for the ordering customers, some are not */
	genrand_integer(&nGiftPct, DIST_UNIFORM, 0, 99, 0, CS_SHIP_CUSTOMER_SK);
	if (nGiftPct <= CS_GIFT_PCT) {
		r->cs_ship_customer_sk = mk_join(CS_SHIP_CUSTOMER_SK, CUSTOMER, 2);
		r->cs_ship_cdemo_sk = mk_join(CS_SHIP_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2);
		r->cs_ship_hdemo_sk = mk_join(CS_SHIP_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2);
		r->cs_ship_addr_sk = mk_join(CS_SHIP_ADDR_SK, CUSTOMER_ADDRESS, 2);
	} else {
		r->cs_ship_customer_sk = r->cs_bill_customer_sk;
		r->cs_ship_cdemo_sk = r->cs_bill_cdemo_sk;
		r->cs_ship_hdemo_sk = r->cs_bill_hdemo_sk;
		r->cs_ship_addr_sk = r->cs_bill_addr_sk;
	}

	r->cs_order_number = index;
	genrand_integer(&nTicketItemBase, DIST_UNIFORM, 1, nItemCount, 0, CS_SOLD_ITEM_SK);

	return;
}

static void mk_detail(void *info_arr, int bPrint) {
	static decimal_t dZero, dHundred, dOne, dOneHalf;
	int nShipLag, nTemp;
	ds_key_t kItem;
	static ds_key_t kNewDateIndex = 0;
	static ds_key_t jDate;
	struct W_CATALOG_SALES_TBL *r;
	tdef *pTdef = getSimpleTdefsByNumber(CATALOG_SALES);

	r = &g_w_catalog_sales;

	if (!InitConstants::mk_detail_catalog_sales_init) {
		strtodec(&dZero, "0.00");
		strtodec(&dHundred, "100.00");
		strtodec(&dOne, "1.00");
		strtodec(&dOneHalf, "0.50");
		jDate = skipDays(CATALOG_SALES, &kNewDateIndex);

		InitConstants::mk_detail_catalog_sales_init = 1;
	}

	nullSet(&pTdef->kNullBitMap, CS_NULLS);

	/* orders are shipped some number of days after they are ordered */
	genrand_integer(&nShipLag, DIST_UNIFORM, CS_MIN_SHIP_DELAY, CS_MAX_SHIP_DELAY, 0, CS_SHIP_DATE_SK);
	r->cs_ship_date_sk = (r->cs_sold_date_sk == -1) ? -1 : r->cs_sold_date_sk + nShipLag;

	/*
	 * items need to be unique within an order
	 * use a sequence within the permutation
	 * NB: Permutations are 1-based
	 */
	if (++nTicketItemBase > nItemCount)
		nTicketItemBase = 1;
	kItem = getPermutationEntry(pItemPermutation, nTicketItemBase);
	r->cs_sold_item_sk = matchSCDSK(kItem, r->cs_sold_date_sk, ITEM);

	/* catalog page needs to be from a catlog active at the time of the sale */
	r->cs_catalog_page_sk =
	    (r->cs_sold_date_sk == -1) ? -1 : mk_join(CS_CATALOG_PAGE_SK, CATALOG_PAGE, r->cs_sold_date_sk);

	r->cs_ship_mode_sk = mk_join(CS_SHIP_MODE_SK, SHIP_MODE, 1);
	r->cs_warehouse_sk = mk_join(CS_WAREHOUSE_SK, WAREHOUSE, 1);
	r->cs_promo_sk = mk_join(CS_PROMO_SK, PROMOTION, 1);
	set_pricing(CS_PRICING, &r->cs_pricing);

	/**
	 * having gone to the trouble to make the sale, now let's see if it gets
	 * returned
	 */
	genrand_integer(&nTemp, DIST_UNIFORM, 0, 99, 0, CR_IS_RETURNED);
	if (nTemp < CR_RETURN_PCT) {
		struct W_CATALOG_RETURNS_TBL w_catalog_returns;
		struct W_CATALOG_RETURNS_TBL *rr = &w_catalog_returns;
		mk_w_catalog_returns(rr, 1);

		void *info = append_info_get(info_arr, CATALOG_RETURNS);
		append_row_start(info);

		append_key(info, rr->cr_returned_date_sk);
		append_key(info, rr->cr_returned_time_sk);
		append_key(info, rr->cr_item_sk);
		append_key(info, rr->cr_refunded_customer_sk);
		append_key(info, rr->cr_refunded_cdemo_sk);
		append_key(info, rr->cr_refunded_hdemo_sk);
		append_key(info, rr->cr_refunded_addr_sk);
		append_key(info, rr->cr_returning_customer_sk);
		append_key(info, rr->cr_returning_cdemo_sk);
		append_key(info, rr->cr_returning_hdemo_sk);
		append_key(info, rr->cr_returning_addr_sk);
		append_key(info, rr->cr_call_center_sk);
		append_key(info, rr->cr_catalog_page_sk);
		append_key(info, rr->cr_ship_mode_sk);
		append_key(info, rr->cr_warehouse_sk);
		append_key(info, rr->cr_reason_sk);
		append_key(info, rr->cr_order_number);
		append_integer(info, rr->cr_pricing.quantity);
		append_decimal(info, &rr->cr_pricing.net_paid);
		append_decimal(info, &rr->cr_pricing.ext_tax);
		append_decimal(info, &rr->cr_pricing.net_paid_inc_tax);
		append_decimal(info, &rr->cr_pricing.fee);
		append_decimal(info, &rr->cr_pricing.ext_ship_cost);
		append_decimal(info, &rr->cr_pricing.refunded_cash);
		append_decimal(info, &rr->cr_pricing.reversed_charge);
		append_decimal(info, &rr->cr_pricing.store_credit);
		append_decimal(info, &rr->cr_pricing.net_loss);

		append_row_end(info);
	}

	void *info = append_info_get(info_arr, CATALOG_SALES);
	append_row_start(info);

	append_key(info, r->cs_sold_date_sk);
	append_key(info, r->cs_sold_time_sk);
	append_key(info, r->cs_ship_date_sk);
	append_key(info, r->cs_bill_customer_sk);
	append_key(info, r->cs_bill_cdemo_sk);
	append_key(info, r->cs_bill_hdemo_sk);
	append_key(info, r->cs_bill_addr_sk);
	append_key(info, r->cs_ship_customer_sk);
	append_key(info, r->cs_ship_cdemo_sk);
	append_key(info, r->cs_ship_hdemo_sk);
	append_key(info, r->cs_ship_addr_sk);
	append_key(info, r->cs_call_center_sk);
	append_key(info, r->cs_catalog_page_sk);
	append_key(info, r->cs_ship_mode_sk);
	append_key(info, r->cs_warehouse_sk);
	append_key(info, r->cs_sold_item_sk);
	append_key(info, r->cs_promo_sk);
	append_key(info, r->cs_order_number);
	append_integer(info, r->cs_pricing.quantity);
	append_decimal(info, &r->cs_pricing.wholesale_cost);
	append_decimal(info, &r->cs_pricing.list_price);
	append_decimal(info, &r->cs_pricing.sales_price);
	append_decimal(info, &r->cs_pricing.ext_discount_amt);
	append_decimal(info, &r->cs_pricing.ext_sales_price);
	append_decimal(info, &r->cs_pricing.ext_wholesale_cost);
	append_decimal(info, &r->cs_pricing.ext_list_price);
	append_decimal(info, &r->cs_pricing.ext_tax);
	append_decimal(info, &r->cs_pricing.coupon_amt);
	append_decimal(info, &r->cs_pricing.ext_ship_cost);
	append_decimal(info, &r->cs_pricing.net_paid);
	append_decimal(info, &r->cs_pricing.net_paid_inc_tax);
	append_decimal(info, &r->cs_pricing.net_paid_inc_ship);
	append_decimal(info, &r->cs_pricing.net_paid_inc_ship_tax);
	append_decimal(info, &r->cs_pricing.net_profit);

	append_row_end(info);

	return;
}

/*
 * Routine: mk_catalog_sales()
 * Purpose: build rows for the catalog sales table
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 * 20020902 jms Need to link order date/time to call center record
 * 20020902 jms Should promos be tied to item id?
 */
int mk_w_catalog_sales(void *info_arr, ds_key_t index) {
	int nLineitems, i;

	mk_master(info_arr, index);

	/*
	 * now we select the number of lineitems in this order, and loop through
	 * them, printing as we go
	 */
	genrand_integer(&nLineitems, DIST_UNIFORM, 4, 14, 0, CS_ORDER_NUMBER);
	for (i = 1; i <= nLineitems; i++) {
		mk_detail(info_arr, 1);
	}

	/**
	 * and finally return 1 since we have already printed the rows.
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
int vld_w_catalog_sales(int nTable, ds_key_t kRow, int *Permutation) {
	int nLineitem, nMaxLineitem, i;

	row_skip(nTable, kRow - 1);
	row_skip(CATALOG_RETURNS, (kRow - 1));
	jDate = skipDays(CATALOG_SALES, &kNewDateIndex);
	mk_master(NULL, kRow);
	genrand_integer(&nMaxLineitem, DIST_UNIFORM, 4, 14, 9, CS_ORDER_NUMBER);
	genrand_integer(&nLineitem, DIST_UNIFORM, 1, nMaxLineitem, 0, CS_PRICING_QUANTITY);
	for (i = 1; i < nLineitem; i++) {
		mk_detail(NULL, 0);
	}
	mk_detail(NULL, 1);

	return (0);
}
