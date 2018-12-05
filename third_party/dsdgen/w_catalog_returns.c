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
#include "genrand.h"
#include "w_catalog_returns.h"
#include "w_catalog_sales.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "constants.h"
#include "nulls.h"
#include "tdefs.h"
#include "parallel.h"

struct W_CATALOG_RETURNS_TBL g_w_catalog_returns;
extern struct W_CATALOG_SALES_TBL g_w_catalog_sales;

/*
 * Routine: mk_catalog_returns()
 * Purpose: populate a return fact *sync'd with a sales fact*
 * Algorithm: Since the returns need to be in line with a prior sale, they need
 *	to use the output of the mk_catalog_sales() routine, and then add
 *return-related information Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 * 20020902 jms Need to link call center to date/time of return
 * 20031023 jms removed ability for stand alone generation
 */
int mk_w_catalog_returns(void *row, ds_key_t index) {
	int res = 0;

	static decimal_t dHundred;
	int nTemp;
	struct W_CATALOG_RETURNS_TBL *r;
	struct W_CATALOG_SALES_TBL *sale = &g_w_catalog_sales;
	static int bInit = 0;
	static int bStandAlone = 0;
	tdef *pTdef = getSimpleTdefsByNumber(CATALOG_RETURNS);

	if (row == NULL)
		r = &g_w_catalog_returns;
	else
		r = row;

	if (!bInit) {
		strtodec(&dHundred, "100.00");
	}

	/* if we were not called from the parent table's mk_xxx routine, then
	 * move to a parent row that needs to be returned, and generate it
	 */
	nullSet(&pTdef->kNullBitMap, CR_NULLS);
	if (bStandAlone) {
		genrand_integer(&nTemp, DIST_UNIFORM, 0, 99, 0, CR_IS_RETURNED);
		if (nTemp >= CR_RETURN_PCT) {
			row_skip(CATALOG_SALES, 1);
			return (1);
		}
		mk_w_catalog_sales(&g_w_catalog_sales, index);
	}

	/*
	 * Some of the information in the return is taken from the original sale
	 * which has been regenerated
	 */
	r->cr_item_sk = sale->cs_sold_item_sk;
	r->cr_catalog_page_sk = sale->cs_catalog_page_sk;
	r->cr_order_number = sale->cs_order_number;
	memcpy(&r->cr_pricing, &sale->cs_pricing, sizeof(ds_pricing_t));
	r->cr_refunded_customer_sk = sale->cs_bill_customer_sk;
	r->cr_refunded_cdemo_sk = sale->cs_bill_cdemo_sk;
	r->cr_refunded_hdemo_sk = sale->cs_bill_hdemo_sk;
	r->cr_refunded_addr_sk = sale->cs_bill_addr_sk;
	r->cr_call_center_sk = sale->cs_call_center_sk;

	/*
	 * some of the fields are conditionally taken from the sale
	 */
	r->cr_returning_customer_sk = mk_join(CR_RETURNING_CUSTOMER_SK, CUSTOMER, 2);
	r->cr_returning_cdemo_sk = mk_join(CR_RETURNING_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2);
	r->cr_returning_hdemo_sk = mk_join(CR_RETURNING_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2);
	r->cr_returning_addr_sk = mk_join(CR_RETURNING_ADDR_SK, CUSTOMER_ADDRESS, 2);
	if (genrand_integer(NULL, DIST_UNIFORM, 0, 99, 0, CR_RETURNING_CUSTOMER_SK) < CS_GIFT_PCT) {
		r->cr_returning_customer_sk = sale->cs_ship_customer_sk;
		r->cr_returning_cdemo_sk = sale->cs_ship_cdemo_sk;
		/* cr_returning_hdemo_sk removed, since it doesn't exist on the sales
		 * record */
		r->cr_returning_addr_sk = sale->cs_ship_addr_sk;
	}

	/**
	 * the rest of the columns are generated for this specific return
	 */
	/* the items cannot be returned until they are shipped; offset is handled in
	 * mk_join, based on sales date */
	r->cr_returned_date_sk = mk_join(CR_RETURNED_DATE_SK, DATET, sale->cs_ship_date_sk);

	/* the call center determines the time of the return */
	r->cr_returned_time_sk = mk_join(CR_RETURNED_TIME_SK, TIME, 1);

	r->cr_ship_mode_sk = mk_join(CR_SHIP_MODE_SK, SHIP_MODE, 1);
	r->cr_warehouse_sk = mk_join(CR_WAREHOUSE_SK, WAREHOUSE, 1);
	r->cr_reason_sk = mk_join(CR_REASON_SK, REASON, 1);
	if (sale->cs_pricing.quantity != -1)
		genrand_integer(&r->cr_pricing.quantity, DIST_UNIFORM, 1, sale->cs_pricing.quantity, 0, CR_PRICING);
	else
		r->cr_pricing.quantity = -1;
	set_pricing(CR_PRICING, &r->cr_pricing);

	return (res);
}
