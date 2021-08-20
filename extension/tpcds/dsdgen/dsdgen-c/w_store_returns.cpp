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
#include "w_store_returns.h"
#include "w_store_sales.h"
#include "tables.h"
#include "pricing.h"
#include "columns.h"
#include "genrand.h"
#include "build_support.h"
#include "nulls.h"
#include "tdefs.h"

struct W_STORE_RETURNS_TBL g_w_store_returns;
extern struct W_STORE_SALES_TBL g_w_store_sales;

/*
 * Routine: mk_store_returns()
 * Purpose: populate a return fact *sync'd with a sales fact*
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
int mk_w_store_returns(void *row, ds_key_t index) {
	int res = 0, nTemp;
	struct W_STORE_RETURNS_TBL *r;
	struct W_STORE_SALES_TBL *sale = &g_w_store_sales;
	tdef *pT = getSimpleTdefsByNumber(STORE_RETURNS);

	static decimal_t dMin, dMax;
	/* begin locals declarations */

	if (row == NULL)
		r = &g_w_store_returns;
	else
		r = (W_STORE_RETURNS_TBL *)row;

	if (!InitConstants::mk_w_store_returns_init) {
		strtodec(&dMin, "1.00");
		strtodec(&dMax, "100000.00");
		InitConstants::mk_w_store_returns_init = 1;
	}

	nullSet(&pT->kNullBitMap, SR_NULLS);
	/*
	 * Some of the information in the return is taken from the original sale
	 * which has been regenerated
	 */
	r->sr_ticket_number = sale->ss_ticket_number;
	r->sr_item_sk = sale->ss_sold_item_sk;
	memcpy((void *)&r->sr_pricing, (void *)&sale->ss_pricing, sizeof(ds_pricing_t));

	/*
	 * some of the fields are conditionally taken from the sale
	 */
	r->sr_customer_sk = mk_join(SR_CUSTOMER_SK, CUSTOMER, 1);
	if (genrand_integer(NULL, DIST_UNIFORM, 1, 100, 0, SR_TICKET_NUMBER) < SR_SAME_CUSTOMER)
		r->sr_customer_sk = sale->ss_sold_customer_sk;

	/*
	 * the rest of the columns are generated for this specific return
	 */
	/* the items cannot be returned until they are sold; offset is handled in
	 * mk_join, based on sales date */
	r->sr_returned_date_sk = mk_join(SR_RETURNED_DATE_SK, DATET, sale->ss_sold_date_sk);
	genrand_integer(&nTemp, DIST_UNIFORM, (8 * 3600) - 1, (17 * 3600) - 1, 0, SR_RETURNED_TIME_SK);
	r->sr_returned_time_sk = nTemp;
	r->sr_cdemo_sk = mk_join(SR_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);
	r->sr_hdemo_sk = mk_join(SR_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);
	r->sr_addr_sk = mk_join(SR_ADDR_SK, CUSTOMER_ADDRESS, 1);
	r->sr_store_sk = mk_join(SR_STORE_SK, STORE, 1);
	r->sr_reason_sk = mk_join(SR_REASON_SK, REASON, 1);
	genrand_integer(&r->sr_pricing.quantity, DIST_UNIFORM, 1, sale->ss_pricing.quantity, 0, SR_PRICING);
	set_pricing(SR_PRICING, &r->sr_pricing);

	return (res);
}
