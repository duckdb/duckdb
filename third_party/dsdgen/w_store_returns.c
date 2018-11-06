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
#include "print.h"
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
int
mk_w_store_returns (void * row, ds_key_t index)
{
	int res = 0,
		nTemp;
	struct W_STORE_RETURNS_TBL *r;
	struct W_STORE_SALES_TBL *sale = &g_w_store_sales;
	static int bInit = 0;
   tdef *pT = getSimpleTdefsByNumber(STORE_RETURNS);
	
	static decimal_t dMin,
		dMax;
	/* begin locals declarations */
	
	if (row == NULL)
		r = &g_w_store_returns;
	else
		r = row;

	if (!bInit)
	{
        strtodec (&dMin, "1.00");
        strtodec (&dMax, "100000.00");
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
	r->sr_customer_sk = mk_join (SR_CUSTOMER_SK, CUSTOMER, 1);
	if (genrand_integer(NULL, DIST_UNIFORM, 1, 100, 0, SR_TICKET_NUMBER) < SR_SAME_CUSTOMER)
		r->sr_customer_sk = sale->ss_sold_customer_sk;

	/*
	* the rest of the columns are generated for this specific return
	*/
	/* the items cannot be returned until they are sold; offset is handled in mk_join, based on sales date */
	r->sr_returned_date_sk = mk_join (SR_RETURNED_DATE_SK, DATE, sale->ss_sold_date_sk);
	genrand_integer(&nTemp, DIST_UNIFORM, (8 * 3600) - 1, (17 * 3600) - 1, 0, SR_RETURNED_TIME_SK);
	r->sr_returned_time_sk = nTemp;
	r->sr_cdemo_sk =
		mk_join (SR_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);
	r->sr_hdemo_sk =
		mk_join (SR_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);
	r->sr_addr_sk = mk_join (SR_ADDR_SK, CUSTOMER_ADDRESS, 1);
	r->sr_store_sk = mk_join (SR_STORE_SK, STORE, 1);
	r->sr_reason_sk = mk_join (SR_REASON_SK, REASON, 1);
	genrand_integer(&r->sr_pricing.quantity, DIST_UNIFORM,
		1, sale->ss_pricing.quantity, 0, SR_PRICING);
	set_pricing(SR_PRICING, &r->sr_pricing);
	
	return (res);
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
int
pr_w_store_returns(void *row)
{
	struct W_STORE_RETURNS_TBL *r;


	if (row == NULL)
		r = &g_w_store_returns;
	else
		r = row;
	print_start(STORE_RETURNS);
	print_key(SR_RETURNED_DATE_SK, r->sr_returned_date_sk, 1);
	print_key(SR_RETURNED_TIME_SK, r->sr_returned_time_sk, 1);
	print_key(SR_ITEM_SK, r->sr_item_sk, 1);
	print_key(SR_CUSTOMER_SK, r->sr_customer_sk, 1);
	print_key(SR_CDEMO_SK, r->sr_cdemo_sk, 1);
	print_key(SR_HDEMO_SK, r->sr_hdemo_sk, 1);
	print_key(SR_ADDR_SK, r->sr_addr_sk, 1);
	print_key(SR_STORE_SK, r->sr_store_sk, 1);
	print_key(SR_REASON_SK, r->sr_reason_sk, 1);
	print_key(SR_TICKET_NUMBER, r->sr_ticket_number, 1);
	print_integer(SR_PRICING_QUANTITY, r->sr_pricing.quantity, 1);
	print_decimal(SR_PRICING_NET_PAID, &r->sr_pricing.net_paid, 1);
	print_decimal(SR_PRICING_EXT_TAX, &r->sr_pricing.ext_tax, 1);
	print_decimal(SR_PRICING_NET_PAID_INC_TAX, &r->sr_pricing.net_paid_inc_tax, 1);
	print_decimal(SR_PRICING_FEE, &r->sr_pricing.fee, 1);
	print_decimal(SR_PRICING_EXT_SHIP_COST, &r->sr_pricing.ext_ship_cost, 1);
	print_decimal(SR_PRICING_REFUNDED_CASH, &r->sr_pricing.refunded_cash, 1);
	print_decimal(SR_PRICING_REVERSED_CHARGE, &r->sr_pricing.reversed_charge, 1);
	print_decimal(SR_PRICING_STORE_CREDIT, &r->sr_pricing.store_credit, 1);
	print_decimal(SR_PRICING_NET_LOSS, &r->sr_pricing.net_loss, 0);
	print_end(STORE_RETURNS);
	
	return(0);
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
int 
ld_w_store_returns(void *pSrc)
{
	struct W_STORE_RETURNS_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_store_returns;
	else
		r = pSrc;
	
	return(0);
}

