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
#include "w_catalog_sales.h"
#include "w_catalog_returns.h"
#include "decimal.h"
#include "date.h"
#include "genrand.h"
#include "columns.h"
#include "tables.h"
#include "constants.h"
#include "build_support.h"
#include "print.h"
#include "nulls.h"
#include "tdefs.h"
#include "scaling.h"
#include "permute.h"
#include "params.h"
#include "parallel.h"
#include "scd.h"

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
static void                                                   
mk_master (void *row, ds_key_t index)                         
{                                      
	static decimal_t dZero,
		dHundred,
		dOne, dOneHalf;
	int nGiftPct;
	struct W_CATALOG_SALES_TBL *r;
	static int bInit = 0;


	if (row == NULL)
		r = &g_w_catalog_sales;
	else
		r = row;

	if (!bInit)
	{
	    strtodec (&dZero, "0.00");
        strtodec (&dHundred, "100.00");
        strtodec (&dOne, "1.00");
        strtodec (&dOneHalf, "0.50");
		jDate = skipDays(CATALOG_SALES, &kNewDateIndex);
		pItemPermutation = makePermutation(NULL, (nItemCount = (int)getIDCount(ITEM)), CS_PERMUTE);

		bInit = 1;
	}

   while (index > kNewDateIndex)	/* need to move to a new date */
   {
      jDate += 1;
      kNewDateIndex += dateScaling(CATALOG_SALES, jDate);
   }

	/***
	 * some attributes remain the same for each lineitem in an order; others are different
	 * for each lineitem. 
    *
    * Parallel generation causes another problem, since the values that get seeded may come from a prior row.
    * If we are seeding at the start of a parallel chunk, hunt backwards in the RNG stream to find the most
    * recent values that were used to set the values of the orderline-invariant columns
 	 */
   
		r->cs_sold_date_sk = jDate;
		r->cs_sold_time_sk = mk_join (CS_SOLD_TIME_SK, TIME, 
			r->cs_call_center_sk);
		r->cs_call_center_sk =
			(r->cs_sold_date_sk == -1)?-1:mk_join (CS_CALL_CENTER_SK, CALL_CENTER, r->cs_sold_date_sk);
		
		r->cs_bill_customer_sk =
			mk_join (CS_BILL_CUSTOMER_SK, CUSTOMER, 1);
		r->cs_bill_cdemo_sk =
			mk_join (CS_BILL_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);
		r->cs_bill_hdemo_sk =
			mk_join (CS_BILL_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);
		r->cs_bill_addr_sk =
			mk_join (CS_BILL_ADDR_SK, CUSTOMER_ADDRESS, 1);

		/* most orders are for the ordering customers, some are not */
		genrand_integer(&nGiftPct, DIST_UNIFORM, 0, 99, 0, CS_SHIP_CUSTOMER_SK);
		if (nGiftPct <= CS_GIFT_PCT)
		{
			r->cs_ship_customer_sk =
				mk_join (CS_SHIP_CUSTOMER_SK, CUSTOMER, 2);
			r->cs_ship_cdemo_sk =
				mk_join (CS_SHIP_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2);
			r->cs_ship_hdemo_sk =
				mk_join (CS_SHIP_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2);
			r->cs_ship_addr_sk =
				mk_join (CS_SHIP_ADDR_SK, CUSTOMER_ADDRESS, 2);
		}
		else
		{
			r->cs_ship_customer_sk =	r->cs_bill_customer_sk;
			r->cs_ship_cdemo_sk =	r->cs_bill_cdemo_sk;
			r->cs_ship_hdemo_sk =	r->cs_bill_hdemo_sk;
			r->cs_ship_addr_sk =	r->cs_bill_addr_sk;
		}	

      r->cs_order_number = index;
	  genrand_integer(&nTicketItemBase, DIST_UNIFORM, 1, nItemCount, 0, CS_SOLD_ITEM_SK);

      return;
}

static void
mk_detail(void *row, int bPrint)
{
	static decimal_t dZero,
		dHundred,
		dOne, dOneHalf;
	int nShipLag, 
		nTemp;
   ds_key_t kItem;
	static ds_key_t kNewDateIndex = 0;
	static ds_key_t jDate;
	struct W_CATALOG_SALES_TBL *r;
	static int bInit = 0;
   tdef *pTdef = getSimpleTdefsByNumber(CATALOG_SALES);


	if (row == NULL)
		r = &g_w_catalog_sales;
	else
		r = row;

	if (!bInit)
	{
	    strtodec (&dZero, "0.00");
        strtodec (&dHundred, "100.00");
        strtodec (&dOne, "1.00");
        strtodec (&dOneHalf, "0.50");
		jDate = skipDays(CATALOG_SALES, &kNewDateIndex);

		bInit = 1;
	}


   nullSet(&pTdef->kNullBitMap, CS_NULLS);

	/* orders are shipped some number of days after they are ordered */
	genrand_integer (&nShipLag, DIST_UNIFORM, 
		CS_MIN_SHIP_DELAY, CS_MAX_SHIP_DELAY, 0, CS_SHIP_DATE_SK);
	r->cs_ship_date_sk = (r->cs_sold_date_sk == -1)?-1:r->cs_sold_date_sk + nShipLag;


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
		(r->cs_sold_date_sk == -1)?-1:mk_join (CS_CATALOG_PAGE_SK, CATALOG_PAGE, r->cs_sold_date_sk);

	r->cs_ship_mode_sk = mk_join (CS_SHIP_MODE_SK, SHIP_MODE, 1);
	r->cs_warehouse_sk = mk_join (CS_WAREHOUSE_SK, WAREHOUSE, 1);
	r->cs_promo_sk = mk_join (CS_PROMO_SK, PROMOTION, 1);
	set_pricing(CS_PRICING, &r->cs_pricing);

	/** 
	* having gone to the trouble to make the sale, now let's see if it gets returned
	*/
	genrand_integer(&nTemp, DIST_UNIFORM, 0, 99, 0, CR_IS_RETURNED);
	if (nTemp < CR_RETURN_PCT)
	{
		mk_w_catalog_returns(NULL, 1);
      if (bPrint)
         pr_w_catalog_returns(NULL);
	}

   /**
   * now we print out the order and lineitem together as a single row
   */
   if (bPrint)
      pr_w_catalog_sales(NULL);

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
int
mk_w_catalog_sales (void* row, ds_key_t index)
{
   int nLineitems,
      i;

   mk_master(row, index);

   /*
    * now we select the number of lineitems in this order, and loop through them, printing
    * as we go
    */
   genrand_integer(&nLineitems, DIST_UNIFORM, 4, 14, 0, CS_ORDER_NUMBER);
   for (i=1; i <= nLineitems; i++)
   {
      mk_detail(NULL, 1);
   }

   /**
    * and finally return 1 since we have already printed the rows.
    */
    return(1);
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
pr_w_catalog_sales(void *row)
{
		struct W_CATALOG_SALES_TBL *r;

	if (row == NULL)
		r = &g_w_catalog_sales;
	else
		r = row;

	print_start(CATALOG_SALES);
	print_key(CS_SOLD_DATE_SK, r->cs_sold_date_sk, 1);
	print_key(CS_SOLD_TIME_SK, r->cs_sold_time_sk, 1);
	print_key(CS_SHIP_DATE_SK, r->cs_ship_date_sk, 1);
	print_key(CS_BILL_CUSTOMER_SK, r->cs_bill_customer_sk, 1);
	print_key(CS_BILL_CDEMO_SK, r->cs_bill_cdemo_sk, 1);
	print_key(CS_BILL_HDEMO_SK, r->cs_bill_hdemo_sk, 1);
	print_key(CS_BILL_ADDR_SK, r->cs_bill_addr_sk, 1);
	print_key(CS_SHIP_CUSTOMER_SK, r->cs_ship_customer_sk, 1);
	print_key(CS_SHIP_CDEMO_SK, r->cs_ship_cdemo_sk, 1);
	print_key(CS_SHIP_HDEMO_SK, r->cs_ship_hdemo_sk, 1);
	print_key(CS_SHIP_ADDR_SK, r->cs_ship_addr_sk, 1);
	print_key(CS_CALL_CENTER_SK, r->cs_call_center_sk, 1);
	print_key(CS_CATALOG_PAGE_SK, r->cs_catalog_page_sk, 1);
	print_key(CS_SHIP_MODE_SK, r->cs_ship_mode_sk, 1);
	print_key(CS_WAREHOUSE_SK, r->cs_warehouse_sk, 1);
    print_key(CS_SOLD_ITEM_SK, r->cs_sold_item_sk, 1);
	print_key(CS_PROMO_SK, r->cs_promo_sk, 1);
	print_key(CS_ORDER_NUMBER, r->cs_order_number, 1);
	print_integer(CS_PRICING_QUANTITY, r->cs_pricing.quantity, 1);
	print_decimal(CS_PRICING_WHOLESALE_COST, &r->cs_pricing.wholesale_cost, 1);
	print_decimal(CS_PRICING_LIST_PRICE, &r->cs_pricing.list_price, 1);
	print_decimal(CS_PRICING_SALES_PRICE, &r->cs_pricing.sales_price, 1);
	print_decimal(CS_PRICING_EXT_DISCOUNT_AMOUNT, &r->cs_pricing.ext_discount_amt, 1);
	print_decimal(CS_PRICING_EXT_SALES_PRICE, &r->cs_pricing.ext_sales_price, 1);
	print_decimal(CS_PRICING_EXT_WHOLESALE_COST, &r->cs_pricing.ext_wholesale_cost, 1);
	print_decimal(CS_PRICING_EXT_LIST_PRICE, &r->cs_pricing.ext_list_price, 1);
	print_decimal(CS_PRICING_EXT_TAX, &r->cs_pricing.ext_tax, 1);
	print_decimal(CS_PRICING_COUPON_AMT, &r->cs_pricing.coupon_amt, 1);
	print_decimal(CS_PRICING_EXT_SHIP_COST, &r->cs_pricing.ext_ship_cost, 1);
	print_decimal(CS_PRICING_NET_PAID, &r->cs_pricing.net_paid, 1);
	print_decimal(CS_PRICING_NET_PAID_INC_TAX, &r->cs_pricing.net_paid_inc_tax, 1);
	print_decimal(CS_PRICING_NET_PAID_INC_SHIP, &r->cs_pricing.net_paid_inc_ship, 1);
	print_decimal(CS_PRICING_NET_PAID_INC_SHIP_TAX, &r->cs_pricing.net_paid_inc_ship_tax, 1);
	print_decimal(CS_PRICING_NET_PROFIT, &r->cs_pricing.net_profit, 0);
	print_end(CATALOG_SALES);

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
ld_w_catalog_sales(void *row)
{
	struct W_CATALOG_SALES_TBL *r;

	if (row == NULL)
		r = &g_w_catalog_sales;
	else
		r = row;

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
vld_w_catalog_sales(int nTable, ds_key_t kRow, int *Permutation)
{
	int nLineitem,
		nMaxLineitem,
		i;

	row_skip(nTable, kRow - 1);
	row_skip(CATALOG_RETURNS, (kRow - 1) );
	jDate = skipDays(CATALOG_SALES, &kNewDateIndex);		
	mk_master(NULL, kRow);
	genrand_integer(&nMaxLineitem, DIST_UNIFORM, 4, 14, 9, CS_ORDER_NUMBER);
	genrand_integer(&nLineitem, DIST_UNIFORM, 1, nMaxLineitem, 0, CS_PRICING_QUANTITY);
	for (i = 1; i < nLineitem; i++)
	{
		mk_detail(NULL, 0);
	}
   mk_detail(NULL, 1);

	return(0);
}

