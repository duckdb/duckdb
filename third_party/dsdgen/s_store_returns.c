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
#include "s_store_returns.h"
#include "s_purchase.h"
#include "s_pline.h"
#include "s_purchase.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "parallel.h"

struct S_STORE_RETURNS_TBL g_s_store_returns;
extern struct S_PURCHASE_TBL g_s_purchase;
extern struct S_PURCHASE_LINEITEM_TBL g_s_purchase_lineitem;

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
mk_s_store_returns(void *pDest, ds_key_t kIndex)
{
	static int bInit = 0;
	struct S_STORE_RETURNS_TBL *r;
	int nReturnLag;
	
	if (pDest == NULL)
		r = &g_s_store_returns;
	else
		r = pDest;

	if (!bInit)
	{
		memset(&g_s_store_returns, 0, sizeof(struct S_STORE_RETURNS_TBL));
		bInit = 1;
	}
	
	genrand_integer(&nReturnLag, DIST_UNIFORM, 0, 60, 0, S_SRET_RETURN_DATE);
   if (g_s_purchase.dtPurchaseDate.julian == -1)
      r->dtReturnDate.julian = -1;
   else
      jtodt(&r->dtReturnDate, g_s_purchase.dtPurchaseDate.julian + nReturnLag);
   r->kReturnTime = mk_join(S_SRET_RETURN_TIME, TIME, 1);
   r->kReasonID = mk_join(S_SRET_REASON_ID, REASON, 1);
	r->Pricing = g_s_purchase_lineitem.Pricing;
   if (g_s_purchase_lineitem.Pricing.quantity != -1)
	   genrand_integer(&r->Pricing.quantity, DIST_UNIFORM, 1, g_s_purchase_lineitem.Pricing.quantity, 0, S_SRET_PRICING);
   else
      r->Pricing.quantity = -1;
	set_pricing(S_SRET_PRICING, &r->Pricing);
	
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
pr_s_store_returns(void *pSrc)
{
	struct S_STORE_RETURNS_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_store_returns;
	else
		r = pSrc;
	
	print_start(S_STORE_RETURNS);
	print_id(S_SRET_STORE_ID, g_s_purchase.kStoreID, 1);
	print_key(S_SRET_PURCHASE_ID, g_s_purchase.kID, 1);
	print_key(S_SRET_LINENUMBER, g_s_purchase_lineitem.kLineNumber, 1);
	print_id(S_SRET_ITEM_ID, g_s_purchase_lineitem.kItemID, 1);
	print_id(S_SRET_CUSTOMER_ID, g_s_purchase.kCustomerID, 1);
	print_date(S_SRET_RETURN_DATE, r->dtReturnDate.julian, 1);
	print_time(S_SRET_RETURN_TIME, r->kReturnTime, 1);
	print_key(S_SRET_TICKET_NUMBER, g_s_purchase.kID, 1);
	print_integer(S_SRET_RETURN_QUANTITY, r->Pricing.quantity, 1);
	print_decimal(S_SRET_RETURN_AMT, &r->Pricing.sales_price, 1);
	print_decimal(S_SRET_RETURN_TAX, &r->Pricing.ext_tax, 1);
	print_decimal(S_SRET_RETURN_FEE, &r->Pricing.fee, 1);
	print_decimal(S_SRET_RETURN_SHIP_COST, &r->Pricing.ext_ship_cost, 1);
	print_decimal(S_SRET_REFUNDED_CASH, &r->Pricing.refunded_cash, 1);
	print_decimal(S_SRET_REVERSED_CHARGE, &r->Pricing.reversed_charge, 1);
	print_decimal(S_SRET_MERCHANT_CREDIT, &r->Pricing.store_credit, 1);
	print_id(S_SRET_REASON_ID, r->kReasonID, 0);
	print_end(S_STORE_RETURNS);
	
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
ld_s_store_returns(void *pSrc)
{
	struct S_STORE_RETURNS_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_store_returns;
	else
		r = pSrc;
	
	return(0);
}

