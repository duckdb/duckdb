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
#include "s_web_returns.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "w_web_sales.h"
#include "s_web_order.h"
#include "s_web_order_lineitem.h"
#include "parallel.h"

struct S_WEB_RETURNS_TBL g_s_web_returns;
extern struct S_WEB_ORDER_TBL g_s_web_order;
extern struct S_WEB_ORDER_LINEITEM_TBL g_s_web_order_lineitem;


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
mk_s_web_return(void *pDest, ds_key_t kIndex)
{
	static int bInit = 0;
	struct S_WEB_RETURNS_TBL *r;
	int nReturnLag;
	static date_t MinDataDate,
		MaxDataDate;
	
	if (pDest == NULL)
		r = &g_s_web_returns;
	else
		r = pDest;

	if (!bInit)
	{
		memset(&g_s_web_returns, 0, sizeof(struct S_WEB_RETURNS_TBL));
		strtodt(&MinDataDate, DATA_START_DATE);
		strtodt(&MaxDataDate, DATA_END_DATE);
		bInit = 1;
	}

	genrand_integer(&nReturnLag, DIST_UNIFORM, 1, 60, 0, S_WRET_RETURN_DATE);
   if (g_s_web_order.dtOrderDate.julian == -1)
      r->dtReturnDate.julian = -1;
   else
      jtodt(&r->dtReturnDate, g_s_web_order.dtOrderDate.julian + nReturnLag);
	r->kReturnTime = mk_join(S_WRET_RETURN_TIME, TIME, 1);
	r->kReasonID = mk_join(S_WRET_REASON_ID, REASON, 1);
	r->kSiteID = g_s_web_order.kWebSiteID;
	r->kOrderID = g_s_web_order.kID;
	r->nLineNumber = kIndex;
	r->kItemID = g_s_web_order_lineitem.kItemID;
	r->kReturningCustomerID = g_s_web_order.kShipCustomerID;
	r->kRefundedCustomerID = g_s_web_order.kShipCustomerID;
	r->Pricing = g_s_web_order_lineitem.Pricing;
   if (g_s_web_order_lineitem.Pricing.quantity == -1)
      r->Pricing.quantity = -1;
   else
      genrand_integer(&r->Pricing.quantity, DIST_UNIFORM, 1, g_s_web_order_lineitem.Pricing.quantity, 0, S_WRET_PRICING);
	set_pricing(S_WRET_PRICING, &r->Pricing);
	r->kReasonID = mk_join(S_WRET_REASON_ID, REASON, 1);
	
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
pr_s_web_return(void *pSrc)
{
	struct S_WEB_RETURNS_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_web_returns;
	else
		r = pSrc;
	
	print_start(S_WEB_RETURNS);
	print_id(S_WRET_SITE_ID, r->kSiteID, 1);
	print_key(S_WRET_ORDER_ID,r->kOrderID, 1);
	print_integer(S_WRET_LINE_NUMBER, r->nLineNumber, 1);
	print_id(S_WRET_ITEM_ID, r->kItemID, 1);
	print_id(S_WRET_RETURN_CUST_ID, r->kReturningCustomerID, 1);
	print_id(S_WRET_REFUND_CUST_ID,r->kRefundedCustomerID, 1);
	print_date(S_WRET_RETURN_DATE, r->dtReturnDate.julian, 1);
	print_time(S_WRET_RETURN_TIME, r->kReturnTime, 1);
	print_integer(S_WRET_PRICING, r->Pricing.quantity, 1);
	print_decimal(S_WRET_PRICING, &r->Pricing.ext_sales_price, 1);
	print_decimal(S_WRET_PRICING, &r->Pricing.ext_tax, 1);
   print_decimal(S_WRET_PRICING, &r->Pricing.fee, 1);
	print_decimal(S_WRET_PRICING, &r->Pricing.ext_ship_cost, 1);
	print_decimal(S_WRET_PRICING, &r->Pricing.refunded_cash, 1);
	print_decimal(S_WRET_PRICING, &r->Pricing.reversed_charge, 1);
	print_decimal(S_WRET_PRICING, &r->Pricing.store_credit, 1);
   print_id(S_WRET_REASON_ID, r->kReasonID, 0);
	print_end(S_WEB_RETURNS);
	
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
ld_s_web_return(void *pSrc)
{
	struct S_WEB_RETURNS_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_web_returns;
	else
		r = pSrc;
	
	return(0);
}

