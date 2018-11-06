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
#include "s_catalog_returns.h"
#include "s_catalog_order.h"
#include "s_catalog_order_lineitem.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "parallel.h"

struct S_CATALOG_RETURNS_TBL g_s_catalog_returns;
extern struct S_CATALOG_ORDER_LINEITEM_TBL g_s_catalog_order_lineitem;
extern struct S_CATALOG_ORDER_TBL g_s_catalog_order;

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
mk_s_catalog_returns(void *pDest, ds_key_t kIndex)
{
	static int bInit = 0;
	struct S_CATALOG_RETURNS_TBL *r;
	int nReturnLag;
	
	
	if (pDest == NULL)
		r = &g_s_catalog_returns;
	else
		r = pDest;

	if (!bInit)
	{
		memset(&g_s_catalog_returns, 0, sizeof(struct S_CATALOG_RETURNS_TBL));
		bInit = 1;
	}

	genrand_integer(&nReturnLag, DIST_UNIFORM, 0, 60, 0, S_CRET_DATE);
	r->kReturnTime = mk_join(S_CRET_TIME, TIME, 1);
	jtodt(&r->dtReturnDate, g_s_catalog_order_lineitem.dtShipDate.julian + nReturnLag);
	r->kCallCenterID = mk_join(S_CRET_CALL_CENTER_ID, CALL_CENTER, 1);
	r->kItemID = g_s_catalog_order_lineitem.kItemID;
	r->kReasonID = mk_join(S_CRET_REASON_ID, REASON, 1);
	r->kOrderID = g_s_catalog_order.kID;
	r->kLineNumber = kIndex;
	r->kItemID = g_s_catalog_order_lineitem.kItemID;
	r->kReturnCustomerID= g_s_catalog_order.kShipCustomerID;
	r->kRefundCustomerID = g_s_catalog_order.kShipCustomerID;
   r->Pricing = g_s_catalog_order_lineitem.Pricing;
   genrand_integer(&r->Pricing.quantity, DIST_UNIFORM, 1, g_s_catalog_order_lineitem.Pricing.quantity, 0, S_CRET_PRICING);
   set_pricing(S_CRET_PRICING, &r->Pricing);
	r->kShipModeID = mk_join (S_CRET_SHIPMODE_ID, SHIP_MODE, 1);
	r->kWarehouseID = mk_join (S_CRET_WAREHOUSE_ID, WAREHOUSE, 1);
   r->kCatalogPageID = g_s_catalog_order_lineitem.kCatalogPage;
	
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
pr_s_catalog_returns(void *pSrc)
{
	struct S_CATALOG_RETURNS_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_catalog_returns;
	else
		r = pSrc;
	
	print_start(S_CATALOG_RETURNS);
   print_id(S_CRET_CALL_CENTER_ID, r->kCallCenterID, 1);
	print_key(S_CRET_ORDER_ID, r->kOrderID, 1);
	print_key(S_CRET_LINE_NUMBER, r->kLineNumber, 1);
	print_id(S_CRET_ITEM_ID, r->kItemID, 1);
	print_id(S_CRET_RETURN_CUSTOMER_ID, r->kReturnCustomerID, 1);
	print_id(S_CRET_REFUND_CUSTOMER_ID, r->kRefundCustomerID, 1);
	print_date(S_CRET_DATE, r->dtReturnDate.julian, 1);
	print_time(S_CRET_TIME, r->kReturnTime, 1);
	print_integer(S_CRET_QUANTITY, r->Pricing.quantity, 1);
	print_decimal(S_CRET_AMOUNT, &r->Pricing.net_paid, 1);
	print_decimal(S_CRET_TAX, &r->Pricing.ext_tax, 1);
   print_decimal(S_CRET_FEE, &r->Pricing.fee, 1);
	print_decimal(S_CRET_SHIP_COST, &r->Pricing.ext_ship_cost, 1);
   print_decimal(S_CRET_REFUNDED_CASH, &r->Pricing.refunded_cash, 1);
   print_decimal(S_CRET_REVERSED_CHARGE, &r->Pricing.reversed_charge, 1);
   print_decimal(S_CRET_MERCHANT_CREDIT, &r->Pricing.store_credit, 1);
   print_id(S_CRET_REASON_ID, r->kReasonID, 1);
   print_id(S_CRET_SHIPMODE_ID, r->kShipModeID, 1);
   print_id(S_CRET_WAREHOUSE_ID, r->kWarehouseID, 1);
   print_id(S_CRET_CATALOG_PAGE_ID, r->kCatalogPageID, 0);
   print_end(S_CATALOG_RETURNS);
   
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
ld_s_catalog_returns(void *pSrc)
{
	struct S_CATALOG_RETURNS_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_catalog_returns;
	else
		r = pSrc;
	
	return(0);
}

