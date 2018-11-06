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
#include "s_catalog_order_lineitem.h"
#include "s_catalog_order.h"
#include "w_web_sales.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "pricing.h"
#include "parallel.h"
#include "permute.h"
#include "scaling.h"
#include "constants.h"
#include "scd.h"

extern struct S_CATALOG_ORDER_LINEITEM_TBL g_s_catalog_order_lineitem;
extern struct S_CATALOG_ORDER_TBL g_s_catalog_order;
extern int nItemIndex;

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
mk_s_catalog_order_lineitem(void *pDest, ds_key_t kIndex)
{
	static int bInit = 0;
	struct S_CATALOG_ORDER_LINEITEM_TBL *r;
	static decimal_t dMin,
		dMax;
	static date_t dtMax;
	int nShipLag;
   static int *pItemPermutation;
	
	if (pDest == NULL)
		r = &g_s_catalog_order_lineitem;
	else
		r = pDest;

	if (!bInit)
	{
		memset(&g_s_catalog_order_lineitem, 0, sizeof(struct S_CATALOG_ORDER_LINEITEM_TBL));
		strtodec(&dMin, "1.00");
		strtodec(&dMax, "1000.00");
      pItemPermutation = makePermutation(NULL, (int)getIDCount(ITEM), S_CLIN_PERMUTE);
      strtodt(&dtMax, TODAYS_DATE);
		bInit = 1;
	}
	
	r->kOrderID = g_s_catalog_order.kID;
	r->kLineNumber = kIndex;
   nItemIndex += 1;
   if (nItemIndex > getIDCount(ITEM))
      nItemIndex = 1;

   /*
    * select a unique item, and then map to the appropriate business key
	*/
	r->kItemID = getPermutationEntry(pItemPermutation, nItemIndex);
	r->kItemID = getFirstSK(r->kItemID);

	r->kPromotionID = mk_join(S_CLIN_PROMOTION_ID, PROMOTION, 1);
	r->kWarehouseID = mk_join(S_CLIN_WAREHOUSE_ID, WAREHOUSE, 1);
	/*
	 * an order cannot ship until its has been made
	 * an order cannot be recorded as shipping if its is outside the date window
	 */
	genrand_integer(&nShipLag, DIST_UNIFORM, 0, 60, 0, S_CLIN_SHIP_DATE);
	jtodt(&r->dtShipDate, g_s_catalog_order.dtOrderDate.julian + nShipLag);
	if (r->dtShipDate.julian > dtMax.julian)
		r->dtShipDate.julian = -1;
	r->kCatalogPage = mk_join(S_CLIN_CATALOG_PAGE_ID, CATALOG_PAGE, g_s_catalog_order.dtOrderDate.julian);
	r->kCatalogID = getCatalogNumberFromPage(r->kCatalogPage);
	// genrand_integer(&r->Pricing.quantity, DIST_UNIFORM, 1, 100, 0, S_CLIN_QUANTITY);
	set_pricing(S_CLIN_PRICING, &r->Pricing);
	
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
pr_s_catalog_order_lineitem(void *pSrc)
{
	struct S_CATALOG_ORDER_LINEITEM_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_catalog_order_lineitem;
	else
		r = pSrc;
	
	print_start(S_CATALOG_ORDER_LINEITEM);
	print_key(S_CLIN_ORDER_ID, r->kOrderID, 1);
	print_key(S_CLIN_LINE_NUMBER, r->kLineNumber, 1);
	print_id(S_CLIN_ITEM_ID, r->kItemID, 1);
   print_id(S_CLIN_PROMOTION_ID, r->kPromotionID, 1);
	print_integer(S_CLIN_QUANTITY, r->Pricing.quantity, 1);
	print_decimal(S_CLIN_PRICING, &r->Pricing.sales_price, 1);
	print_decimal(S_CLIN_COUPON_AMT, &r->Pricing.coupon_amt, 1);
	print_id(S_CLIN_WAREHOUSE_ID, r->kWarehouseID, 1);
	print_date(S_CLIN_SHIP_DATE, r->dtShipDate.julian, 1);
	print_key(S_CLIN_CATALOG_ID, r->kCatalogID, 1);
	print_key(S_CLIN_CATALOG_PAGE_ID, r->kCatalogPage, 1);
	print_decimal(S_CLIN_SHIP_COST, &r->Pricing.ship_cost, 0);
	print_end(S_CATALOG_ORDER_LINEITEM);

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
ld_s_catalog_order_lineitem(void *pSrc)
{
	struct S_CATALOG_ORDER_LINEITEM_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_catalog_order_lineitem;
	else
		r = pSrc;
	
	return(0);
}

