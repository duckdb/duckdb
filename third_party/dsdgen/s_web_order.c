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
#include "s_web_order.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "misc.h"
#include "s_web_order_lineitem.h"
#include "params.h"
#include "scaling.h"
#include "w_web_sales.h"
#include "s_web_returns.h"
#include "parallel.h"

struct S_WEB_ORDER_TBL g_s_web_order;
struct S_WEB_ORDER_LINEITEM_TBL g_s_web_order_lineitem;
struct S_WEB_RETURNS_TBL g_s_web_return;
int nItemIndex = -1;

	/* now build/print the lineitems for this order */
	/* for each lineitem, randomly determine if it is to be returned */
/**
 ** TODO: need to make this variable, as in the warehouse
**/
static void 
mk_detail(int i, int bPrint)
{
      int nTemp;

		mk_s_web_order_lineitem(&g_s_web_order_lineitem, i);
      if (bPrint)
         pr_s_web_order_lineitem(&g_s_web_order_lineitem);
		genrand_integer(&nTemp, DIST_UNIFORM, 0, 99, 0, S_WLIN_IS_RETURNED);
		if (nTemp < WR_RETURN_PCT)
		{
			mk_s_web_return(&g_s_web_return, i);
			if (bPrint)
            pr_s_web_return(&g_s_web_return);
		}

return;
}

static void 
mk_master(void *pDest, ds_key_t kIndex)
{
   struct S_WEB_ORDER_TBL *r;
   int nGiftPct;
   
   static int bInit = 0;
   
   if (pDest == NULL)
      r = &g_s_web_order;
   else
      r = pDest;
   
   if (!bInit)
   {
      memset(&g_s_web_order, 0, sizeof(struct S_WEB_ORDER_TBL));
      bInit = 1;
   }

   jtodt(&r->dtOrderDate, getUpdateDate(S_WEB_ORDER, kIndex));
	r->kID = getUpdateBase(S_WEB_ORDER) + kIndex;
	genrand_integer(&r->nOrderTime , DIST_UNIFORM, 0, (24 * 3600) - 1, 0, S_WORD_ORDER_TIME);
	r->kBillCustomerID = mk_join(S_WORD_BILL_CUSTOMER_ID, CUSTOMER, 1);

	/* most orders are for the ordering customers, some are not */
	genrand_integer(&nGiftPct, DIST_UNIFORM, 0, 99, 0, S_WORD_SHIP_CUSTOMER_ID);
	if (nGiftPct > WS_GIFT_PCT)
		r->kShipCustomerID =
			mk_join (S_WORD_SHIP_CUSTOMER_ID, CUSTOMER, 2);
	else
		r->kShipCustomerID = r->kBillCustomerID;
	r->kShipModeID = mk_join(S_WORD_SHIP_MODE_ID, SHIP_MODE, 1);
	r->kWebSiteID = mk_join(S_WORD_WEB_SITE_ID, WEB_SITE, 1);
	gen_text(&r->szComment[0], (int)(RS_S_WORD_COMMENT * 0.6), RS_S_WORD_COMMENT, S_WORD_COMMENT);
	
	return;
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
mk_s_web_order(void *pDest, ds_key_t kIndex)
{
   int i;

   mk_master(pDest, kIndex);
   genrand_integer(&nItemIndex, DIST_UNIFORM, 1, (int)getIDCount(ITEM), 0, S_WLIN_ITEM_ID);
	for (i=1; i <= 12; i++)
	{
      mk_detail(i, 1);
   }

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
pr_s_web_order(void *pSrc)
{
	struct S_WEB_ORDER_TBL *r;
   char szTemp[RS_BKEY + 1];

	if (pSrc == NULL)
		r = &g_s_web_order;
	else
		r = pSrc;
	
	print_start(S_WEB_ORDER);
	print_key(S_WORD_ID, r->kID, 1);
   mk_bkey(szTemp, r->kID, 0);
	print_id(S_WORD_BILL_CUSTOMER_ID, r->kBillCustomerID, 1);
	print_id(S_WORD_SHIP_CUSTOMER_ID, r->kShipCustomerID, 1);
	print_date(S_WORD_ORDER_DATE, r->dtOrderDate.julian, 1);
	print_integer(S_WORD_ORDER_TIME, r->nOrderTime, 1);
	print_id(S_WORD_SHIP_MODE_ID, r->kShipModeID, 1);
	print_id(S_WORD_WEB_SITE_ID, r->kWebSiteID, 1);
	print_varchar(S_WORD_COMMENT, r->szComment, 0);
	print_end(S_WEB_ORDER);
	
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
ld_s_web_order(void *pSrc)
{
	struct S_WEB_ORDER_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_web_order;
	else
		r = pSrc;
	
	return(0);
}

int 
vld_s_web_order(int nTable, ds_key_t kRow, int* bPermutation)
{
   int nLineitem,
      i;

   row_skip(S_WEB_ORDER, kRow - 1);
   row_skip(S_WEB_ORDER_LINEITEM, kRow - 1);
   row_skip(S_WEB_RETURNS, kRow - 1);

   mk_master(NULL, kRow);
   genrand_integer(&nLineitem, DIST_UNIFORM, 1, 12, 0, S_WLIN_LINE_NUMBER);
   genrand_integer(&nItemIndex, DIST_UNIFORM, 1, (int)getIDCount(ITEM), 0, S_WLIN_ITEM_ID);
   for (i=1; i < nLineitem; i++)
      mk_detail(i, 0);
   print_start(S_WEB_ORDER_LINEITEM);
   print_key(0, (kRow - 1) * 12 + nLineitem, 1);
   mk_detail(i, 1);

   return(0);
}

