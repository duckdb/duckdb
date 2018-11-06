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
#include "s_pline.h"
#include "s_purchase.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "misc.h"
#include "parallel.h"
#include "scaling.h"
#include "permute.h"
#include "scd.h"

struct S_PURCHASE_LINEITEM_TBL g_s_pline;
extern struct S_PURCHASE_TBL g_s_purchase;
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
mk_s_pline(void *pDest, ds_key_t kIndex)
{
	static int bInit = 0;
	struct S_PURCHASE_LINEITEM_TBL *r;
	static decimal_t dMin,
		dMax;
   static int *pItemPermutation,
      nItemIDCount;

	if (pDest == NULL)
		r = &g_s_pline;
	else
		r = pDest;

	if (!bInit)
	{
		memset(&g_s_pline, 0, sizeof(struct S_PURCHASE_LINEITEM_TBL));
		strtodec(&dMin, "1.00");
		strtodec(&dMax, "1000.00");
      /*
       * need to assure that a given item only appears in a single lineitem within an order
       * use adjacent orders from within a permutation of possible values;
       * since item is an SCD, use the item count
       */
      nItemIDCount = (int)getIDCount(ITEM);
      pItemPermutation = makePermutation(NULL, nItemIDCount, S_PLINE_PERMUTE);
		bInit = 1;
	}
	
	r->kPurchaseID = g_s_purchase.kID;
	r->kLineNumber = kIndex;
   nItemIndex += 1;
   if (nItemIndex > nItemIDCount)
      nItemIndex = 1;

	/*
    * pick the next entry in the permutation, to assure uniqueness within order
    * shift to SK value to align with printID() expectations
    */
   r->kItemID = getPermutationEntry(pItemPermutation, nItemIndex);
   r->kItemID = getFirstSK(r->kItemID);

	r->kPromotionID = mk_join(S_PLINE_PROMOTION_ID, PROMOTION, 1);
	genrand_integer(&r->Pricing.quantity, DIST_UNIFORM, PLINE_MIN_QUANTITY, PLINE_MAX_QUANTITY, 0, S_PLINE_QUANTITY);
	set_pricing(S_PLINE_PRICING, &r->Pricing);
	gen_text(r->szComment, 1, RS_S_PLINE_COMMENT, S_PLINE_COMMENT);
	/* row_stop(S_PURCHASE_LINEITEM); */
	
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
pr_s_pline(void *pSrc)
{
	struct S_PURCHASE_LINEITEM_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_pline;
	else
		r = pSrc;
	
	print_start(S_PURCHASE_LINEITEM);
	print_key(S_PLINE_PURCHASE_ID, r->kPurchaseID, 1);
	print_key(S_PLINE_NUMBER, r->kLineNumber, 1);
	print_id(S_PLINE_ITEM_ID, r->kItemID, 1);
	print_id(S_PLINE_PROMOTION_ID, r->kPromotionID, 1);
	print_integer(S_PLINE_QUANTITY, r->Pricing.quantity, 1);
	print_decimal(S_PLINE_SALE_PRICE, &r->Pricing.sales_price, 1);
	print_decimal(S_PLINE_COUPON_AMT, &r->Pricing.coupon_amt, 1);
	print_varchar(S_PLINE_COMMENT, r->szComment,0);
	print_end(S_PURCHASE_LINEITEM);
	
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
ld_s_pline(void *pSrc)
{
	struct S_PURCHASE_LINEITEM_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_pline;
	else
		r = pSrc;
	
	return(0);
}

