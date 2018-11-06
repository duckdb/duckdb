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
#include "s_purchase.h"
#include "s_pline.h"
#include "s_store_returns.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "r_params.h"
#include "misc.h"
#include "scaling.h"
#include "parallel.h"

struct S_PURCHASE_TBL g_s_purchase;
struct S_PURCHASE_LINEITEM_TBL g_s_purchase_lineitem;
struct S_STORE_RETURNS_TBL g_s_store_return;
int nItemIndex;

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
mk_master(void *pDest, ds_key_t kIndex)
{
   static int bInit = 0;
   struct S_PURCHASE_TBL *r;
	
	if (pDest == NULL)
		r = &g_s_purchase;
	else
		r = pDest;


	if (!bInit)
	{
		memset(&g_s_purchase, 0, sizeof(struct S_PURCHASE_TBL));
		bInit = 1;
	}
    
   
   r->kID = kIndex + getUpdateBase(S_PURCHASE);
	r->kStoreID = mk_join(S_PURCHASE_STORE_ID, STORE, 1);
	r->kCustomerID = mk_join(S_PURCHASE_CUSTOMER_ID, CUSTOMER, 1);
	jtodt(&r->dtPurchaseDate, getUpdateDate(S_PURCHASE, kIndex));
	genrand_integer(&r->nRegister, DIST_UNIFORM, 1, 17, 0, S_PURCHASE_REGISTER);
	genrand_integer(&r->nClerk, DIST_UNIFORM, 101, 300, 0, S_PURCHASE_CLERK);
	gen_text(&r->szComment[0], (int)(RS_S_PURCHASE_COMMENT * 0.6), RS_S_PURCHASE_COMMENT, S_PURCHASE_COMMENT);

   return(0);
}

int
mk_detail(int i, int bPrint)
{
   int nTemp;

		mk_s_pline(&g_s_purchase_lineitem, i);
		if (bPrint)
         pr_s_pline(&g_s_purchase_lineitem);
		genrand_integer(&nTemp, DIST_UNIFORM, 0, 99, 0, S_PLINE_IS_RETURNED);
		if (nTemp < WR_RETURN_PCT)
		{
			mk_s_store_returns(&g_s_store_return, 1);
			if (bPrint)
            pr_s_store_returns(&g_s_store_return);
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
pr_s_purchase(void *pSrc)
{
	struct S_PURCHASE_TBL *r;
   char szKey[RS_BKEY + 1];

   if (pSrc == NULL)
		r = &g_s_purchase;
	else
		r = pSrc;
	
	print_start(S_PURCHASE);
	print_key(S_PURCHASE_ID, r->kID, 1);
   mk_bkey(szKey, r->kID, 0);
   mk_bkey(szKey, r->kStoreID, 0);
	print_varchar(S_PURCHASE_STORE_ID, szKey, 1);
   mk_bkey(szKey, r->kCustomerID, 0);
	print_varchar(S_PURCHASE_CUSTOMER_ID, szKey, 1);
	print_date(S_PURCHASE_DATE, r->dtPurchaseDate.julian, 1);
	print_integer(S_PURCHASE_TIME, r->nPurchaseTime, 1);
	print_integer(S_PURCHASE_REGISTER, r->nRegister, 1);
	print_integer(S_PURCHASE_CLERK, r->nClerk, 1);
	print_varchar(S_PURCHASE_COMMENT, r->szComment, 0);
	print_end(S_PURCHASE);

	return(0);
}

int
mk_s_purchase(void *pDest, ds_key_t kIndex)
{
   int i;

   mk_master(pDest, kIndex);
   genrand_integer(&nItemIndex, DIST_UNIFORM, 1, (int)getIDCount(ITEM), 0, S_PLINE_ITEM_ID);
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
ld_s_purchase(void *pSrc)
{
	struct S_PURCHASE_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_purchase;
	else
		r = pSrc;
	
	return(0);
}

int 
vld_s_purchase(int nTable, ds_key_t kRow, int* bPermutation)
{
   int nLineitem,
      i;

   row_skip(S_PURCHASE, kRow - 1);
   row_skip(S_PURCHASE_LINEITEM, (kRow - 1));
   row_skip(S_STORE_RETURNS, (kRow - 1));

   mk_master(NULL, kRow);
   genrand_integer(&nLineitem, DIST_UNIFORM, 1, 12, 0, S_PLINE_NUMBER);
   genrand_integer(&nItemIndex, DIST_UNIFORM, 1, (int)getIDCount(ITEM), 0, S_PLINE_ITEM_ID);
   for (i=1; i < nLineitem; i++)
      mk_detail(i, 0);
   print_start(S_PURCHASE_LINEITEM);
   print_key(0, (kRow - 1) * 12 + nLineitem, 1);
   mk_detail(i, 1);

   return(0);
}

