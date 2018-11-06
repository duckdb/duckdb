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
#include "s_item.h"
#include "w_item.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "misc.h"
#include "parallel.h"
#include "permute.h"
#include "scaling.h"
#include "scd.h"
#include "tdef_functions.h"
#include "r_params.h"

extern struct W_ITEM_TBL g_w_item;
extern struct W_ITEM_TBL g_OldValues;
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
mk_s_item (void* row, ds_key_t index)
{
   static int bInit = 0;
   static int *pPermutation;
   ds_key_t kIndex;

   if (!bInit)
   {
      pPermutation = makePermutation(NULL, (int)getIDCount(ITEM),
      S_ITEM_PERMUTE);
      bInit = 1;
   }

   kIndex = getPermutationEntry(pPermutation, (int)index);
   mk_w_item(NULL, getSKFromID(kIndex, S_ITEM_ID));
   row_stop(ITEM);

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
pr_s_item(void *pSrc)
{
	struct W_ITEM_TBL *r;

	if (pSrc == NULL)
		r = &g_w_item;
	else
		r = pSrc;
	
	print_start(S_ITEM);
	print_varchar(S_ITEM_ID, r->i_item_id, 1);
	print_varchar(S_ITEM_DESC, r->i_item_desc, 1);
	print_decimal(S_ITEM_LIST_PRICE, &r->i_current_price, 1);
	print_decimal(S_ITEM_WHOLESALE_COST, &r->i_wholesale_cost, 1);
	print_varchar(S_ITEM_SIZE, r->i_size, 1);
	print_varchar(S_ITEM_FORMULATION, r->i_formulation, 1);
	print_varchar(S_ITEM_FLAVOR, r->i_color, 1);
	print_varchar(S_ITEM_UNITS, r->i_units, 1);
	print_varchar(S_ITEM_CONTAINER, r->i_container, 1);
	print_key(S_ITEM_MANAGER_ID, r->i_manager_id, 0);
	print_end(S_ITEM);
	
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
ld_s_item(void *pSrc)
{
	struct W_ITEM_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_item;
	else
		r = pSrc;
	
	return(0);
}

int
vld_s_item(int nTable, ds_key_t kRow, int *Permutation)
{
   static int bInit = 0;
   static int *pPermutation;
   table_func_t *pTF = getTdefFunctionsByNumber(ITEM);

   if (!bInit)
   {
      pPermutation = makePermutation(NULL, (int)getIDCount(ITEM),
      S_ITEM_PERMUTE);
      bInit = 1;
   }

   memset(&g_OldValues, 0, sizeof(struct W_ITEM_TBL));
   pTF->validate(S_ITEM, kRow, pPermutation);

   return(0);
}


