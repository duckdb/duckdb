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
#include "s_promotion.h"
#include "w_promotion.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "misc.h"
#include "parallel.h"
#include "date.h"
#include "permute.h"
#include "scaling.h"
#include "tdef_functions.h"
#include "scd.h"
#include "r_params.h"

extern struct W_PROMOTION_TBL g_w_promotion;


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
mk_s_promotion (void* row, ds_key_t index)
{
   static int bInit = 0;
   static int *pPermutation;
   ds_key_t kIndex;

   if (!bInit)
   {
      pPermutation = makePermutation(NULL, (int)getIDCount(PROMOTION), S_PROMOTION_ID);
      bInit = 1;
   }

   kIndex = getPermutationEntry(pPermutation, (int)index);
   mk_w_promotion(NULL, kIndex);
   row_stop(PROMOTION);

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
pr_s_promotion(void *pSrc)
{
	struct W_PROMOTION_TBL *r;

	if (pSrc == NULL)
		r = &g_w_promotion;
	else
		r = pSrc;
	
	print_start(S_PROMOTION);
	print_varchar(P_PROMO_ID, r->p_promo_id, 1);
	print_varchar(P_PROMO_NAME, &r->p_promo_name[0], 1);
   print_date(P_START_DATE_ID, r->p_start_date_id, 1);
   print_date(P_START_DATE_ID, r->p_end_date_id, 1);
	print_decimal(P_COST, &r->p_cost, 1);
	print_integer(P_RESPONSE_TARGET, r->p_response_target, 1);
	print_boolean(P_CHANNEL_DMAIL, r->p_channel_dmail, 1);
	print_boolean(P_CHANNEL_EMAIL, r->p_channel_email, 1);
	print_boolean(P_CHANNEL_CATALOG, r->p_channel_catalog, 1);
	print_boolean(P_CHANNEL_TV, r->p_channel_tv, 1);
	print_boolean(P_CHANNEL_RADIO, r->p_channel_radio, 1);
	print_boolean(P_CHANNEL_PRESS, r->p_channel_press, 1);
	print_boolean(P_CHANNEL_EVENT, r->p_channel_event, 1);
	print_boolean(P_CHANNEL_DEMO, r->p_channel_demo, 1);
	print_varchar(P_CHANNEL_DETAILS, &r->p_channel_details[0], 1);
	print_varchar(P_PURPOSE, r->p_purpose, 1);
	print_boolean(P_DISCOUNT_ACTIVE, r->p_discount_active, 0);
	print_end(S_PROMOTION);
	
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
ld_s_promotion(void *pSrc)
{
	struct W_PROMOTION_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_promotion;
	else
		r = pSrc;
	
	return(0);
}


int
vld_s_promotion(int nTable, ds_key_t kRow, int *Permutation)
{
   static int bInit = 0;
   static int *pPermutation;
   ds_key_t kIndex;
   table_func_t *pTF = getTdefFunctionsByNumber(PROMOTION);

   if (!bInit)
   {
      pPermutation = 
         makePermutation(NULL, (int)getIDCount(PROMOTION), S_PROMOTION_ID);
      bInit = 1;
   }

   kIndex = getPermutationEntry(pPermutation, (int)kRow);

   row_skip(PROMOTION, kRow - 1);
	pTF->builder(NULL, kIndex);
   row_stop(PROMOTION);

	return(0);
}

