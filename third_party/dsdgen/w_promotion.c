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
#include "w_promotion.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "misc.h"
#include "nulls.h"
#include "tdefs.h"

struct W_PROMOTION_TBL g_w_promotion;

/*
* Routine: mk_promotion
* Purpose: populate the promotion table
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
* 20020829 jms RNG usage on p_promo_name may be too large
* 20020829 jms RNG usage on P_CHANNEL_DETAILS may be too large
*/
int
mk_w_promotion(void *pDest, ds_key_t index)
{
	static int bInit = 0;
	struct W_PROMOTION_TBL *r;
	int res = 0;
	
	/* begin locals declarations */
	static date_t *start_date;
	ds_key_t nTemp;
	int nFlags;
   tdef *pTdef = getSimpleTdefsByNumber(PROMOTION);
	
	if (pDest == NULL)
		r = &g_w_promotion;
	else
		r = pDest;

	if (!bInit)
	{
		memset(&g_w_promotion, 0, sizeof(struct W_PROMOTION_TBL));
		bInit = 1;
        start_date = strtodate (DATE_MINIMUM);
	}
	
	nullSet(&pTdef->kNullBitMap, P_NULLS);
	r->p_promo_sk = index;
	mk_bkey(&r->p_promo_id[0], index, P_PROMO_ID);
	nTemp = index;
	r->p_start_date_id =
		start_date->julian +
		genrand_integer (NULL, DIST_UNIFORM,
		PROMO_START_MIN, PROMO_START_MAX, PROMO_START_MEAN,
		P_START_DATE_ID);
	r->p_end_date_id =
		r->p_start_date_id + genrand_integer (NULL, DIST_UNIFORM,
		PROMO_LEN_MIN,
		PROMO_LEN_MAX,
		PROMO_LEN_MEAN,
		P_END_DATE_ID);
	r->p_item_sk = mk_join(P_ITEM_SK, ITEM, 1);
	strtodec (&r->p_cost, "1000.00");
	r->p_response_target = 1;
	mk_word (&r->p_promo_name[0], "syllables", (int) index,
		PROMO_NAME_LEN, P_PROMO_NAME);
	nFlags = genrand_integer (NULL, DIST_UNIFORM, 0, 511, 0, P_CHANNEL_DMAIL);
	r->p_channel_dmail = nFlags & 0x01;
	nFlags <<= 1;
	r->p_channel_email = nFlags & 0x01;
	nFlags <<= 1;
	r->p_channel_catalog = nFlags & 0x01;
	nFlags <<= 1;
	r->p_channel_tv = nFlags & 0x01;
	nFlags <<= 1;
	r->p_channel_radio = nFlags & 0x01;
	nFlags <<= 1;
	r->p_channel_press = nFlags & 0x01;
	nFlags <<= 1;
	r->p_channel_event = nFlags & 0x01;
	nFlags <<= 1;
	r->p_channel_demo = nFlags & 0x01;
	nFlags <<= 1;
	r->p_discount_active = nFlags & 0x01;
	gen_text (&r->p_channel_details[0], PROMO_DETAIL_LEN_MIN,
		PROMO_DETAIL_LEN_MAX, P_CHANNEL_DETAILS);
	pick_distribution (&r->p_purpose, "promo_purpose", 1, 1,
		P_PURPOSE);
	
	
	return (res);
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
pr_w_promotion(void *row)
{
	struct W_PROMOTION_TBL *r;

	if (row == NULL)
		r = &g_w_promotion;
	else
		r = row;

	print_start(PROMOTION);
	print_key(P_PROMO_SK, r->p_promo_sk, 1);
	print_varchar(P_PROMO_ID, r->p_promo_id, 1);
	print_key(P_START_DATE_ID, r->p_start_date_id, 1);
	print_key(P_END_DATE_ID, r->p_end_date_id, 1);
	print_key(P_ITEM_SK, r->p_item_sk, 1);
	print_decimal(P_COST, &r->p_cost, 1);
	print_integer(P_RESPONSE_TARGET, r->p_response_target, 1);
	print_varchar(P_PROMO_NAME, &r->p_promo_name[0], 1);
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
	print_end(PROMOTION);

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
ld_w_promotion(void *pSrc)
{
	struct W_PROMOTION_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_promotion;
	else
		r = pSrc;
	
	return(0);
}

