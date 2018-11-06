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
#include "s_web_promotional_item.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "parallel.h"

struct S_WEB_PROMOTIONAL_ITEM_TBL g_s_web_promotional_item;

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
mk_s_web_promotional_item(void *pDest, ds_key_t kIndex)
{
	static int bInit = 0;
	struct S_WEB_PROMOTIONAL_ITEM_TBL *r;
	
	if (pDest == NULL)
		r = &g_s_web_promotional_item;
	else
		r = pDest;

	if (!bInit)
	{
		memset(&g_s_web_promotional_item, 0, sizeof(struct S_WEB_PROMOTIONAL_ITEM_TBL));
		bInit = 1;
	}
	
	r->kSiteID = mk_join(S_WITM_SITE_ID, S_WEB_SITE, 1);
	r->kPageID = mk_join(S_WITM_PAGE_ID, S_WEB_PAGE, 1);
	r->kItemID = mk_join(S_WITM_ITEM_ID, S_ITEM, 1);
	r->kPromotionID = mk_join(S_WITM_PROMOTION_ID, S_PROMOTION, 1);
	row_stop(S_WEB_PROMOTIONAL_ITEM);
	
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
pr_s_web_promotional_item(void *pSrc)
{
	struct S_WEB_PROMOTIONAL_ITEM_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_web_promotional_item;
	else
		r = pSrc;
	
	print_start(S_WEB_PROMOTIONAL_ITEM);
	print_key(S_WITM_SITE_ID, r->kSiteID, 1);
	print_key(S_WITM_PAGE_ID, r->kPageID, 1);
	print_key(S_WITM_ITEM_ID, r->kItemID, 1);
	print_key(S_WITM_PROMOTION_ID, r->kPromotionID, 0);
	print_end(S_WEB_PROMOTIONAL_ITEM);
	
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
ld_s_web_promotional_item(void *pSrc)
{
	struct S_WEB_PROMOTIONAL_ITEM_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_web_promotional_item;
	else
		r = pSrc;
	
	return(0);
}

