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
#include "s_catalog_promotional_item.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"

struct S_CATALOG_PROMOTIONAL_ITEM_TBL g_s_catalog_promotional_item;

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
mk_s_catalog_promotional_item(void *pDest, ds_key_t kIndex)
{
	struct S_CATALOG_PROMOTIONAL_ITEM_TBL *r;
	static int bInit = 0;
	
	if (pDest == NULL)
		r = &g_s_catalog_promotional_item;
	else
		r = pDest;

	if (!bInit)
	{
		memset(&g_s_catalog_promotional_item, 0, sizeof(struct S_CATALOG_PROMOTIONAL_ITEM_TBL));
		bInit = 1;
	}
	r->catalog_promotional_item_catalog_number = mk_join(S_CATALOG_PROMOTIONAL_ITEM_CATALOG_NUMBER,S_CATALOG, 1);
	genrand_integer(&r->catalog_promotional_item_catalog_page_number, DIST_UNIFORM, 1, PAGES_PER_CATALOG, 0, S_CATALOG_PROMOTIONAL_ITEM_CATALOG_PAGE_NUMBER);
	r->catalog_promotional_item_item_id = mk_join(S_CATALOG_PROMOTIONAL_ITEM_ITEM_ID, S_ITEM, 1);
	r->catalog_promotional_item_promotion_id = kIndex / ITEMS_PER_PROMOTION;
	
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
pr_s_catalog_promotional_item(void *pSrc)
{
	struct S_CATALOG_PROMOTIONAL_ITEM_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_catalog_promotional_item;
	else
		r = pSrc;
	
	print_start(S_CATALOG_PROMOTIONAL_ITEM);
	print_key(S_CATALOG_PROMOTIONAL_ITEM_CATALOG_NUMBER, r->catalog_promotional_item_catalog_number, 1);
	print_key(S_CATALOG_PROMOTIONAL_ITEM_CATALOG_PAGE_NUMBER, r->catalog_promotional_item_catalog_page_number, 1);
	print_key(S_CATALOG_PROMOTIONAL_ITEM_ITEM_ID, r->catalog_promotional_item_item_id, 1);
	print_key(S_CATALOG_PROMOTIONAL_ITEM_PROMOTION_ID, r->catalog_promotional_item_promotion_id, 0);
	print_end(S_CATALOG_PROMOTIONAL_ITEM);
	
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
ld_s_catalog_promotional_item(void *pSrc)
{
	struct S_CATALOG_PROMOTIONAL_ITEM_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_catalog_promotional_item;
	else
		r = pSrc;
	
	return(0);

}
