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
#include "s_inventory.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "r_params.h"
#include "parallel.h"
#include "scaling.h"
#include "scd.h"

struct S_INVENTORY_TBL g_s_inventory;

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
mk_s_inventory(void *pDest, ds_key_t kIndex)
{
	static int bInit = 0;
	struct S_INVENTORY_TBL *r;
	static ds_key_t item_count;
	static ds_key_t warehouse_count;
	ds_key_t kItem;
	int nTemp;
	
	if (pDest == NULL)
		r = &g_s_inventory;
	else
		r = pDest;

	if (!bInit)
	{
        item_count = getIDCount(ITEM);
        warehouse_count = get_rowcount (WAREHOUSE);
		memset(&g_s_inventory, 0, sizeof(struct S_INVENTORY_TBL));
		bInit = 1;
	}
	
	nTemp = (int) kIndex - 1;
	kItem = (nTemp % item_count) + 1;
	nTemp /= (int) item_count;
	r->warehouse_id = (nTemp % warehouse_count) + 1;


	/*
	 * the generation of SCD ids in the warehouse is tied to the monotonically increasing surrogate key.
	 * this isn't ideal, but changing the data set on the warehouse side is a problem at this late date, so we
	 * need to mimic the behavior.
	 */
   r->invn_date.julian = getUpdateDate(S_INVENTORY, kIndex);
	r->item_id = getFirstSK(kItem);
	genrand_integer(&r->quantity, DIST_UNIFORM, INVN_MIN_QUANTITY, INVN_MAX_QUANTITY, 0, S_INVN_QUANTITY);

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
pr_s_inventory(void *pSrc)
{
	struct S_INVENTORY_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_inventory;
	else
		r = pSrc;
	
	print_start(S_INVENTORY);
	print_id(S_INVN_WAREHOUSE, r->warehouse_id, 1);
	print_id(S_INVN_ITEM, r->item_id, 1);
	print_date(S_INVN_DATE, r->invn_date.julian, 1);
	print_integer(S_INVN_QUANTITY, r->quantity, 0);
	print_end(S_INVENTORY);
	
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
ld_s_inventory(void *pSrc)
{
	struct S_INVENTORY_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_inventory;
	else
		r = pSrc;
	
	return(0);
}

