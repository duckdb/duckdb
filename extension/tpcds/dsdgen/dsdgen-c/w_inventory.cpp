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
#include "w_inventory.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "constants.h"
#include "date.h"
#include "genrand.h"
#include "nulls.h"
#include "porting.h"
#include "scaling.h"
#include "scd.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>

struct W_INVENTORY_TBL g_w_inventory;

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
int mk_w_inventory(void *info_arr, ds_key_t index) {
	struct W_INVENTORY_TBL *r;
	static ds_key_t item_count;
	static ds_key_t warehouse_count;
	static int jDate;
	date_t base_date_storage;
	date_t *base_date = &base_date_storage;
	int nTemp;
	tdef *pTdef = getSimpleTdefsByNumber(INVENTORY);

	r = &g_w_inventory;

	if (!InitConstants::mk_w_inventory_init) {
		memset(&g_w_inventory, 0, sizeof(struct W_INVENTORY_TBL));
		item_count = getIDCount(ITEM);
		warehouse_count = get_rowcount(WAREHOUSE);
		strtodt(base_date, DATE_MINIMUM);
		jDate = base_date->julian;
		set_dow(base_date);
		/* Make exceptions to the 1-rng-call-per-row rule */
		InitConstants::mk_w_inventory_init = 1;
	}

	nullSet(&pTdef->kNullBitMap, INV_NULLS);
	nTemp = (int)index - 1;
	r->inv_item_sk = (nTemp % item_count) + 1;
	nTemp /= (int)item_count;
	r->inv_warehouse_sk = (nTemp % warehouse_count) + 1;
	nTemp /= (int)warehouse_count;
	r->inv_date_sk = jDate + (nTemp * 7); /* inventory is updated weekly */

	/*
	 * the join between item and inventory is tricky. The item_id selected above
	 * identifies a unique part num but item is an SCD, so we need to account
	 * for that in selecting the SK to join with
	 */
	r->inv_item_sk = matchSCDSK(r->inv_item_sk, r->inv_date_sk, ITEM);

	genrand_integer(&r->inv_quantity_on_hand, DIST_UNIFORM, INV_QUANTITY_MIN, INV_QUANTITY_MAX, 0,
	                INV_QUANTITY_ON_HAND);

	void *info = append_info_get(info_arr, INVENTORY);
	append_row_start(info);
	append_key(info, r->inv_date_sk);
	append_key(info, r->inv_item_sk);
	append_key(info, r->inv_warehouse_sk);
	append_integer(info, r->inv_quantity_on_hand);
	append_row_end(info);

	return 0;
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
ds_key_t sc_w_inventory(int nScale) {
	ds_key_t kRes;
	date_t dTemp;
	int nDays;

	kRes = getIDCount(ITEM);
	kRes *= get_rowcount(WAREHOUSE);
	strtodt(&dTemp, DATE_MAXIMUM);
	nDays = dTemp.julian;
	strtodt(&dTemp, DATE_MINIMUM);
	nDays -= dTemp.julian;
	nDays += 1;
	nDays += 6;
	nDays /= 7; /* each items inventory is updated weekly */
	kRes *= nDays;

	return (kRes);
}
