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
#include "w_ship_mode.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "genrand.h"
#include "nulls.h"
#include "porting.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>

struct W_SHIP_MODE_TBL g_w_ship_mode;

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
int mk_w_ship_mode(void *info_arr, ds_key_t kIndex) {
	struct W_SHIP_MODE_TBL *r;
	ds_key_t nTemp;
	tdef *pTdef = getSimpleTdefsByNumber(SHIP_MODE);

	r = &g_w_ship_mode;

	if (!InitConstants::mk_w_ship_mode_init) {
		memset(&g_w_ship_mode, 0, sizeof(struct W_SHIP_MODE_TBL));
		InitConstants::mk_w_ship_mode_init = 1;
	}

	nullSet(&pTdef->kNullBitMap, SM_NULLS);
	r->sm_ship_mode_sk = kIndex;
	mk_bkey(&r->sm_ship_mode_id[0], kIndex, SM_SHIP_MODE_ID);
	nTemp = (long)kIndex;
	bitmap_to_dist(&r->sm_type, "ship_mode_type", &nTemp, 1, SHIP_MODE);
	bitmap_to_dist(&r->sm_code, "ship_mode_code", &nTemp, 1, SHIP_MODE);
	dist_member(&r->sm_carrier, "ship_mode_carrier", (int)kIndex, 1);
	gen_charset(r->sm_contract, ALPHANUM, 1, RS_SM_CONTRACT, SM_CONTRACT);

	void *info = append_info_get(info_arr, SHIP_MODE);
	append_row_start(info);
	append_key(info, r->sm_ship_mode_sk);
	append_varchar(info, r->sm_ship_mode_id);
	append_varchar(info, r->sm_type);
	append_varchar(info, r->sm_code);
	append_varchar(info, r->sm_carrier);
	append_varchar(info, &r->sm_contract[0]);
	append_row_end(info);

	return 0;
}
