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
#include "w_household_demographics.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "genrand.h"
#include "nulls.h"
#include "porting.h"
#include "sparse.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>

struct W_HOUSEHOLD_DEMOGRAPHICS_TBL g_w_household_demographics;

/*
 * mk_household_demographics
 */
int mk_w_household_demographics(void *info_arr, ds_key_t index) {
	/* begin locals declarations */
	ds_key_t nTemp;
	struct W_HOUSEHOLD_DEMOGRAPHICS_TBL *r;
	tdef *pTdef = getSimpleTdefsByNumber(HOUSEHOLD_DEMOGRAPHICS);

	r = &g_w_household_demographics;

	nullSet(&pTdef->kNullBitMap, HD_NULLS);
	r->hd_demo_sk = index;
	nTemp = r->hd_demo_sk;
	r->hd_income_band_id = (nTemp % distsize("income_band")) + 1;
	nTemp /= distsize("income_band");
	bitmap_to_dist(&r->hd_buy_potential, "buy_potential", &nTemp, 1, HOUSEHOLD_DEMOGRAPHICS);
	bitmap_to_dist(&r->hd_dep_count, "dependent_count", &nTemp, 1, HOUSEHOLD_DEMOGRAPHICS);
	bitmap_to_dist(&r->hd_vehicle_count, "vehicle_count", &nTemp, 1, HOUSEHOLD_DEMOGRAPHICS);

	void *info = append_info_get(info_arr, HOUSEHOLD_DEMOGRAPHICS);
	append_row_start(info);
	append_key(info, r->hd_demo_sk);
	append_key(info, r->hd_income_band_id);
	append_varchar(info, r->hd_buy_potential);
	append_integer(info, r->hd_dep_count);
	append_integer(info, r->hd_vehicle_count);
	append_row_end(info);

	return 0;
}
