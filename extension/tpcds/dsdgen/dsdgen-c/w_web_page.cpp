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
#include "w_web_page.h"

#include "append_info.h"
#include "build_support.h"
#include "config.h"
#include "constants.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "misc.h"
#include "nulls.h"
#include "porting.h"
#include "scaling.h"
#include "scd.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>

struct W_WEB_PAGE_TBL g_w_web_page;
static struct W_WEB_PAGE_TBL g_OldValues;

/*
 * Routine: mk_web_page()
 * Purpose: populate the web_page table
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
 * 20020815 jms check text generation/seed usage
 */
int mk_w_web_page(void *info_arr, ds_key_t index) {
	int32_t bFirstRecord = 0, nFieldChangeFlags;
	static date_t dToday;
	static ds_key_t nConcurrent, nRevisions;

	/* begin locals declarations */
	int32_t nTemp, nAccess;
	char szTemp[16];
	struct W_WEB_PAGE_TBL *r, *rOldValues = &g_OldValues;
	tdef *pT = getSimpleTdefsByNumber(WEB_PAGE);

	r = &g_w_web_page;

	if (!InitConstants::mk_w_web_page_init) {
		/* setup invariant values */
		sprintf(szTemp, "%d-%d-%d", CURRENT_YEAR, CURRENT_MONTH, CURRENT_DAY);
		strtodt(&dToday, szTemp);

		/* set up for the SCD handling */
		nConcurrent = (int)get_rowcount(CONCURRENT_WEB_SITES);
		nRevisions = (int)get_rowcount(WEB_PAGE) / nConcurrent;

		InitConstants::mk_w_web_page_init = 1;
	}

	nullSet(&pT->kNullBitMap, WP_NULLS);
	r->wp_page_sk = index;

	/* if we have generated the required history for this business key and
	 * generate a new one then reset associate fields (e.g., rec_start_date
	 * minimums)
	 */
	if (setSCDKeys(WP_PAGE_ID, index, r->wp_page_id, &r->wp_rec_start_date_id, &r->wp_rec_end_date_id)) {

		/*
		 * some fields are not changed, even when a new version of the row is
		 * written
		 */
		bFirstRecord = 1;
	}

	/*
	 * this is  where we select the random number that controls if a field
	 * changes from one record to the next.
	 */
	nFieldChangeFlags = next_random(WP_SCD);

	r->wp_creation_date_sk = mk_join(WP_CREATION_DATE_SK, DATET, index);
	changeSCD(SCD_KEY, &r->wp_creation_date_sk, &rOldValues->wp_creation_date_sk, &nFieldChangeFlags, bFirstRecord);

	genrand_integer(&nAccess, DIST_UNIFORM, 0, WP_IDLE_TIME_MAX, 0, WP_ACCESS_DATE_SK);
	r->wp_access_date_sk = dToday.julian - nAccess;
	changeSCD(SCD_KEY, &r->wp_access_date_sk, &rOldValues->wp_access_date_sk, &nFieldChangeFlags, bFirstRecord);
	if (r->wp_access_date_sk == 0)
		r->wp_access_date_sk = -1; /* special case for dates */

	genrand_integer(&nTemp, DIST_UNIFORM, 0, 99, 0, WP_AUTOGEN_FLAG);
	r->wp_autogen_flag = (nTemp < WP_AUTOGEN_PCT) ? 1 : 0;
	changeSCD(SCD_INT, &r->wp_autogen_flag, &rOldValues->wp_autogen_flag, &nFieldChangeFlags, bFirstRecord);

	r->wp_customer_sk = mk_join(WP_CUSTOMER_SK, CUSTOMER, 1);
	changeSCD(SCD_KEY, &r->wp_customer_sk, &rOldValues->wp_customer_sk, &nFieldChangeFlags, bFirstRecord);

	if (!r->wp_autogen_flag)
		r->wp_customer_sk = -1;

	genrand_url(r->wp_url, WP_URL);
	changeSCD(SCD_CHAR, &r->wp_url, &rOldValues->wp_url, &nFieldChangeFlags, bFirstRecord);

	pick_distribution(&r->wp_type, "web_page_use", 1, 1, WP_TYPE);
	changeSCD(SCD_PTR, &r->wp_type, &rOldValues->wp_type, &nFieldChangeFlags, bFirstRecord);

	genrand_integer(&r->wp_link_count, DIST_UNIFORM, WP_LINK_MIN, WP_LINK_MAX, 0, WP_LINK_COUNT);
	changeSCD(SCD_INT, &r->wp_link_count, &rOldValues->wp_link_count, &nFieldChangeFlags, bFirstRecord);

	genrand_integer(&r->wp_image_count, DIST_UNIFORM, WP_IMAGE_MIN, WP_IMAGE_MAX, 0, WP_IMAGE_COUNT);
	changeSCD(SCD_INT, &r->wp_image_count, &rOldValues->wp_image_count, &nFieldChangeFlags, bFirstRecord);

	genrand_integer(&r->wp_max_ad_count, DIST_UNIFORM, WP_AD_MIN, WP_AD_MAX, 0, WP_MAX_AD_COUNT);
	changeSCD(SCD_INT, &r->wp_max_ad_count, &rOldValues->wp_max_ad_count, &nFieldChangeFlags, bFirstRecord);

	genrand_integer(&r->wp_char_count, DIST_UNIFORM, r->wp_link_count * 125 + r->wp_image_count * 50,
	                r->wp_link_count * 300 + r->wp_image_count * 150, 0, WP_CHAR_COUNT);
	changeSCD(SCD_INT, &r->wp_char_count, &rOldValues->wp_char_count, &nFieldChangeFlags, bFirstRecord);

	void *info = append_info_get(info_arr, WEB_PAGE);
	append_row_start(info);

	append_key(info, r->wp_page_sk);
	append_varchar(info, r->wp_page_id);
	append_date(info, r->wp_rec_start_date_id);
	append_date(info, r->wp_rec_end_date_id);
	append_key(info, r->wp_creation_date_sk);
	append_key(info, r->wp_access_date_sk);
	append_varchar(info, r->wp_autogen_flag ? "Y" : "N");
	append_key(info, r->wp_customer_sk);
	append_varchar(info, &r->wp_url[0]);
	append_varchar(info, &r->wp_type[0]);
	append_integer(info, r->wp_char_count);
	append_integer(info, r->wp_link_count);
	append_integer(info, r->wp_image_count);
	append_integer(info, r->wp_max_ad_count);
	append_row_end(info);

	return 0;
}
