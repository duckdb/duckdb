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
#include "constants.h"
#include "w_store.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "build_support.h"
#include "misc.h"
#include "tables.h"
#include "scaling.h"
#include "nulls.h"
#include "tdefs.h"
#include "scd.h"

#include "append_info.h"

struct W_STORE_TBL g_w_store;
static struct W_STORE_TBL g_OldValues;

/*
 * mk_store
 */
int mk_w_store(void *info_arr, ds_key_t index) {
	int32_t nFieldChangeFlags, bFirstRecord = 0;

	/* begin locals declarations */
	static decimal_t dRevMin, dRevMax;
	char *sName1, *sName2, *szTemp;
	int32_t nHierarchyTotal, nStoreType, nPercentage, nDaysOpen, nMin, nMax;
	static date_t tDate;
	static decimal_t min_rev_growth, max_rev_growth, dMinTaxPercentage, dMaxTaxPercentage;
	static int32_t bInit = 0;
	struct W_STORE_TBL *r, *rOldValues = &g_OldValues;
	tdef *pT = getSimpleTdefsByNumber(STORE);

	r = &g_w_store;

	if (!bInit) {
		nHierarchyTotal = (int)get_rowcount(DIVISIONS);
		nHierarchyTotal *= (int)get_rowcount(COMPANY);
		strtodt(&tDate, DATE_MINIMUM);
		strtodec(&min_rev_growth, STORE_MIN_REV_GROWTH);
		strtodec(&max_rev_growth, STORE_MAX_REV_GROWTH);
		strtodec(&dRevMin, "1.00");
		strtodec(&dRevMax, "1000000.00");
		strtodec(&dMinTaxPercentage, STORE_MIN_TAX_PERCENTAGE);
		strtodec(&dMaxTaxPercentage, STORE_MAX_TAX_PERCENTAGE);

		/* columns that should be dynamic */
		r->rec_end_date_id = -1;
	}

	nullSet(&pT->kNullBitMap, W_STORE_NULLS);
	r->store_sk = index;

	/* if we have generated the required history for this business key and
	 * generate a new one then reset associate fields (e.g., rec_start_date
	 * minimums)
	 */
	if (setSCDKeys(S_STORE_ID, index, r->store_id, &r->rec_start_date_id, &r->rec_end_date_id)) {
		bFirstRecord = 1;
	}

	/*
	 * this is  where we select the random number that controls if a field
	 * changes from one record to the next.
	 */
	nFieldChangeFlags = next_random(W_STORE_SCD);

	/* the rest of the record in a history-keeping dimension can either be a new
	 * data value or not; use a random number and its bit pattern to determine
	 * which fields to replace and which to retain
	 */
	nPercentage = genrand_integer(NULL, DIST_UNIFORM, 1, 100, 0, W_STORE_CLOSED_DATE_ID);
	nDaysOpen =
	    genrand_integer(NULL, DIST_UNIFORM, STORE_MIN_DAYS_OPEN, STORE_MAX_DAYS_OPEN, 0, W_STORE_CLOSED_DATE_ID);
	if (nPercentage < STORE_CLOSED_PCT)
		r->closed_date_id = tDate.julian + nDaysOpen;
	else
		r->closed_date_id = -1;
	changeSCD(SCD_KEY, &r->closed_date_id, &rOldValues->closed_date_id, &nFieldChangeFlags, bFirstRecord);
	if (!r->closed_date_id)
		r->closed_date_id = -1; /* dates use a special NULL indicator */

	mk_word(r->store_name, "syllables", (long)index, 5, W_STORE_NAME);
	changeSCD(SCD_CHAR, &r->store_name, &rOldValues->store_name, &nFieldChangeFlags, bFirstRecord);

	/*
	 * use the store type to set the parameters for the rest of the attributes
	 */
	nStoreType = pick_distribution(&szTemp, "store_type", 1, 1, W_STORE_TYPE);
	dist_member(&nMin, "store_type", nStoreType, 2);
	dist_member(&nMax, "store_type", nStoreType, 3);
	genrand_integer(&r->employees, DIST_UNIFORM, nMin, nMax, 0, W_STORE_EMPLOYEES);
	changeSCD(SCD_INT, &r->employees, &rOldValues->employees, &nFieldChangeFlags, bFirstRecord);

	dist_member(&nMin, "store_type", nStoreType, 4);
	dist_member(&nMax, "store_type", nStoreType, 5),
	    genrand_integer(&r->floor_space, DIST_UNIFORM, nMin, nMax, 0, W_STORE_FLOOR_SPACE);
	changeSCD(SCD_INT, &r->floor_space, &rOldValues->floor_space, &nFieldChangeFlags, bFirstRecord);

	pick_distribution(&r->hours, "call_center_hours", 1, 1, W_STORE_HOURS);
	changeSCD(SCD_PTR, &r->hours, &rOldValues->hours, &nFieldChangeFlags, bFirstRecord);

	pick_distribution(&sName1, "first_names", 1, 1, W_STORE_MANAGER);
	pick_distribution(&sName2, "last_names", 1, 1, W_STORE_MANAGER);
	sprintf(r->store_manager, "%s %s", sName1, sName2);
	changeSCD(SCD_CHAR, &r->store_manager, &rOldValues->store_manager, &nFieldChangeFlags, bFirstRecord);

	r->market_id = genrand_integer(NULL, DIST_UNIFORM, 1, 10, 0, W_STORE_MARKET_ID);
	changeSCD(SCD_INT, &r->market_id, &rOldValues->market_id, &nFieldChangeFlags, bFirstRecord);

	genrand_decimal(&r->dTaxPercentage, DIST_UNIFORM, &dMinTaxPercentage, &dMaxTaxPercentage, NULL,
	                W_STORE_TAX_PERCENTAGE);
	changeSCD(SCD_DEC, &r->dTaxPercentage, &rOldValues->dTaxPercentage, &nFieldChangeFlags, bFirstRecord);

	pick_distribution(&r->geography_class, "geography_class", 1, 1, W_STORE_GEOGRAPHY_CLASS);
	changeSCD(SCD_PTR, &r->geography_class, &rOldValues->geography_class, &nFieldChangeFlags, bFirstRecord);

	gen_text(&r->market_desc[0], STORE_DESC_MIN, RS_S_MARKET_DESC, W_STORE_MARKET_DESC);
	changeSCD(SCD_CHAR, &r->market_desc, &rOldValues->market_desc, &nFieldChangeFlags, bFirstRecord);

	pick_distribution(&sName1, "first_names", 1, 1, W_STORE_MARKET_MANAGER);
	pick_distribution(&sName2, "last_names", 1, 1, W_STORE_MARKET_MANAGER);
	sprintf(r->market_manager, "%s %s", sName1, sName2);
	changeSCD(SCD_CHAR, &r->market_manager, &rOldValues->market_manager, &nFieldChangeFlags, bFirstRecord);

	r->division_id = pick_distribution(&r->division_name, "divisions", 1, 1, W_STORE_DIVISION_NAME);
	changeSCD(SCD_KEY, &r->division_id, &rOldValues->division_id, &nFieldChangeFlags, bFirstRecord);
	changeSCD(SCD_PTR, &r->division_name, &rOldValues->division_name, &nFieldChangeFlags, bFirstRecord);

	r->company_id = pick_distribution(&r->company_name, "stores", 1, 1, W_STORE_COMPANY_NAME);
	changeSCD(SCD_KEY, &r->company_id, &rOldValues->company_id, &nFieldChangeFlags, bFirstRecord);
	changeSCD(SCD_PTR, &r->company_name, &rOldValues->company_name, &nFieldChangeFlags, bFirstRecord);

	mk_address(&r->address, W_STORE_ADDRESS);
	changeSCD(SCD_PTR, &r->address.city, &rOldValues->address.city, &nFieldChangeFlags, bFirstRecord);
	changeSCD(SCD_PTR, &r->address.county, &rOldValues->address.county, &nFieldChangeFlags, bFirstRecord);
	changeSCD(SCD_INT, &r->address.gmt_offset, &rOldValues->address.gmt_offset, &nFieldChangeFlags, bFirstRecord);
	changeSCD(SCD_PTR, &r->address.state, &rOldValues->address.state, &nFieldChangeFlags, bFirstRecord);
	changeSCD(SCD_PTR, &r->address.street_type, &rOldValues->address.street_type, &nFieldChangeFlags, bFirstRecord);
	changeSCD(SCD_PTR, &r->address.street_name1, &rOldValues->address.street_name1, &nFieldChangeFlags, bFirstRecord);
	changeSCD(SCD_PTR, &r->address.street_name2, &rOldValues->address.street_name2, &nFieldChangeFlags, bFirstRecord);
	changeSCD(SCD_INT, &r->address.street_num, &rOldValues->address.street_num, &nFieldChangeFlags, bFirstRecord);
	changeSCD(SCD_INT, &r->address.zip, &rOldValues->address.zip, &nFieldChangeFlags, bFirstRecord);

	char szTemp2[128];

	void *info = append_info_get(info_arr, STORE);
	append_row_start(info);

	append_key(info, r->store_sk);
	append_varchar(info, r->store_id);
	append_date(info, r->rec_start_date_id);
	append_date(info, r->rec_end_date_id);
	append_key(info, r->closed_date_id);
	append_varchar(info, r->store_name);
	append_integer(info, r->employees);
	append_integer(info, r->floor_space);
	append_varchar(info, r->hours);
	append_varchar(info, &r->store_manager[0]);
	append_integer(info, r->market_id);
	append_varchar(info, r->geography_class);
	append_varchar(info, &r->market_desc[0]);
	append_varchar(info, &r->market_manager[0]);
	append_key(info, r->division_id);
	append_varchar(info, r->division_name);
	append_key(info, r->company_id);
	append_varchar(info, r->company_name);
	append_integer(info, r->address.street_num);
	if (r->address.street_name2) {
		sprintf(szTemp2, "%s %s", r->address.street_name1, r->address.street_name2);
		append_varchar(info, szTemp2);
	} else
		append_varchar(info, r->address.street_name1);
	append_varchar(info, r->address.street_type);
	append_varchar(info, r->address.suite_num);
	append_varchar(info, r->address.city);
	append_varchar(info, r->address.county);
	append_varchar(info, r->address.state);
	sprintf(szTemp2, "%05d", r->address.zip);
	append_varchar(info, szTemp2);
	append_varchar(info, r->address.country);
	append_integer(info, r->address.gmt_offset);
	append_decimal(info, &r->dTaxPercentage);

	append_row_end(info);

	return 0;
}
