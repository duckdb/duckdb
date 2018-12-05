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
#include "columns.h"
#include "w_customer.h"
#include "genrand.h"
#include "build_support.h"
#include "tables.h"
#include "nulls.h"
#include "tdefs.h"

#include "append_info.h"

struct W_CUSTOMER_TBL g_w_customer;
/* extern tdef w_tdefs[]; */

/*
 * Routine: mk_customer
 * Purpose: populate the customer dimension
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
 */
int mk_w_customer(void *info_arr, ds_key_t index) {
	int nTemp;

	static int nBaseDate;
	/* begin locals declarations */
	int nNameIndex, nGender;
	struct W_CUSTOMER_TBL *r;
	static int bInit = 0;
	date_t dtTemp;
	static date_t dtBirthMin, dtBirthMax, dtToday, dt1YearAgo, dt10YearsAgo;
	tdef *pT = getSimpleTdefsByNumber(CUSTOMER);

	r = &g_w_customer;

	if (!bInit) {
		date_t min_date;
		strtodt(&min_date, DATE_MINIMUM);
		nBaseDate = dttoj(&min_date);

		strtodt(&dtBirthMax, "1992-12-31");
		strtodt(&dtBirthMin, "1924-01-01");
		strtodt(&dtToday, TODAYS_DATE);
		jtodt(&dt1YearAgo, dtToday.julian - 365);
		jtodt(&dt10YearsAgo, dtToday.julian - 3650);

		bInit = 1;
	}

	nullSet(&pT->kNullBitMap, C_NULLS);
	r->c_customer_sk = index;
	mk_bkey(&r->c_customer_id[0], index, C_CUSTOMER_ID);
	genrand_integer(&nTemp, DIST_UNIFORM, 1, 100, 0, C_PREFERRED_CUST_FLAG);
	r->c_preferred_cust_flag = (nTemp < C_PREFERRED_PCT) ? 1 : 0;

	/* demographic keys are a composite of values. rebuild them a la
	 * bitmap_to_dist */
	r->c_current_hdemo_sk = mk_join(C_CURRENT_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1);

	r->c_current_cdemo_sk = mk_join(C_CURRENT_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1);

	r->c_current_addr_sk = mk_join(C_CURRENT_ADDR_SK, CUSTOMER_ADDRESS, r->c_customer_sk);
	nNameIndex = pick_distribution(&r->c_first_name, "first_names", 1, 3, C_FIRST_NAME);
	pick_distribution(&r->c_last_name, "last_names", 1, 1, C_LAST_NAME);
	dist_weight(&nGender, "first_names", nNameIndex, 2);
	pick_distribution(&r->c_salutation, "salutations", 1, (nGender == 0) ? 2 : 3, C_SALUTATION);

	genrand_date(&dtTemp, DIST_UNIFORM, &dtBirthMin, &dtBirthMax, NULL, C_BIRTH_DAY);
	r->c_birth_day = dtTemp.day;
	r->c_birth_month = dtTemp.month;
	r->c_birth_year = dtTemp.year;
	genrand_email(r->c_email_address, r->c_first_name, r->c_last_name, C_EMAIL_ADDRESS);
	genrand_date(&dtTemp, DIST_UNIFORM, &dt1YearAgo, &dtToday, NULL, C_LAST_REVIEW_DATE);
	r->c_last_review_date = dtTemp.julian;
	genrand_date(&dtTemp, DIST_UNIFORM, &dt10YearsAgo, &dtToday, NULL, C_FIRST_SALES_DATE_ID);
	r->c_first_sales_date_id = dtTemp.julian;
	r->c_first_shipto_date_id = r->c_first_sales_date_id + 30;

	pick_distribution(&r->c_birth_country, "countries", 1, 1, C_BIRTH_COUNTRY);

	void *info = append_info_get(info_arr, CUSTOMER);
	append_row_start(info);

	append_key(info, r->c_customer_sk);
	append_varchar(info, r->c_customer_id);
	append_key(info, r->c_current_cdemo_sk);
	append_key(info, r->c_current_hdemo_sk);
	append_key(info, r->c_current_addr_sk);
	append_integer(info, r->c_first_shipto_date_id);
	append_integer(info, r->c_first_sales_date_id);
	append_varchar(info, r->c_salutation);
	append_varchar(info, r->c_first_name);
	append_varchar(info, r->c_last_name);
	append_boolean(info, r->c_preferred_cust_flag);
	append_integer(info, r->c_birth_day);
	append_integer(info, r->c_birth_month);
	append_integer(info, r->c_birth_year);
	append_varchar(info, r->c_birth_country);
	append_varchar(info, &r->c_login[0]);
	append_varchar(info, &r->c_email_address[0]);
	append_integer(info, r->c_last_review_date);

	append_row_end(info);

	return 0;
}
