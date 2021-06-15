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
#include "w_customer_demographics.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "constants.h"
#include "genrand.h"
#include "nulls.h"
#include "porting.h"
#include "sparse.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>

struct W_CUSTOMER_DEMOGRAPHICS_TBL g_w_customer_demographics;

/*
 * mk_customer_demographics
 */
int mk_w_customer_demographics(void *info_arr, ds_key_t index) {

	struct W_CUSTOMER_DEMOGRAPHICS_TBL *r;
	ds_key_t kTemp;
	tdef *pTdef = getSimpleTdefsByNumber(CUSTOMER_DEMOGRAPHICS);

	r = &g_w_customer_demographics;

	nullSet(&pTdef->kNullBitMap, CD_NULLS);
	r->cd_demo_sk = index;
	kTemp = r->cd_demo_sk - 1;
	bitmap_to_dist(&r->cd_gender, "gender", &kTemp, 1, CUSTOMER_DEMOGRAPHICS);
	bitmap_to_dist(&r->cd_marital_status, "marital_status", &kTemp, 1, CUSTOMER_DEMOGRAPHICS);
	bitmap_to_dist(&r->cd_education_status, "education", &kTemp, 1, CUSTOMER_DEMOGRAPHICS);
	bitmap_to_dist(&r->cd_purchase_estimate, "purchase_band", &kTemp, 1, CUSTOMER_DEMOGRAPHICS);
	bitmap_to_dist(&r->cd_credit_rating, "credit_rating", &kTemp, 1, CUSTOMER_DEMOGRAPHICS);
	r->cd_dep_count = (int)(kTemp % (ds_key_t)CD_MAX_CHILDREN);
	kTemp /= (ds_key_t)CD_MAX_CHILDREN;
	r->cd_dep_employed_count = (int)(kTemp % (ds_key_t)CD_MAX_EMPLOYED);
	kTemp /= (ds_key_t)CD_MAX_EMPLOYED;
	r->cd_dep_college_count = (int)(kTemp % (ds_key_t)CD_MAX_COLLEGE);

	void *info = append_info_get(info_arr, CUSTOMER_DEMOGRAPHICS);
	append_row_start(info);

	append_key(info, r->cd_demo_sk);
	append_varchar(info, r->cd_gender);
	append_varchar(info, r->cd_marital_status);
	append_varchar(info, r->cd_education_status);
	append_integer(info, r->cd_purchase_estimate);
	append_varchar(info, r->cd_credit_rating);
	append_integer(info, r->cd_dep_count);
	append_integer(info, r->cd_dep_employed_count);
	append_integer(info, r->cd_dep_college_count);

	append_row_end(info);

	return 0;
}
