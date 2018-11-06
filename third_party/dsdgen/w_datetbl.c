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
#include "w_datetbl.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "nulls.h"
#include "tdefs.h"

struct W_DATE_TBL g_w_date;
/* extern tdef w_tdefs[]; */

/*
* Routine: mk_datetbl
* Purpose: populate the date dimension
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
int
mk_w_date (void * row, ds_key_t index)
{
	int res = 0;
	
	/* begin locals declarations */
	static date_t base_date;
	int day_index,
		nTemp;
	date_t temp_date, dTemp2;
	struct W_DATE_TBL *r;
	static int bInit = 0;
   tdef *pT = getSimpleTdefsByNumber(DATE);

	if (row == NULL)
		r = &g_w_date;
	else
		r = row;

	if (!bInit)
	{
	    r->d_month_seq = 0;
        r->d_week_seq = 1;
        r->d_quarter_seq = 1;
        r->d_current_month = 0;
        r->d_current_quarter = 0;
        r->d_current_week = 0;
		strtodt(&base_date, "1900-01-01");
        /* Make exceptions to the 1-rng-call-per-row rule */
	bInit = 1;
	}
	
	nullSet(&pT->kNullBitMap, D_NULLS);
	nTemp = (long)index + base_date.julian;
	r->d_date_sk = nTemp;
	mk_bkey(&r->d_date_id[0], nTemp, D_DATE_ID);
	jtodt (&temp_date, nTemp);
	r->d_year = temp_date.year;
	r->d_dow = set_dow (&temp_date);
	r->d_moy = temp_date.month;
	r->d_dom = temp_date.day;
	/* set the sequence counts; assumes that the date table starts on a year boundary */
	r->d_week_seq = ((int)index + 6) / 7;
	r->d_month_seq = (r->d_year - 1900) * 12 + r->d_moy - 1;
	r->d_quarter_seq = (r->d_year - 1900) * 4 + r->d_moy / 3 + 1;
	day_index = day_number (&temp_date);
	dist_member (&r->d_qoy, "calendar", day_index, 6);
	/* fiscal year is identical to calendar year */
	r->d_fy_year = r->d_year;
	r->d_fy_quarter_seq = r->d_quarter_seq;
	r->d_fy_week_seq = r->d_week_seq;
	r->d_day_name = weekday_names[r->d_dow + 1];
	dist_member (&r->d_holiday, "calendar", day_index, 8);
	if ((r->d_dow == 5) || (r->d_dow == 6))
		r->d_weekend = 1;
	else
		r->d_weekend = 0;
	if (day_index == 1)
		dist_member (&r->d_following_holiday, "calendar",
		365 + is_leap (r->d_year - 1), 8);
	else
		dist_member (&r->d_following_holiday, "calendar", day_index - 1, 8);
	date_t_op (&dTemp2, OP_FIRST_DOM, &temp_date, 0);
	r->d_first_dom = dTemp2.julian;
	date_t_op (&dTemp2, OP_LAST_DOM, &temp_date, 0);
	r->d_last_dom = dTemp2.julian;
	date_t_op (&dTemp2, OP_SAME_LY, &temp_date, 0);
	r->d_same_day_ly = dTemp2.julian;
	date_t_op (&dTemp2, OP_SAME_LQ, &temp_date, 0);
	r->d_same_day_lq = dTemp2.julian;
	r->d_current_day = (r->d_date_sk == CURRENT_DAY) ? 1 : 0;
	r->d_current_year = (r->d_year == CURRENT_YEAR) ? 1 : 0;
	if (r->d_current_year)
	{
        r->d_current_month = (r->d_moy == CURRENT_MONTH) ? 1 : 0;
        r->d_current_quarter = (r->d_qoy == CURRENT_QUARTER) ? 1 : 0;
        r->d_current_week = (r->d_week_seq == CURRENT_WEEK) ? 1 : 0;
	}
	
	return (res);
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
pr_w_date(void *row)
{
	char sQuarterName[7];
	struct W_DATE_TBL *r;

	if (row == NULL)
		r = &g_w_date;
	else
		r = row;

	print_start(DATE);
	print_key(D_DATE_SK, r->d_date_sk, 1);
	print_varchar(D_DATE_ID, r->d_date_id, 1);
	/* output the same information in a different format */
	print_date(D_DATE_SK, r->d_date_sk, 1);
	print_integer(D_MONTH_SEQ, r->d_month_seq, 1);
	print_integer(D_WEEK_SEQ, r->d_week_seq, 1);
	print_integer(D_QUARTER_SEQ, r->d_quarter_seq, 1);
	print_integer(D_YEAR, r->d_year, 1);
	print_integer(D_DOW, r->d_dow, 1);
	print_integer(D_MOY, r->d_moy, 1);
	print_integer(D_DOM, r->d_dom, 1);
	print_integer(D_QOY, r->d_qoy, 1);
	print_integer(D_FY_YEAR, r->d_fy_year, 1);
	print_integer(D_FY_QUARTER_SEQ, r->d_fy_quarter_seq, 1);
	print_integer(D_FY_WEEK_SEQ, r->d_fy_week_seq, 1);
	print_varchar(D_DAY_NAME, r->d_day_name, 1);
	sprintf(sQuarterName, "%4dQ%d", r->d_year, r->d_qoy);
	print_varchar(D_QUARTER_NAME, sQuarterName, 1);
	print_boolean(D_HOLIDAY, r->d_holiday, 1);
	print_boolean(D_WEEKEND, r->d_weekend, 1);
	print_boolean(D_FOLLOWING_HOLIDAY, r->d_following_holiday, 1);
	print_integer(D_FIRST_DOM, r->d_first_dom, 1);
	print_integer(D_LAST_DOM, r->d_last_dom, 1);
	print_integer(D_SAME_DAY_LY, r->d_same_day_ly, 1);
	print_integer(D_SAME_DAY_LQ, r->d_same_day_lq, 1);
	print_boolean(D_CURRENT_DAY, r->d_current_day, 1);
	print_boolean(D_CURRENT_WEEK, r->d_current_week, 1);
	print_boolean(D_CURRENT_MONTH, r->d_current_month, 1);
	print_boolean(D_CURRENT_QUARTER, r->d_current_quarter, 1);
	print_boolean(D_CURRENT_YEAR, r->d_current_year, 0);
	print_end(DATE);

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
ld_w_date(void *pSrc)
{
	struct W_DATE_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_date;
	else
		r = pSrc;
	
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
vld_w_date(int nTable, ds_key_t kRow, int *Permutation)
{
	int res = 0;
	
	/* begin locals declarations */
	static date_t base_date;
	int day_index,
		nTemp;
	date_t temp_date, dTemp2;
	struct W_DATE_TBL *r;
   tdef *pT = getSimpleTdefsByNumber(DATE);

		r = &g_w_date;

	    r->d_month_seq = 0;
        r->d_week_seq = 1;
        r->d_quarter_seq = 1;
        r->d_current_month = 0;
        r->d_current_quarter = 0;
        r->d_current_week = 0;
		strtodt(&base_date, "1900-01-01");
        /* Make exceptions to the 1-rng-call-per-row rule */
	
	nullSet(&pT->kNullBitMap, D_NULLS);
	nTemp = (long)kRow + base_date.julian;
	r->d_date_sk = nTemp;
	mk_bkey(&r->d_date_id[0], nTemp, D_DATE_ID);
	jtodt (&temp_date, nTemp);
	r->d_year = temp_date.year;
	r->d_dow = set_dow (&temp_date);
	r->d_moy = temp_date.month;
	r->d_dom = temp_date.day;
	/* set the sequence counts; assumes that the date table starts on a year boundary */
	r->d_week_seq = ((int)kRow + 6) / 7;
	r->d_month_seq = (r->d_year - 1900) * 12 + r->d_moy - 1;
	r->d_quarter_seq = (r->d_year - 1900) * 4 + r->d_moy / 3 + 1;
	day_index = day_number (&temp_date);
	dist_member (&r->d_qoy, "calendar", day_index, 6);
	/* fiscal year is identical to calendar year */
	r->d_fy_year = r->d_year;
	r->d_fy_quarter_seq = r->d_quarter_seq;
	r->d_fy_week_seq = r->d_week_seq;
	r->d_day_name = weekday_names[r->d_dow + 1];
	dist_member (&r->d_holiday, "calendar", day_index, 8);
	if ((r->d_dow == 5) || (r->d_dow == 6))
		r->d_weekend = 1;
	else
		r->d_weekend = 0;
	if (day_index == 1)
		dist_member (&r->d_following_holiday, "calendar",
		365 + is_leap (r->d_year - 1), 8);
	else
		dist_member (&r->d_following_holiday, "calendar", day_index - 1, 8);
	date_t_op (&dTemp2, OP_FIRST_DOM, &temp_date, 0);
	r->d_first_dom = dTemp2.julian;
	date_t_op (&dTemp2, OP_LAST_DOM, &temp_date, 0);
	r->d_last_dom = dTemp2.julian;
	date_t_op (&dTemp2, OP_SAME_LY, &temp_date, 0);
	r->d_same_day_ly = dTemp2.julian;
	date_t_op (&dTemp2, OP_SAME_LQ, &temp_date, 0);
	r->d_same_day_lq = dTemp2.julian;
	r->d_current_day = (r->d_date_sk == CURRENT_DAY) ? 1 : 0;
	r->d_current_year = (r->d_year == CURRENT_YEAR) ? 1 : 0;
	if (r->d_current_year)
	{
        r->d_current_month = (r->d_moy == CURRENT_MONTH) ? 1 : 0;
        r->d_current_quarter = (r->d_qoy == CURRENT_QUARTER) ? 1 : 0;
        r->d_current_week = (r->d_week_seq == CURRENT_WEEK) ? 1 : 0;
	}

	return (res);
}

