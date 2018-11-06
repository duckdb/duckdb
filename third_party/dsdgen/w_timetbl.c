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
#include "w_timetbl.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "build_support.h"
#include "misc.h"
#include "print.h"
#include "tables.h"
#include "nulls.h"
#include "tdefs.h"

struct W_TIME_TBL g_w_time;

/*
* mk_time
*/
int
mk_w_time(void* row, ds_key_t index)
{
	int res = 0;
	
	/* begin locals declarations */
	int nTemp;
	struct W_TIME_TBL *r;
   tdef *pT = getSimpleTdefsByNumber(TIME);

	if (row == NULL)
		r = &g_w_time;
	else
		r = row;

	
	nullSet(&pT->kNullBitMap, T_NULLS);
	r->t_time_sk = index - 1;
	mk_bkey(&r->t_time_id[0], index, T_TIME_ID);
	r->t_time = (long)index - 1;
	nTemp = (long)index - 1;
	r->t_second = nTemp % 60;
	nTemp /= 60;
	r->t_minute = nTemp % 60;
	nTemp /= 60;
	r->t_hour = nTemp % 24;
	dist_member (&r->t_am_pm, "hours", r->t_hour + 1, 2);
	dist_member (&r->t_shift, "hours", r->t_hour + 1, 3);
	dist_member (&r->t_sub_shift, "hours", r->t_hour + 1, 4);
	dist_member (&r->t_meal_time, "hours", r->t_hour + 1, 5);
	
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
pr_w_time(void *row)
{
	struct W_TIME_TBL *r;

	if (row == NULL)
		r = &g_w_time;
	else
		r = row;

	print_start(TIME);
	print_key(T_TIME_SK, r->t_time_sk, 1);	
	print_varchar(T_TIME_ID, r->t_time_id, 1);	
	print_integer(T_TIME, r->t_time, 1);
	print_integer(T_HOUR, r->t_hour, 1);
	print_integer(T_MINUTE, r->t_minute, 1);
	print_integer(T_SECOND, r->t_second, 1);
	print_varchar(T_AM_PM, r->t_am_pm, 1);
	print_varchar(T_SHIFT, r->t_shift, 1);
	print_varchar(T_SUB_SHIFT, r->t_sub_shift, 1);
	print_varchar(T_MEAL_TIME, r->t_meal_time, 0);
	print_end(TIME);
	
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
ld_w_time(void *pSrc)
{
	struct W_TIME_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_time;
	else
		r = pSrc;
	
	return(0);
}

