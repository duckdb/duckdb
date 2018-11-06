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
#include "w_income_band.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "nulls.h"
#include "tdefs.h"

struct W_INCOME_BAND_TBL g_w_income_band;

/*
* mk_income_band
*/
int
mk_w_income_band (void* row, ds_key_t index)
{
	int res = 0;
	struct W_INCOME_BAND_TBL *r;
	static int bInit = 0;
   tdef *pTdef = getSimpleTdefsByNumber(INCOME_BAND);

	if (row == NULL)
		r = &g_w_income_band;
	else
		r = row;

	if (!bInit)
	{
        /* Make exceptions to the 1-rng-call-per-row rule */
		bInit = 1;
	}
	
	nullSet(&pTdef->kNullBitMap, IB_NULLS);
	r->ib_income_band_id = (long)index;
	dist_member (&r->ib_lower_bound, "income_band", (long)index, 1);
	dist_member (&r->ib_upper_bound, "income_band", (long)index, 2);
	
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
pr_w_income_band(void *row)
{
	struct W_INCOME_BAND_TBL *r;

	if (row == NULL)
		r = &g_w_income_band;
	else
		r = row;	
	
	print_start(INCOME_BAND);
	print_integer(IB_INCOME_BAND_ID, r->ib_income_band_id, 1);
	print_integer(IB_LOWER_BOUND, r->ib_lower_bound, 1);
	print_integer(IB_UPPER_BOUND, r->ib_upper_bound, 0);
	print_end(INCOME_BAND);

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
ld_w_income_band(void *pSrc)
{
	struct W_INCOME_BAND_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_income_band;
	else
		r = pSrc;
	
	return(0);
}

