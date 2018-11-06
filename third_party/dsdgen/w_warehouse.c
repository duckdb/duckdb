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
#include "w_warehouse.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "build_support.h"
#include "misc.h"
#include "print.h"
#include "address.h"
#include "constants.h"
#include "tables.h"
#include "nulls.h"
#include "tdefs.h"

struct W_WAREHOUSE_TBL g_w_warehouse;

/*
* mk_warehouse
*/
int
mk_w_warehouse (void* row, ds_key_t index)
{
	int res = 0;
	
	/* begin locals declarations */
	struct W_WAREHOUSE_TBL *r;
   tdef *pT = getSimpleTdefsByNumber(WAREHOUSE);

	if (row == NULL)
		r = &g_w_warehouse;
	else
		r = row;
	
	
	nullSet(&pT->kNullBitMap, W_NULLS);
	r->w_warehouse_sk = index;
	mk_bkey(&r->w_warehouse_id[0], index, W_WAREHOUSE_ID);
	gen_text (&r->w_warehouse_name[0], W_NAME_MIN,
		RS_W_WAREHOUSE_NAME, W_WAREHOUSE_NAME);
	r->w_warehouse_sq_ft =
		genrand_integer (NULL, DIST_UNIFORM,
		W_SQFT_MIN, W_SQFT_MAX, 0, W_WAREHOUSE_SQ_FT);

	mk_address(&r->w_address, W_WAREHOUSE_ADDRESS);
	
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
pr_w_warehouse(void *row)
{
	struct W_WAREHOUSE_TBL *r;
	char szTemp[128];

	if (row == NULL)
		r = &g_w_warehouse;
	else
		r = row;

	print_start(WAREHOUSE);
	print_key(W_WAREHOUSE_SK, r->w_warehouse_sk, 1);
	print_varchar(W_WAREHOUSE_ID, r->w_warehouse_id, 1);
	print_varchar(W_WAREHOUSE_NAME, &r->w_warehouse_name[0], 1);
	print_integer(W_WAREHOUSE_SQ_FT, r->w_warehouse_sq_ft, 1);
	print_integer(W_ADDRESS_STREET_NUM, r->w_address.street_num, 1);
	if (r->w_address.street_name2 != NULL)
	{
		sprintf(szTemp, "%s %s", r->w_address.street_name1, r->w_address.street_name2);
		print_varchar(W_ADDRESS_STREET_NAME1, szTemp, 1);
	}
	else
		print_varchar(W_ADDRESS_STREET_NAME1, r->w_address.street_name1, 1);
	print_varchar(W_ADDRESS_STREET_TYPE, r->w_address.street_type, 1);
	print_varchar(W_ADDRESS_SUITE_NUM, r->w_address.suite_num, 1);
	print_varchar(W_ADDRESS_CITY, r->w_address.city, 1);
	print_varchar(W_ADDRESS_COUNTY, r->w_address.county, 1);
	print_varchar(W_ADDRESS_STATE, r->w_address.state, 1);
	sprintf(szTemp, "%05d", r->w_address.zip);
	print_varchar(W_ADDRESS_ZIP, szTemp, 1);
	print_varchar(W_ADDRESS_COUNTRY, r->w_address.country, 1);
	print_integer(W_ADDRESS_GMT_OFFSET, r->w_address.gmt_offset, 0);
	print_end(WAREHOUSE);

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
ld_w_warehouse(void *pSrc)
{
	struct W_WAREHOUSE_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_warehouse;
	else
		r = pSrc;
	
	return(0);
}

