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
#include "s_catalog.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "misc.h"
#include "tables.h"

struct S_CATALOG_TBL g_s_catalog;

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
mk_s_catalog(void *pDest, ds_key_t kIndex)
{
	struct S_CATALOG_TBL *r;
	static int bInit = 0;
	int nDateDelta;
	static date_t dtStartMin,
		dtStartMax;
	
	if (pDest == NULL)
		r = &g_s_catalog;
	else
		r = pDest;
	
	if (!bInit)
	{
		memset(&g_s_catalog, 0, sizeof(struct S_CATALOG_TBL));
		strtodt(&dtStartMin, "1999-01-01");
		strtodt(&dtStartMax, "2002-12-31");
		bInit = 1;
	}

	r->s_catalog_number = kIndex;
	genrand_date(&r->s_catalog_start_date, DIST_UNIFORM, &dtStartMin, &dtStartMax, NULL, S_CATALOG_START_DATE);
	genrand_integer(&nDateDelta, DIST_UNIFORM, S_CATALOG_DURATION_MIN, S_CATALOG_DURATION_MAX, 0, S_CATALOG_END_DATE);
	jtodt(&r->s_catalog_end_date, r->s_catalog_start_date.julian + nDateDelta);
	gen_text(r->s_catalog_catalog_desc, S_CATALOG_DESC_MIN, S_CATALOG_DESC_MAX, S_CATALOG_DESC);
	genrand_integer(&r->s_catalog_catalog_type, DIST_UNIFORM, 1, S_CATALOG_TYPE_COUNT, 0, S_CATALOG_TYPE);

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
pr_s_catalog(void *pSrc)
{
	struct S_CATALOG_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_catalog;
	else
		r = pSrc;

	print_start(S_CATALOG);
	print_key(S_CATALOG_NUMBER, r->s_catalog_number, 1);
	print_date(S_CATALOG_START_DATE, r->s_catalog_start_date.julian, 1);
	print_date(S_CATALOG_END_DATE, r->s_catalog_end_date.julian, 1);
	print_varchar(S_CATALOG_DESC, r->s_catalog_catalog_desc, 1);
	print_integer(S_CATALOG_TYPE, r->s_catalog_catalog_type, 0);
	print_end(S_CATALOG);

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
ld_s_catalog(void *pSrc)
{
	struct S_CATALOG_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_catalog;
	else
		r = pSrc;

	return(0);
}

