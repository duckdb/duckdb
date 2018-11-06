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
#include "w_catalog_page.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "build_support.h"
#include "misc.h"
#include "print.h"
#include "tables.h"
#include "scaling.h"
#include "nulls.h"
#include "tdefs.h"

struct CATALOG_PAGE_TBL g_w_catalog_page;

/*
* Routine: mk_catalog_page()
* Purpose: populate the catalog_page table
* Algorithm:
*	catalogs are issued either monthly, quarterly or bi-annually (cp_type)
*	there is 1 of each type circulating at all times
* Data tdefsures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: 
* 20020903 jms cp_department needs to be randomized
* 20020903 jms cp_description needs to be randomized
*/
int
mk_w_catalog_page (void *row, ds_key_t index)
{
	int res = 0;
	static date_t *dStartDate;
	static int nCatalogPageMax;
	int nDuration, 
		nOffset,
		nType;
	static int bInit = 0;
	struct CATALOG_PAGE_TBL *r;
	int nCatalogInterval;
   tdef *pTdef = getSimpleTdefsByNumber(CATALOG_PAGE);

	if (row == NULL)
		r = &g_w_catalog_page;
	else
		r = row;
	
	if (!bInit)
	{
		nCatalogPageMax = ((int)get_rowcount(CATALOG_PAGE) / CP_CATALOGS_PER_YEAR) / (YEAR_MAXIMUM - YEAR_MINIMUM + 2); 
		dStartDate = strtodate(DATA_START_DATE);

		/* columns that still need to be populated */
        strcpy (r->cp_department, "DEPARTMENT");

		bInit = 1;
	}
	
	nullSet(&pTdef->kNullBitMap, CP_NULLS);
	r->cp_catalog_page_sk = index;
	mk_bkey(&r->cp_catalog_page_id[0], index, CP_CATALOG_PAGE_ID);
	r->cp_catalog_number = (long)(index - 1) / nCatalogPageMax + 1;
	r->cp_catalog_page_number = (long)(index - 1) % nCatalogPageMax + 1;
	switch(nCatalogInterval = ((r->cp_catalog_number - 1)%CP_CATALOGS_PER_YEAR))
	{
	case 0:			/* bi-annual */
	case 1:
		nType = 1;
		nDuration = 182;
		nOffset = nCatalogInterval * nDuration;
		break;
	case 2:
	case 3:			/* Q2 */
	case 4:			/* Q3 */
	case 5:			/* Q4 */
		nDuration = 91;
		nOffset = (nCatalogInterval- 2) * nDuration;
		nType = 2;
		break;
	default:
		nDuration = 30;
		nOffset = (nCatalogInterval - 6) * nDuration;
		nType = 3;	/* monthly */
	}
	r->cp_start_date_id = dStartDate->julian + nOffset;
   r->cp_start_date_id += ((r->cp_catalog_number - 1) / CP_CATALOGS_PER_YEAR) * 365;
	r->cp_end_date_id = r->cp_start_date_id + nDuration - 1;
	dist_member(&r->cp_type, "catalog_page_type", nType, 1);
	gen_text(&r->cp_description[0], RS_CP_DESCRIPTION / 2, RS_CP_DESCRIPTION - 1, CP_DESCRIPTION);

	return (res);
}

/*
* Routine: 
* Purpose: 
* Algorithm:
* Data tdefsures:
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
pr_w_catalog_page(void *row)
{
	struct CATALOG_PAGE_TBL *r;

	if (row == NULL)
		r = &g_w_catalog_page;
	else
		r = row;

	print_start(CATALOG_PAGE);
	print_key(CP_CATALOG_PAGE_SK, r->cp_catalog_page_sk, 1);
	print_varchar(CP_CATALOG_PAGE_ID, r->cp_catalog_page_id, 1);
	print_key(CP_START_DATE_ID, r->cp_start_date_id, 1);
	print_key(CP_END_DATE_ID, r->cp_end_date_id, 1);
	print_varchar(CP_DEPARTMENT, &r->cp_department[0], 1);
	print_integer(CP_CATALOG_NUMBER, r->cp_catalog_number, 1);
	print_integer(CP_CATALOG_PAGE_NUMBER, r->cp_catalog_page_number, 1);
	print_varchar(CP_DESCRIPTION, &r->cp_description[0], 1);
	print_varchar(CP_TYPE, r->cp_type, 0);
	print_end(CATALOG_PAGE);

	return(0);
}

/*
* Routine: 
* Purpose: 
* Algorithm:
* Data tdefsures:
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
ld_w_catalog_page(void *r)
{
	return(0);
}

