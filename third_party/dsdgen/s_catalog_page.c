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
#include "s_catalog_page.h"
#include "w_catalog_page.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "scaling.h"
#include "tdef_functions.h"
#include "validate.h"
#include "parallel.h"

struct CATALOG_PAGE_TBL g_w_catalog_page;

int
mk_s_catalog_page(void *pDest, ds_key_t kRow)
{
   mk_w_catalog_page(pDest, kRow);
   row_stop(CATALOG_PAGE);

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
pr_s_catalog_page(void *pSrc)
{
	struct CATALOG_PAGE_TBL *r;
	
	if (pSrc == NULL)
		r = &g_w_catalog_page;
	else
		r = pSrc;
	
	print_start(S_CATALOG_PAGE);
	print_integer(S_CATALOG_PAGE_CATALOG_NUMBER, r->cp_catalog_number, 1);
	print_integer(S_CATALOG_PAGE_NUMBER, r->cp_catalog_page_number, 1);
	print_varchar(S_CATALOG_PAGE_DEPARTMENT, &r->cp_department[0], 1);
	print_varchar(S_CP_ID, &r->cp_catalog_page_id[0], 1);
   print_date(S_CP_START_DATE, r->cp_start_date_id, 1);
   print_date(S_CP_END_DATE, r->cp_end_date_id, 1);
   print_varchar(S_CP_DESCRIPTION, r->cp_description, 1);
   print_varchar(S_CP_TYPE, r->cp_type, 0);
	print_end(S_CATALOG_PAGE);
	
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
ld_s_catalog_page(void *pSrc)
{
	struct CATALOG_PAGE_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_catalog_page;
	else
		r = pSrc;
	
	return(0);
}

int 
vld_s_catalog_page(int nTable, ds_key_t kRow, int *Permutation)
{
   return(validateGeneric(S_CATALOG_PAGE, kRow, NULL));
}
