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
#include "s_web_page.h"
#include "w_web_page.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "scaling.h"
#include "parallel.h"
#include "permute.h"
#include "scd.h"
#include "tdef_functions.h"

extern struct W_WEB_PAGE_TBL g_w_web_page;


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
mk_s_web_page (void* row, ds_key_t index)
{
   static int bInit = 0;
   static int *pPermutation;
   ds_key_t kIndex;

   if (!bInit)
   {
      pPermutation = makePermutation(NULL, (int)getIDCount(WEB_PAGE), S_WPAG_PERMUTE);
      bInit = 1;
   }

   kIndex = getPermutationEntry(pPermutation, (int)index);
   mk_w_web_page(NULL, getSKFromID(kIndex, S_WPAG_ID));

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
pr_s_web_page(void *pSrc)
{
	struct W_WEB_PAGE_TBL *r;

	if (pSrc == NULL)
		r = &g_w_web_page;
	else
		r = pSrc;
	
	print_start(S_WEB_PAGE);
	print_varchar(WP_PAGE_ID, r->wp_page_id, 1);
	print_date(WP_CREATION_DATE_SK, r->wp_creation_date_sk, 1);
	print_date(WP_ACCESS_DATE_SK, r->wp_access_date_sk, 1);
	print_boolean(WP_AUTOGEN_FLAG, r->wp_autogen_flag, 1);
	print_varchar(WP_URL, &r->wp_url[0], 1);
	print_varchar(WP_TYPE, &r->wp_type[0], 1);
	print_integer(WP_CHAR_COUNT, r->wp_char_count, 1);
	print_integer(WP_LINK_COUNT, r->wp_link_count, 1);
	print_integer(WP_IMAGE_COUNT, r->wp_image_count, 1);
	print_integer(WP_MAX_AD_COUNT, r->wp_max_ad_count, 0);
	print_end(S_WEB_PAGE);
	
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
ld_s_web_page(void *pSrc)
{
	struct W_WEB_PAGE_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_web_page;
	else
		r = pSrc;
	
	return(0);
}

int
vld_s_web_page(int nTable, ds_key_t kRow, int *Permutation)
{
   static int bInit = 0;
   static int *pPermutation;
   table_func_t *pTF = getTdefFunctionsByNumber(WEB_PAGE);

   if (!bInit)
   {
      pPermutation = makePermutation(NULL, (int)getIDCount(WEB_PAGE),
      S_WPAG_PERMUTE);
      bInit = 1;
   }

   pTF->validate(S_WEB_PAGE, kRow, pPermutation);

   return(0);
   }
                                        
