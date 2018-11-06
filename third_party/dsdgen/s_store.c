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
#include "s_store.h"
#include "w_store.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "w_store.h"
#include "parallel.h"
#include "permute.h"
#include "scaling.h"
#include "scd.h"

extern struct W_STORE_TBL g_w_store;

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
mk_s_store (void* row, ds_key_t index)
{
   static int bInit = 0;
   static int *pPermutation;
   ds_key_t kIndex;

   if (!bInit)
   {
      pPermutation = makePermutation(NULL, (int)getIDCount(STORE), S_STORE_ID);
      bInit = 1;
   }

   kIndex = getPermutationEntry(pPermutation, (int)index);
   mk_w_store(NULL,getSKFromID(kIndex, S_STORE_ID));
   if (!g_w_store.closed_date_id)
      g_w_store.closed_date_id = -1; /* dates use a special NULL indicator */

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
pr_s_store(void *pSrc)
{
	struct W_STORE_TBL *r;
   
   if (pSrc == NULL)
		r = &g_w_store;
	else
		r = pSrc;
	
	print_start(S_STORE);
	print_varchar(W_STORE_ID, r->store_id, 1);
	print_date(W_STORE_CLOSED_DATE_ID, r->closed_date_id, 1);
	print_varchar(W_STORE_NAME, r->store_name, 1);
	print_integer(W_STORE_EMPLOYEES, r->employees, 1);
	print_integer(W_STORE_FLOOR_SPACE, r->floor_space, 1);
	print_varchar(W_STORE_HOURS, r->hours, 1);
	print_varchar(W_STORE_MANAGER, &r->store_manager[0], 1);
	print_integer(W_STORE_MARKET_ID, r->market_id, 1);
	print_varchar(W_STORE_GEOGRAPHY_CLASS, r->geography_class, 1);
	print_varchar(W_STORE_MARKET_MANAGER, &r->market_manager[0], 1);
   print_decimal(W_STORE_TAX_PERCENTAGE,&r->dTaxPercentage, 0);
	print_end(S_STORE);
	
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
ld_s_store(void *pSrc)
{
	struct W_STORE_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_store;
	else
		r = pSrc;
	
	return(0);
}

