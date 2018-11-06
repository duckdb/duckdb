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
#include <assert.h>
#include <stdio.h>
#include "s_call_center.h"
#include "genrand.h"
#include "r_params.h"
#include "scaling.h"
#include "constants.h"
#include "date.h"
#include "tables.h"
#include "dist.h"
#include "build_support.h"
#include "columns.h"
#include "print.h"
#include "w_call_center.h"
#include "decimal.h"
#include "permute.h"
#include "scd.h"

extern struct CALL_CENTER_TBL g_w_call_center;

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
mk_s_call_center (void* row, ds_key_t index)
{
   static int bInit = 0;
   static int *pPermutation;
   ds_key_t kIndex;

   if (!bInit)
   {
      pPermutation = makePermutation(NULL, (int)getIDCount(CALL_CENTER), S_CALL_CENTER_ID);
      bInit = 1;
   }

   kIndex = getPermutationEntry(pPermutation, (int)index);
   mk_w_call_center(NULL, getSKFromID(kIndex, S_CALL_CENTER_ID));

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
pr_s_call_center(void *row)
{
	struct CALL_CENTER_TBL *r;
   
   if (row == NULL)
		r = &g_w_call_center;
	else
		r = row;

	print_start(S_CALL_CENTER);
	print_id(CC_CALL_CENTER_SK, r->cc_call_center_sk, 1);
	print_date(CC_OPEN_DATE_ID, r->cc_open_date_id, 1);
	print_date(CC_CLOSED_DATE_ID, r->cc_closed_date_id, 1);
	print_varchar(CC_NAME, r->cc_name, 1);
	print_varchar(CC_CLASS, &r->cc_class[0], 1);
	print_integer(CC_EMPLOYEES, r->cc_employees, 1);
	print_integer(CC_SQ_FT, r->cc_sq_ft, 1);
	print_varchar(CC_HOURS, r->cc_hours, 1);
	print_varchar(CC_MANAGER, &r->cc_manager[0], 1);
	print_decimal(CC_TAX_PERCENTAGE, &r->cc_tax_percentage, 0);
	print_end(S_CALL_CENTER);

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
ld_s_call_center(void *r)
{
	return(0);
}

