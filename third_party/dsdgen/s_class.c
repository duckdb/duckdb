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
#include "s_class.h"
#include "print.h"
#include "build_support.h"
#include "columns.h"
#include "tables.h"
#include "misc.h"
#include "parallel.h"

struct S_CLASS_TBL g_s_class;

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
* TODO: 
* 20031022 jms RNGUsage for descrition is an estimate
*/
int
mk_s_class(void *pDest, ds_key_t kIndex)
{
	struct S_CLASS_TBL *r;
	static int bInit = 0;
	
	if (pDest == NULL)
		r = &g_s_class;
	else
		r = pDest;

	if (!bInit)
	{
		memset(&g_s_class, 0, sizeof(struct S_CLASS_TBL));
		bInit = 1;
	}
	
	r->id = kIndex;
	r->subcat_id = mk_join(S_CLASS_SUBCAT_ID, S_SUBCATEGORY, 1);
	gen_text(r->desc, S_CLASS_DESC_MIN, RS_CLASS_DESC, S_CLASS_DESC);
	row_stop(S_CLASS);

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
pr_s_class(void *pSrc)
{
	struct S_CLASS_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_class;
	else
		r = pSrc;
	
	print_start(S_CLASS);
	print_key(S_CLASS_ID, r->id, 1);
	print_key(S_CLASS_SUBCAT_ID, r->subcat_id, 1);
	print_varchar(S_CLASS_DESC, r->desc, 0);
	print_end(S_CLASS);
	
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
ld_s_class(void *pSrc)
{
	struct S_CLASS_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_class;
	else
		r = pSrc;
	
	return(0);
}

