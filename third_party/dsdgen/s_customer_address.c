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
#include "w_customer_address.h"
#include "s_customer_address.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "columns.h"
#include "build_support.h"
#include "print.h"
#include "tables.h"
#include "nulls.h"
#include "tdefs.h"
#include "tdef_functions.h"

extern struct W_CUSTOMER_ADDRESS_TBL g_w_customer_address;

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
pr_s_customer_address(void *row)
{
	struct W_CUSTOMER_ADDRESS_TBL *r;
   char szTemp[11];

   if (row == NULL)
		r = &g_w_customer_address;
	else
		r = row;

	print_start(S_CUSTOMER_ADDRESS);
	print_varchar(S_CADR_ID, r->ca_addr_id, 1);
	print_integer(S_CADR_ADDRESS_STREET_NUMBER, r->ca_address.street_num, 1);
	print_varchar(S_CADR_ADDRESS_STREET_NAME1, r->ca_address.street_name1, 1);
	print_varchar(S_CADR_ADDRESS_STREET_NAME2, r->ca_address.street_name2, 1);
	print_varchar(S_CADR_ADDRESS_STREET_TYPE, r->ca_address.street_type, 1);
	print_varchar(S_CADR_ADDRESS_SUITE_NUM, &r->ca_address.suite_num[0], 1);
	print_varchar(S_CADR_ADDRESS_CITY, r->ca_address.city, 1);
	print_varchar(S_CADR_ADDRESS_COUNTY, r->ca_address.county, 1);
	print_varchar(S_CADR_ADDRESS_STATE, r->ca_address.state, 1);
	sprintf(szTemp, "%05d", r->ca_address.zip);
	print_varchar(S_CADR_ADDRESS_ZIP, szTemp, 1);
	print_varchar(S_CADR_ADDRESS_COUNTRY, &r->ca_address.country[0], 0);
	print_end(S_CUSTOMER_ADDRESS);

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
ld_s_customer_address(void *row)
{
	struct W_CUSTOMER_ADDRESS_TBL *r;

	if (row == NULL)
		r = &g_w_customer_address;
	else
		r = row;

	return(0);
}

int
vld_s_customer_address(int nTable, ds_key_t kRow, int *Permutation)
{
   return(validateGeneric(S_CUSTOMER_ADDRESS, kRow, Permutation));
}
