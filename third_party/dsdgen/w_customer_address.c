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
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "columns.h"
#include "build_support.h"
#include "print.h"
#include "tables.h"
#include "nulls.h"
#include "tdefs.h"

struct W_CUSTOMER_ADDRESS_TBL g_w_customer_address;

/*
* mk_customer_address
*/
int
mk_w_customer_address (void* row, ds_key_t index)
{
	
	int res = 0;
	
	/* begin locals declarations */
	struct W_CUSTOMER_ADDRESS_TBL *r;
   tdef *pTdef = getSimpleTdefsByNumber(CUSTOMER_ADDRESS);

	if (row == NULL)
		r = &g_w_customer_address;
	else
		r = row;
	
	nullSet(&pTdef->kNullBitMap, CA_NULLS);
	r->ca_addr_sk = index;
	mk_bkey(&r->ca_addr_id[0], index, CA_ADDRESS_ID);
	pick_distribution (&r->ca_location_type, "location_type", 1, 1, CA_LOCATION_TYPE);
	mk_address(&r->ca_address, CA_ADDRESS);
	
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
pr_w_customer_address(void *row)
{
	struct W_CUSTOMER_ADDRESS_TBL *r;
	char szTemp[128];

	if (row == NULL)
		r = &g_w_customer_address;
	else
		r = row;

	print_start(CUSTOMER_ADDRESS);
	print_key(CA_ADDRESS_SK, r->ca_addr_sk, 1);
	print_varchar(CA_ADDRESS_ID, r->ca_addr_id, 1);
	print_integer(CA_ADDRESS_STREET_NUM, r->ca_address.street_num, 1);
	if (r->ca_address.street_name2)
	{
		sprintf(szTemp, "%s %s", r->ca_address.street_name1, r->ca_address.street_name2);
		print_varchar(CA_ADDRESS_STREET_NAME1, szTemp, 1);
	}
	else
		print_varchar(CA_ADDRESS_STREET_NAME1, r->ca_address.street_name1, 1);
	print_varchar(CA_ADDRESS_STREET_TYPE, r->ca_address.street_type, 1);
	print_varchar(CA_ADDRESS_SUITE_NUM, &r->ca_address.suite_num[0], 1);
	print_varchar(CA_ADDRESS_CITY, r->ca_address.city, 1);
	print_varchar(CA_ADDRESS_COUNTY, r->ca_address.county, 1);
	print_varchar(CA_ADDRESS_STATE, r->ca_address.state, 1);
	sprintf(szTemp, "%05d", r->ca_address.zip);
	print_varchar(CA_ADDRESS_ZIP, szTemp, 1);
	print_varchar(CA_ADDRESS_COUNTRY, &r->ca_address.country[0], 1);
	print_integer(CA_ADDRESS_GMT_OFFSET, r->ca_address.gmt_offset, 1);
	print_varchar(CA_LOCATION_TYPE, r->ca_location_type, 0);
	print_end(CUSTOMER_ADDRESS);

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
ld_w_customer_address(void *row)
{
	struct W_CUSTOMER_ADDRESS_TBL *r;

	if (row == NULL)
		r = &g_w_customer_address;
	else
		r = row;

	return(0);
}

