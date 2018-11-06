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
#ifdef NCR
#include <sys/types.h>
#endif
#ifndef WIN32
#include <netinet/in.h>
#endif
#include "constants.h"
#include "address.h"
#include "w_web_site.h"
#include "genrand.h"
#include "tables.h"
#include "columns.h"
#include "print.h"
#include "scaling.h"
#include "build_support.h"
#include "misc.h"
#include "nulls.h"
#include "tdefs.h"
#include "scd.h"

struct W_WEB_SITE_TBL g_w_web_site;
static struct W_WEB_SITE_TBL g_OldValues;

/*
* Routine: mk_web_site()
* Purpose: populate the web_site table
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
*/
int
mk_w_web_site (void *row, ds_key_t index)
{
	int32_t res = 0,
		nFieldChangeFlags,
		bFirstRecord = 0;
	static date_t *dToday;
	static decimal_t dMinTaxPercentage,
		dMaxTaxPercentage;
	static int32_t bInit = 0;

	/* begin locals declarations */
	char szTemp[16],
		*sName1,
		*sName2;
	struct W_WEB_SITE_TBL *r,
				*rOldValues = &g_OldValues;
   tdef *pT = getSimpleTdefsByNumber(WEB_SITE);
	
	if (row == NULL)
		r = &g_w_web_site;
	else
		r = row;

	if (!bInit)
	{
		/* setup invariant values */
		sprintf(szTemp, "%d-%d-%d", CURRENT_YEAR, CURRENT_MONTH, CURRENT_DAY);
		dToday = strtodate(szTemp);
		strcpy(r->web_class, "Unknown");
		strtodec(&dMinTaxPercentage, WEB_MIN_TAX_PERCENTAGE);
		strtodec(&dMaxTaxPercentage, WEB_MAX_TAX_PERCENTAGE);

		bInit = 1;
	}

	nullSet(&pT->kNullBitMap, WEB_NULLS);
	r->web_site_sk = index;

	/* if we have generated the required history for this business key and generate a new one 
	 * then reset associate fields (e.g., rec_start_date minimums)
	 */
	if (setSCDKeys(WEB_SITE_ID, index, r->web_site_id, &r->web_rec_start_date_id, &r->web_rec_end_date_id))
	{
		r->web_open_date = mk_join(WEB_OPEN_DATE, DATE, index);
		r->web_close_date = mk_join(WEB_CLOSE_DATE, DATE, index);
	if (r->web_close_date > r->web_rec_end_date_id)
		r->web_close_date = -1;
		sprintf(r->web_name, "site_%d", (int)(index / 6));
		bFirstRecord = 1;
	}
	
 /*
  * this is  where we select the random number that controls if a field changes from 
  * one record to the next.
  */
	nFieldChangeFlags = next_random(WEB_SCD);


	/* the rest of the record in a history-keeping dimension can either be a new data value or not;
	 * use a random number and its bit pattern to determine which fields to replace and which to retain
	 */	
	pick_distribution (&sName1, "first_names", 1, 1, WEB_MANAGER);
	pick_distribution (&sName2, "last_names", 1, 1, WEB_MANAGER);
	sprintf (r->web_manager, "%s %s", sName1, sName2);
	changeSCD(SCD_CHAR, &r->web_manager, &rOldValues->web_manager,  &nFieldChangeFlags,  bFirstRecord);

	genrand_integer (&r->web_market_id, DIST_UNIFORM, 1, 6, 0, WEB_MARKET_ID);
	changeSCD(SCD_INT, &r->web_market_id, &rOldValues->web_market_id,  &nFieldChangeFlags,  bFirstRecord);

	gen_text (r->web_market_class, 20, RS_WEB_MARKET_CLASS,
		WEB_MARKET_CLASS);
	changeSCD(SCD_CHAR, &r->web_market_class, &rOldValues->web_market_class,  &nFieldChangeFlags,  bFirstRecord);

	gen_text (r->web_market_desc, 20, RS_WEB_MARKET_DESC,
		WEB_MARKET_DESC);
	changeSCD(SCD_CHAR, &r->web_market_desc, &rOldValues->web_market_desc,  &nFieldChangeFlags,  bFirstRecord);

	pick_distribution (&sName1, "first_names", 1, 1, WEB_MARKET_MANAGER);
	pick_distribution (&sName2, "last_names", 1, 1, WEB_MARKET_MANAGER);
	sprintf (r->web_market_manager, "%s %s", sName1, sName2);
	changeSCD(SCD_CHAR, &r->web_market_manager, &rOldValues->web_market_manager,  &nFieldChangeFlags,  bFirstRecord);

	genrand_integer (&r->web_company_id, DIST_UNIFORM, 1, 6, 0, 
		WEB_COMPANY_ID);
	changeSCD(SCD_INT, &r->web_company_id, &rOldValues->web_company_id,  &nFieldChangeFlags,  bFirstRecord);

	mk_word(r->web_company_name, "Syllables", r->web_company_id, RS_WEB_COMPANY_NAME, WEB_COMPANY_NAME);
	changeSCD(SCD_CHAR, &r->web_company_name, &rOldValues->web_company_name,  &nFieldChangeFlags,  bFirstRecord);

	mk_address(&r->web_address, WEB_ADDRESS);
	changeSCD(SCD_PTR, &r->web_address.city, &rOldValues->web_address.city,  &nFieldChangeFlags,  bFirstRecord);
	changeSCD(SCD_PTR, &r->web_address.county, &rOldValues->web_address.county,  &nFieldChangeFlags,  bFirstRecord);
	changeSCD(SCD_INT, &r->web_address.gmt_offset, &rOldValues->web_address.gmt_offset,  &nFieldChangeFlags,  bFirstRecord);
	changeSCD(SCD_PTR, &r->web_address.state, &rOldValues->web_address.state,  &nFieldChangeFlags,  bFirstRecord);
	changeSCD(SCD_PTR, &r->web_address.street_type, &rOldValues->web_address.street_type,  &nFieldChangeFlags,  bFirstRecord);
	changeSCD(SCD_PTR, &r->web_address.street_name1, &rOldValues->web_address.street_name1,  &nFieldChangeFlags,  bFirstRecord);
	changeSCD(SCD_PTR, &r->web_address.street_name2, &rOldValues->web_address.street_name2,  &nFieldChangeFlags,  bFirstRecord);
	changeSCD(SCD_INT, &r->web_address.street_num, &rOldValues->web_address.street_num,  &nFieldChangeFlags,  bFirstRecord);
	changeSCD(SCD_INT, &r->web_address.zip, &rOldValues->web_address.zip,  &nFieldChangeFlags,  bFirstRecord);

	genrand_decimal(&r->web_tax_percentage, DIST_UNIFORM, &dMinTaxPercentage, &dMaxTaxPercentage, NULL, WEB_TAX_PERCENTAGE);
	changeSCD(SCD_DEC, &r->web_tax_percentage, &rOldValues->web_tax_percentage,  &nFieldChangeFlags,  bFirstRecord);

	
	return(res);
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
pr_w_web_site(void *row)
{
	struct W_WEB_SITE_TBL *r;
	char szStreetName[128];

		
	if (row == NULL)
		r = &g_w_web_site;
	else
		r = row;

	print_start(WEB_SITE);
	print_key(WEB_SITE_SK, r->web_site_sk, 1);
	print_varchar(WEB_SITE_ID, &r->web_site_id[0], 1);
	print_date(WEB_REC_START_DATE_ID, (int)r->web_rec_start_date_id, 1);
	print_date(WEB_REC_END_DATE_ID, (int)r->web_rec_end_date_id, 1);
	print_varchar(WEB_NAME, &r->web_name[0], 1);
	print_key(WEB_OPEN_DATE, r->web_open_date, 1);
	print_key(WEB_CLOSE_DATE, r->web_close_date, 1);
	print_varchar(WEB_CLASS, &r->web_class[0], 1);
	print_varchar(WEB_MANAGER, &r->web_manager[0], 1);
	print_integer(WEB_MARKET_ID, r->web_market_id, 1);
	print_varchar(WEB_MARKET_CLASS, &r->web_market_class[0], 1);
	print_varchar(WEB_MARKET_DESC, &r->web_market_desc[0], 1);
	print_varchar(WEB_MARKET_MANAGER, &r->web_market_manager[0], 1);
	print_integer(WEB_COMPANY_ID, r->web_company_id, 1);
	print_varchar(WEB_COMPANY_NAME, &r->web_company_name[0], 1);
	print_integer(WEB_ADDRESS_STREET_NUM, r->web_address.street_num, 1);
	if (r->web_address.street_name2)
	{
		sprintf(szStreetName, "%s %s", r->web_address.street_name1, r->web_address.street_name2);
		print_varchar(WEB_ADDRESS_STREET_NAME1, szStreetName, 1);
	}
	else
		print_varchar(WEB_ADDRESS_STREET_NAME1, r->web_address.street_name1, 1);
	print_varchar(WEB_ADDRESS_STREET_TYPE, r->web_address.street_type, 1);
	print_varchar(WEB_ADDRESS_SUITE_NUM, r->web_address.suite_num, 1);
	print_varchar(WEB_ADDRESS_CITY, r->web_address.city, 1);
	print_varchar(WEB_ADDRESS_COUNTY, r->web_address.county, 1);
	print_varchar(WEB_ADDRESS_STATE, r->web_address.state, 1);
	sprintf(szStreetName, "%05d", r->web_address.zip);
	print_varchar(WEB_ADDRESS_ZIP, szStreetName, 1);
	print_varchar(WEB_ADDRESS_COUNTRY, r->web_address.country, 1);
	print_integer(WEB_ADDRESS_GMT_OFFSET, r->web_address.gmt_offset, 1);
	print_decimal(WEB_TAX_PERCENTAGE, &r->web_tax_percentage, 0);
	print_end(WEB_SITE);

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
ld_w_web_site(void *pSrc)
{
	struct W_WEB_SITE_TBL *r;
		
	if (pSrc == NULL)
		r = &g_w_web_site;
	else
		r = pSrc;
	
	return(0);
}

