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
#include "s_zip_to_gmt.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "tdef_functions.h"
#include "r_params.h"
#include "parallel.h"
#include "tdefs.h"

struct S_ZIP_GMT_TBL g_s_zip;

struct ZIP_MAP  {
	int nMin;
	int nMax;
	int	nOffset;
} arGMTOffset[63] = 
{
{0,599, -99}, /* List of unused values  */
{600,999, -5}, /* List of ZIP Codes in Puerto Rico and U.S. Virgin Islands  */
{1000,2799, -5}, /* List of ZIP Codes in Massachusetts  */
{2800,2999, -5}, /* List of ZIP Codes in Rhode Island  */
{3000,3899, -5}, /* List of ZIP Codes in New Hampshire  */
{3900,4999, -5}, /* List of ZIP Codes in Maine  */
{5000,5999, -5}, /* List of ZIP Codes in Vermont  */
{6000,6999, -5}, /* List of ZIP Codes in Connecticut  */
{7000,8999, -5}, /* List of ZIP Codes in New Jersey  */
{9000,9999, -5}, /* List of ZIP Codes in the military  */
{10000, 14999, -5}, /* List of ZIP Codes in New York  */
{15000, 19699, -5}, /* List of ZIP Codes in Pennsylvania  */
{19700, 19999, -5}, /* List of ZIP Codes in Delaware  */
{20000, 20599, -5}, /* List of ZIP Codes in District of Columbia  */
{20600, 21999, -5}, /* List of ZIP Codes in Maryland  */
{22000, 24699, -5}, /* List of ZIP Codes in Virginia  */
{24700, 26999, -5}, /* List of ZIP Codes in West Virginia  */
{27000, 28999, -5}, /* List of ZIP Codes in North Carolina  */
{29000, 29999, -5}, /* List of ZIP Codes in South Carolina  */
{30000, 31999, -5}, /* List of ZIP Codes in Georgia  */
{32000, 33999, -5}, /* List of ZIP Codes in Florida  */
{34000, 34999, -6}, /* List of ZIP Codes in Florida  */
{34090, 34095, -5}, /* List of ZIP Codes in the military  */
{35000, 36999, -6}, /* List of ZIP Codes in Alabama  */
{37000, 38599, -5}, /* List of ZIP Codes in Tennessee  */
{38600, 39999, -6}, /* List of ZIP Codes in Mississippi  */
{40000, 41799, -5}, /* List of ZIP Codes in Kentucky  */
{41800, 42799, -6}, /* List of ZIP Codes in Kentucky  */
{43000, 45999, -5}, /* List of ZIP Codes in Ohio  */
{46000, 47999, -5}, /* List of ZIP Codes in Indiana  */
{48000, 49999, -5}, /* List of ZIP Codes in Michigan  */
{50000, 52999, -6}, /* List of ZIP Codes in Iowa  */
{53000, 54999, -6}, /* List of ZIP Codes in Wisconsin  */
{55000, 56999, -6}, /* List of ZIP Codes in Minnesota  */
{57000, 57499, -6}, /* List of ZIP Codes in South Dakota  */
{57500, 57999, -7}, /* List of ZIP Codes in South Dakota  */
{58000, 58499, -6}, /* List of ZIP Codes in North Dakota  */
{58500, 58499, -7}, /* List of ZIP Codes in North Dakota  */
{59000, 59999, -7}, /* List of ZIP Codes in Montana  */
{60000, 62999, -6}, /* List of ZIP Codes in Illinois  */
{63000, 65999, -6}, /* List of ZIP Codes in Missouri  */
{66000, 67999, -6}, /* List of ZIP Codes in Kansas  */
{68000, 68999, -6}, /* List of ZIP Codes in Nebraska  */
{69000, 69999, -7}, /* List of ZIP Codes in Nebraska  */
{70000, 71599, -6}, /* List of ZIP Codes in Louisiana  */
{71600, 72999, -6}, /* List of ZIP Codes in Arkansas  */
{73000, 74999, -6}, /* List of ZIP Codes in Oklahoma  */
{75000, 78999, -6}, /* List of ZIP Codes in Texas  */
{79000, 79999, -7}, /* List of ZIP Codes in Texas  */
{80000, 81999, -7}, /* List of ZIP Codes in Colorado  */
{82000, 83199, -7}, /* List of ZIP Codes in Wyoming  */
{83200, 83699, -7}, /* List of ZIP Codes in Idaho  */
{83700, 83999, -8}, /* List of ZIP Codes in Idaho  */
{84000, 84999, -7}, /* List of ZIP Codes in Utah  */
{85000, 86999, -7}, /* List of ZIP Codes in Arizona  */
{87000, 88999, -7}, /* List of ZIP Codes in New Mexico  */
{89000, 89999, -8}, /* List of ZIP Codes in Nevada  */
{90000, 95999, -8}, /* List of ZIP Codes in California  */
{96000, 96699, -8}, /* List of ZIP Codes in the military  */
{96700, 96899, -10}, /* List of ZIP Codes in Hawaii  */
{97000, 97999, -8}, /* List of ZIP Codes in Oregon  */
{98000, 99499, -8}, /* List of ZIP Codes in Washington  */
{99500, 99999, -9}, /* List of ZIP Codes in Alaska  */
};

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
mk_s_zip(void *pDest, ds_key_t kIndex)
{
	struct S_ZIP_GMT_TBL *r;
	static struct ZIP_MAP *pMap;
	
	if (pDest == NULL)
		r = &g_s_zip;
	else
		r = pDest;
	
	kIndex -= 1;	/* zip codes are 0-based */
	pMap = &arGMTOffset[0];
	while (kIndex > pMap->nMax)
		pMap += 1;
	sprintf(r->szZip, "%05lld", kIndex);
	r->nGMTOffset = pMap->nOffset;
	
	return(r->nGMTOffset == -99?1:0);
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
pr_s_zip(void *pSrc)
{
	struct S_ZIP_GMT_TBL *r;
	
	if (pSrc == NULL)
		r = &g_s_zip;
	else
		r = pSrc;
	
	print_start(S_ZIPG);
	print_varchar(S_ZIPG_ZIP, r->szZip, 1);
	print_integer(S_ZIPG_GMT, r->nGMTOffset, 0);
	print_end(S_ZIPG);
	
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
ld_s_zip(void *pSrc)
{
	struct S_ZIP_GMT_TBL *r;
		
	if (pSrc == NULL)
		r = &g_s_zip;
	else
		r = pSrc;
	
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
vld_s_zip(int nTable, ds_key_t kRow, int *Permutation)
{
   table_func_t *pTF = getTdefFunctionsByNumber(nTable);

   row_skip(nTable + S_BRAND, kRow - 1);
	if (!pTF->builder(NULL, kRow))
	{
      /* there is a 600 offset for the first row */
      print_key(0, kRow - 600, 1);
      if (pTF->loader[is_set("DBLOAD")](NULL))
      {
         fprintf(stderr, "ERROR: Load failed on %s!\n", getTableNameByID(nTable));
         exit(-1);
      }
      row_stop(nTable);
	}

	return(0);
}






