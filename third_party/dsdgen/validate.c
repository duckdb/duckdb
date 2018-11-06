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
#include "tdefs.h"
#include "tdef_functions.h"
#include "r_params.h"
#include "parallel.h"
#include "constants.h"
#include "scd.h"
#include "permute.h"
#include "print.h"

/* extern tdef w_tdefs[]; */

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
validateGeneric(int nTable, ds_key_t kRow, int *Permute)
{
   tdef *pT = getSimpleTdefsByNumber(nTable);
   tdef *pChild;
   table_func_t *pTF = getTdefFunctionsByNumber(nTable);

	if (is_set("UPDATE") )
   {
      /*
      if (nTable < S_BRAND)
         nTable += S_BRAND;

         */
      if (!(pT->flags & FL_PASSTHRU)) 
         row_skip(nTable, kRow - 1);
      else
      {
         switch(nTable)
         {
         case S_CUSTOMER_ADDRESS: nTable = CUSTOMER_ADDRESS; break;
         case S_CATALOG_PAGE: nTable = CATALOG_PAGE; break;
         }
         pT = getSimpleTdefsByNumber(nTable);
         pTF = getTdefFunctionsByNumber(nTable);
         row_skip(nTable, kRow - 1);
      }
   }
   else
         row_skip(nTable, kRow - 1);
	if (pT->flags & FL_PARENT)
   {
      pChild = getSimpleTdefsByNumber(pT->nParam);
		row_skip(pT->nParam, kRow - 1);
   }
   pTF->builder(NULL, kRow);

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
validateSCD(int nTable, ds_key_t kRow, int *Permutation)
{
	ds_key_t kStart, 
      kEnd;
	char szID[RS_BKEY + 1];
	int nColumn, 
      nOffset = 0,
      nSKColumn,
      nID,
      nSkipTable = 0; /* some source schema tales are generated using the warehouse routine */
   table_func_t *pTF = getTdefFunctionsByNumber(nTable);
   table_func_t *pTS;

   nSkipTable = 0;
   switch(nTable + nOffset)
	{
	case CALL_CENTER: nColumn = CC_CALL_CENTER_ID; break;
	case ITEM: nColumn = I_ITEM_ID; break;
	case S_ITEM: nColumn = I_ITEM_ID; nSkipTable = ITEM; nSKColumn = S_ITEM_ID;break;
	case STORE: nColumn = W_STORE_ID; break;
	case WEB_PAGE: nColumn = WP_PAGE_ID; break;
	case WEB_SITE: nColumn = WEB_SITE_ID; break;
	case S_WEB_PAGE: nColumn = WP_PAGE_SK; nSkipTable = WEB_PAGE; nSKColumn = S_WPAG_ID;break;
	default:
		fprintf(stderr, "ERROR: Invalid table %d at %s:%d!\n", nTable, __FILE__, __LINE__);
		exit(-1);
	}

      row_skip(nTable, kRow - 1);
      if (nSkipTable)
      {
         nID = getPermutationEntry(Permutation, kRow); 
         nID = getSKFromID( nID, nSKColumn); 
         pTS = getTdefFunctionsByNumber(nSkipTable);
      }
      else
         nID = kRow;

      /* back up to the base row for SCD's */
      if ((!setSCDKeys(nColumn, nID, &szID[0], &kStart, &kEnd)) && (kRow > 1))
         validateSCD(nTable, kRow - 1, Permutation);

	/* set up to start building rows */
	row_skip((nSkipTable)?nSkipTable:nTable, kRow - 1);

	/* and output the target */
   if (nSkipTable)
      pTS->builder(NULL, nID);
   else
      pTF->builder(NULL, kRow);


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
void
printValidation(int nTable, ds_key_t kRow)
{
   table_func_t *pTdef = getTdefFunctionsByNumber(nTable);

   print_start(nTable);

	print_key(0, kRow, 1);
	if (pTdef->loader[is_set("DBLOAD")](NULL))
	{
		fprintf(stderr, "ERROR: Load failed on %s!\n", pTdef->name);
		exit(-1);
	}

   return;
}




