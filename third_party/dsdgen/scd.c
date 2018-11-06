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
#include "tdefs.h"
#include "scd.h"
#include "tables.h"
#include "build_support.h"
#include "dist.h"
#include "scaling.h"
#include "genrand.h"
#include "constants.h"
#include "parallel.h"
#include "params.h"
#include "tdef_functions.h"
#include "print.h"
#include "permute.h"

/* an array of the most recent business key for each table */
char arBKeys[MAX_TABLE][17];

/*
* Routine: setSCDKey
* Purpose: handle the versioning and date stamps for slowly changing dimensions
* Algorithm:
* Data Structures:
*
* Params: 1 if there is a new id; 0 otherwise
* Returns:
* Called By: 
* Calls: 
* Assumptions: Table indexs (surrogate keys) are 1-based. This assures that the arBKeys[] entry for each table is 
*	initialized. Otherwise, parallel generation would be more difficult.
* Side Effects:
* TODO: None
*/
int
setSCDKeys(int nColumnID, ds_key_t kIndex, char *szBKey, ds_key_t *pkBeginDateKey, ds_key_t *pkEndDateKey)
{
	int bNewBKey = 0,
		nModulo;
	static int bInit = 0;
	static ds_key_t jMinimumDataDate,
		jMaximumDataDate,
		jH1DataDate,
		jT1DataDate,
		jT2DataDate;
	date_t dtTemp;
	int nTableID;

	if (!bInit)
	{
		strtodt(&dtTemp, DATA_START_DATE);
		jMinimumDataDate = dtTemp.julian;
		strtodt(&dtTemp, DATA_END_DATE);
		jMaximumDataDate = dtTemp.julian;
		jH1DataDate = jMinimumDataDate + (jMaximumDataDate - jMinimumDataDate) / 2;
		jT2DataDate = (jMaximumDataDate - jMinimumDataDate) / 3;
		jT1DataDate = jMinimumDataDate + jT2DataDate;
		jT2DataDate += jT1DataDate; 
		bInit = 1;
	}
	
	nTableID = getTableFromColumn(nColumnID);
	nModulo = (int)(kIndex % 6);
	switch(nModulo)
	{
	case 1: /* 1 revision */
		mk_bkey(arBKeys[nTableID], kIndex, nColumnID);
		bNewBKey = 1;
		*pkBeginDateKey = jMinimumDataDate - nTableID * 6;
		*pkEndDateKey = -1;
		break;
	case 2:	/* 1 of 2 revisions */
		mk_bkey(arBKeys[nTableID], kIndex, nColumnID);
		bNewBKey = 1;
		*pkBeginDateKey = jMinimumDataDate - nTableID * 6;
		*pkEndDateKey = jH1DataDate - nTableID * 6;
		break;
	case 3:	/* 2 of 2 revisions */
		mk_bkey(arBKeys[nTableID], kIndex - 1, nColumnID);
		*pkBeginDateKey = jH1DataDate - nTableID * 6 + 1;
		*pkEndDateKey = -1;
		break;
	case 4:	/* 1 of 3 revisions */
		mk_bkey(arBKeys[nTableID], kIndex, nColumnID);
		bNewBKey = 1;
		*pkBeginDateKey = jMinimumDataDate - nTableID * 6;
		*pkEndDateKey = jT1DataDate - nTableID * 6;
		break;
	case 5:	/* 2 of 3 revisions */
		mk_bkey(arBKeys[nTableID], kIndex - 1, nColumnID);
		*pkBeginDateKey = jT1DataDate - nTableID * 6 + 1;
		*pkEndDateKey = jT2DataDate - nTableID * 6;
		break;
	case 0:	/* 3 of 3 revisions */
		mk_bkey(arBKeys[nTableID], kIndex - 2, nColumnID);
		*pkBeginDateKey = jT2DataDate - nTableID * 6 + 1;
		*pkEndDateKey = -1;
		break;
	}

	/* can't have a revision in the future, per bug 114 */
	if (*pkEndDateKey > jMaximumDataDate)
		*pkEndDateKey = -1;
	
	strcpy(szBKey, arBKeys[nTableID]);
	
	return(bNewBKey);
}

/*
* Routine: scd_join(int tbl, int col, ds_key_t jDate)
* Purpose: create joins to slowly changing dimensions
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
ds_key_t
scd_join(int tbl, int col, ds_key_t jDate)
{
	ds_key_t res,
		kRowcount;
	static int bInit = 0,
		jMinimumDataDate,
		jMaximumDataDate,
		jH1DataDate,
		jT1DataDate,
		jT2DataDate;
	date_t dtTemp;

	if (!bInit)
	{
		strtodt(&dtTemp, DATA_START_DATE);
		jMinimumDataDate = dtTemp.julian;
		strtodt(&dtTemp, DATA_END_DATE);
		jMaximumDataDate = dtTemp.julian;
		jH1DataDate = jMinimumDataDate + (jMaximumDataDate - jMinimumDataDate) / 2;
		jT2DataDate = (jMaximumDataDate - jMinimumDataDate) / 3;
		jT1DataDate = jMinimumDataDate + jT2DataDate;
		jT2DataDate += jT1DataDate; 
		bInit = 1;
	}

	kRowcount = getIDCount(tbl);
	genrand_key(&res, DIST_UNIFORM, 1, kRowcount, 0, col);	/* pick the id */
	res = matchSCDSK(res, jDate, tbl); /* map to the date-sensitive surrogate key */

	/* can't have a revision in the future, per bug 114 */
	if (jDate > jMaximumDataDate)
		res = -1;
	
	return((res > get_rowcount(tbl))?-1:res);
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
ds_key_t
matchSCDSK(ds_key_t kUnique, ds_key_t jDate, int nTable)
{
	ds_key_t kReturn = -1;
	static int bInit = 0;
	int jMinimumDataDate,
		jMaximumDataDate;
	static int jH1DataDate,
		jT1DataDate,
		jT2DataDate;
	date_t dtTemp;
	
	if (!bInit)
	{
		strtodt(&dtTemp, DATA_START_DATE);
		jMinimumDataDate = dtTemp.julian;
		strtodt(&dtTemp, DATA_END_DATE);
		jMaximumDataDate = dtTemp.julian;
		jH1DataDate = jMinimumDataDate + (jMaximumDataDate - jMinimumDataDate) / 2;
		jT2DataDate = (jMaximumDataDate - jMinimumDataDate) / 3;
		jT1DataDate = jMinimumDataDate + jT2DataDate;
		jT2DataDate += jT1DataDate; 
		bInit = 1;
	}
	
	switch(kUnique % 3)	/* number of revisions for the ID */
	{
	case 1:	/* only one occurrence of this ID */
		kReturn = (kUnique / 3) * 6;
		kReturn += 1;
		break;
	case 2: /* two revisions of this ID */
		kReturn = (kUnique / 3) * 6;
		kReturn += 2;
		if (jDate > jH1DataDate)
			kReturn += 1;
		break;
	case 0:	/* three revisions of this ID */
		kReturn = (kUnique / 3) * 6;
		kReturn += - 2;
		if (jDate > jT1DataDate)
			kReturn += 1;
		if (jDate > jT2DataDate)
			kReturn += 1;
		break;
	}

	if (kReturn > get_rowcount(nTable))
      kReturn = get_rowcount(nTable);
   
   return(kReturn);
}

/*
* Routine: 
* Purpose: map from a unique ID to a random SK
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
ds_key_t
getSKFromID(ds_key_t kID, int nColumn)
{
   ds_key_t kTemp = -1;

   switch(kID % 3)
   {
   case 1:  /* single revision */
      kTemp = kID / 3;
      kTemp *= 6;
      kTemp += 1;
      break;
   case 2:  /* two revisions */
      kTemp = kID / 3;
      kTemp *= 6;
      kTemp += genrand_integer(NULL, DIST_UNIFORM, 2, 3, 0, nColumn);
      break;
   case 0:  /* three revisions */
      kTemp = kID / 3;
      kTemp -= 1;
      kTemp *= 6;
      kTemp += genrand_integer(NULL, DIST_UNIFORM, 4, 6, 0, nColumn);
      break;
   }

   return(kTemp);
}

/*
* Routine: getFirstSK
* Purpose: map from id to an SK that can be mapped back to an id by printID()
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
ds_key_t
getFirstSK(ds_key_t kID)
{
   ds_key_t kTemp = -1;

   switch(kID % 3)
   {
   case 1:  /* single revision */
      kTemp = kID / 3;
      kTemp *= 6;
      kTemp += 1;
      break;
   case 2:  /* two revisions */
      kTemp = kID / 3;
      kTemp *= 6;
      kTemp += 2;
      break;
   case 0:  /* three revisions */
      kTemp = kID / 3;
      kTemp -= 1;
      kTemp *= 6;
      kTemp += 4;
      break;
   }

   return(kTemp);
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
changeSCD(int nDataType, void *pNewData, void *pOldData, int *nFlags, int bFirst)
{
   
  /**
   * if nFlags is odd, then this value will be retained
   */
   if ((*nFlags != ((*nFlags / 2) * 2)) && (bFirst == 0))
   {

      /*
       * the method to retain the old value depends on the data type 
       */
      switch(nDataType)
      {
      case SCD_INT:
         *(int *)pNewData = *(int *)pOldData;
         break;
      case SCD_PTR:
         pNewData = pOldData;
         break;
      case SCD_KEY:
         *(ds_key_t *)pNewData = *(ds_key_t *)pOldData;
         break;
     case SCD_CHAR:
         strcpy((char *)pNewData, (char *)pOldData);
         break;
      case SCD_DEC:
         memcpy(pNewData, pOldData, sizeof(decimal_t));
         break;
      }
   }
   else {

      /*
       * the method to set the old value depends on the data type 
       */
      switch(nDataType)
      {
      case SCD_INT:
         *(int *)pOldData = *(int *)pNewData;
         break;
      case SCD_PTR:
         pOldData = pNewData;
         break;
      case SCD_KEY:
         *(ds_key_t *)pOldData = *(ds_key_t *)pNewData;
         break;
      case SCD_CHAR:
         strcpy((char *)pOldData, (char *)pNewData);
         break;
      case SCD_DEC:
         memcpy(pOldData, pNewData, sizeof(decimal_t));
         break;
      }
   }
  
   *nFlags /= 2;
   
   
   return;
}
