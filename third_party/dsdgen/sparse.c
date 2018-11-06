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
#include "scaling.h"
#include "genrand.h"
#include "sparse.h"
#include "tdefs.h"
#include "error_msg.h"

/*
* Routine: initSparseKeys()
* Purpose: set up the set of valid key values for a sparse table.
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions: The total population will fit in 32b
* Side Effects:
* TODO: None
*/
int
initSparseKeys(int nTable)
{
	ds_key_t kRowcount,
		kOldSeed;
	int k;
	tdef *pTdef;

	kRowcount = get_rowcount(nTable);
	pTdef = getTdefsByNumber(nTable);
	
	pTdef->arSparseKeys = (ds_key_t *)malloc((long)kRowcount * sizeof(ds_key_t));
	MALLOC_CHECK(pTdef->arSparseKeys);
	if (pTdef->arSparseKeys == NULL)
		ReportError(QERR_NO_MEMORY, "initSparseKeys()", 1);
	memset(pTdef->arSparseKeys, 0, (long)kRowcount * sizeof(ds_key_t));

	kOldSeed = setSeed(0, nTable);
	for (k = 0; k < kRowcount; k++)
		 genrand_key(&pTdef->arSparseKeys[k], DIST_UNIFORM, 1, pTdef->nParam, 0, 0);
	setSeed(0, (int)kOldSeed);

	return(0);
}

/*
* Routine: randomSparseKey()
* Purpose: randomly select one of the valid key values for a sparse table
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
randomSparseKey(int nTable, int nColumn)
{
	int nKeyIndex;
	ds_key_t kRowcount;
	tdef *pTdef;

	pTdef = getTdefsByNumber(nTable);
	kRowcount = get_rowcount(nTable);
	genrand_integer(&nKeyIndex, DIST_UNIFORM, 1, (long)kRowcount, 0, nColumn);

	return(pTdef->arSparseKeys[nKeyIndex]);
}



