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
#include "nulls.h"
#include "genrand.h"
#include "tdefs.h"

/*
 * Routine: nullCheck(int nColumn)
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
int nullCheck(int nColumn) {
	static int nLastTable = 0;
	tdef *pTdef;
	ds_key_t kBitMask = 1;

	nLastTable = getTableFromColumn(nColumn);
	pTdef = getSimpleTdefsByNumber(nLastTable);

	kBitMask <<= nColumn - pTdef->nFirstColumn;

	return ((pTdef->kNullBitMap & kBitMask) != 0);
}

/*
* Routine: nullSet(int *pDest, int nStream)
* Purpose: set the kNullBitMap for a particular table
* Algorithm:
*	1. if random[1,100] >= table's NULL pct, clear map and return
*	2. set map

* Data Structures:
*
* Params:
* Returns:
* Called By:
* Calls:
* Assumptions:
* Side Effects: uses 2 RNG calls
* TODO: None
*/
void nullSet(ds_key_t *pDest, int nStream) {
	int nThreshold;
	ds_key_t kBitMap;
	static int nLastTable = 0;
	tdef *pTdef;

	nLastTable = getTableFromColumn(nStream);
	pTdef = getSimpleTdefsByNumber(nLastTable);

	/* burn the RNG calls */
	genrand_integer(&nThreshold, DIST_UNIFORM, 0, 9999, 0, nStream);
	genrand_key(&kBitMap, DIST_UNIFORM, 1, MAXINT, 0, nStream);

	/* set the bitmap based on threshold and NOT NULL definitions */
	*pDest = 0;
	if (nThreshold < pTdef->nNullPct) {
		*pDest = kBitMap;
		*pDest &= ~pTdef->kNotNullBitMap;
	}

	return;
}
