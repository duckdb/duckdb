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
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include <stdio.h>
#include "genrand.h"

/*
 * Routine: MakePermutation(int nSize)
 * Purpose: Permute the integers in [1..nSize]
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
int *makePermutation(int *nNumberSet, int nSize, int nStream) {
	int i, nTemp, nIndex, *pInt;

	if (nSize <= 0)
		return (NULL);

	if (!nNumberSet) {
		nNumberSet = (int *)malloc(nSize * sizeof(int));
		MALLOC_CHECK(nNumberSet);
		pInt = nNumberSet;
		for (i = 0; i < nSize; i++)
			*pInt++ = i;
	}

	for (i = 0; i < nSize; i++) {
		nIndex = genrand_integer(NULL, DIST_UNIFORM, 0, nSize - 1, 0, nStream);
		nTemp = nNumberSet[i];
		nNumberSet[i] = nNumberSet[nIndex];
		nNumberSet[nIndex] = nTemp;
	}

	return (nNumberSet);
}

/*
 * Routine: MakePermutation(int nSize)
 * Purpose: Permute the integers in [1..nSize]
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
ds_key_t *makeKeyPermutation(ds_key_t *nNumberSet, ds_key_t nSize, int nStream) {
	ds_key_t i, nTemp, nIndex, *pInt;
	if (nSize <= 0)
		return (NULL);

	if (!nNumberSet) {
		nNumberSet = (ds_key_t *)malloc(nSize * sizeof(ds_key_t));
		MALLOC_CHECK(nNumberSet);
		pInt = nNumberSet;
		for (i = 0; i < nSize; i++)
			*pInt++ = i;
	}

	for (i = 0; i < nSize; i++) {
		nIndex = genrand_key(NULL, DIST_UNIFORM, 0, nSize - 1, 0, nStream);
		nTemp = nNumberSet[i];
		nNumberSet[i] = nNumberSet[nIndex];
		nNumberSet[nIndex] = nTemp;
	}

	return (nNumberSet);
}
