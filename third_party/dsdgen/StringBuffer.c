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
#include <assert.h>
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include "StringBuffer.h"

/*
 * Routine: InitBuffer
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
StringBuffer_t *InitBuffer(int nSize, int nIncrement) {
	StringBuffer_t *pBuf;

	pBuf = (StringBuffer_t *)malloc(sizeof(struct STRING_BUFFER_T));
	MALLOC_CHECK(pBuf);
	if (pBuf == NULL)
		return (NULL);
	memset((void *)pBuf, 0, sizeof(struct STRING_BUFFER_T));

	pBuf->pText = (char *)malloc(sizeof(char) * nSize);
	MALLOC_CHECK(pBuf->pText);
	if (pBuf->pText == NULL)
		return (NULL);
	memset((void *)pBuf->pText, 0, sizeof(char) * nSize);

	pBuf->nIncrement = nIncrement;
	pBuf->nBytesAllocated = nSize;
	pBuf->nFlags = SB_INIT;

	return (pBuf);
}

/*
 * Routine: AddBuffer
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
int AddBuffer(StringBuffer_t *pBuf, char *pStr) {
	int nRemaining = pBuf->nBytesAllocated - pBuf->nBytesUsed, nRequested = strlen(pStr);

	if (!nRequested)
		return (0);

	while (nRequested >= nRemaining) {
		pBuf->pText = (char *)realloc((void *)pBuf->pText, pBuf->nBytesAllocated + pBuf->nIncrement);
		if (!pBuf->pText)
			return (-1);
		pBuf->nBytesAllocated += pBuf->nIncrement;
		nRemaining += pBuf->nIncrement;
	}

	strcat(pBuf->pText, pStr);
	if (pBuf->nBytesUsed == 0) /* first string adds a terminator */
		pBuf->nBytesUsed = 1;
	pBuf->nBytesUsed += nRequested;

	return (0);
}

/*
 * Routine: ResetStringBuffer
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
int ResetBuffer(StringBuffer_t *pBuf) {
	pBuf->nBytesUsed = 0;
	if (pBuf->nBytesAllocated)
		pBuf->pText[0] = '\0';

	return (0);
}

/*
 * Routine: GetBuffer
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
char *GetBuffer(StringBuffer_t *pBuf) {
	return (pBuf->pText);
}

/*
 * Routine: FreeBuffer
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
void FreeBuffer(StringBuffer_t *pBuf) {
	if (!pBuf)
		return;
	if (pBuf->pText)
		free((void *)pBuf->pText);
	free((void *)pBuf);

	return;
}
