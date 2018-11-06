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
#include "list.h"
#include "error_msg.h"

list_t *
makeList(int nFlags, int (*SortFunc)(const void *d1, const void *d2))
{
	list_t *pRes;

	pRes = (list_t *)malloc(sizeof(list_t));
	MALLOC_CHECK(pRes);
	if (pRes == NULL)
		ReportError(QERR_NO_MEMORY, "client list", 1);
	memset(pRes, 0, sizeof(list_t));
	pRes->nFlags = nFlags;
	pRes->pSortFunc = SortFunc;

	return(pRes);
}

list_t *
addList(list_t *pList, void *pData)
{
	node_t *pNode;
	node_t *pInsertPoint;
	int bMoveForward = (pList->nFlags & L_FL_HEAD);

   pNode = (node_t *)malloc(sizeof(node_t));
	MALLOC_CHECK(pNode);
   if (!pNode)
      ReportErrorNoLine(QERR_NO_MEMORY, "client node", 1);
	memset(pNode, 0, sizeof(node_t));
	pNode->pData = pData;

	if (pList->nMembers == 0)	/* first node */
	{
		pList->head = pNode;
		pList->tail = pNode;
		pList->nMembers = 1;
		return(pList);
	}

	if (pList->nFlags & L_FL_SORT)
	{
			if (pList->pSortFunc(pData, pList->head->pData) <= 0)
			{
				/* new node become list head */
				pNode->pNext = pList->head;
				pList->head->pPrev = pNode;
				pList->head = pNode;
				pList->nMembers += 1;
				return(pList);
			}
			pInsertPoint = pList->head;

		/* find the correct point to insert new node */
		while (pInsertPoint)
		{
			if (pList->pSortFunc(pInsertPoint->pData, pData) < 0)
				break;
			pInsertPoint = (bMoveForward)?pInsertPoint->pNext:pInsertPoint->pPrev;
		}
		if (pInsertPoint) /* mid-list insert */
		{
			pNode->pNext = pInsertPoint->pNext;
			pNode->pPrev = pInsertPoint;
			pInsertPoint->pNext = pNode;
		}
		else
		{
			if (bMoveForward)
			{
				/* new node becomes list tail */
				pNode->pPrev = pList->tail;
				pList->tail->pNext = pNode;
				pList->tail = pNode;
			}
			else
			{
				/* new node become list head */
				pNode->pNext = pList->head;
				pList->head->pPrev = pNode;
				pList->head = pNode;
			}
		}

		pList->nMembers += 1;
		
		return(pList);
	}

		if (pList->nFlags & L_FL_HEAD)
		{
			pNode->pNext = pList->head;
			pList->head->pPrev = pNode;
			pList->head = pNode;
			pList->nMembers += 1;
		}
		else
		{
			pNode->pPrev = pList->tail;
			pList->tail->pNext = pNode;
			pList->tail = pNode;
			pList->nMembers += 1;
		}

	return(pList);
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
void *
removeItem(list_t *pList, int bHead)
{
	void *pResult;
	
	if (pList->nMembers == 0)
		return(NULL);

	if (!bHead)
	{
		pResult = pList->tail->pData;
		pList->tail = pList->tail->pPrev;
		pList->tail->pNext = NULL;
	}
	else
	{
		pResult = pList->head->pData;
		pList->head = pList->head->pNext;
		pList->head->pPrev = NULL;
	}

	pList->nMembers -= 1;

	return(pResult);
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
void *
getHead(list_t *pList)
{
	assert(pList);

	if (!pList->head)
		return(NULL);

	pList->pCurrent = pList->head;

	return(pList->pCurrent->pData);
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
void *
getTail(list_t *pList)
{
	assert(pList);

	if (!pList->tail)
		return(NULL);

	pList->pCurrent = pList->tail;

	return(pList->pCurrent->pData);
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
void *
getNext(list_t *pList)
{
	assert(pList);

	if (!pList->pCurrent->pNext)
		return(NULL);

	pList->pCurrent = pList->pCurrent->pNext;

	return(pList->pCurrent->pData);
}

/*
* Routine: 
* Purpose: findList(list_t *pList, void *pData)
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
void *
findList(list_t *pList, void *pData)
{
	void *pNode;
	struct LIST_NODE_T *pOldCurrent = pList->pCurrent;
	
	for (pNode = getHead(pList); pNode; pNode = getNext(pList))
		if (pList->pSortFunc(pNode, pData) == 0)
		{
			pList->pCurrent = pOldCurrent;
			return(pNode);
		}

		pList->pCurrent = pOldCurrent;
		return(NULL);
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
void *
getItem(list_t *pList, int nIndex)
{
	void *pResult;
	struct LIST_NODE_T *pOldCurrent = pList->pCurrent;
	
	if (nIndex > length(pList))
		return(NULL);


	for (pResult = getHead(pList); --nIndex; pResult = getNext(pList));

	pList->pCurrent = pOldCurrent;
	return(pResult);
}

