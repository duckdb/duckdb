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
#ifndef LIST_H
#define LIST_H
typedef struct LIST_NODE_T {
	struct LIST_NODE_T *pNext;
	struct LIST_NODE_T *pPrev;
	void *pData;
} node_t;

typedef struct LIST_T {
	struct LIST_NODE_T *head;
	struct LIST_NODE_T *tail;
	struct LIST_NODE_T *pCurrent;
	int (*pSortFunc)(const void *pD1, const void *pD2);
	int nMembers;
	int nFlags;
} list_t;

/* list_t flags */
#define L_FL_HEAD 0x01 /* add at head */
#define L_FL_TAIL 0x02 /* add at tail */
#define L_FL_SORT 0x04 /* create sorted list */

#define length(list) list->nMembers

list_t *makeList(int nFlags, int (*pSortFunc)(const void *pD1, const void *pD2));
list_t *addList(list_t *pList, void *pData);
void *findList(list_t *pList, void *pData);
void *removeItem(list_t *pList, int bFromHead);
void *getHead(list_t *pList);
void *getTail(list_t *pList);
void *getNext(list_t *pList);
void *getItem(list_t *pList, int nIndex);
#endif
