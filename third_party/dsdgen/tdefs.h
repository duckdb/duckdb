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
#ifndef TDEFS_H
#define TDEFS_H

#include <stdio.h>
#include "tables.h"
#include "columns.h"
#include "tdef_functions.h"

/*
 * Flag field definitions used in tdefs[]
 */
#define FL_NONE			0x0000		/* this table is not defined */
#define FL_NOP			0x0001		/* this table is not defined */
#define FL_DATE_BASED	0x0002		/* this table is produced in date order */
#define FL_CHILD		0x0004		/* this table is the child in a parent/child link */
#define FL_OPEN			0x0008		/* this table has a valid output destination */
#define FL_DUP_NAME		0x0010		/* to keep find_table() from complaining twice */
#define FL_TYPE_2		0x0020		/* this dimension keeps history -- rowcount shows unique entities (not including revisions) */
#define FL_SMALL        0x0040   /* this table has low rowcount; used by address.c */
#define FL_SPARSE    0x0080 
/* unused 0x0100 */
#define FL_NO_UPDATE	0x0200		/* this table is not altered by the update process */
#define FL_SOURCE_DDL	0x0400		/* table in the souce schema */
#define FL_JOIN_ERROR	0x0800		/* join called without an explicit rule */
#define FL_PARENT		0x1000		/* this table has a child in nParam */
#define FL_FACT_TABLE   0x2000
#define FL_PASSTHRU     0x4000   /* verify routine uses warehouse without change */
#define FL_VPRINT   0x8000       /* verify routine includes print function */

/*
* general table descriptions. 
* NOTE: This table contains the constant elements in the table descriptions; it must be kept in sync with the declararions of 
*   assocaited functions, found in tdef_functions.h
*/
typedef struct TDEF_T {
	char *name;		/* -- name of the table; */
	char *abreviation;	/* -- shorthand name of the table */
	int flags;		/* -- control table options */
	int nFirstColumn;	/* -- first column/RNG for this table */
	int nLastColumn;	/* -- last column/RNG for this table */
	int nTableIndex;	/* used for rowcount calculations */
	int nParam;		/* -- additional table param (revision count, child number, etc.) */
	FILE *outfile;		/* -- output destination */
	int nUpdateSize;	/* -- percentage of base rowcount in each update set (basis points) */
	int nNewRowPct;
	int nNullPct;		/* percentage of rows with nulls (basis points) */
	ds_key_t kNullBitMap;	/* colums that should be NULL in the current row */
	ds_key_t kNotNullBitMap;	/* columns that are defined NOT NULL */
	ds_key_t *arSparseKeys;	/* sparse key set for table; used if FL_SPARSE is set */
	} tdef;

/*
extern tdef *tdefs;
extern tdef w_tdefs[];
extern tdef s_tdefs[];
*/

#define tdefIsFlagSet(t, f)	(tdefs[t].flags & f)
ds_key_t GetRowcountByName(char *szName);
int GetTableNumber(char *szName);
char *getTableNameByID(int id);
int getTableFromColumn(int id);
int initSpareKeys(int id);
tdef *getSimpleTdefsByNumber(int nTable);
tdef *getTdefsByNumber(int nTable);


#endif
