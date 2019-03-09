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
#include "tables.h"
#include "columns.h"
#include "genrand.h"
#include "tdefs.h"
#include "scaling.h"
#include "w_tdefs.h"
#include "s_tdefs.h"
#include "tdef_functions.h"
#include "r_params.h"

extern tdef w_tdefs[];
extern tdef s_tdefs[];
extern table_func_t s_tdef_funcs[];
extern table_func_t w_tdef_funcs[];

/*
 * Routine: get_rowcount(int table)
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
ds_key_t GetRowcountByName(char *szName) {
	int nTable = -1;

	nTable = GetTableNumber(szName);
	if (nTable >= 0)
		return (get_rowcount(nTable - 1));

	nTable = distsize(szName);
	return (nTable);
}

/*
 * Routine: GetTableNumber(char *szName)
 * Purpose: Return size of table, pseudo table or distribution
 * Algorithm: Need to use rowcount distribution, since argument could be a
 * pseudo table Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int GetTableNumber(char *szName) {
	int i;
	char *szTable;

	for (i = 1; i <= distsize("rowcounts"); i++) {
		dist_member(&szTable, "rowcounts", i, 1);
		if (strcasecmp(szTable, szName) == 0)
			return (i - 1);
	}

	return (-1);
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
/*
tdef *
getTdefsByNumber(int nTable)
{
   if (is_set("UPDATE"))
   {
      if (s_tdefs[nTable].flags & FL_PASSTHRU)
      {
         switch(nTable + S_BRAND)
         {
         case S_CATALOG_PAGE: nTable = CATALOG_PAGE; break;
         case S_CUSTOMER_ADDRESS: nTable = CUSTOMER_ADDRESS; break;
         case S_PROMOTION: nTable = PROMOTION; break;
         }
         return(&w_tdefs[nTable]);
      }
      else
         return(&s_tdefs[nTable]);
   }
    else
        return(&w_tdefs[nTable]);
}
*/
tdef *getSimpleTdefsByNumber(nTable) {
	if (nTable >= S_BRAND)
		return (&s_tdefs[nTable - S_BRAND]);
	return (&w_tdefs[nTable]);
}

tdef *getTdefsByNumber(int nTable) {
	if (is_set("UPDATE") && is_set("VALIDATE")) {
		if (s_tdefs[nTable].flags & FL_PASSTHRU) {
			switch (nTable + S_BRAND) {
			case S_CATALOG_PAGE:
				nTable = CATALOG_PAGE;
				break;
			case S_CUSTOMER_ADDRESS:
				nTable = CUSTOMER_ADDRESS;
				break;
			case S_PROMOTION:
				nTable = PROMOTION;
				break;
			}
			return (&w_tdefs[nTable]);
		} else
			return (&s_tdefs[nTable]);
	}

	return (getSimpleTdefsByNumber(nTable));
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
char *getTableNameByID(int i) {
	tdef *pT = getSimpleTdefsByNumber(i);

	return (pT->name);
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
int getTableFromColumn(int nColumn) {
	int i;
	tdef *pT;

	for (i = 0; i <= MAX_TABLE; i++) {
		pT = getSimpleTdefsByNumber(i);
		if ((nColumn >= pT->nFirstColumn) && (nColumn <= pT->nLastColumn))
			return (i);
	}
	return (-1);
}
