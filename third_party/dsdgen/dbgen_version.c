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
#include <time.h>
#include "dbgen_version.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "misc.h"
#include "release.h"

struct DBGEN_VERSION_TBL g_dbgen_version;
extern char g_szCommandLine[];

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
mk_dbgen_version(void *pDest, ds_key_t kIndex)
{
	static int bInit = 0;
	struct DBGEN_VERSION_TBL *r;
	time_t ltime;
	struct tm *pTimeStamp;
	
	if (pDest == NULL)
		r = &g_dbgen_version;
	else
		r = pDest;
	
	if (!bInit)
	{
		memset(&g_dbgen_version, 0, sizeof(struct DBGEN_VERSION_TBL));
		bInit = 1;
	}
	
	
	time( &ltime );                 /* Get time in seconds */
	pTimeStamp = localtime( &ltime );  /* Convert time to struct */
	
	sprintf(r->szDate, "%4d-%02d-%02d", pTimeStamp->tm_year + 1900, pTimeStamp->tm_mon + 1, pTimeStamp->tm_mday);
	sprintf(r->szTime, "%02d:%02d:%02d", pTimeStamp->tm_hour, pTimeStamp->tm_min, pTimeStamp->tm_sec);
	sprintf (r->szVersion,"%d.%d.%d%s", VERSION, RELEASE, MODIFICATION, PATCH);
	strcpy(r->szCmdLineArgs, g_szCommandLine);
	
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
pr_dbgen_version(void *pSrc)
{
	struct DBGEN_VERSION_TBL *r;
	
	if (pSrc == NULL)
		r = &g_dbgen_version;
	else
		r = pSrc;
	
	print_start(DBGEN_VERSION);
	print_varchar(DV_VERSION, r->szVersion, 1);
	print_varchar(DV_CREATE_DATE, r->szDate, 1);
	print_varchar(DV_CREATE_TIME, r->szTime, 1);
	print_varchar(DV_CMDLINE_ARGS, r->szCmdLineArgs, 0);
	print_end(DBGEN_VERSION);
	
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
ld_dbgen_version(void *pSrc)
{
	struct DBGEN_VERSION_TBL *r;
		
	if (pSrc == NULL)
		r = &g_dbgen_version;
	else
		r = pSrc;
	
	return(0);
}

