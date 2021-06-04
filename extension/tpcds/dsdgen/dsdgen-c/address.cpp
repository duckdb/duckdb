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
#include "address.h"
#include "dist.h"
#include "r_params.h"
#include "genrand.h"
#include "columns.h"
#include "tables.h"
#include "tdefs.h"
#include "permute.h"
#include "scaling.h"

static int s_nCountyCount = 0;
static int s_nCityCount = 0;

void resetCountCount(void) {
	s_nCountyCount = 0;
	s_nCityCount = 0;

	return;
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
int mk_address(ds_addr_t *pAddr, int nColumn) {
	int i, nRegion;
	char *szZipPrefix, szAddr[100];
	static int nMaxCities, nMaxCounties, bInit = 0;
	tdef *pTdef;

	if (!bInit) {
		nMaxCities = (int)get_rowcount(ACTIVE_CITIES);
		nMaxCounties = (int)get_rowcount(ACTIVE_COUNTIES);
		bInit = 1;
	}

	/* street_number is [1..1000] */
	genrand_integer(&pAddr->street_num, DIST_UNIFORM, 1, 1000, 0, nColumn);

	/* street names are picked from a distribution */
	pick_distribution(&pAddr->street_name1, "street_names", 1, 1, nColumn);
	pick_distribution(&pAddr->street_name2, "street_names", 1, 2, nColumn);

	/* street type is picked from a distribution */
	pick_distribution(&pAddr->street_type, "street_type", 1, 1, nColumn);

	/* suite number is alphabetic 50% of the time */
	genrand_integer(&i, DIST_UNIFORM, 1, 100, 0, nColumn);
	if (i & 0x01) {
		sprintf(pAddr->suite_num, "Suite %d", (i >> 1) * 10);
	} else {
		sprintf(pAddr->suite_num, "Suite %c", ((i >> 1) % 25) + 'A');
	}

	pTdef = getTdefsByNumber(getTableFromColumn(nColumn));

	/* city is picked from a distribution which maps to large/medium/small */
	if (pTdef->flags & FL_SMALL) {
		i = (int)get_rowcount(getTableFromColumn(nColumn));
		genrand_integer(&i, DIST_UNIFORM, 1, (nMaxCities > i) ? i : nMaxCities, 0, nColumn);
		dist_member(&pAddr->city, "cities", i, 1);
	} else
		pick_distribution(&pAddr->city, "cities", 1, 6, nColumn);

	/* county is picked from a distribution, based on population and keys the
	 * rest */
	if (pTdef->flags & FL_SMALL) {
		i = (int)get_rowcount(getTableFromColumn(nColumn));
		genrand_integer(&nRegion, DIST_UNIFORM, 1, (nMaxCounties > i) ? i : nMaxCounties, 0, nColumn);
		dist_member(&pAddr->county, "fips_county", nRegion, 2);
	} else
		nRegion = pick_distribution(&pAddr->county, "fips_county", 2, 1, nColumn);

	/* match state with the selected region/county */
	dist_member(&pAddr->state, "fips_county", nRegion, 3);

	/* match the zip prefix with the selected region/county */
	pAddr->zip = city_hash(0, pAddr->city);
	/* 00000 - 00600 are unused. Avoid them */
	dist_member((void *)&szZipPrefix, "fips_county", nRegion, 5);
	if (!(szZipPrefix[0] - '0') && (pAddr->zip < 9400))
		pAddr->zip += 600;
	pAddr->zip += (szZipPrefix[0] - '0') * 10000;

	sprintf(szAddr, "%d %s %s %s", pAddr->street_num, pAddr->street_name1, pAddr->street_name2, pAddr->street_type);
	pAddr->plus4 = city_hash(0, szAddr);
	dist_member(&pAddr->gmt_offset, "fips_county", nRegion, 6);
	strcpy(pAddr->country, "United States");

	return (0);
}

/*
 * Routine: mk_streetnumber
 * Purpose:
 *	one of a set of routines that creates addresses
 * Algorithm:
 * Data Structures:
 *
 * Params:
 *	nTable: target table (and, by extension, address) to allow differing
 *distributions dest: destination for the random number Returns: Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20030422 jms should be replaced if there is no table variation
 */
int mk_streetnumber(int nTable, int *dest) {
	genrand_integer(dest, DIST_UNIFORM, 1, 1000, 0, nTable);

	return (0);
}

/*
 * Routine: mk_suitenumber()
 * Purpose:
 *	one of a set of routines that creates addresses
 * Algorithm:
 * Data Structures:
 *
 * Params:
 *	nTable: target table (and, by extension, address) to allow differing
 *distributions dest: destination for the random number Returns: Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20010615 JMS return code is meaningless
 */
int mk_suitenumber(int nTable, char *dest) {
	int i;

	genrand_integer(&i, DIST_UNIFORM, 1, 100, 0, nTable);
	if (i <= 50) {
		genrand_integer(&i, DIST_UNIFORM, 1, 1000, 0, nTable);
		sprintf(dest, "Suite %d", i);
	} else {
		genrand_integer(&i, DIST_UNIFORM, 0, 25, 0, nTable);
		sprintf(dest, "Suite %c", i + 'A');
	}

	return (0);
}

/*
 * Routine: mk_streetname()
 * Purpose:
 *	one of a set of routines that creates addresses
 * Algorithm:
 *	use a staggered distibution and the 150 most common street names in the US
 * Data Structures:
 *
 * Params:
 *	nTable: target table (and, by extension, address) to allow differing
 *distributions dest: destination for the street name Returns: Called By: Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20010615 JMS return code is meaningless
 */
int mk_streetname(int nTable, char *dest) {
	char *pTemp1 = NULL, *pTemp2 = NULL;

	pick_distribution((void *)&pTemp1, "street_names", (int)1, (int)1, nTable);
	pick_distribution((void *)&pTemp2, "street_names", (int)1, (int)2, nTable);
	if (strlen(pTemp2))
		sprintf(dest, "%s %s", pTemp1, pTemp2);
	else
		strcpy(dest, pTemp1);

	return (0);
}

/*
 * Routine: mk_city
 * Purpose:
 *	one of a set of routines that creates addresses
 * Algorithm:
 *	use a staggered distibution of 1000 most common place names in the US
 * Data Structures:
 *
 * Params:
 *	nTable: target table (and, by extension, address) to allow differing
 *distributions dest: destination for the city name Returns: Called By: Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20030423 jms should be replaced if there is no per-table variation
 */
int mk_city(int nTable, char **dest) {
	pick_distribution((void *)dest, "cities", (int)1, (int)get_int("_SCALE_INDEX"), 11);

	return (0);
}

/*
 * Routine: city_hash()
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
int city_hash(int nTable, char *name) {
	char *cp;
	int hash_value = 0, res = 0;

	for (cp = name; *cp; cp++) {
		hash_value *= 26;
		hash_value -= 'A';
		hash_value += *cp;
		if (hash_value > 1000000) {
			hash_value %= 10000;
			res += hash_value;
			hash_value = 0;
		}
	}
	hash_value %= 1000;
	res += hash_value;
	res %= 10000; /* looking for a 4 digit result */

	return (res);
}

/*
 * Routine:
 *	one of a set of routines that creates addresses
 * Algorithm:
 *	use a compound distribution of the 3500 counties in the US
 * Data Structures:
 *
 * Params:
 *	nTable: target table (and, by extension, address) to allow differing
 *distributions dest: destination for the city name nRegion: the county selected
 *	city: the city name selected
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20010615 JMS return code is meaningless
 */
int mk_zipcode(int nTable, char *dest, int nRegion, char *city) {
	char *szZipPrefix = NULL;
	int nCityCode;
	int nPlusFour;

	dist_member((void *)&szZipPrefix, "fips_county", nRegion, 5);
	nCityCode = city_hash(nTable, city);
	genrand_integer(&nPlusFour, DIST_UNIFORM, 1, 9999, 0, nTable);
	sprintf(dest, "%s%04d-%04d", szZipPrefix, nCityCode, nPlusFour);

	return (0);
}
