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
#include "genrand.h"
#include "s_customer.h"
#include "print.h"
#include "columns.h"
#include "build_support.h"
#include "tables.h"
#include "address.h"
#include "scaling.h"
#include "parallel.h"
#include "w_customer_demographics.h"
#include "w_customer_address.h"
#include "permute.h"

struct S_CUSTOMER_TBL g_s_customer;
extern struct W_CUSTOMER_ADDRESS_TBL g_w_customer_address;

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
mk_s_customer (void *pDest, ds_key_t kIndex)
{
  static int bInit = 0;
  struct S_CUSTOMER_TBL *r;
  static date_t dtMin,
    dtMax, dtBirthMin, dtBirthMax, dtToday, dt1YearAgo, dt10YearsAgo;
  static decimal_t dMinIncome, dMaxIncome;
  int nTemp;
  ds_key_t kTempDemographic;
  char *szTemp;
  static int *pPermutation;

  if (pDest == NULL)
    r = &g_s_customer;
  else
    r = pDest;

  if (!bInit)
    {
      memset (&g_s_customer, 0, sizeof (struct S_CUSTOMER_TBL));
      setUpdateDateRange (S_CUSTOMER, &dtMin, &dtMax);
      strtodec (&dMinIncome, "0.00");
      strtodec (&dMaxIncome, "200000.00");
      r->pBirthCountry = strdup ("<UNKNOWN>");
      strtodt (&dtBirthMin, "1924-01-01");
      strtodt (&dtBirthMax, "1992-12-31");
      strtodt (&dtToday, TODAYS_DATE);
      jtodt (&dt1YearAgo, dtToday.julian - 365);
      jtodt (&dt10YearsAgo, dtToday.julian - 365);
      pPermutation =
	makePermutation (NULL, (int) getIDCount (CUSTOMER), S_CUST_ID);

      bInit = 1;
    }

  r->kID = getPermutationEntry (pPermutation, (int) kIndex);
  kTempDemographic = mk_join (S_CUST_GENDER, CUSTOMER_DEMOGRAPHICS, 1) - 1;
  bitmap_to_dist (&szTemp, "gender", &kTempDemographic, 1,
		  CUSTOMER_DEMOGRAPHICS);
  switch (*szTemp)
    {
    case 'M':			/* male */
      r->sGender[0] = 'M';
      pick_distribution (&r->pFirstName, "first_names", 1, 1,
			 S_CUST_FIRST_NAME);
      pick_distribution (&r->pSalutation, "salutations", 1, 2,
			 S_CUST_SALUTATION);
      break;
    case 'F':			/* female */
      r->sGender[0] = 'F';
      pick_distribution (&r->pFirstName, "first_names", 1, 2,
			 S_CUST_FIRST_NAME);
      pick_distribution (&r->pSalutation, "salutations", 1, 3,
			 S_CUST_SALUTATION);
      break;
    default:			/* gender neutral */
      r->sGender[0] = 'U';
      pick_distribution (&r->pFirstName, "first_names", 1, 3,
			 S_CUST_FIRST_NAME);
      pick_distribution (&r->pSalutation, "salutations", 1, 1,
			 S_CUST_SALUTATION);
      break;
    }
  bitmap_to_dist (&r->pMaritalStatus, "marital_status", &kTempDemographic, 1,
		  CUSTOMER_DEMOGRAPHICS);
  bitmap_to_dist (&r->pEducation, "education", &kTempDemographic, 1,
		  CUSTOMER_DEMOGRAPHICS);
  bitmap_to_dist (&r->nPurchaseEstimate, "purchase_band", &kTempDemographic,
		  1, CUSTOMER_DEMOGRAPHICS);
  bitmap_to_dist (&r->pCreditRating, "credit_rating", &kTempDemographic, 1,
		  CUSTOMER_DEMOGRAPHICS);
  r->nDependents = (int) (kTempDemographic % (ds_key_t) CD_MAX_CHILDREN);
  kTempDemographic /= (ds_key_t) CD_MAX_CHILDREN;
  r->nEmployed = (int) (kTempDemographic % (ds_key_t) CD_MAX_EMPLOYED);
  kTempDemographic /= (ds_key_t) CD_MAX_EMPLOYED;
  r->nCollege = (int) (kTempDemographic % (ds_key_t) CD_MAX_COLLEGE);

  pick_distribution (&r->pLastName, "last_names", 1, 1, S_CUST_LAST_NAME);
  r->bPreferredFlag =
    (genrand_integer (NULL, DIST_UNIFORM, 1, 100, 0, S_CUST_PREFERRED_FLAG) >
     50) ? 1 : 0;
  genrand_date (&r->dtBirthDate, DIST_UNIFORM, &dtBirthMin, &dtBirthMax, NULL,
		S_CUST_BIRTH_DATE);
  genrand_date (&r->dtFirstPurchaseDate, DIST_UNIFORM, &dt10YearsAgo,
		&dt1YearAgo, NULL, S_CUST_FIRST_PURCHASE_DATE);
  genrand_integer (&nTemp, DIST_UNIFORM, 1, 30, 0, S_CUST_FIRST_SHIPTO_DATE);
  jtodt (&r->dtFirstShipToDate, r->dtFirstPurchaseDate.julian + nTemp);
  gen_charset (r->szLogin, ALPHANUM, 5, RS_S_CUST_LOGIN, S_CUST_LOGIN);
  genrand_email (r->szEmail, r->pFirstName, r->pLastName, S_CUST_EMAIL);
  genrand_date (&r->dtLastLogin, DIST_UNIFORM, &dt1YearAgo, &dtToday, NULL,
		S_CUST_LAST_LOGIN);
  genrand_date (&r->dtReview, DIST_UNIFORM, &dt1YearAgo, &dtToday, NULL,
		S_CUST_LAST_REVIEW);
  genrand_ipaddr (r->szPrimaryMachine, S_CUST_PRIMARY_MACHINE);
  genrand_ipaddr (r->szSecondaryMachine, S_CUST_SECONDARY_MACHINE);
  pick_distribution (&r->pLocationType, "location_type", 1, 1,
		     S_CUST_LOCATION_TYPE);
  pick_distribution (&r->nVehicle, "vehicle_count", 1, 1, S_CUST_VEHICLE_CNT);
  genrand_decimal (&r->dIncome, DIST_UNIFORM, &dMinIncome, &dMaxIncome, NULL,
		   S_CUST_INCOME);
  pick_distribution (&r->pBuyPotential, "buy_potential", 1, 1,
		     S_CUST_PURCHASE_ESTIMATE);
  mk_w_customer_address (NULL, kIndex);

  return (0);
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
pr_s_customer (void *pSrc)
{
  struct S_CUSTOMER_TBL *r;
  char szTemp[6];

  if (pSrc == NULL)
    r = &g_s_customer;
  else
    r = pSrc;

  print_start (S_CUSTOMER);
  print_id (S_CUST_ID, r->kID, 1);
  print_varchar (S_CUST_SALUTATION, r->pSalutation, 1);
  print_varchar (S_CUST_LAST_NAME, r->pLastName, 1);
  print_varchar (S_CUST_FIRST_NAME, r->pFirstName, 1);
  print_boolean (S_CUST_PREFERRED_FLAG, r->bPreferredFlag, 1);
  print_date (S_CUST_BIRTH_DATE, r->dtBirthDate.julian, 1);
  print_varchar (S_CUST_BIRTH_COUNTRY, r->pBirthCountry, 1);
  print_varchar (S_CUST_LOGIN, r->szLogin, 1);
  print_varchar (S_CUST_EMAIL, r->szEmail, 1);
  print_date (S_CUST_LAST_LOGIN, r->dtLastLogin.julian, 1);
  print_date (S_CUST_FIRST_SHIPTO_DATE, r->dtFirstShipToDate.julian, 1);
  print_date (S_CUST_FIRST_PURCHASE_DATE, r->dtFirstPurchaseDate.julian, 1);
  print_date (S_CUST_LAST_REVIEW, r->dtReview.julian, 1);
  print_varchar (S_CUST_PRIMARY_MACHINE, r->szPrimaryMachine, 1);
  print_varchar (S_CUST_SECONDARY_MACHINE, r->szSecondaryMachine, 1);
  print_integer (S_CUST_ADDRESS_STREET_NUM,
		 g_w_customer_address.ca_address.street_num, 1);
  print_varchar (S_CUST_ADDRESS_SUITE_NUM,
		 g_w_customer_address.ca_address.suite_num, 1);
  print_varchar (S_CUST_ADDRESS_STREET_NAME1,
		 g_w_customer_address.ca_address.street_name1, 1);
  print_varchar (S_CUST_ADDRESS_STREET_NAME2,
		 g_w_customer_address.ca_address.street_name2, 1);
  print_varchar (S_CUST_ADDRESS_STREET_TYPE,
		 g_w_customer_address.ca_address.street_type, 1);
  print_varchar (S_CUST_ADDRESS_CITY, g_w_customer_address.ca_address.city,
		 1);
  sprintf (szTemp, "%05d", g_w_customer_address.ca_address.zip);
  print_varchar (S_CUST_ADDRESS_ZIP, szTemp, 1);
  print_varchar (S_CUST_ADDRESS_COUNTY,
		 g_w_customer_address.ca_address.county, 1);
  print_varchar (S_CUST_ADDRESS_STATE, g_w_customer_address.ca_address.state,
		 1);
  print_varchar (S_CUST_ADDRESS_COUNTRY,
		 g_w_customer_address.ca_address.country, 1);
  print_varchar (S_CUST_LOCATION_TYPE, r->pLocationType, 1);
  print_varchar (S_CUST_GENDER, r->sGender, 1);
  print_varchar (S_CUST_MARITAL_STATUS, r->pMaritalStatus, 1);
  print_varchar (S_CUST_EDUCATION, r->pEducation, 1);
  print_varchar (S_CUST_CREDIT_RATING, r->pCreditRating, 1);
  print_integer (S_CUST_PURCHASE_ESTIMATE, r->nPurchaseEstimate, 1);
  print_varchar (S_CUST_BUY_POTENTIAL, r->pBuyPotential, 1);
  print_integer (S_CUST_DEPENDENT_CNT, r->nDependents, 1);
  print_integer (S_CUST_EMPLOYED_CNT, r->nEmployed, 1);
  print_integer (S_CUST_COLLEGE_CNT, r->nCollege, 1);
  print_integer (S_CUST_VEHICLE_CNT, r->nVehicle, 1);
  print_decimal (S_CUST_INCOME, &r->dIncome, 0);
  print_end (S_CUSTOMER);

  return (0);
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
ld_s_customer (void *pSrc)
{
  struct S_CUSTOMER_TBL *r;

  if (pSrc == NULL)
    r = &g_s_customer;
  else
    r = pSrc;

  return (0);
}
