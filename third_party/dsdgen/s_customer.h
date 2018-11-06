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
#ifndef S_CUSTOMER_H
#define S_CUSTOMER_H

#define RS_S_CUST_LOGIN	13
#define RS_S_CUST_EMAIL	50
#define RS_S_CUST_MACHINE	15

struct S_CUSTOMER_TBL {
   ds_key_t	kID;
   char		*pSalutation;
   char		*pLastName;
   char		*pFirstName;
   int			bPreferredFlag;
   date_t		dtBirthDate;
   date_t		dtFirstPurchaseDate;
   date_t		dtFirstShipToDate;
   char		*pBirthCountry;
   char		szLogin[RS_S_CUST_LOGIN + 1];
   char		szEmail[RS_S_CUST_EMAIL + 1];
   date_t		dtLastLogin;
   date_t		dtReview;
   char		szPrimaryMachine[RS_S_CUST_MACHINE + 1];
   char		szSecondaryMachine[RS_S_CUST_MACHINE + 1];
   ds_addr_t	adAddress;
   char		*pLocationType;
   char		sGender[2];
   char		*pMaritalStatus;
   char		*pEducation;
   char		*pCreditRating;
   int	   nPurchaseEstimate;
   char     *pBuyPotential;
   int			nDependents;
   int			nEmployed;
   int			nCollege;
   int			nVehicle;
   decimal_t	dIncome;
};

int mk_s_customer(void *pDest, ds_key_t kIndex);
int pr_s_customer(void *pSrc);
int ld_s_customer(void *pSrc);
#endif
