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
#ifndef CONSTANTS_H
#define CONSTANTS_H
 /***
 *** Multi-table/Global Defines
 ***/
#define DATA_START_DATE		"1998-01-01"	/* earliest date in the data set */
#define DATA_END_DATE		"2003-12-31"	/* latest date in the data set */
#define LINES_PER_ORDER    16              /* max number of lineitems per order for all channels */

 /***
 *** C_xxx Cutomer Defines
 ***/
#define C_PREFERRED_PCT	50

/***
 *** CC_xxx Call Center Defines
 ***/
#define CC_EMPLOYEE_MAX		7				/* rises ~ scale ^ 2 */


/***
 *** CP_xxx Catalog Page Defines
 ***/
#define CP_CATALOGS_PER_YEAR	18
#define CP_SK(c, s, p)	(c * s + p)

 /***
 *** CR_xxx Catalog Returns Defines
 ***/
#define CR_RETURN_PCT	10	/* percentage of catalog sales that are returned */

 /***
 *** CS_xxx Customer Sales Defines
 ***/
#define CS_QUANTITY_MAX		"100"
#define CS_MARKUP_MAX		"2.00"
#define CS_DISCOUNT_MAX		"1.00"
#define CS_WHOLESALE_MAX	"100.00"
#define CS_COUPON_MAX		"0.50"
#define CS_MIN_SHIP_DELAY	2		/* minimum days from order to ship */
#define CS_MAX_SHIP_DELAY	90		/* maximum days from order to ship */
#define CS_ITEMS_PER_ORDER	10		/* number of items in each order */
#define CS_GIFT_PCT			10		/* ship-to != bill-to */

 /*
 * DATE SETTINGS
 *
 * The benchmarks sense of "today". Should this be a sliding scale/parameter?
 */
#define CURRENT_YEAR	2003
#define CURRENT_MONTH	1
#define CURRENT_DAY		8
#define CURRENT_QUARTER	1
#define CURRENT_WEEK	2
#define DATE_MINIMUM	"1998-01-01"
#define DATE_MAXIMUM	"2002-12-31"
#define YEAR_MINIMUM	1998
#define YEAR_MAXIMUM	2002
#define WAREHOUSE_LOAD_DATE	"2001-07-18"
#define UPDATE_INTERVAL		30	/* refresh interval in days */
#define TODAYS_DATE	"2003-01-08"

/***
 *** INV_xxx Inventory Defines
 ***/
#define INV_QUANTITY_MIN	0
#define INV_QUANTITY_MAX	1000

/***
 *** ITEM_xxx Item Defines
 ***/
#define ITEM_DESC_LEN		5
#define ITEM_NAME_LEN		10
#define ITEM_MANFACTURER_COUNT 1000	/* number of brands handled by a particular manufacturer */

/***
 *** PROMO_xxx Promotions Defines
 ***/
#define PROMO_NAME_LEN		5
#define PROMO_START_MIN		-720
#define PROMO_START_MAX		100
#define PROMO_START_MEAN	0
#define PROMO_LEN_MIN		1
#define PROMO_LEN_MAX		60
#define PROMO_LEN_MEAN		0
#define PROMO_DETAIL_LEN_MIN		20
#define PROMO_DETAIL_LEN_MAX		60

 /***
 *** SR_xxx Store Returns Defines
 ***/
#define SR_RETURN_PCT	10	/* percentage of store sales that are returned */

 /***
 *** SS_xxx Store Sales Defines
 ***/
#define SS_MIN_SHIP_DELAY	2		/* minimum days from order to ship */
#define SS_MAX_SHIP_DELAY	90		/* maximum days from order to ship */
#define SS_QUANTITY_MAX		"100"
#define SS_MARKUP_MAX		"1.00"
#define SS_DISCOUNT_MAX		"1.00"
#define SS_WHOLESALE_MAX	"100.00"
#define SS_COUPON_MAX		"0.50"

 /***
 *** WP_xxx Web Page Defines
 ***/
#define WP_AUTOGEN_PCT	30
#define WP_LINK_MIN		2
#define WP_LINK_MAX		25
#define WP_IMAGE_MIN	1
#define WP_IMAGE_MAX	7
#define WP_AD_MIN		0
#define WP_AD_MAX		4
#define WP_MAX_REC_DURATION	1000	/* maximum time from start to end of record */
#define WP_IDLE_TIME_MAX	100		/* maximum time since last page access */

 /***
 *** W_xxx Warehouse Defines
 ***/
#define W_DESC_MIN		5
#define W_SQFT_MIN		50000
#define W_SQFT_MAX		1000000
#define W_NAME_MIN		10

 /***
 *** WR_xxx Web Returns Defines
 ***/
#define WR_RETURN_PCT	10	/* percentage of web sales that are returned */
#define WR_SHIP_LAG_MIN	2	/* lag time between receiving and returning */
#define WR_SHIP_LAG_MAX	12

 /***
 *** WEB_xxx Web Site Defines
 ***/
#define WEB_START_DATE			DATE_MINIMUM	/* range of open/close dates; actual dates can exceed these values */
#define WEB_END_DATE			DATE_MAXIMUM	/* due to staggered start of each site */
#define WEB_DATE_STAGGER		17				/* time between site creation on leading/trailing edge */
#define WEB_PAGES_PER_SITE		123				/* number of pages on a web site */
/* some of the web sites are completely replaced in the date range. */
#define WEB_MORTALITY			50				/* percentage of sites that "die" between start and end */
#define WEB_IS_REPLACED(j)		((j % (100 / WEB_MORTALITY)) == 0)	/* does this site get replaced? */
#define WEB_IS_REPLACEMENT(j)	((j / (100 / WEB_MORTALITY)) % 2)	/* is this the replacement? */

/***
 *** SOURCE SCHEMA CONSTANTS
 ***/
#define DAYS_PER_UPDATE	3

 /***
 *** RS_xxx: Row and column sizes
 ***/
/* sizes used in various tables */
#define RS_BKEY				16
/* table-specific sizes */

#define RS_BRND_NAME		50
#define RS_C_SALUTATION		5
#define RS_C_FIRST_NAME		20
#define RS_C_LAST_NAME		30
#define RS_C_BIRTH_COUNTRY	20
#define RS_C_LOGIN			13
#define RS_C_PASSWORD		13
#define RS_C_EMAIL			50
#define RS_C_PRIMARY_MACHINE_ID		15
#define RS_C_SECONDARY_MACHINE_ID	15
#define RS_CA_SUITE_NUMBER	10
#define RS_CA_STREET_NAME	60
#define RS_CA_STREET_TYPE	15
#define RS_CA_CITY			60
#define RS_CA_COUNTY		30
#define RS_CA_STATE			2
#define RS_CA_COUNTRY		20
#define RS_CA_ZIP			10
#define RS_CA_LOCATION_TYPE	20
#define RS_CATG_DESC		20
#define RS_CC_NAME			50
#define RS_CC_CLASS			50
#define RS_CC_HOURS			20
#define RS_CC_MANAGER		40
#define RS_CC_MARKET_MANAGER	40
#define RS_CC_MARKET_CLASS	50
#define RS_CC_MARKET_DESC	100
#define RS_CC_DIVISION_NAME	50
#define RS_CC_COMPANY_NAME	60
#define RS_CC_SUITE_NUM		10
#define RS_CC_STREET_NAME	60
#define RS_CC_STREET_TYPE	15
#define RS_CC_CITY			60
#define RS_CC_COUNTY		30
#define RS_CC_STATE			2
#define RS_CC_COUNTRY		20
#define RS_CC_ZIP			10
#define RS_CD_GENDER		1
#define RS_CD_MARITAL_STATUS	1
#define RS_CD_EDUCATION_STATUS	20
#define RS_CD_CREDIT_RATING	10
#define RS_CP_DEPARTMENT	20
#define RS_CLAS_DESC		100
#define RS_CMPY_NAME	50
#define RS_CP_DESCRIPTION	100
#define RS_CP_TYPE			100
#define RS_CTGR_NAME		25
#define RS_CTGR_DESC		100
#define RS_CUST_CREDIT		100
#define RS_D_DAY_NAME		4
#define RS_D_QUARTER_NAME	4
#define RS_DVSN_NAME		50
#define RS_HD_BUY_POTENTIAL	7
#define RS_I_ITEM_DESC		200
#define RS_I_BRAND			50
#define RS_I_SUBCLASS		50
#define RS_I_CLASS			50
#define RS_I_SUBCATEGORY	50
#define RS_I_CATEGORY		50
#define RS_I_MANUFACT		50
#define RS_I_SIZE			20
#define RS_I_FORMULATION	20
#define RS_I_FLAVOR			20
#define RS_I_UNITS			10
#define	RS_I_CONTAINER		10
#define RS_I_PRODUCT_NAME	50
#define RS_MANF_NAME		50
#define RS_MNGR_NAME		50
#define RS_P_PROMO_NAME		50
#define RS_P_CHANNEL_DETAILS	100
#define RS_P_PURPOSE		15
#define RS_PB_DESCRIPTION	100
#define RS_PLIN_COMMENT		100
#define RS_PROD_NAME		100
#define RS_PROD_TYPE		100
#define RS_R_REASON_DESCRIPTION	100
#define RS_STORE_NAME		50
#define RS_STORE_HOURS			20
#define RS_S_STORE_MANAGER		40
#define RS_S_GEOGRAPHY_CLASS	100
#define RS_S_MARKET_DESC	100
#define RS_S_MARKET_MANAGER		40
#define RS_S_DIVISION_NAME	50
#define RS_S_COMPANY_NAME	50
#define RS_S_SUITE_NUM		10
#define RS_S_STREET_NAME	60
#define RS_S_STREET_TYPE	15
#define RS_S_CITY			60
#define RS_S_STATE			2
#define RS_S_COUNTY			30
#define RS_S_COUNTRY		30
#define RS_S_ZIP			10
#define RS_SM_TYPE			30
#define RS_SM_CODE			10
#define RS_SM_CONTRACT		20
#define RS_SM_CARRIER		20
#define RS_SBCT_NAME		100
#define RS_SBCT_DESC		100
#define RS_SUBC_NAME		100
#define RS_SUBC_DESC		100
#define RS_T_AM_PM			2
#define RS_T_SHIFT			20
#define RS_T_SUB_SHIFT		20
#define RS_T_MEAL_TIME		20
#define RS_W_WAREHOUSE_NAME	20
#define RS_W_STREET_NAME	60
#define RS_W_SUITE_NUM		10
#define RS_W_STREET_TYPE	15
#define RS_W_CITY			60
#define RS_W_COUNTY			30
#define RS_W_STATE			2
#define RS_W_COUNTRY		20
#define RS_W_ZIP			10
#define RS_WEB_MANAGER			50
#define RS_WEB_NAME			50
#define RS_WEB_CLASS		50
#define RS_WEB_MARKET_CLASS	50
#define RS_WEB_MARKET_DESC		100
#define RS_WEB_MARKET_MANAGER		40
#define RS_WEB_COMPANY_NAME	100
#define RS_WEB_SUITE_NUMBER	10
#define RS_WEB_STREET_NAME	60
#define RS_WEB_STREET_TYPE	15
#define RS_WEB_CITY			60
#define RS_WEB_COUNTY		30
#define RS_WEB_STATE		2
#define RS_WEB_COUNTRY		20
#define RS_WEB_ZIP			10
#define RS_WP_URL			100
#define RS_WEB_TYPE			50
#define RS_WRHS_DESC		100
#define RS_WORD_COMMENT		100
#define RS_ZIPG_ZIP			5
#endif /* CONSTANTS_H */
