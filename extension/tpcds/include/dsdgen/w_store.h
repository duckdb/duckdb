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
#ifndef W_STORE_H
#define W_STORE_H

#include "address.h"
#include "decimal.h"

#define RS_W_STORE_NAME          50
#define RS_W_STORE_MGR           40
#define RS_W_MARKET_MGR          40
#define RS_W_MARKET_DESC         100
#define STORE_MIN_TAX_PERCENTAGE "0.00"
#define STORE_MAX_TAX_PERCENTAGE "0.11"

/*
 * STORE table structure
 */
struct W_STORE_TBL {
	ds_key_t store_sk;
	char store_id[RS_BKEY + 1];
	ds_key_t rec_start_date_id;
	ds_key_t rec_end_date_id;
	ds_key_t closed_date_id;
	char store_name[RS_W_STORE_NAME + 1];
	int employees;
	int floor_space;
	char *hours;
	char store_manager[RS_W_STORE_MGR + 1];
	int market_id;
	decimal_t dTaxPercentage;
	char *geography_class;
	char market_desc[RS_W_MARKET_DESC + 1];
	char market_manager[RS_W_MARKET_MGR + 1];
	ds_key_t division_id;
	char *division_name;
	ds_key_t company_id;
	char *company_name;
	ds_addr_t address;
};

/***
 *** STORE_xxx Store Defines
 ***/
#define STORE_MIN_DAYS_OPEN  5
#define STORE_MAX_DAYS_OPEN  500
#define STORE_CLOSED_PCT     30
#define STORE_MIN_REV_GROWTH "-0.05"
#define STORE_MAX_REV_GROWTH "0.50"
#define STORE_DESC_MIN       15

int mk_w_store(void *info_arr, ds_key_t kIndex);

#endif
