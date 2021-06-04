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
#ifndef W_WEB_SITE_H
#define W_WEB_SITE_H

#include "address.h"
#include "constants.h"
#include "decimal.h"

#define WEB_MIN_TAX_PERCENTAGE "0.00"
#define WEB_MAX_TAX_PERCENTAGE "0.12"

/*
 * WEB_SITE table structure
 */
struct W_WEB_SITE_TBL {
	ds_key_t web_site_sk;
	char web_site_id[RS_BKEY + 1];
	ds_key_t web_rec_start_date_id;
	ds_key_t web_rec_end_date_id;
	char web_name[RS_WEB_NAME + 1];
	ds_key_t web_open_date;
	ds_key_t web_close_date;
	char web_class[RS_WEB_CLASS + 1];
	char web_manager[RS_WEB_MANAGER + 1];
	int web_market_id;
	char web_market_class[RS_WEB_MARKET_CLASS + 1];
	char web_market_desc[RS_WEB_MARKET_DESC + 1];
	char web_market_manager[RS_WEB_MARKET_MANAGER + 1];
	int web_company_id;
	char web_company_name[RS_WEB_COMPANY_NAME + 1];
	ds_addr_t web_address;
	decimal_t web_tax_percentage;
};

int mk_w_web_site(void *info_arr, ds_key_t kIndex);
#endif
