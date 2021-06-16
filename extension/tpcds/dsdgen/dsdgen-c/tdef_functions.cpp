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
#include "w_call_center.h"
#include "w_catalog_page.h"
#include "w_catalog_returns.h"
#include "w_catalog_sales.h"
#include "w_customer.h"
#include "w_customer_address.h"
#include "w_customer_demographics.h"
#include "w_datetbl.h"
#include "w_household_demographics.h"
#include "w_income_band.h"
#include "w_inventory.h"
#include "w_item.h"
#include "w_promotion.h"
#include "w_reason.h"
#include "w_ship_mode.h"
#include "w_store.h"
#include "w_store_returns.h"
#include "w_store_sales.h"
#include "w_timetbl.h"
#include "w_warehouse.h"
#include "w_web_page.h"
#include "w_web_returns.h"
#include "w_web_sales.h"
#include "w_web_site.h"
#include "dbgen_version.h"
#include "tdef_functions.h"

table_func_t w_tdef_funcs[] = {{"call_center", mk_w_call_center, {NULL, NULL}, NULL},
                               {"catalog_page", mk_w_catalog_page, {NULL, NULL}, NULL},
                               {"catalog_returns", NULL, {NULL, NULL}, NULL},
                               {"catalog_sales", mk_w_catalog_sales, {NULL, NULL}, NULL},
                               {"customer", mk_w_customer, {NULL, NULL}, NULL},
                               {"customer_address", mk_w_customer_address, {NULL, NULL}, NULL},
                               {"customer_demographics", mk_w_customer_demographics, {NULL, NULL}, NULL},
                               {"date", mk_w_date, {NULL, NULL}, NULL},
                               {"household_demographics", mk_w_household_demographics, {NULL, NULL}, NULL},
                               {"income_band", mk_w_income_band, {NULL, NULL}, NULL},
                               {"inventory", mk_w_inventory, {NULL, NULL}, NULL},
                               {"item", mk_w_item, {NULL, NULL}, NULL},
                               {"promotion", mk_w_promotion, {NULL, NULL}, NULL},
                               {"reason", mk_w_reason, {NULL, NULL}, NULL},
                               {"ship_mode", mk_w_ship_mode, {NULL, NULL}, NULL},
                               {"store", mk_w_store, {NULL, NULL}, NULL},
                               {"store_returns", mk_w_store_returns, {NULL, NULL}, NULL},
                               {"store_sales", mk_w_store_sales, {NULL, NULL}, NULL},
                               {"time", mk_w_time, {NULL, NULL}, NULL},
                               {"warehouse", mk_w_warehouse, {NULL, NULL}, NULL},
                               {"web_page", mk_w_web_page, {NULL, NULL}, NULL},
                               {"web_returns", mk_w_web_returns, {NULL, NULL}, NULL},
                               {"web_sales", mk_w_web_sales, {NULL, NULL}, NULL},
                               {"web_site", mk_w_web_site, {NULL, NULL}, NULL},
                               {"dbgen_version", mk_dbgen_version, {NULL, NULL}, NULL},
                               {NULL}};

table_func_t *getTdefFunctionsByNumber(int nTable) {
	return (&w_tdef_funcs[nTable]);
}
