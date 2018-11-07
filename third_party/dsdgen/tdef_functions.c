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
#include "validate.h"

table_func_t w_tdef_funcs[] = {
    {"call_center",
     mk_w_call_center,
     {pr_w_call_center, ld_w_call_center},
     validateSCD},
    {"catalog_page",
     mk_w_catalog_page,
     {pr_w_catalog_page, ld_w_catalog_page},
     validateGeneric},
    {"catalog_returns",
     mk_w_catalog_returns,
     {pr_w_catalog_returns, ld_w_catalog_returns},
     NULL},
    {"catalog_sales",
     mk_w_catalog_sales,
     {pr_w_catalog_sales, ld_w_catalog_sales},
     vld_w_catalog_sales},
    {"customer",
     mk_w_customer,
     {pr_w_customer, ld_w_customer},
     validateGeneric},
    {"customer_address",
     mk_w_customer_address,
     {pr_w_customer_address, ld_w_customer_address},
     validateGeneric},
    {"customer_demographics",
     mk_w_customer_demographics,
     {pr_w_customer_demographics, ld_w_customer_demographics},
     validateGeneric},
    {"date", mk_w_date, {pr_w_date, ld_w_date}, vld_w_date},
    {"household_demographics",
     mk_w_household_demographics,
     {pr_w_household_demographics, ld_w_household_demographics},
     validateGeneric},
    {"income_band",
     mk_w_income_band,
     {pr_w_income_band, ld_w_income_band},
     validateGeneric},
    {"inventory",
     mk_w_inventory,
     {pr_w_inventory, ld_w_inventory},
     validateGeneric},
    {"item", mk_w_item, {pr_w_item, ld_w_item}, validateSCD},
    {"promotion",
     mk_w_promotion,
     {pr_w_promotion, ld_w_promotion},
     validateGeneric},
    {"reason", mk_w_reason, {pr_w_reason, ld_w_reason}, validateGeneric},
    {"ship_mode",
     mk_w_ship_mode,
     {pr_w_ship_mode, ld_w_ship_mode},
     validateGeneric},
    {"store", mk_w_store, {pr_w_store, ld_w_store}, validateSCD},
    {"store_returns",
     mk_w_store_returns,
     {pr_w_store_returns, ld_w_store_returns},
     NULL},
    {"store_sales",
     mk_w_store_sales,
     {pr_w_store_sales, ld_w_store_sales},
     vld_w_store_sales},
    {"time", mk_w_time, {pr_w_time, ld_w_time}, validateGeneric},
    {"warehouse",
     mk_w_warehouse,
     {pr_w_warehouse, ld_w_warehouse},
     validateGeneric},
    {"web_page", mk_w_web_page, {pr_w_web_page, ld_w_web_page}, validateSCD},
    {"web_returns",
     mk_w_web_returns,
     {pr_w_web_returns, ld_w_web_returns},
     NULL},
    {"web_sales",
     mk_w_web_sales,
     {pr_w_web_sales, ld_w_web_sales},
     vld_web_sales},
    {"web_site", mk_w_web_site, {pr_w_web_site, ld_w_web_site}, validateSCD},
    {"dbgen_version",
     mk_dbgen_version,
     {pr_dbgen_version, ld_dbgen_version},
     NULL},
    {NULL}};


table_func_t *getTdefFunctionsByNumber(nTable) {
	return (&w_tdef_funcs[nTable]);
}
