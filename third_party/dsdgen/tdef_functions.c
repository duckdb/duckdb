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
#include "s_brand.h"
#include "s_customer_address.h"
#include "s_call_center.h"
#include "s_catalog.h"
#include "s_catalog_order.h"
#include "s_catalog_order_lineitem.h"
#include "s_catalog_page.h"
#include "s_catalog_promotional_item.h"
#include "s_catalog_returns.h"
#include "s_category.h"
#include "s_class.h"
#include "s_company.h"
#include "s_customer.h"
#include "s_division.h"
#include "s_inventory.h"
#include "s_item.h"
#include "s_manager.h"
#include "s_manufacturer.h"
#include "s_market.h"
#include "s_pline.h"
#include "s_product.h"
#include "s_promotion.h"
#include "s_purchase.h"
#include "s_reason.h"
#include "s_store.h"
#include "s_store_promotional_item.h"
#include "s_store_returns.h"
#include "s_subcategory.h"
#include "s_subclass.h"
#include "s_warehouse.h"
#include "s_web_order.h"
#include "s_web_order_lineitem.h"
#include "s_web_page.h"
#include "s_web_promotional_item.h"
#include "s_web_returns.h"
#include "s_web_site.h"
#include "s_zip_to_gmt.h"
#include "tdef_functions.h"
#include "validate.h"

table_func_t w_tdef_funcs[] = { 
{"call_center",    mk_w_call_center, {pr_w_call_center, ld_w_call_center}, validateSCD},
{"catalog_page",   mk_w_catalog_page, {pr_w_catalog_page, ld_w_catalog_page}, validateGeneric}, 
{"catalog_returns",mk_w_catalog_returns, {pr_w_catalog_returns, ld_w_catalog_returns}, NULL}, 
{"catalog_sales",  mk_w_catalog_sales, {pr_w_catalog_sales, ld_w_catalog_sales}, vld_w_catalog_sales},
{"customer",       mk_w_customer, {pr_w_customer, ld_w_customer}, validateGeneric},
{"customer_address",mk_w_customer_address, {pr_w_customer_address, ld_w_customer_address}, validateGeneric},
{"customer_demographics",mk_w_customer_demographics, {pr_w_customer_demographics, ld_w_customer_demographics}, validateGeneric},
{"date",           mk_w_date, {pr_w_date, ld_w_date}, vld_w_date},
{"household_demographics",mk_w_household_demographics, {pr_w_household_demographics, ld_w_household_demographics}, validateGeneric},
{"income_band",    mk_w_income_band, {pr_w_income_band, ld_w_income_band}, validateGeneric},
{"inventory",      mk_w_inventory, {pr_w_inventory, ld_w_inventory}, validateGeneric},
{"item",           mk_w_item, {pr_w_item, ld_w_item}, validateSCD},
{"promotion",      mk_w_promotion, {pr_w_promotion, ld_w_promotion}, validateGeneric},
{"reason",         mk_w_reason, {pr_w_reason, ld_w_reason}, validateGeneric},
{"ship_mode",      mk_w_ship_mode, {pr_w_ship_mode, ld_w_ship_mode}, validateGeneric},
{"store",          mk_w_store, {pr_w_store, ld_w_store}, validateSCD},
{"store_returns",  mk_w_store_returns, {pr_w_store_returns, ld_w_store_returns}, NULL},
{"store_sales",    mk_w_store_sales, {pr_w_store_sales, ld_w_store_sales}, vld_w_store_sales},
{"time",           mk_w_time, {pr_w_time, ld_w_time}, validateGeneric},
{"warehouse",      mk_w_warehouse, {pr_w_warehouse, ld_w_warehouse}, validateGeneric},
{"web_page",       mk_w_web_page, {pr_w_web_page, ld_w_web_page}, validateSCD},
{"web_returns",    mk_w_web_returns, {pr_w_web_returns, ld_w_web_returns}, NULL},
{"web_sales",      mk_w_web_sales, {pr_w_web_sales, ld_w_web_sales}, vld_web_sales},
{"web_site",       mk_w_web_site, {pr_w_web_site, ld_w_web_site}, validateSCD},
{"dbgen_version",  mk_dbgen_version, {pr_dbgen_version, ld_dbgen_version}, NULL},
{NULL}
};


/* source schema tables below this point */
table_func_t s_tdef_funcs[] = { 
{"s_brand",  mk_s_brand, {pr_s_brand, ld_s_brand}, validateGeneric},
{"s_customer_address",  mk_w_customer_address, {pr_s_customer_address, ld_s_customer_address}, vld_s_customer_address},
{"s_call_center",  mk_s_call_center, {pr_s_call_center, ld_s_call_center}, validateGeneric},
{"s_catalog",  mk_s_catalog, {pr_s_catalog, ld_s_catalog}, validateGeneric},
{"s_catalog_order",  mk_s_catalog_order, {pr_s_catalog_order, ld_s_catalog_order}, vld_s_catalog_order},
{"s_catalog_order_lineitem",  mk_s_catalog_order_lineitem, {pr_s_catalog_order_lineitem, ld_s_catalog_order_lineitem}, NULL},
{"s_catalog_page",  mk_s_catalog_page, {pr_s_catalog_page, ld_s_catalog_page}, vld_s_catalog_page},
{"s_catalog_promotional_item",  mk_s_catalog_promotional_item, {pr_s_catalog_promotional_item, ld_s_catalog_promotional_item}, validateGeneric},
{"s_catalog_returns",  mk_s_catalog_returns, {pr_s_catalog_returns, ld_s_catalog_returns}, NULL},
{"s_category",  mk_s_category, {pr_s_category, ld_s_category}, validateGeneric},
{"s_class",  mk_s_class, {pr_s_class, ld_s_class}, validateGeneric},
{"s_company",  mk_s_company, {pr_s_company, ld_s_company}, validateGeneric},
{"s_customer",  mk_s_customer, {pr_s_customer, ld_s_customer}, validateGeneric},
{"s_division",  mk_s_division, {pr_s_division, ld_s_division}, validateGeneric},
{"s_inventory",  mk_s_inventory, {pr_s_inventory, ld_s_inventory}, validateGeneric},
{"s_item",  mk_s_item, {pr_s_item, ld_s_item}, vld_s_item},
{"s_manager",  mk_s_manager, {pr_s_manager, ld_s_manager}, validateGeneric},
{"s_manufacturer",  mk_s_manufacturer, {pr_s_manufacturer, ld_s_manufacturer}, validateGeneric},
{"s_market",  mk_s_market, {pr_s_market, ld_s_market}, validateGeneric},
{"s_product",  mk_s_product, {pr_s_product, ld_s_product}, validateGeneric},
{"s_promotion",  mk_s_promotion, {pr_s_promotion, ld_s_promotion}, vld_s_promotion},
{"s_purchase",  mk_s_purchase, {pr_s_purchase, ld_s_purchase}, vld_s_purchase},
{"s_purchase_lineitem",  mk_s_pline, {pr_s_pline, ld_s_pline}, NULL},
{"s_reason",  mk_s_reason, {pr_s_reason, ld_s_reason}, validateGeneric},
{"s_store",  mk_s_store, {pr_s_store, ld_s_store}, validateGeneric},
{"s_store_promotional_item",  mk_s_store_promotional_item, {pr_s_store_promotional_item, ld_s_store_promotional_item}, validateGeneric},
{"s_store_returns",  mk_s_store_returns, {pr_s_store_returns, ld_s_store_returns}, NULL},
{"s_subcategory",  mk_s_subcategory, {pr_s_subcategory, ld_s_subcategory}, validateGeneric},
{"s_subclass",  mk_s_subclass, {pr_s_subclass, ld_s_subclass}, validateGeneric},
{"s_warehouse",  mk_s_warehouse, {pr_s_warehouse, ld_s_warehouse}, validateGeneric},
{"s_web_order",  mk_s_web_order, {pr_s_web_order, ld_s_web_order}, vld_s_web_order},
{"s_web_order_lineitem",  mk_s_web_order_lineitem, {pr_s_web_order_lineitem, ld_s_web_order_lineitem}, NULL},
{"s_web_page",  mk_s_web_page, {pr_s_web_page, ld_s_web_page}, vld_s_web_page},
{"s_web_promotional_item",  mk_s_web_promotional_item, {pr_s_web_promotional_item, ld_s_web_promotional_item}, validateGeneric},
{"s_web_returns",  mk_s_web_return, {pr_s_web_return, ld_s_web_return}, NULL},
{"s_web_site",  mk_w_web_site, {pr_s_web_site, ld_s_web_site}, validateGeneric},
{"s_zip_to_gmt",  mk_s_zip, {pr_s_zip, ld_s_zip}, vld_s_zip},
{NULL}
};

table_func_t *
getTdefFunctionsByNumber(nTable)
{
   if (nTable >= S_BRAND)
      return(&s_tdef_funcs[nTable - S_BRAND]);
   return(&w_tdef_funcs[nTable]);
}

