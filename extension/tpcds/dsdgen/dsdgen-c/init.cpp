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
#include "init.h"

int InitConstants::init_rand_init = 0;
int InitConstants::mk_address_init = 0;
int InitConstants::setUpdateDateRange_init = 0;
int InitConstants::mk_dbgen_version_init = 0;
int InitConstants::getCatalogNumberFromPage_init = 0;
int InitConstants::checkSeeds_init = 0;
int InitConstants::dateScaling_init = 0;
int InitConstants::mk_w_call_center_init = 0;
int InitConstants::mk_w_catalog_page_init = 0;
int InitConstants::mk_master_catalog_sales_init = 0;
int InitConstants::dectostr_init = 0;
int InitConstants::date_join_init = 0;
int InitConstants::setSCDKeys_init = 0;
int InitConstants::scd_join_init = 0;
int InitConstants::matchSCDSK_init = 0;
int InitConstants::skipDays_init = 0;
int InitConstants::mk_w_catalog_returns_init = 0;
int InitConstants::mk_detail_catalog_sales_init = 0;
int InitConstants::mk_w_customer_init = 0;
int InitConstants::mk_w_date_init = 0;
int InitConstants::mk_w_inventory_init = 0;
int InitConstants::mk_w_item_init = 0;
int InitConstants::mk_w_promotion_init = 0;
int InitConstants::mk_w_reason_init = 0;
int InitConstants::mk_w_ship_mode_init = 0;
int InitConstants::mk_w_store_returns_init = 0;
int InitConstants::mk_master_store_sales_init = 0;
int InitConstants::mk_w_store_init = 0;
int InitConstants::mk_w_web_page_init = 0;
int InitConstants::mk_w_web_returns_init = 0;
int InitConstants::mk_master_init = 0;
int InitConstants::mk_detail_init = 0;
int InitConstants::mk_w_web_site_init = 0;
int InitConstants::mk_cust_init = 0;
int InitConstants::mk_order_init = 0;
int InitConstants::mk_part_init = 0;
int InitConstants::mk_supp_init = 0;
int InitConstants::dbg_text_init = 0;
int InitConstants::find_dist_init = 0;
int InitConstants::cp_join_init = 0;
int InitConstants::web_join_init = 0;
int InitConstants::set_pricing_init = 0;
int InitConstants::init_params_init = 0;
int InitConstants::get_rowcount_init = 0;

void InitConstants::Reset() {
	init_rand_init = 0;
	mk_address_init = 0;
	setUpdateDateRange_init = 0;
	mk_dbgen_version_init = 0;
	getCatalogNumberFromPage_init = 0;
	checkSeeds_init = 0;
	dateScaling_init = 0;
	mk_w_call_center_init = 0;
	mk_w_catalog_page_init = 0;
	mk_master_catalog_sales_init = 0;
	dectostr_init = 0;
	date_join_init = 0;
	setSCDKeys_init = 0;
	scd_join_init = 0;
	matchSCDSK_init = 0;
	skipDays_init = 0;
	mk_w_catalog_returns_init = 0;
	mk_detail_catalog_sales_init = 0;
	mk_w_customer_init = 0;
	mk_w_date_init = 0;
	mk_w_inventory_init = 0;
	mk_w_item_init = 0;
	mk_w_promotion_init = 0;
	mk_w_reason_init = 0;
	mk_w_ship_mode_init = 0;
	mk_w_store_returns_init = 0;
	mk_master_store_sales_init = 0;
	mk_w_store_init = 0;
	mk_w_web_page_init = 0;
	mk_w_web_returns_init = 0;
	mk_master_init = 0;
	mk_detail_init = 0;
	mk_w_web_site_init = 0;
	mk_cust_init = 0;
	mk_order_init = 0;
	mk_part_init = 0;
	mk_supp_init = 0;
	dbg_text_init = 0;
	find_dist_init = 0;
	cp_join_init = 0;
	web_join_init = 0;
	set_pricing_init = 0;
	init_params_init = 0;
	get_rowcount_init = 0;
}
