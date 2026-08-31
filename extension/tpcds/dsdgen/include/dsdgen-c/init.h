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

#ifndef DS_INIT_H
#define DS_INIT_H

struct InitConstants {
	static thread_local int init_rand_init;
	static thread_local int mk_address_init;
	static thread_local int setUpdateDateRange_init;
	static thread_local int mk_dbgen_version_init;
	static thread_local int getCatalogNumberFromPage_init;
	static thread_local int checkSeeds_init;
	static thread_local int dateScaling_init;
	static thread_local int mk_w_call_center_init;
	static thread_local int mk_w_catalog_page_init;
	static thread_local int mk_master_catalog_sales_init;
	static thread_local int dectostr_init;
	static thread_local int date_join_init;
	static thread_local int setSCDKeys_init;
	static thread_local int scd_join_init;
	static thread_local int matchSCDSK_init;
	static thread_local int skipDays_init;
	static thread_local int mk_w_catalog_returns_init;
	static thread_local int mk_detail_catalog_sales_init;
	static thread_local int mk_w_customer_init;
	static thread_local int mk_w_date_init;
	static thread_local int mk_w_inventory_init;
	static thread_local int mk_w_item_init;
	static thread_local int mk_w_promotion_init;
	static thread_local int mk_w_reason_init;
	static thread_local int mk_w_ship_mode_init;
	static thread_local int mk_w_store_returns_init;
	static thread_local int mk_master_store_sales_init;
	static thread_local int mk_w_store_init;
	static thread_local int mk_w_web_page_init;
	static thread_local int mk_w_web_returns_init;
	static thread_local int mk_master_init;
	static thread_local int mk_detail_init;
	static thread_local int mk_w_web_site_init;
	static thread_local int mk_cust_init;
	static thread_local int mk_order_init;
	static thread_local int mk_part_init;
	static thread_local int mk_supp_init;
	static thread_local int dbg_text_init;
	static thread_local int find_dist_init;
	static thread_local int cp_join_init;
	static thread_local int web_join_init;
	static thread_local int set_pricing_init;
	static thread_local int init_params_init;
	static thread_local int get_rowcount_init;

	static void Reset();
};

#endif
