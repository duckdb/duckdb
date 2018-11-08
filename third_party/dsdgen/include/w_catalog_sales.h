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
#ifndef W_CATALOG_SALES_H
#define W_CATALOG_SALES_H

#include "pricing.h"

/*
 * CATALOG_SALES table structure
 */
struct W_CATALOG_SALES_TBL {
	ds_key_t cs_sold_date_sk;
	ds_key_t cs_sold_time_sk;
	ds_key_t cs_ship_date_sk;
	ds_key_t cs_bill_customer_sk;
	ds_key_t cs_bill_cdemo_sk;
	ds_key_t cs_bill_hdemo_sk;
	ds_key_t cs_bill_addr_sk;
	ds_key_t cs_ship_customer_sk;
	ds_key_t cs_ship_cdemo_sk;
	ds_key_t cs_ship_hdemo_sk;
	ds_key_t cs_ship_addr_sk;
	ds_key_t cs_call_center_sk;
	ds_key_t cs_catalog_page_sk;
	ds_key_t cs_ship_mode_sk;
	ds_key_t cs_warehouse_sk;
	ds_key_t cs_sold_item_sk;
	ds_key_t cs_promo_sk;
	ds_key_t cs_order_number;
	ds_pricing_t cs_pricing;
};

int mk_w_catalog_sales(void *info_arr, ds_key_t index);
int vld_w_catalog_sales(int nTable, ds_key_t kRow, int *Permutation);
#endif
