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
#ifndef W_CATALOG_RETURNS_H
#define W_CATALOG_RETURNS_H

#include "pricing.h"

/*
 * CATALOG_RETURNS table structure
 */
struct W_CATALOG_RETURNS_TBL {
	ds_key_t cr_returned_date_sk;
	ds_key_t cr_returned_time_sk;
	ds_key_t cr_item_sk;
	ds_key_t cr_refunded_customer_sk;
	ds_key_t cr_refunded_cdemo_sk;
	ds_key_t cr_refunded_hdemo_sk;
	ds_key_t cr_refunded_addr_sk;
	ds_key_t cr_returning_customer_sk;
	ds_key_t cr_returning_cdemo_sk;
	ds_key_t cr_returning_hdemo_sk;
	ds_key_t cr_returning_addr_sk;
	ds_key_t cr_call_center_sk;
	ds_key_t cr_catalog_page_sk;
	ds_key_t cr_ship_mode_sk;
	ds_key_t cr_warehouse_sk;
	ds_key_t cr_reason_sk;
	ds_key_t cr_order_number;
	ds_pricing_t cr_pricing;
	decimal_t cr_fee;
	decimal_t cr_refunded_cash;
	decimal_t cr_reversed_charge;
	decimal_t cr_store_credit;
	decimal_t cr_net_loss;
};

int mk_w_catalog_returns(void *row, ds_key_t kIndex);

#endif
