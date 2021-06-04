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

#include "porting.h"
#include "pricing.h"

/*
 * WEB_SALES table structure
 */
struct W_WEB_SALES_TBL {
	ds_key_t ws_sold_date_sk;
	ds_key_t ws_sold_time_sk;
	ds_key_t ws_ship_date_sk;
	ds_key_t ws_item_sk;
	ds_key_t ws_bill_customer_sk;
	ds_key_t ws_bill_cdemo_sk;
	ds_key_t ws_bill_hdemo_sk;
	ds_key_t ws_bill_addr_sk;
	ds_key_t ws_ship_customer_sk;
	ds_key_t ws_ship_cdemo_sk;
	ds_key_t ws_ship_hdemo_sk;
	ds_key_t ws_ship_addr_sk;
	ds_key_t ws_web_page_sk;
	ds_key_t ws_web_site_sk;
	ds_key_t ws_ship_mode_sk;
	ds_key_t ws_warehouse_sk;
	ds_key_t ws_promo_sk;
	ds_key_t ws_order_number;
	ds_pricing_t ws_pricing;
};

/***
*** WS_xxx Web Sales Defines
***/
#define WS_QUANTITY_MAX  "100"
#define WS_MARKUP_MAX    "2.00"
#define WS_DISCOUNT_MAX  "1.00"
#define WS_WHOLESALE_MAX "100.00"
#define WS_COUPON_MAX    "0.50"
#define WS_GIFT_PCT                                                                                                    \
	7                        /* liklihood that a purchase is shipped to someone else                                   \
	                          */
#define WS_ITEMS_PER_ORDER 9 /* number of lineitems in an order */
#define WS_MIN_SHIP_DELAY  1 /* time between order date and ship date */
#define WS_MAX_SHIP_DELAY  120

int mk_w_web_sales(void *info_arr, ds_key_t kIndex);
int vld_web_sales(int nTable, ds_key_t kRow, int *Permutation);
