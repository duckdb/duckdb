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
#ifndef PRICING_H
#define PRICING_H
#include "decimal.h"

typedef struct DS_PRICING_T {
	decimal_t wholesale_cost;
	decimal_t list_price;
	decimal_t sales_price;
	int quantity;
	decimal_t ext_discount_amt;
	decimal_t ext_sales_price;
	decimal_t ext_wholesale_cost;
	decimal_t ext_list_price;
	decimal_t tax_pct;
	decimal_t ext_tax;
	decimal_t coupon_amt;
	decimal_t ship_cost;
	decimal_t ext_ship_cost;
	decimal_t net_paid;
	decimal_t net_paid_inc_tax;
	decimal_t net_paid_inc_ship;
	decimal_t net_paid_inc_ship_tax;
	decimal_t net_profit;
	decimal_t refunded_cash;
	decimal_t reversed_charge;
	decimal_t store_credit;
	decimal_t fee;
	decimal_t net_loss;
} ds_pricing_t;

typedef struct DS_LIMITS_T {
	int nId;
	char *szQuantity;
	char *szMarkUp;
	char *szDiscount;
	char *szWholesale;
	char *szCoupon;
} ds_limits_t;

void set_pricing(int nTabId, ds_pricing_t *pPricing);
#endif
