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
#ifndef W_ITEM_H
#define W_ITEM_H
#include "constants.h"

#define I_PROMO_PERCENTAGE 20 /* percent of items that have associated promotions */
#define MIN_ITEM_MARKDOWN_PCT "0.30"
#define MAX_ITEM_MARKDOWN_PCT "0.90"

/*
 * ITEM table structure
 */
struct W_ITEM_TBL {
	ds_key_t i_item_sk;
	char i_item_id[RS_BKEY + 1];
	ds_key_t i_rec_start_date_id;
	ds_key_t i_rec_end_date_id;
	char i_item_desc[RS_I_ITEM_DESC + 1];
	decimal_t i_current_price; /* list price */
	decimal_t i_wholesale_cost;
	ds_key_t i_brand_id;
	char i_brand[RS_I_BRAND + 1];
	ds_key_t i_class_id;
	char *i_class;
	ds_key_t i_category_id;
	char *i_category;
	ds_key_t i_manufact_id;
	char i_manufact[RS_I_MANUFACT + 1];
	char *i_size;
	char i_formulation[RS_I_FORMULATION + 1];
	char *i_color;
	char *i_units;
	char *i_container;
	ds_key_t i_manager_id;
	char i_product_name[RS_I_PRODUCT_NAME + 1];
	ds_key_t i_promo_sk;
};

int mk_w_item(void *info_arr, ds_key_t kIndex);
int vld_w_item(int nTable, ds_key_t kRow, int *Permutation);
#endif
