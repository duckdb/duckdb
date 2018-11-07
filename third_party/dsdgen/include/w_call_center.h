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
#ifndef W_CALL_CENTER_H
#define W_CALL_CENTER_H
#include "constants.h"
#include "pricing.h"
#include "address.h"
#include "decimal.h"
#include "date.h"

#define MIN_CC_TAX_PERCENTAGE	"0.00"
#define MAX_CC_TAX_PERCENTAGE	"0.12"

/*
 * CALL_CENTER table structure 
 */
struct CALL_CENTER_TBL {
	ds_key_t	cc_call_center_sk;
	char		cc_call_center_id[RS_BKEY + 1]; 
	ds_key_t	cc_rec_start_date_id;
	ds_key_t	cc_rec_end_date_id;
	ds_key_t	cc_closed_date_id;
	ds_key_t	cc_open_date_id;
	char		cc_name[RS_CC_NAME + 1];
	char		*cc_class;
	int			cc_employees;
	int			cc_sq_ft;
	char		*cc_hours;
	char		cc_manager[RS_CC_MANAGER + 1];
	int			cc_market_id;
	char		cc_market_class[RS_CC_MARKET_CLASS + 1];
	char		cc_market_desc[RS_CC_MARKET_DESC + 1];
	char		cc_market_manager[RS_CC_MARKET_MANAGER + 1];
	int			cc_division_id;
	char		cc_division_name[RS_CC_DIVISION_NAME + 1];
	int			cc_company;
	char		cc_company_name[RS_CC_COMPANY_NAME + 1];
	ds_addr_t	cc_address;
	decimal_t	cc_tax_percentage;
};

int mk_w_call_center(void *pDest, ds_key_t kIndex);
int pr_w_call_center(void *pSrc);
int ld_w_call_center(void *r);

#endif

