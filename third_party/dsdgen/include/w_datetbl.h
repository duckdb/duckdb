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
/*
 * DATE table structure 
 */
#ifndef W_DATETBL_H
#define W_DATETBL_H
#include "constants.h"
struct W_DATE_TBL {
ds_key_t	d_date_sk;
char		d_date_id[RS_BKEY + 1];
/* this is generated at output from d_date_sk */
/* date_t		d_date; */
int			d_month_seq;
int			d_week_seq;
int			d_quarter_seq;
int			d_year;
int			d_dow;
int			d_moy;
int			d_dom;
int			d_qoy;
int			d_fy_year;
int			d_fy_quarter_seq;
int			d_fy_week_seq;
char		*d_day_name;
/* char		d_quarter_name[RS_D_QUARTER_NAME + 1]; derived at print time */
int			d_holiday;
int			d_weekend;
int			d_following_holiday;
int			d_first_dom;
int			d_last_dom;
int			d_same_day_ly;
int			d_same_day_lq;
int			d_current_day;
int			d_current_week;
int			d_current_month;
int			d_current_quarter;
int			d_current_year;
};

int mk_w_date(void *pDest, ds_key_t kIndex);
int pr_w_date(void *pSrc);
int ld_w_date(void *pSrc);
int vld_w_date(int nTable, ds_key_t kRow, int *Permutation);
#endif


