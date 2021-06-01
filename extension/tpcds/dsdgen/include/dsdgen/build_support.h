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
#ifndef BUILD_SUPPORT_H
#define BUILD_SUPPORT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "decimal.h"
#include "date.h"
#include "dist.h"
#include "columns.h"
#include "pricing.h"

void bitmap_to_dist(void *pDest, char *distname, ds_key_t *modulus, int vset, int stream);
void dist_to_bitmap(int *pDest, char *szDistName, int nValue, int nWeight, int nStream);
void random_to_bitmap(int *pDest, int nDist, int nMin, int nMax, int nMean, int nStream);
int city_hash(int nTable, char *city);
void hierarchy_item(int h_level, ds_key_t *id, char **name, ds_key_t kIndex);
ds_key_t mk_join(int from_tbl, int to_tbl, ds_key_t ref_key);
ds_key_t getCatalogNumberFromPage(ds_key_t kPageNumber);
void mk_word(char *dest, char *syl_set, ds_key_t src, int char_cnt, int col);
int set_locale(int nRegion, decimal_t *longitude, decimal_t *latitude);
int adj_time(ds_key_t *res_date, ds_key_t *res_time, ds_key_t base_date, ds_key_t base_time, ds_key_t offset_key,
             int tabid);
void mk_bkey(char *szDest, ds_key_t kPrimary, int nStream);
int embed_string(char *szDest, char *szDist, int nValue, int nWeight, int nStream);
int SetScaleIndex(char *szName, char *szValue);
int mk_companyname(char *dest, int nTable, int nCompany);
void setUpdateDateRange(int nTable, date_t *pMinDate, date_t *pMaxDate);

#ifdef __cplusplus
};
#endif

#endif /* BUILD_SUPPORT_H */
