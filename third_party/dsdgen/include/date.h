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
#ifndef R_DATE_H
#define R_DATE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "mathops.h"

typedef struct DATE_T {
	int flags;
	int year;
	int month;
	int day;
	int julian;
} date_t;

date_t *mk_date(void);

int jtodt(date_t *dest, int i);
int strtodt(date_t *dest, char *s);
int strtotime(char *str);

char *dttostr(date_t *d);
int dttoj(date_t *d);

int date_t_op(date_t *dest, int o, date_t *d1, date_t *d2);
int set_dow(date_t *d);
int is_leap(int year);
int day_number(date_t *d);
int date_part(date_t *d, int p);
int set_outfile(int i);
int getDateWeightFromJulian(int jDay, int nDistribution);
#define CENTURY_SHIFT 20 /* years before this are assumed to be 2000's */
/*
 * DATE OPERATORS
 */
#define OP_FIRST_DOM 0x01 /* get date of first day of current month */
#define OP_LAST_DOM 0x02  /* get date of last day of current month; LY == 2/28) */
#define OP_SAME_LY 0x03   /* get date for same day/month, last year */
#define OP_SAME_LQ 0x04   /* get date for same offset in the prior quarter */

extern char *weekday_names[];

#ifdef __cplusplus
}
#endif

#endif /* R_DATE_H */
