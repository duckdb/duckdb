/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under extension/tpch/dbgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */
/*
 * general definitions and control information for the DSS data types
 * and function prototypes
 */

#pragma once

/*
 * typedefs
 */
typedef struct {
	DSS_HUGE custkey;
	char name[C_NAME_LEN + 3];
	char address[C_ADDR_MAX + 1];
	int alen;
	DSS_HUGE nation_code;
	char phone[PHONE_LEN + 1];
	DSS_HUGE acctbal;
	char mktsegment[MAXAGG_LEN + 1];
	char comment[C_CMNT_MAX + 1];
	int clen;
} customer_t;
/* customers.c */
long mk_cust PROTO((DSS_HUGE n_cust, customer_t *c, DBGenContext *ctx));
int pr_cust PROTO((customer_t * c, int mode));
int ld_cust PROTO((customer_t * c, int mode));

typedef struct {
	DSS_HUGE okey;
	DSS_HUGE partkey;
	DSS_HUGE suppkey;
	DSS_HUGE lcnt;
	DSS_HUGE quantity;
	DSS_HUGE eprice;
	DSS_HUGE discount;
	DSS_HUGE tax;
	char rflag[1];
	char lstatus[1];
	char cdate[DATE_LEN];
	char sdate[DATE_LEN];
	char rdate[DATE_LEN];
	char shipinstruct[MAXAGG_LEN + 1];
	char shipmode[MAXAGG_LEN + 1];
	char comment[L_CMNT_MAX + 1];
	int clen;
} line_t;

typedef struct {
	DSS_HUGE okey;
	DSS_HUGE custkey;
	char orderstatus;
	DSS_HUGE totalprice;
	char odate[DATE_LEN];
	char opriority[MAXAGG_LEN + 1];
	char clerk[O_CLRK_LEN + 1];
	long spriority;
	DSS_HUGE lines;
	char comment[O_CMNT_MAX + 1];
	int clen;
	line_t l[O_LCNT_MAX];
} order_t;

/* order.c */
long mk_order PROTO((DSS_HUGE index, order_t *o, DBGenContext *ctx, long upd_num));
int pr_order PROTO((order_t * o, int mode));
int ld_order PROTO((order_t * o, int mode));
void mk_sparse PROTO((DSS_HUGE index, DSS_HUGE *ok, long seq));

typedef struct {
	DSS_HUGE partkey;
	DSS_HUGE suppkey;
	DSS_HUGE qty;
	DSS_HUGE scost;
	char comment[PS_CMNT_MAX + 1];
	int clen;
} partsupp_t;

typedef struct {
	DSS_HUGE partkey;
	char name[P_NAME_LEN + 1];
	int nlen;
	char mfgr[P_MFG_LEN + 1];
	char brand[P_BRND_LEN + 1];
	char type[P_TYPE_LEN + 1];
	int tlen;
	DSS_HUGE size;
	char container[P_CNTR_LEN + 1];
	DSS_HUGE retailprice;
	char comment[P_CMNT_MAX + 1];
	int clen;
	partsupp_t s[SUPP_PER_PART];
} part_t;

/* parts.c */
long mk_part PROTO((DSS_HUGE index, part_t *p, DBGenContext *ctx));
int pr_part PROTO((part_t * part, int mode));
int ld_part PROTO((part_t * part, int mode));

typedef struct {
	DSS_HUGE suppkey;
	char name[S_NAME_LEN + 1];
	char address[S_ADDR_MAX + 1];
	int alen;
	DSS_HUGE nation_code;
	char phone[PHONE_LEN + 1];
	DSS_HUGE acctbal;
	char comment[S_CMNT_MAX + 1];
	int clen;
} supplier_t;
/* supplier.c */
long mk_supp PROTO((DSS_HUGE index, supplier_t *s, DBGenContext *ctx));
int pr_supp PROTO((supplier_t * supp, int mode));
int ld_supp PROTO((supplier_t * supp, int mode));

typedef struct {
	DSS_HUGE timekey;
	char alpha[DATE_LEN];
	long year;
	long month;
	long week;
	long day;
} dss_time_t;

/* time.c */
long mk_time PROTO((DSS_HUGE h, dss_time_t *t));

/*
 * this assumes that N_CMNT_LEN >= R_CMNT_LEN
 */
typedef struct {
	DSS_HUGE code;
	char *text;
	long join;
	char comment[N_CMNT_MAX + 1];
	int clen;
} code_t;

/* code table */
int mk_nation PROTO((DSS_HUGE i, code_t *c, DBGenContext *ctx));
int pr_nation PROTO((code_t * c, int mode));
int ld_nation PROTO((code_t * c, int mode));
int mk_region PROTO((DSS_HUGE i, code_t *c, DBGenContext *ctx));
int pr_region PROTO((code_t * c, int mode));
int ld_region PROTO((code_t * c, int mode));

/* speed seed - advances seeds `skip_count` times */
long sd_nation(int child, DSS_HUGE skip_count, DBGenContext *ctx);
long sd_region(int child, DSS_HUGE skip_coun, DBGenContext *ctx);
long sd_order(int child, DSS_HUGE skip_count, DBGenContext *ctx);
long sd_line(int child, DSS_HUGE skip_count, DBGenContext *ctx);
long sd_supp(int child, DSS_HUGE skip_count, DBGenContext *ctx);
long sd_part(int child, DSS_HUGE skip_count, DBGenContext *ctx);
long sd_psupp(int child, DSS_HUGE skip_count, DBGenContext *ctx);
long sd_cust(int child, DSS_HUGE skip_count, DBGenContext *ctx);
