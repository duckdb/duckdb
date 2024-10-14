/* @(#)print.c	2.1.8.2 */
/* generate flat files for data load */
#include <stdio.h>
#ifndef VMS
#include <sys/types.h>
#endif

#if defined(SUN)
#include <unistd.h>
#endif

#if defined(LINUX)
#include <unistd.h>
#endif /*LINUX*/

#include <math.h>

#include "include/dss.h"
#include "include/dsstypes.h"
#include <string.h>

#include <stdio.h>
#include <time.h>

/*
 * Function Prototypes
 */
FILE *print_prep PROTO((int table, int update));
int pr_drange PROTO((int tbl, long min, long cnt, long num));

FILE *
print_prep(int table, int update)
{
    char upath[128];
    FILE *res;

    if (updates)
    {
        if (update > 0) /* updates */
            if (insert_segments)
            {
                int this_segment;
                if (strcmp(tdefs[table].name, "orders.tbl"))
                    this_segment = ++insert_orders_segment;
                else
                    this_segment = ++insert_lineitem_segment;
                sprintf(upath, "%s%c%s.u%d.%d",
                        env_config(PATH_TAG, PATH_DFLT),
                        PATH_SEP, tdefs[table].name, update % 10000, this_segment);
            }
            else
            {
                sprintf(upath, "%s%c%s.u%d",
                        env_config(PATH_TAG, PATH_DFLT),
                        PATH_SEP, tdefs[table].name, update);
            }
        else /* deletes */
            if (delete_segments)
            {
                ++delete_segment;
                sprintf(upath, "%s%cdelete.u%d.%d",
                        env_config(PATH_TAG, PATH_DFLT), PATH_SEP, -update % 10000,
                        delete_segment);
            }
            else
            {
                sprintf(upath, "%s%cdelete.%d",
                        env_config(PATH_TAG, PATH_DFLT), PATH_SEP, -update);
            }
        return (fopen(upath, "w"));
    }
    res = tbl_open(table, "w");
    OPEN_CHECK(res, tdefs[table].name);
    return (res);
}

int dbg_print(int format, FILE *target, void *data, int len, int sep)
{
    int dollars,
        cents;

    switch (format)
    {
    case DT_STR:
        if (columnar)
            fprintf(target, "%-*s", len, (char *)data);
        else
            fprintf(target, "\"%s\"", (char *)data);
        break;
#ifdef MVS
    case DT_VSTR:
        /* note: only used in MVS, assumes columnar output */
        fprintf(target, "%c%c%-*s",
                (len >> 8) & 0xFF, len & 0xFF, len, (char *)data);
        break;
#endif /* MVS */
    case DT_INT:
        if (columnar)
            fprintf(target, "%12ld", (long)data);
        else
            fprintf(target, "%ld", (long)data);
        break;
    case DT_HUGE:
#ifndef SUPPORT_64BITS
        if (*(long *)((long *)data + 1) == 0)
            if (columnar)
                fprintf(target, "%12ld", *(long *)data);
            else
                fprintf(target, "%ld", *(long *)data);
        else if (columnar)
            fprintf(target, "%5ld%07ld",
                    *(long *)((long *)data + 1), *(long *)data);
        else
            fprintf(target, "%ld%07ld",
                    *(long *)((long *)data + 1), *(long *)data);
#else
        fprintf(target, HUGE_FORMAT, *(DSS_HUGE *)data);
#endif /* SUPPORT_64BITS */
        break;
    case DT_KEY:
        fprintf(target, "%ld", (long)data);
        break;
    case DT_MONEY:
        cents = (long)data;
        if (cents < 0)
        {
            fprintf(target, "-");
            cents = -cents;
        }
        dollars = cents / 100;
        cents %= 100;
        if (columnar)
            fprintf(target, "%12ld.%02ld", dollars, cents);
        else
            fprintf(target, "%ld.%02ld", dollars, cents);
        break;
    case DT_CHR:
        if (columnar)
            fprintf(target, "%c ", (char)data);
        else
            fprintf(target, "%c", (char)data);
        break;
    }

#ifdef EOL_HANDLING
    if (sep)
#endif /* EOL_HANDLING */
        if (!columnar && (sep != -1))
            fprintf(target, "%c", SEPARATOR);

    return (0);
}

int pr_cust(customer_t *c, int mode)
{
    static FILE *fp = NULL;

    if (fp == NULL)
        fp = print_prep(CUST, 0);

    PR_STRT(fp);
    PR_INT(fp, c->custkey);
    PR_VSTR(fp, c->name, C_NAME_LEN);
    PR_VSTR(fp, c->address,
            (columnar) ? (long)(ceil(C_ADDR_LEN * V_STR_HGH)) : c->alen);
    PR_STR(fp, c->city, CITY_FIX);
    PR_STR(fp, c->nation_name, C_NATION_NAME_LEN);
    PR_STR(fp, c->region_name, C_REGION_NAME_LEN);
    PR_STR(fp, c->phone, PHONE_LEN);
    PR_STR(fp, c->mktsegment, MAXAGG_LEN);
    PR_END(fp);

    return (0);
}

/*
 * print an order's lineitems
 */
int pr_line(order_t *o, int mode)
{

    static FILE *fp_l = NULL;
    static int last_mode = 0;
    long i;
    int days;
    char buf[100];

    if (fp_l == NULL || mode != last_mode)
    {
        if (fp_l)
            fclose(fp_l);
        fp_l = print_prep(LINE, mode);
        last_mode = mode;
    }

    for (i = 0; i < o->lines; i++)
    {
        PR_STRT(fp_l);
        PR_HUGE(fp_l, o->lineorders[i].okey);
        PR_INT(fp_l, o->lineorders[i].linenumber);
        PR_INT(fp_l, o->lineorders[i].custkey);
        PR_INT(fp_l, o->lineorders[i].partkey);
        PR_INT(fp_l, o->lineorders[i].suppkey);
        PR_STR(fp_l, o->lineorders[i].orderdate, DATE_LEN);
        PR_STR(fp_l, o->lineorders[i].opriority, O_OPRIO_LEN);
        PR_INT(fp_l, o->lineorders[i].ship_priority);
        PR_INT(fp_l, o->lineorders[i].quantity);
        PR_INT(fp_l, o->lineorders[i].extended_price);
        PR_INT(fp_l, o->lineorders[i].order_totalprice);
        PR_INT(fp_l, o->lineorders[i].discount);
        PR_INT(fp_l, o->lineorders[i].revenue);
        PR_INT(fp_l, o->lineorders[i].supp_cost);
        PR_INT(fp_l, o->lineorders[i].tax);
        PR_STR(fp_l, o->lineorders[i].commit_date, DATE_LEN);
        PR_STR(fp_l, o->lineorders[i].shipmode, O_SHIP_MODE_LEN);
        PR_END(fp_l);
    }

    return (0);
}

/*
 * print the given part
 */
int pr_part(part_t *part, int mode)
{
    static FILE *p_fp = NULL;

    if (p_fp == NULL)
        p_fp = print_prep(PART, 0);

    PR_STRT(p_fp);
    PR_INT(p_fp, part->partkey);
    PR_VSTR(p_fp, part->name,
            (columnar) ? (long)P_NAME_LEN : part->nlen);
    PR_STR(p_fp, part->mfgr, P_MFG_LEN);
    PR_STR(p_fp, part->category, P_CAT_LEN);
    PR_STR(p_fp, part->brand, P_BRND_LEN);

    /*need to handle color*/
    PR_VSTR(p_fp, part->color, (columnar) ? (long)P_COLOR_LEN : part->clen);
    PR_VSTR(p_fp, part->type,
            (columnar) ? (long)P_TYPE_LEN : part->tlen);
    PR_INT(p_fp, part->size);
    PR_STR(p_fp, part->container, P_CNTR_LEN);
    PR_END(p_fp);
    return (0);
}

int pr_supp(supplier_t *supp, int mode)
{
    static FILE *fp = NULL;

    if (fp == NULL)
        fp = print_prep(SUPP, mode);

    PR_STRT(fp);
    PR_INT(fp, supp->suppkey);
    PR_STR(fp, supp->name, S_NAME_LEN);

    PR_VSTR(fp, supp->address,
            (columnar) ? (long)(ceil(S_ADDR_LEN * V_STR_HGH)) : supp->alen);
    PR_STR(fp, supp->city, CITY_FIX);
    PR_STR(fp, supp->nation_name, C_NATION_NAME_LEN);
    PR_STR(fp, supp->region_name, C_REGION_NAME_LEN);
    PR_STR(fp, supp->phone, PHONE_LEN);
    PR_END(fp);

    return (0);
}

/*
 * NOTE: this routine does NOT use the BCD2_* routines. As a result,
 * it WILL fail if the keys being deleted exceed 32 bits. Since this
 * would require ~660 update iterations, this seems an acceptable
 * oversight
 */
int pr_drange(int tbl, long min, long cnt, long num)
{
    static int last_num = 0;
    static FILE *dfp = NULL;
    int child = -1;
    long start, last, new;

    static int rows_per_segment = 0;
    static int rows_this_segment = 0;
    static int residual_rows = 0;

    if (last_num != num)
    {
        if (dfp)
            fclose(dfp);
        dfp = print_prep(tbl, -num);
        if (dfp == NULL)
            return (-1);
        last_num = num;
        rows_this_segment = 0;
    }

    start = MK_SPARSE(min, (num - 1) / (10000 / refresh));
    last = start - 1;
    for (child = min; cnt > 0; child++, cnt--)
    {
        new = MK_SPARSE(child, (num - 1) / (10000 / refresh));
        if (gen_rng == 1 && new - last == 1)
        {
            last = new;
            continue;
        }
        if (gen_sql)
        {
            fprintf(dfp,
                    "delete from %s where %s between %ld and %ld;\n",
                    tdefs[ORDER].name, "o_orderkey", start, last);
            fprintf(dfp,
                    "delete from %s where %s between %ld and %ld;\n",
                    tdefs[LINE].name, "l_orderkey", start, last);
            fprintf(dfp, "commit work;\n");
        }
        else if (gen_rng)
        {
            PR_STRT(dfp);
            PR_INT(dfp, start);
            PR_INT(dfp, last);
            PR_END(dfp);
        }
        else
        {
            if (delete_segments)
            {
                if (rows_per_segment == 0)
                {
                    rows_per_segment = (cnt / delete_segments);
                    residual_rows = (cnt % delete_segments);
                    rows_per_segment++;
                }
                if (delete_segment <= residual_rows)
                {
                    if ((++rows_this_segment) > rows_per_segment)
                    {
                        fclose(dfp);
                        dfp = print_prep(tbl, -num);
                        if (dfp == NULL)
                            return (-1);
                        last_num = num;
                        rows_this_segment = 1;
                    }
                }
                else
                {
                    if ((++rows_this_segment) >= rows_per_segment)
                    {
                        fclose(dfp);
                        dfp = print_prep(tbl, -num);
                        if (dfp == NULL)
                            return (-1);
                        last_num = num;
                        rows_this_segment = 1;
                    }
                }
            }
            PR_STRT(dfp);
            PR_KEY(dfp, new);
            PR_END(dfp);
        }
        start = new;
        last = new;
    }
    if (gen_rng)
    {
        PR_STRT(dfp);
        PR_INT(dfp, start);
        PR_INT(dfp, last);
        PR_END(dfp);
    }

    return (0);
}

int pr_date(ssb_date_t *d, int mode)
{
    static FILE *d_fp = NULL;

    if (d_fp == NULL)
        d_fp = print_prep(DATE, 0);

    PR_STRT(d_fp);
    PR_STR(d_fp, d->datekey, D_DATE_LEN);
    PR_STR(d_fp, d->date, D_DATE_LEN);
    PR_STR(d_fp, d->dayofweek, D_DAYWEEK_LEN);
    PR_STR(d_fp, d->month, D_MONTH_LEN);
    PR_INT(d_fp, d->year);
    PR_INT(d_fp, d->yearmonthnum);
    PR_STR(d_fp, d->yearmonth, D_YEARMONTH_LEN);
    PR_INT(d_fp, d->daynuminweek);
    PR_INT(d_fp, d->daynuminmonth);
    PR_INT(d_fp, d->daynuminyear);
    PR_INT(d_fp, d->monthnuminyear);
    PR_INT(d_fp, d->weeknuminyear);
    PR_VSTR(d_fp,
            d->sellingseason, (columnar) ? (long)D_SEASON_LEN : d->slen);
    PR_STR(d_fp, d->lastdayinweekfl, 2);
    PR_STR(d_fp, d->lastdayinmonthfl, 2);
    PR_STR(d_fp, d->holidayfl, 2);
    PR_STR(d_fp, d->weekdayfl, 2);

    PR_END(d_fp);
    return (0);
}

/*
 * verify functions: routines which replace the pr_routines and generate a pseudo checksum
 * instead of generating the actual contents of the tables. Meant to allow large scale data
 * validation without requiring a large amount of storage
 */
int vrf_cust(customer_t *c, int mode)
{
    VRF_STRT(CUST);
    VRF_INT(CUST, c->custkey);
    VRF_STR(CUST, c->name);
    VRF_STR(CUST, c->address);
    VRF_STR(CUST, c->city);
    VRF_STR(CUST, c->nation_name);
    VRF_STR(CUST, c->region_name);
    VRF_STR(CUST, c->phone);
    VRF_STR(CUST, c->mktsegment);
    VRF_END(CUST);

    return (0);
}

/*
 * print an order's lineitems
 */
int vrf_line(order_t *o, int mode)
{
    int i;

    for (i = 0; i < o->lines; i++)
    {
        VRF_STRT(LINE);
        VRF_HUGE(LINE, o->lineorders[i].okey);
        VRF_INT(LINE, o->lineorders[i].linenumber);
        VRF_INT(LINE, o->lineorders[i].custkey);
        VRF_INT(LINE, o->lineorders[i].partkey);
        VRF_INT(LINE, o->lineorders[i].suppkey);
        VRF_STR(LINE, o->lineorders[i].orderdate);
        VRF_STR(LINE, o->lineorders[i].opriority);
        VRF_INT(LINE, o->lineorders[i].ship_priority);
        VRF_INT(LINE, o->lineorders[i].quantity);
        VRF_INT(LINE, o->lineorders[i].extended_price);
        VRF_INT(LINE, o->lineorders[i].order_totalprice);
        VRF_INT(LINE, o->lineorders[i].discount);
        VRF_INT(LINE, o->lineorders[i].revenue);
        VRF_INT(LINE, o->lineorders[i].supp_cost);
        VRF_INT(LINE, o->lineorders[i].tax);
        VRF_STR(LINE, o->lineorders[i].commit_date);
        VRF_STR(LINE, o->lineorders[i].shipmode);
        VRF_END(LINE);
    }

    return (0);
}

/*
 * print the given part
 */
int vrf_part(part_t *part, int mode)
{

    VRF_STRT(PART);
    VRF_INT(PART, part->partkey);
    VRF_STR(PART, part->name);
    VRF_STR(PART, part->mfgr);
    VRF_STR(PART, part->brand);
    VRF_STR(PART, part->type);
    VRF_INT(PART, part->size);
    VRF_STR(PART, part->container);
    VRF_STR(PART, part->category);
    VRF_END(PART);

    return (0);
}

int vrf_supp(supplier_t *supp, int mode)
{
    VRF_STRT(SUPP);
    VRF_INT(SUPP, supp->suppkey);
    VRF_STR(SUPP, supp->name);

    VRF_STR(CUST, supp->address);
    VRF_INT(CUST, supp->nation_key);
    VRF_STR(CUST, supp->nation_name);
    VRF_INT(CUST, supp->region_key);
    VRF_STR(CUST, supp->region_name);
    VRF_STR(CUST, supp->phone);
    VRF_END(SUPP);

    return (0);
}

int vrf_date(ssb_date_t *d, int mode)
{
    VRF_STRT(DATE);
    VRF_STR(DATE, d->datekey);
    VRF_STR(DATE, d->date);
    VRF_STR(DATE, d->dayofweek);
    VRF_STR(DATE, d->month);
    VRF_INT(DATE, d->year);
    VRF_INT(DATE, d->yearmonthnum);
    VRF_STR(DATE, d->yearmonth);
    VRF_INT(DATE, d->daynuminweek);
    VRF_INT(DATE, d->daynuminmonth);
    VRF_INT(DATE, d->daynuminyear);
    VRF_INT(DATE, d->monthnuminyear);
    VRF_INT(DATE, d->weeknuminyear);
    VRF_STR(DATE, d->sellingseason);
    VRF_STR(DATE, d->lastdayinweekfl);
    VRF_STR(DATE, d->lastdayinmonthfl);
    VRF_STR(DATE, d->weekdayfl);
    VRF_END(DATE);
    return (0);
}
