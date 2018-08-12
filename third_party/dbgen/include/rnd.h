/*
 * rnd.h -- header file for use withthe portable random number generator
 * provided by Frank Stephens of Unisys
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/* function protypes */
DSS_HUGE NextRand PROTO((DSS_HUGE));
DSS_HUGE UnifInt PROTO((DSS_HUGE, DSS_HUGE, long));
double dM = 2147483647.0;

/*
 * macros to control RNG and assure reproducible multi-stream
 * runs without the need for seed files. Keep track of invocations of RNG
 * and always round-up to a known per-row boundary.
 */
/*
 * preferred solution, but not initializing correctly
 */
#define VSTR_MAX(len) (long)(len / 5 + (len % 5 == 0) ? 0 : 1 + 1)
seed_t Seed[MAX_STREAM + 1] = {
    {PART, 1, 0, 1},                           /* P_MFG_SD     0 */
    {PART, 46831694, 0, 1},                    /* P_BRND_SD    1 */
    {PART, 1841581359, 0, 1},                  /* P_TYPE_SD    2 */
    {PART, 1193163244, 0, 1},                  /* P_SIZE_SD    3 */
    {PART, 727633698, 0, 1},                   /* P_CNTR_SD    4 */
    {NONE, 933588178, 0, 1},                   /* text pregeneration  5 */
    {PART, 804159733, 0, 2},                   /* P_CMNT_SD    6 */
    {PSUPP, 1671059989, 0, SUPP_PER_PART},     /* PS_QTY_SD    7 */
    {PSUPP, 1051288424, 0, SUPP_PER_PART},     /* PS_SCST_SD   8 */
    {PSUPP, 1961692154, 0, SUPP_PER_PART * 2}, /* PS_CMNT_SD   9 */
    {ORDER, 1227283347, 0, 1},                 /* O_SUPP_SD    10 */
    {ORDER, 1171034773, 0, 1},                 /* O_CLRK_SD    11 */
    {ORDER, 276090261, 0, 2},                  /* O_CMNT_SD    12 */
    {ORDER, 1066728069, 0, 1},                 /* O_ODATE_SD   13 */
    {LINE, 209208115, 0, O_LCNT_MAX},          /* L_QTY_SD     14 */
    {LINE, 554590007, 0, O_LCNT_MAX},          /* L_DCNT_SD    15 */
    {LINE, 721958466, 0, O_LCNT_MAX},          /* L_TAX_SD     16 */
    {LINE, 1371272478, 0, O_LCNT_MAX},         /* L_SHIP_SD    17 */
    {LINE, 675466456, 0, O_LCNT_MAX},          /* L_SMODE_SD   18 */
    {LINE, 1808217256, 0, O_LCNT_MAX},         /* L_PKEY_SD    19 */
    {LINE, 2095021727, 0, O_LCNT_MAX},         /* L_SKEY_SD    20 */
    {LINE, 1769349045, 0, O_LCNT_MAX},         /* L_SDTE_SD    21 */
    {LINE, 904914315, 0, O_LCNT_MAX},          /* L_CDTE_SD    22 */
    {LINE, 373135028, 0, O_LCNT_MAX},          /* L_RDTE_SD    23 */
    {LINE, 717419739, 0, O_LCNT_MAX},          /* L_RFLG_SD    24 */
    {LINE, 1095462486, 0, O_LCNT_MAX * 2},     /* L_CMNT_SD    25 */
    {CUST, 881155353, 0, 9},                   /* C_ADDR_SD    26 */
    {CUST, 1489529863, 0, 1},                  /* C_NTRG_SD    27 */
    {CUST, 1521138112, 0, 3},                  /* C_PHNE_SD    28 */
    {CUST, 298370230, 0, 1},                   /* C_ABAL_SD    29 */
    {CUST, 1140279430, 0, 1},                  /* C_MSEG_SD    30 */
    {CUST, 1335826707, 0, 2},                  /* C_CMNT_SD    31 */
    {SUPP, 706178559, 0, 9},                   /* S_ADDR_SD    32 */
    {SUPP, 110356601, 0, 1},                   /* S_NTRG_SD    33 */
    {SUPP, 884434366, 0, 3},                   /* S_PHNE_SD    34 */
    {SUPP, 962338209, 0, 1},                   /* S_ABAL_SD    35 */
    {SUPP, 1341315363, 0, 2},                  /* S_CMNT_SD    36 */
    {PART, 709314158, 0, 92},                  /* P_NAME_SD    37 */
    {ORDER, 591449447, 0, 1},                  /* O_PRIO_SD    38 */
    {LINE, 431918286, 0, 1},                   /* HVAR_SD      39 */
    {ORDER, 851767375, 0, 1},                  /* O_CKEY_SD    40 */
    {NATION, 606179079, 0, 2},                 /* N_CMNT_SD    41 */
    {REGION, 1500869201, 0, 2},                /* R_CMNT_SD    42 */
    {ORDER, 1434868289, 0, 1},                 /* O_LCNT_SD    43 */
    {SUPP, 263032577, 0, 1},                   /* BBB offset   44 */
    {SUPP, 753643799, 0, 1},                   /* BBB type     45 */
    {SUPP, 202794285, 0, 1},                   /* BBB comment  46 */
    {SUPP, 715851524, 0, 1}                    /* BBB junk     47 */
};

#ifdef __cplusplus
};
#endif
