/*-------------------------------------------------------------------------
 *
 * pg_aggregate.h
 *	  definition of the system "aggregate" relation (pg_aggregate)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_aggregate.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AGGREGATE_H
#define PG_AGGREGATE_H

#include "catalog/genbki.h"
#include "catalog/objectaddress.h"
#include "nodes/pg_list.h"

/* ----------------------------------------------------------------
 *		pg_aggregate definition.
 *
 *		cpp turns this into typedef struct FormData_pg_aggregate
 *
 *	aggfnoid			pg_proc OID of the aggregate itself
 *	aggkind				aggregate kind, see AGGKIND_ categories below
 *	aggnumdirectargs	number of arguments that are "direct" arguments
 *	aggtransfn			transition function
 *	aggfinalfn			final function (0 if none)
 *	aggmtransfn			forward function for moving-aggregate mode (0 if none)
 *	aggminvtransfn		inverse function for moving-aggregate mode (0 if none)
 *	aggmfinalfn			final function for moving-aggregate mode (0 if none)
 *	aggfinalextra		true to pass extra dummy arguments to aggfinalfn
 *	aggmfinalextra		true to pass extra dummy arguments to aggmfinalfn
 *	aggsortop			associated sort operator (0 if none)
 *	aggtranstype		type of aggregate's transition (state) data
 *	aggtransspace		estimated size of state data (0 for default estimate)
 *	aggmtranstype		type of moving-aggregate state data (0 if none)
 *	aggmtransspace		estimated size of moving-agg state (0 for default est)
 *	agginitval			initial value for transition state (can be NULL)
 *	aggminitval			initial value for moving-agg state (can be NULL)
 * ----------------------------------------------------------------
 */
#define AggregateRelationId  2600

CATALOG(pg_aggregate,2600) BKI_WITHOUT_OIDS
{
	regproc		aggfnoid;
	char		aggkind;
	int16		aggnumdirectargs;
	regproc		aggtransfn;
	regproc		aggfinalfn;
	regproc		aggmtransfn;
	regproc		aggminvtransfn;
	regproc		aggmfinalfn;
	bool		aggfinalextra;
	bool		aggmfinalextra;
	Oid			aggsortop;
	Oid			aggtranstype;
	int32		aggtransspace;
	Oid			aggmtranstype;
	int32		aggmtransspace;

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		agginitval;
	text		aggminitval;
#endif
} FormData_pg_aggregate;

/* ----------------
 *		Form_pg_aggregate corresponds to a pointer to a tuple with
 *		the format of pg_aggregate relation.
 * ----------------
 */
typedef FormData_pg_aggregate *Form_pg_aggregate;

/* ----------------
 *		compiler constants for pg_aggregate
 * ----------------
 */

#define Natts_pg_aggregate					17
#define Anum_pg_aggregate_aggfnoid			1
#define Anum_pg_aggregate_aggkind			2
#define Anum_pg_aggregate_aggnumdirectargs	3
#define Anum_pg_aggregate_aggtransfn		4
#define Anum_pg_aggregate_aggfinalfn		5
#define Anum_pg_aggregate_aggmtransfn		6
#define Anum_pg_aggregate_aggminvtransfn	7
#define Anum_pg_aggregate_aggmfinalfn		8
#define Anum_pg_aggregate_aggfinalextra		9
#define Anum_pg_aggregate_aggmfinalextra	10
#define Anum_pg_aggregate_aggsortop			11
#define Anum_pg_aggregate_aggtranstype		12
#define Anum_pg_aggregate_aggtransspace		13
#define Anum_pg_aggregate_aggmtranstype		14
#define Anum_pg_aggregate_aggmtransspace	15
#define Anum_pg_aggregate_agginitval		16
#define Anum_pg_aggregate_aggminitval		17

/*
 * Symbolic values for aggkind column.  We distinguish normal aggregates
 * from ordered-set aggregates (which have two sets of arguments, namely
 * direct and aggregated arguments) and from hypothetical-set aggregates
 * (which are a subclass of ordered-set aggregates in which the last
 * direct arguments have to match up in number and datatypes with the
 * aggregated arguments).
 */
#define AGGKIND_NORMAL			'n'
#define AGGKIND_ORDERED_SET		'o'
#define AGGKIND_HYPOTHETICAL	'h'

/* Use this macro to test for "ordered-set agg including hypothetical case" */
#define AGGKIND_IS_ORDERED_SET(kind)  ((kind) != AGGKIND_NORMAL)


/* ----------------
 * initial contents of pg_aggregate
 * ---------------
 */

/* avg */
DATA(insert ( 2100	n 0 int8_avg_accum	numeric_poly_avg		int8_avg_accum	int8_avg_accum_inv	numeric_poly_avg	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2101	n 0 int4_avg_accum	int8_avg		int4_avg_accum	int4_avg_accum_inv	int8_avg					f f 0	1016	0	1016	0	"{0,0}" "{0,0}" ));
DATA(insert ( 2102	n 0 int2_avg_accum	int8_avg		int2_avg_accum	int2_avg_accum_inv	int8_avg					f f 0	1016	0	1016	0	"{0,0}" "{0,0}" ));
DATA(insert ( 2103	n 0 numeric_avg_accum numeric_avg	numeric_avg_accum numeric_accum_inv numeric_avg					f f 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2104	n 0 float4_accum	float8_avg		-				-				-								f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2105	n 0 float8_accum	float8_avg		-				-				-								f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2106	n 0 interval_accum	interval_avg	interval_accum	interval_accum_inv interval_avg					f f 0	1187	0	1187	0	"{0 second,0 second}" "{0 second,0 second}" ));

/* sum */
DATA(insert ( 2107	n 0 int8_avg_accum	numeric_poly_sum		int8_avg_accum	int8_avg_accum_inv numeric_poly_sum f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2108	n 0 int4_sum		-				int4_avg_accum	int4_avg_accum_inv int2int4_sum					f f 0	20		0	1016	0	_null_ "{0,0}" ));
DATA(insert ( 2109	n 0 int2_sum		-				int2_avg_accum	int2_avg_accum_inv int2int4_sum					f f 0	20		0	1016	0	_null_ "{0,0}" ));
DATA(insert ( 2110	n 0 float4pl		-				-				-				-								f f 0	700		0	0		0	_null_ _null_ ));
DATA(insert ( 2111	n 0 float8pl		-				-				-				-								f f 0	701		0	0		0	_null_ _null_ ));
DATA(insert ( 2112	n 0 cash_pl			-				cash_pl			cash_mi			-								f f 0	790		0	790		0	_null_ _null_ ));
DATA(insert ( 2113	n 0 interval_pl		-				interval_pl		interval_mi		-								f f 0	1186	0	1186	0	_null_ _null_ ));
DATA(insert ( 2114	n 0 numeric_avg_accum	numeric_sum numeric_avg_accum numeric_accum_inv numeric_sum					f f 0	2281	128 2281	128 _null_ _null_ ));

/* max */
DATA(insert ( 2115	n 0 int8larger		-				-				-				-				f f 413		20		0	0		0	_null_ _null_ ));
DATA(insert ( 2116	n 0 int4larger		-				-				-				-				f f 521		23		0	0		0	_null_ _null_ ));
DATA(insert ( 2117	n 0 int2larger		-				-				-				-				f f 520		21		0	0		0	_null_ _null_ ));
DATA(insert ( 2118	n 0 oidlarger		-				-				-				-				f f 610		26		0	0		0	_null_ _null_ ));
DATA(insert ( 2119	n 0 float4larger	-				-				-				-				f f 623		700		0	0		0	_null_ _null_ ));
DATA(insert ( 2120	n 0 float8larger	-				-				-				-				f f 674		701		0	0		0	_null_ _null_ ));
DATA(insert ( 2121	n 0 int4larger		-				-				-				-				f f 563		702		0	0		0	_null_ _null_ ));
DATA(insert ( 2122	n 0 date_larger		-				-				-				-				f f 1097	1082	0	0		0	_null_ _null_ ));
DATA(insert ( 2123	n 0 time_larger		-				-				-				-				f f 1112	1083	0	0		0	_null_ _null_ ));
DATA(insert ( 2124	n 0 timetz_larger	-				-				-				-				f f 1554	1266	0	0		0	_null_ _null_ ));
DATA(insert ( 2125	n 0 cashlarger		-				-				-				-				f f 903		790		0	0		0	_null_ _null_ ));
DATA(insert ( 2126	n 0 timestamp_larger	-			-				-				-				f f 2064	1114	0	0		0	_null_ _null_ ));
DATA(insert ( 2127	n 0 timestamptz_larger	-			-				-				-				f f 1324	1184	0	0		0	_null_ _null_ ));
DATA(insert ( 2128	n 0 interval_larger -				-				-				-				f f 1334	1186	0	0		0	_null_ _null_ ));
DATA(insert ( 2129	n 0 text_larger		-				-				-				-				f f 666		25		0	0		0	_null_ _null_ ));
DATA(insert ( 2130	n 0 numeric_larger	-				-				-				-				f f 1756	1700	0	0		0	_null_ _null_ ));
DATA(insert ( 2050	n 0 array_larger	-				-				-				-				f f 1073	2277	0	0		0	_null_ _null_ ));
DATA(insert ( 2244	n 0 bpchar_larger	-				-				-				-				f f 1060	1042	0	0		0	_null_ _null_ ));
DATA(insert ( 2797	n 0 tidlarger		-				-				-				-				f f 2800	27		0	0		0	_null_ _null_ ));
DATA(insert ( 3526	n 0 enum_larger		-				-				-				-				f f 3519	3500	0	0		0	_null_ _null_ ));
DATA(insert ( 3564	n 0 network_larger	-				-				-				-				f f 1205	869		0	0		0	_null_ _null_ ));

/* min */
DATA(insert ( 2131	n 0 int8smaller		-				-				-				-				f f 412		20		0	0		0	_null_ _null_ ));
DATA(insert ( 2132	n 0 int4smaller		-				-				-				-				f f 97		23		0	0		0	_null_ _null_ ));
DATA(insert ( 2133	n 0 int2smaller		-				-				-				-				f f 95		21		0	0		0	_null_ _null_ ));
DATA(insert ( 2134	n 0 oidsmaller		-				-				-				-				f f 609		26		0	0		0	_null_ _null_ ));
DATA(insert ( 2135	n 0 float4smaller	-				-				-				-				f f 622		700		0	0		0	_null_ _null_ ));
DATA(insert ( 2136	n 0 float8smaller	-				-				-				-				f f 672		701		0	0		0	_null_ _null_ ));
DATA(insert ( 2137	n 0 int4smaller		-				-				-				-				f f 562		702		0	0		0	_null_ _null_ ));
DATA(insert ( 2138	n 0 date_smaller	-				-				-				-				f f 1095	1082	0	0		0	_null_ _null_ ));
DATA(insert ( 2139	n 0 time_smaller	-				-				-				-				f f 1110	1083	0	0		0	_null_ _null_ ));
DATA(insert ( 2140	n 0 timetz_smaller	-				-				-				-				f f 1552	1266	0	0		0	_null_ _null_ ));
DATA(insert ( 2141	n 0 cashsmaller		-				-				-				-				f f 902		790		0	0		0	_null_ _null_ ));
DATA(insert ( 2142	n 0 timestamp_smaller	-			-				-				-				f f 2062	1114	0	0		0	_null_ _null_ ));
DATA(insert ( 2143	n 0 timestamptz_smaller -			-				-				-				f f 1322	1184	0	0		0	_null_ _null_ ));
DATA(insert ( 2144	n 0 interval_smaller	-			-				-				-				f f 1332	1186	0	0		0	_null_ _null_ ));
DATA(insert ( 2145	n 0 text_smaller	-				-				-				-				f f 664		25		0	0		0	_null_ _null_ ));
DATA(insert ( 2146	n 0 numeric_smaller -				-				-				-				f f 1754	1700	0	0		0	_null_ _null_ ));
DATA(insert ( 2051	n 0 array_smaller	-				-				-				-				f f 1072	2277	0	0		0	_null_ _null_ ));
DATA(insert ( 2245	n 0 bpchar_smaller	-				-				-				-				f f 1058	1042	0	0		0	_null_ _null_ ));
DATA(insert ( 2798	n 0 tidsmaller		-				-				-				-				f f 2799	27		0	0		0	_null_ _null_ ));
DATA(insert ( 3527	n 0 enum_smaller	-				-				-				-				f f 3518	3500	0	0		0	_null_ _null_ ));
DATA(insert ( 3565	n 0 network_smaller -				-				-				-				f f 1203	869		0	0		0	_null_ _null_ ));

/* count */
DATA(insert ( 2147	n 0 int8inc_any		-				int8inc_any		int8dec_any		-				f f 0		20		0	20		0	"0" "0" ));
DATA(insert ( 2803	n 0 int8inc			-				int8inc			int8dec			-				f f 0		20		0	20		0	"0" "0" ));

/* var_pop */
DATA(insert ( 2718	n 0 int8_accum	numeric_var_pop		int8_accum		int8_accum_inv	numeric_var_pop					f f 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2719	n 0 int4_accum	numeric_poly_var_pop		int4_accum		int4_accum_inv	numeric_poly_var_pop	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2720	n 0 int2_accum	numeric_poly_var_pop		int2_accum		int2_accum_inv	numeric_poly_var_pop	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2721	n 0 float4_accum	float8_var_pop	-				-				-								f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2722	n 0 float8_accum	float8_var_pop	-				-				-								f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2723	n 0 numeric_accum	numeric_var_pop numeric_accum numeric_accum_inv numeric_var_pop					f f 0	2281	128 2281	128 _null_ _null_ ));

/* var_samp */
DATA(insert ( 2641	n 0 int8_accum	numeric_var_samp	int8_accum		int8_accum_inv	numeric_var_samp				f f 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2642	n 0 int4_accum	numeric_poly_var_samp		int4_accum		int4_accum_inv	numeric_poly_var_samp	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2643	n 0 int2_accum	numeric_poly_var_samp		int2_accum		int2_accum_inv	numeric_poly_var_samp	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2644	n 0 float4_accum	float8_var_samp -				-				-								f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2645	n 0 float8_accum	float8_var_samp -				-				-								f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2646	n 0 numeric_accum	numeric_var_samp numeric_accum numeric_accum_inv numeric_var_samp				f f 0	2281	128 2281	128 _null_ _null_ ));

/* variance: historical Postgres syntax for var_samp */
DATA(insert ( 2148	n 0 int8_accum	numeric_var_samp	int8_accum		int8_accum_inv	numeric_var_samp				f f 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2149	n 0 int4_accum	numeric_poly_var_samp		int4_accum		int4_accum_inv	numeric_poly_var_samp	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2150	n 0 int2_accum	numeric_poly_var_samp		int2_accum		int2_accum_inv	numeric_poly_var_samp	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2151	n 0 float4_accum	float8_var_samp -				-				-								f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2152	n 0 float8_accum	float8_var_samp -				-				-								f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2153	n 0 numeric_accum	numeric_var_samp numeric_accum numeric_accum_inv numeric_var_samp				f f 0	2281	128 2281	128 _null_ _null_ ));

/* stddev_pop */
DATA(insert ( 2724	n 0 int8_accum	numeric_stddev_pop	int8_accum	int8_accum_inv	numeric_stddev_pop					f f 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2725	n 0 int4_accum	numeric_poly_stddev_pop int4_accum	int4_accum_inv	numeric_poly_stddev_pop f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2726	n 0 int2_accum	numeric_poly_stddev_pop int2_accum	int2_accum_inv	numeric_poly_stddev_pop f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2727	n 0 float4_accum	float8_stddev_pop	-				-				-							f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2728	n 0 float8_accum	float8_stddev_pop	-				-				-							f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2729	n 0 numeric_accum	numeric_stddev_pop numeric_accum numeric_accum_inv numeric_stddev_pop			f f 0	2281	128 2281	128 _null_ _null_ ));

/* stddev_samp */
DATA(insert ( 2712	n 0 int8_accum	numeric_stddev_samp		int8_accum	int8_accum_inv	numeric_stddev_samp				f f 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2713	n 0 int4_accum	numeric_poly_stddev_samp	int4_accum	int4_accum_inv	numeric_poly_stddev_samp	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2714	n 0 int2_accum	numeric_poly_stddev_samp	int2_accum	int2_accum_inv	numeric_poly_stddev_samp	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2715	n 0 float4_accum	float8_stddev_samp	-				-				-							f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2716	n 0 float8_accum	float8_stddev_samp	-				-				-							f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2717	n 0 numeric_accum	numeric_stddev_samp numeric_accum numeric_accum_inv numeric_stddev_samp			f f 0	2281	128 2281	128 _null_ _null_ ));

/* stddev: historical Postgres syntax for stddev_samp */
DATA(insert ( 2154	n 0 int8_accum	numeric_stddev_samp		int8_accum	int8_accum_inv	numeric_stddev_samp				f f 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2155	n 0 int4_accum	numeric_poly_stddev_samp	int4_accum	int4_accum_inv	numeric_poly_stddev_samp	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2156	n 0 int2_accum	numeric_poly_stddev_samp	int2_accum	int2_accum_inv	numeric_poly_stddev_samp	f f 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2157	n 0 float4_accum	float8_stddev_samp	-				-				-							f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2158	n 0 float8_accum	float8_stddev_samp	-				-				-							f f 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2159	n 0 numeric_accum	numeric_stddev_samp numeric_accum numeric_accum_inv numeric_stddev_samp			f f 0	2281	128 2281	128 _null_ _null_ ));

/* SQL2003 binary regression aggregates */
DATA(insert ( 2818	n 0 int8inc_float8_float8	-					-				-				-				f f 0	20		0	0		0	"0" _null_ ));
DATA(insert ( 2819	n 0 float8_regr_accum	float8_regr_sxx			-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2820	n 0 float8_regr_accum	float8_regr_syy			-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2821	n 0 float8_regr_accum	float8_regr_sxy			-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2822	n 0 float8_regr_accum	float8_regr_avgx		-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2823	n 0 float8_regr_accum	float8_regr_avgy		-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2824	n 0 float8_regr_accum	float8_regr_r2			-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2825	n 0 float8_regr_accum	float8_regr_slope		-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2826	n 0 float8_regr_accum	float8_regr_intercept	-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2827	n 0 float8_regr_accum	float8_covar_pop		-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2828	n 0 float8_regr_accum	float8_covar_samp		-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2829	n 0 float8_regr_accum	float8_corr				-				-				-				f f 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));

/* boolean-and and boolean-or */
DATA(insert ( 2517	n 0 booland_statefunc	-			bool_accum		bool_accum_inv	bool_alltrue	f f 58	16		0	2281	16	_null_ _null_ ));
DATA(insert ( 2518	n 0 boolor_statefunc	-			bool_accum		bool_accum_inv	bool_anytrue	f f 59	16		0	2281	16	_null_ _null_ ));
DATA(insert ( 2519	n 0 booland_statefunc	-			bool_accum		bool_accum_inv	bool_alltrue	f f 58	16		0	2281	16	_null_ _null_ ));

/* bitwise integer */
DATA(insert ( 2236	n 0 int2and		-					-				-				-				f f 0	21		0	0		0	_null_ _null_ ));
DATA(insert ( 2237	n 0 int2or		-					-				-				-				f f 0	21		0	0		0	_null_ _null_ ));
DATA(insert ( 2238	n 0 int4and		-					-				-				-				f f 0	23		0	0		0	_null_ _null_ ));
DATA(insert ( 2239	n 0 int4or		-					-				-				-				f f 0	23		0	0		0	_null_ _null_ ));
DATA(insert ( 2240	n 0 int8and		-					-				-				-				f f 0	20		0	0		0	_null_ _null_ ));
DATA(insert ( 2241	n 0 int8or		-					-				-				-				f f 0	20		0	0		0	_null_ _null_ ));
DATA(insert ( 2242	n 0 bitand		-					-				-				-				f f 0	1560	0	0		0	_null_ _null_ ));
DATA(insert ( 2243	n 0 bitor		-					-				-				-				f f 0	1560	0	0		0	_null_ _null_ ));

/* xml */
DATA(insert ( 2901	n 0 xmlconcat2	-					-				-				-				f f 0	142		0	0		0	_null_ _null_ ));

/* array */
DATA(insert ( 2335	n 0 array_agg_transfn	array_agg_finalfn	-				-				-				t f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 4053	n 0 array_agg_array_transfn array_agg_array_finalfn -		-				-				t f 0	2281	0	0		0	_null_ _null_ ));

/* text */
DATA(insert ( 3538	n 0 string_agg_transfn	string_agg_finalfn	-				-				-				f f 0	2281	0	0		0	_null_ _null_ ));

/* bytea */
DATA(insert ( 3545	n 0 bytea_string_agg_transfn	bytea_string_agg_finalfn	-				-				-		f f 0	2281	0	0		0	_null_ _null_ ));

/* json */
DATA(insert ( 3175	n 0 json_agg_transfn	json_agg_finalfn			-				-				-				f f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3197	n 0 json_object_agg_transfn json_object_agg_finalfn -				-				-				f f 0	2281	0	0		0	_null_ _null_ ));

/* jsonb */
DATA(insert ( 3267	n 0 jsonb_agg_transfn	jsonb_agg_finalfn			-				-				-				f f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3270	n 0 jsonb_object_agg_transfn jsonb_object_agg_finalfn -				-				-				f f 0	2281	0	0		0	_null_ _null_ ));

/* ordered-set and hypothetical-set aggregates */
DATA(insert ( 3972	o 1 ordered_set_transition			percentile_disc_final					-		-		-		t f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3974	o 1 ordered_set_transition			percentile_cont_float8_final			-		-		-		f f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3976	o 1 ordered_set_transition			percentile_cont_interval_final			-		-		-		f f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3978	o 1 ordered_set_transition			percentile_disc_multi_final				-		-		-		t f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3980	o 1 ordered_set_transition			percentile_cont_float8_multi_final		-		-		-		f f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3982	o 1 ordered_set_transition			percentile_cont_interval_multi_final	-		-		-		f f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3984	o 0 ordered_set_transition			mode_final								-		-		-		t f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3986	h 1 ordered_set_transition_multi	rank_final								-		-		-		t f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3988	h 1 ordered_set_transition_multi	percent_rank_final						-		-		-		t f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3990	h 1 ordered_set_transition_multi	cume_dist_final							-		-		-		t f 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3992	h 1 ordered_set_transition_multi	dense_rank_final						-		-		-		t f 0	2281	0	0		0	_null_ _null_ ));


/*
 * prototypes for functions in pg_aggregate.c
 */
extern ObjectAddress AggregateCreate(const char *aggName,
				Oid aggNamespace,
				char aggKind,
				int numArgs,
				int numDirectArgs,
				oidvector *parameterTypes,
				Datum allParameterTypes,
				Datum parameterModes,
				Datum parameterNames,
				List *parameterDefaults,
				Oid variadicArgType,
				List *aggtransfnName,
				List *aggfinalfnName,
				List *aggmtransfnName,
				List *aggminvtransfnName,
				List *aggmfinalfnName,
				bool finalfnExtraArgs,
				bool mfinalfnExtraArgs,
				List *aggsortopName,
				Oid aggTransType,
				int32 aggTransSpace,
				Oid aggmTransType,
				int32 aggmTransSpace,
				const char *agginitval,
				const char *aggminitval);

#endif   /* PG_AGGREGATE_H */
