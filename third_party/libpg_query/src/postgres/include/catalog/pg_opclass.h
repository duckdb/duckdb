/*-------------------------------------------------------------------------
 *
 * pg_opclass.h
 *	  definition of the system "opclass" relation (pg_opclass)
 *	  along with the relation's initial contents.
 *
 * The primary key for this table is <opcmethod, opcname, opcnamespace> ---
 * that is, there is a row for each valid combination of opclass name and
 * index access method type.  This row specifies the expected input data type
 * for the opclass (the type of the heap column, or the expression output type
 * in the case of an index expression).  Note that types binary-coercible to
 * the specified type will be accepted too.
 *
 * For a given <opcmethod, opcintype> pair, there can be at most one row that
 * has opcdefault = true; this row is the default opclass for such data in
 * such an index.  (This is not currently enforced by an index, because we
 * don't support partial indexes on system catalogs.)
 *
 * Normally opckeytype = InvalidOid (zero), indicating that the data stored
 * in the index is the same as the data in the indexed column.  If opckeytype
 * is nonzero then it indicates that a conversion step is needed to produce
 * the stored index data, which will be of type opckeytype (which might be
 * the same or different from the input datatype).  Performing such a
 * conversion is the responsibility of the index access method --- not all
 * AMs support this.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_opclass.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_OPCLASS_H
#define PG_OPCLASS_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_opclass definition.  cpp turns this into
 *		typedef struct FormData_pg_opclass
 * ----------------
 */
#define OperatorClassRelationId  2616

CATALOG(pg_opclass,2616)
{
	Oid			opcmethod;		/* index access method opclass is for */
	NameData	opcname;		/* name of this opclass */
	Oid			opcnamespace;	/* namespace of this opclass */
	Oid			opcowner;		/* opclass owner */
	Oid			opcfamily;		/* containing operator family */
	Oid			opcintype;		/* type of data indexed by opclass */
	bool		opcdefault;		/* T if opclass is default for opcintype */
	Oid			opckeytype;		/* type of data in index, or InvalidOid */
} FormData_pg_opclass;

/* ----------------
 *		Form_pg_opclass corresponds to a pointer to a tuple with
 *		the format of pg_opclass relation.
 * ----------------
 */
typedef FormData_pg_opclass *Form_pg_opclass;

/* ----------------
 *		compiler constants for pg_opclass
 * ----------------
 */
#define Natts_pg_opclass				8
#define Anum_pg_opclass_opcmethod		1
#define Anum_pg_opclass_opcname			2
#define Anum_pg_opclass_opcnamespace	3
#define Anum_pg_opclass_opcowner		4
#define Anum_pg_opclass_opcfamily		5
#define Anum_pg_opclass_opcintype		6
#define Anum_pg_opclass_opcdefault		7
#define Anum_pg_opclass_opckeytype		8

/* ----------------
 *		initial contents of pg_opclass
 *
 * Note: we hard-wire an OID only for a few entries that have to be explicitly
 * referenced in the C code or in built-in catalog entries.  The rest get OIDs
 * assigned on-the-fly during initdb.
 * ----------------
 */

DATA(insert (	403		abstime_ops			PGNSP PGUID  421  702 t 0 ));
DATA(insert (	403		array_ops			PGNSP PGUID  397 2277 t 0 ));
DATA(insert (	405		array_ops			PGNSP PGUID  627 2277 t 0 ));
DATA(insert (	403		bit_ops				PGNSP PGUID  423 1560 t 0 ));
DATA(insert (	403		bool_ops			PGNSP PGUID  424   16 t 0 ));
DATA(insert (	403		bpchar_ops			PGNSP PGUID  426 1042 t 0 ));
DATA(insert (	405		bpchar_ops			PGNSP PGUID  427 1042 t 0 ));
DATA(insert (	403		bytea_ops			PGNSP PGUID  428   17 t 0 ));
DATA(insert (	403		char_ops			PGNSP PGUID  429   18 t 0 ));
DATA(insert (	405		char_ops			PGNSP PGUID  431   18 t 0 ));
DATA(insert (	403		cidr_ops			PGNSP PGUID 1974  869 f 0 ));
DATA(insert (	405		cidr_ops			PGNSP PGUID 1975  869 f 0 ));
DATA(insert OID = 3122 ( 403	date_ops	PGNSP PGUID  434 1082 t 0 ));
#define DATE_BTREE_OPS_OID 3122
DATA(insert (	405		date_ops			PGNSP PGUID  435 1082 t 0 ));
DATA(insert (	403		float4_ops			PGNSP PGUID 1970  700 t 0 ));
DATA(insert (	405		float4_ops			PGNSP PGUID 1971  700 t 0 ));
DATA(insert OID = 3123 ( 403	float8_ops	PGNSP PGUID 1970  701 t 0 ));
#define FLOAT8_BTREE_OPS_OID 3123
DATA(insert (	405		float8_ops			PGNSP PGUID 1971  701 t 0 ));
DATA(insert (	403		inet_ops			PGNSP PGUID 1974  869 t 0 ));
DATA(insert (	405		inet_ops			PGNSP PGUID 1975  869 t 0 ));
DATA(insert (	783		inet_ops			PGNSP PGUID 3550  869 f 0 ));
DATA(insert OID = 1979 ( 403	int2_ops	PGNSP PGUID 1976   21 t 0 ));
#define INT2_BTREE_OPS_OID 1979
DATA(insert (	405		int2_ops			PGNSP PGUID 1977   21 t 0 ));
DATA(insert OID = 1978 ( 403	int4_ops	PGNSP PGUID 1976   23 t 0 ));
#define INT4_BTREE_OPS_OID 1978
DATA(insert (	405		int4_ops			PGNSP PGUID 1977   23 t 0 ));
DATA(insert OID = 3124 ( 403	int8_ops	PGNSP PGUID 1976   20 t 0 ));
#define INT8_BTREE_OPS_OID 3124
DATA(insert (	405		int8_ops			PGNSP PGUID 1977   20 t 0 ));
DATA(insert (	403		interval_ops		PGNSP PGUID 1982 1186 t 0 ));
DATA(insert (	405		interval_ops		PGNSP PGUID 1983 1186 t 0 ));
DATA(insert (	403		macaddr_ops			PGNSP PGUID 1984  829 t 0 ));
DATA(insert (	405		macaddr_ops			PGNSP PGUID 1985  829 t 0 ));
/*
 * Here's an ugly little hack to save space in the system catalog indexes.
 * btree doesn't ordinarily allow a storage type different from input type;
 * but cstring and name are the same thing except for trailing padding,
 * and we can safely omit that within an index entry.  So we declare the
 * btree opclass for name as using cstring storage type.
 */
DATA(insert (	403		name_ops			PGNSP PGUID 1986   19 t 2275 ));
DATA(insert (	405		name_ops			PGNSP PGUID 1987   19 t 0 ));
DATA(insert OID = 3125 ( 403	numeric_ops PGNSP PGUID 1988 1700 t 0 ));
#define NUMERIC_BTREE_OPS_OID 3125
DATA(insert (	405		numeric_ops			PGNSP PGUID 1998 1700 t 0 ));
DATA(insert OID = 1981 ( 403	oid_ops		PGNSP PGUID 1989   26 t 0 ));
#define OID_BTREE_OPS_OID 1981
DATA(insert (	405		oid_ops				PGNSP PGUID 1990   26 t 0 ));
DATA(insert (	403		oidvector_ops		PGNSP PGUID 1991   30 t 0 ));
DATA(insert (	405		oidvector_ops		PGNSP PGUID 1992   30 t 0 ));
DATA(insert (	403		record_ops			PGNSP PGUID 2994 2249 t 0 ));
DATA(insert (	403		record_image_ops	PGNSP PGUID 3194 2249 f 0 ));
DATA(insert OID = 3126 ( 403	text_ops	PGNSP PGUID 1994   25 t 0 ));
#define TEXT_BTREE_OPS_OID 3126
DATA(insert (	405		text_ops			PGNSP PGUID 1995   25 t 0 ));
DATA(insert (	403		time_ops			PGNSP PGUID 1996 1083 t 0 ));
DATA(insert (	405		time_ops			PGNSP PGUID 1997 1083 t 0 ));
DATA(insert OID = 3127 ( 403	timestamptz_ops PGNSP PGUID  434 1184 t 0 ));
#define TIMESTAMPTZ_BTREE_OPS_OID 3127
DATA(insert (	405		timestamptz_ops		PGNSP PGUID 1999 1184 t 0 ));
DATA(insert (	403		timetz_ops			PGNSP PGUID 2000 1266 t 0 ));
DATA(insert (	405		timetz_ops			PGNSP PGUID 2001 1266 t 0 ));
DATA(insert (	403		varbit_ops			PGNSP PGUID 2002 1562 t 0 ));
DATA(insert (	403		varchar_ops			PGNSP PGUID 1994   25 f 0 ));
DATA(insert (	405		varchar_ops			PGNSP PGUID 1995   25 f 0 ));
DATA(insert OID = 3128 ( 403	timestamp_ops	PGNSP PGUID  434 1114 t 0 ));
#define TIMESTAMP_BTREE_OPS_OID 3128
DATA(insert (	405		timestamp_ops		PGNSP PGUID 2040 1114 t 0 ));
DATA(insert (	403		text_pattern_ops	PGNSP PGUID 2095   25 f 0 ));
DATA(insert (	403		varchar_pattern_ops PGNSP PGUID 2095   25 f 0 ));
DATA(insert (	403		bpchar_pattern_ops	PGNSP PGUID 2097 1042 f 0 ));
DATA(insert (	403		money_ops			PGNSP PGUID 2099  790 t 0 ));
DATA(insert (	405		bool_ops			PGNSP PGUID 2222   16 t 0 ));
DATA(insert (	405		bytea_ops			PGNSP PGUID 2223   17 t 0 ));
DATA(insert (	405		int2vector_ops		PGNSP PGUID 2224   22 t 0 ));
DATA(insert (	403		tid_ops				PGNSP PGUID 2789   27 t 0 ));
DATA(insert (	405		xid_ops				PGNSP PGUID 2225   28 t 0 ));
DATA(insert (	405		cid_ops				PGNSP PGUID 2226   29 t 0 ));
DATA(insert (	405		abstime_ops			PGNSP PGUID 2227  702 t 0 ));
DATA(insert (	405		reltime_ops			PGNSP PGUID 2228  703 t 0 ));
DATA(insert (	405		text_pattern_ops	PGNSP PGUID 2229   25 f 0 ));
DATA(insert (	405		varchar_pattern_ops PGNSP PGUID 2229   25 f 0 ));
DATA(insert (	405		bpchar_pattern_ops	PGNSP PGUID 2231 1042 f 0 ));
DATA(insert (	403		reltime_ops			PGNSP PGUID 2233  703 t 0 ));
DATA(insert (	403		tinterval_ops		PGNSP PGUID 2234  704 t 0 ));
DATA(insert (	405		aclitem_ops			PGNSP PGUID 2235 1033 t 0 ));
DATA(insert (	783		box_ops				PGNSP PGUID 2593  603 t 0 ));
DATA(insert (	783		point_ops			PGNSP PGUID 1029  600 t 603 ));
DATA(insert (	783		poly_ops			PGNSP PGUID 2594  604 t 603 ));
DATA(insert (	783		circle_ops			PGNSP PGUID 2595  718 t 603 ));
DATA(insert (	2742	_int4_ops			PGNSP PGUID 2745  1007 t 23 ));
DATA(insert (	2742	_text_ops			PGNSP PGUID 2745  1009 t 25 ));
DATA(insert (	2742	_abstime_ops		PGNSP PGUID 2745  1023 t 702 ));
DATA(insert (	2742	_bit_ops			PGNSP PGUID 2745  1561 t 1560 ));
DATA(insert (	2742	_bool_ops			PGNSP PGUID 2745  1000 t 16 ));
DATA(insert (	2742	_bpchar_ops			PGNSP PGUID 2745  1014 t 1042 ));
DATA(insert (	2742	_bytea_ops			PGNSP PGUID 2745  1001 t 17 ));
DATA(insert (	2742	_char_ops			PGNSP PGUID 2745  1002 t 18 ));
DATA(insert (	2742	_cidr_ops			PGNSP PGUID 2745  651 t 650 ));
DATA(insert (	2742	_date_ops			PGNSP PGUID 2745  1182 t 1082 ));
DATA(insert (	2742	_float4_ops			PGNSP PGUID 2745  1021 t 700 ));
DATA(insert (	2742	_float8_ops			PGNSP PGUID 2745  1022 t 701 ));
DATA(insert (	2742	_inet_ops			PGNSP PGUID 2745  1041 t 869 ));
DATA(insert (	2742	_int2_ops			PGNSP PGUID 2745  1005 t 21 ));
DATA(insert (	2742	_int8_ops			PGNSP PGUID 2745  1016 t 20 ));
DATA(insert (	2742	_interval_ops		PGNSP PGUID 2745  1187 t 1186 ));
DATA(insert (	2742	_macaddr_ops		PGNSP PGUID 2745  1040 t 829 ));
DATA(insert (	2742	_name_ops			PGNSP PGUID 2745  1003 t 19 ));
DATA(insert (	2742	_numeric_ops		PGNSP PGUID 2745  1231 t 1700 ));
DATA(insert (	2742	_oid_ops			PGNSP PGUID 2745  1028 t 26 ));
DATA(insert (	2742	_oidvector_ops		PGNSP PGUID 2745  1013 t 30 ));
DATA(insert (	2742	_time_ops			PGNSP PGUID 2745  1183 t 1083 ));
DATA(insert (	2742	_timestamptz_ops	PGNSP PGUID 2745  1185 t 1184 ));
DATA(insert (	2742	_timetz_ops			PGNSP PGUID 2745  1270 t 1266 ));
DATA(insert (	2742	_varbit_ops			PGNSP PGUID 2745  1563 t 1562 ));
DATA(insert (	2742	_varchar_ops		PGNSP PGUID 2745  1015 t 1043 ));
DATA(insert (	2742	_timestamp_ops		PGNSP PGUID 2745  1115 t 1114 ));
DATA(insert (	2742	_money_ops			PGNSP PGUID 2745  791 t 790 ));
DATA(insert (	2742	_reltime_ops		PGNSP PGUID 2745  1024 t 703 ));
DATA(insert (	2742	_tinterval_ops		PGNSP PGUID 2745  1025 t 704 ));
DATA(insert (	403		uuid_ops			PGNSP PGUID 2968  2950 t 0 ));
DATA(insert (	405		uuid_ops			PGNSP PGUID 2969  2950 t 0 ));
DATA(insert (	403		pg_lsn_ops			PGNSP PGUID 3253  3220 t 0 ));
DATA(insert (	405		pg_lsn_ops			PGNSP PGUID 3254  3220 t 0 ));
DATA(insert (	403		enum_ops			PGNSP PGUID 3522  3500 t 0 ));
DATA(insert (	405		enum_ops			PGNSP PGUID 3523  3500 t 0 ));
DATA(insert (	403		tsvector_ops		PGNSP PGUID 3626  3614 t 0 ));
DATA(insert (	783		tsvector_ops		PGNSP PGUID 3655  3614 t 3642 ));
DATA(insert (	2742	tsvector_ops		PGNSP PGUID 3659  3614 t 25 ));
DATA(insert (	403		tsquery_ops			PGNSP PGUID 3683  3615 t 0 ));
DATA(insert (	783		tsquery_ops			PGNSP PGUID 3702  3615 t 20 ));
DATA(insert (	403		range_ops			PGNSP PGUID 3901  3831 t 0 ));
DATA(insert (	405		range_ops			PGNSP PGUID 3903  3831 t 0 ));
DATA(insert (	783		range_ops			PGNSP PGUID 3919  3831 t 0 ));
DATA(insert (	4000	range_ops			PGNSP PGUID 3474  3831 t 0 ));
DATA(insert (	4000	quad_point_ops		PGNSP PGUID 4015  600 t 0 ));
DATA(insert (	4000	kd_point_ops		PGNSP PGUID 4016  600 f 0 ));
DATA(insert (	4000	text_ops			PGNSP PGUID 4017  25 t 0 ));
DATA(insert (	403		jsonb_ops			PGNSP PGUID 4033  3802 t 0 ));
DATA(insert (	405		jsonb_ops			PGNSP PGUID 4034  3802 t 0 ));
DATA(insert (	2742	jsonb_ops			PGNSP PGUID 4036  3802 t 25 ));
DATA(insert (	2742	jsonb_path_ops		PGNSP PGUID 4037  3802 f 23 ));

/* BRIN operator classes */
/* no brin opclass for bool */
DATA(insert (	3580	bytea_minmax_ops		PGNSP PGUID 4064	17 t 17 ));
DATA(insert (	3580	char_minmax_ops			PGNSP PGUID 4062	18 t 18 ));
DATA(insert (	3580	name_minmax_ops			PGNSP PGUID 4065	19 t 19 ));
DATA(insert (	3580	int8_minmax_ops			PGNSP PGUID 4054	20 t 20 ));
DATA(insert (	3580	int2_minmax_ops			PGNSP PGUID 4054	21 t 21 ));
DATA(insert (	3580	int4_minmax_ops			PGNSP PGUID 4054	23 t 23 ));
DATA(insert (	3580	text_minmax_ops			PGNSP PGUID 4056	25 t 25 ));
DATA(insert (	3580	oid_minmax_ops			PGNSP PGUID 4068	26 t 26 ));
DATA(insert (	3580	tid_minmax_ops			PGNSP PGUID 4069	27 t 27 ));
DATA(insert (	3580	float4_minmax_ops		PGNSP PGUID 4070   700 t 700 ));
DATA(insert (	3580	float8_minmax_ops		PGNSP PGUID 4070   701 t 701 ));
DATA(insert (	3580	abstime_minmax_ops		PGNSP PGUID 4072   702 t 702 ));
DATA(insert (	3580	reltime_minmax_ops		PGNSP PGUID 4073   703 t 703 ));
DATA(insert (	3580	macaddr_minmax_ops		PGNSP PGUID 4074   829 t 829 ));
DATA(insert (	3580	inet_minmax_ops			PGNSP PGUID 4075   869 f 869 ));
DATA(insert (	3580	inet_inclusion_ops		PGNSP PGUID 4102   869 t 869 ));
DATA(insert (	3580	bpchar_minmax_ops		PGNSP PGUID 4076  1042 t 1042 ));
DATA(insert (	3580	time_minmax_ops			PGNSP PGUID 4077  1083 t 1083 ));
DATA(insert (	3580	date_minmax_ops			PGNSP PGUID 4059  1082 t 1082 ));
DATA(insert (	3580	timestamp_minmax_ops	PGNSP PGUID 4059  1114 t 1114 ));
DATA(insert (	3580	timestamptz_minmax_ops	PGNSP PGUID 4059  1184 t 1184 ));
DATA(insert (	3580	interval_minmax_ops		PGNSP PGUID 4078  1186 t 1186 ));
DATA(insert (	3580	timetz_minmax_ops		PGNSP PGUID 4058  1266 t 1266 ));
DATA(insert (	3580	bit_minmax_ops			PGNSP PGUID 4079  1560 t 1560 ));
DATA(insert (	3580	varbit_minmax_ops		PGNSP PGUID 4080  1562 t 1562 ));
DATA(insert (	3580	numeric_minmax_ops		PGNSP PGUID 4055  1700 t 1700 ));
/* no brin opclass for record, anyarray */
DATA(insert (	3580	uuid_minmax_ops			PGNSP PGUID 4081  2950 t 2950 ));
DATA(insert (	3580	range_inclusion_ops		PGNSP PGUID 4103  3831 t 3831 ));
DATA(insert (	3580	pg_lsn_minmax_ops		PGNSP PGUID 4082  3220 t 3220 ));
/* no brin opclass for enum, tsvector, tsquery, jsonb */
DATA(insert (	3580	box_inclusion_ops		PGNSP PGUID 4104   603 t 603 ));
/* no brin opclass for the geometric types except box */

#endif   /* PG_OPCLASS_H */
