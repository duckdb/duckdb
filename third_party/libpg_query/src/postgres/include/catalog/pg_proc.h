/*-------------------------------------------------------------------------
 *
 * pg_proc.h
 *	  definition of the system "procedure" relation (pg_proc)
 *	  along with the relation's initial contents.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_proc.h
 *
 * NOTES
 *	  The script catalog/genbki.pl reads this file and generates .bki
 *	  information from the DATA() statements.  utils/Gen_fmgrtab.pl
 *	  generates fmgroids.h and fmgrtab.c the same way.
 *
 *	  XXX do NOT break up DATA() statements into multiple lines!
 *		  the scripts are not as smart as you might think...
 *	  XXX (eg. #if 0 #endif won't do what you think)
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROC_H
#define PG_PROC_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_proc definition.  cpp turns this into
 *		typedef struct FormData_pg_proc
 * ----------------
 */
#define ProcedureRelationId  1255
#define ProcedureRelation_Rowtype_Id  81

CATALOG(pg_proc,1255) BKI_BOOTSTRAP BKI_ROWTYPE_OID(81) BKI_SCHEMA_MACRO
{
	NameData	proname;		/* procedure name */
	Oid			pronamespace;	/* OID of namespace containing this proc */
	Oid			proowner;		/* procedure owner */
	Oid			prolang;		/* OID of pg_language entry */
	float4		procost;		/* estimated execution cost */
	float4		prorows;		/* estimated # of rows out (if proretset) */
	Oid			provariadic;	/* element type of variadic array, or 0 */
	regproc		protransform;	/* transforms calls to it during planning */
	bool		proisagg;		/* is it an aggregate? */
	bool		proiswindow;	/* is it a window function? */
	bool		prosecdef;		/* security definer */
	bool		proleakproof;	/* is it a leak-proof function? */
	bool		proisstrict;	/* strict with respect to NULLs? */
	bool		proretset;		/* returns a set? */
	char		provolatile;	/* see PROVOLATILE_ categories below */
	int16		pronargs;		/* number of arguments */
	int16		pronargdefaults;	/* number of arguments with defaults */
	Oid			prorettype;		/* OID of result type */

	/*
	 * variable-length fields start here, but we allow direct access to
	 * proargtypes
	 */
	oidvector	proargtypes;	/* parameter types (excludes OUT params) */

#ifdef CATALOG_VARLEN
	Oid			proallargtypes[1];		/* all param types (NULL if IN only) */
	char		proargmodes[1]; /* parameter modes (NULL if IN only) */
	text		proargnames[1]; /* parameter names (NULL if no names) */
	pg_node_tree proargdefaults;/* list of expression trees for argument
								 * defaults (NULL if none) */
	Oid			protrftypes[1]; /* types for which to apply transforms */
	text prosrc BKI_FORCE_NOT_NULL;		/* procedure source text */
	text		probin;			/* secondary procedure info (can be NULL) */
	text		proconfig[1];	/* procedure-local GUC settings */
	aclitem		proacl[1];		/* access permissions */
#endif
} FormData_pg_proc;

/* ----------------
 *		Form_pg_proc corresponds to a pointer to a tuple with
 *		the format of pg_proc relation.
 * ----------------
 */
typedef FormData_pg_proc *Form_pg_proc;

/* ----------------
 *		compiler constants for pg_proc
 * ----------------
 */
#define Natts_pg_proc					28
#define Anum_pg_proc_proname			1
#define Anum_pg_proc_pronamespace		2
#define Anum_pg_proc_proowner			3
#define Anum_pg_proc_prolang			4
#define Anum_pg_proc_procost			5
#define Anum_pg_proc_prorows			6
#define Anum_pg_proc_provariadic		7
#define Anum_pg_proc_protransform		8
#define Anum_pg_proc_proisagg			9
#define Anum_pg_proc_proiswindow		10
#define Anum_pg_proc_prosecdef			11
#define Anum_pg_proc_proleakproof		12
#define Anum_pg_proc_proisstrict		13
#define Anum_pg_proc_proretset			14
#define Anum_pg_proc_provolatile		15
#define Anum_pg_proc_pronargs			16
#define Anum_pg_proc_pronargdefaults	17
#define Anum_pg_proc_prorettype			18
#define Anum_pg_proc_proargtypes		19
#define Anum_pg_proc_proallargtypes		20
#define Anum_pg_proc_proargmodes		21
#define Anum_pg_proc_proargnames		22
#define Anum_pg_proc_proargdefaults		23
#define Anum_pg_proc_protrftypes		24
#define Anum_pg_proc_prosrc				25
#define Anum_pg_proc_probin				26
#define Anum_pg_proc_proconfig			27
#define Anum_pg_proc_proacl				28

/* ----------------
 *		initial contents of pg_proc
 * ----------------
 */

/*
 * Note: every entry in pg_proc.h is expected to have a DESCR() comment,
 * except for functions that implement pg_operator.h operators and don't
 * have a good reason to be called directly rather than via the operator.
 * (If you do expect such a function to be used directly, you should
 * duplicate the operator's comment.)  initdb will supply suitable default
 * comments for functions referenced by pg_operator.
 *
 * Try to follow the style of existing functions' comments.
 * Some recommended conventions:
 *		"I/O" for typinput, typoutput, typreceive, typsend functions
 *		"I/O typmod" for typmodin, typmodout functions
 *		"aggregate transition function" for aggtransfn functions, unless
 *					they are reasonably useful in their own right
 *		"aggregate final function" for aggfinalfn functions (likewise)
 *		"convert srctypename to desttypename" for cast functions
 *		"less-equal-greater" for B-tree comparison functions
 */

/* keep the following ordered by OID so that later changes can be made easier */

/* OIDS 1 - 99 */

DATA(insert OID = 1242 (  boolin		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "2275" _null_ _null_ _null_ _null_ _null_ boolin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1243 (  boolout		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "16" _null_ _null_ _null_ _null_ _null_ boolout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1244 (  byteain		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "2275" _null_ _null_ _null_ _null_ _null_ byteain _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  31 (  byteaout		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "17" _null_ _null_ _null_ _null_ _null_ byteaout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1245 (  charin		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 18 "2275" _null_ _null_ _null_ _null_ _null_ charin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  33 (  charout		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "18" _null_ _null_ _null_ _null_ _null_ charout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  34 (  namein			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 19 "2275" _null_ _null_ _null_ _null_ _null_ namein _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  35 (  nameout		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "19" _null_ _null_ _null_ _null_ _null_ nameout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  38 (  int2in			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "2275" _null_ _null_ _null_ _null_ _null_ int2in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  39 (  int2out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "21" _null_ _null_ _null_ _null_ _null_ int2out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  40 (  int2vectorin	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 22 "2275" _null_ _null_ _null_ _null_ _null_ int2vectorin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  41 (  int2vectorout	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "22" _null_ _null_ _null_ _null_ _null_ int2vectorout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  42 (  int4in			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "2275" _null_ _null_ _null_ _null_ _null_ int4in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  43 (  int4out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_ int4out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  44 (  regprocin		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 24 "2275" _null_ _null_ _null_ _null_ _null_ regprocin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  45 (  regprocout		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "24" _null_ _null_ _null_ _null_ _null_ regprocout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3494 (  to_regproc		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 24 "2275" _null_ _null_ _null_ _null_ _null_ to_regproc _null_ _null_ _null_ ));
DESCR("convert proname to regproc");
DATA(insert OID = 3479 (  to_regprocedure	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2202 "2275" _null_ _null_ _null_ _null_ _null_ to_regprocedure _null_ _null_ _null_ ));
DESCR("convert proname to regprocedure");
DATA(insert OID =  46 (  textin			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "2275" _null_ _null_ _null_ _null_ _null_ textin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  47 (  textout		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "25" _null_ _null_ _null_ _null_ _null_ textout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  48 (  tidin			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 27 "2275" _null_ _null_ _null_ _null_ _null_ tidin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  49 (  tidout			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "27" _null_ _null_ _null_ _null_ _null_ tidout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  50 (  xidin			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 28 "2275" _null_ _null_ _null_ _null_ _null_ xidin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  51 (  xidout			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "28" _null_ _null_ _null_ _null_ _null_ xidout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  52 (  cidin			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 29 "2275" _null_ _null_ _null_ _null_ _null_ cidin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  53 (  cidout			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "29" _null_ _null_ _null_ _null_ _null_ cidout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  54 (  oidvectorin	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 30 "2275" _null_ _null_ _null_ _null_ _null_ oidvectorin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  55 (  oidvectorout	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "30" _null_ _null_ _null_ _null_ _null_ oidvectorout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  56 (  boollt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "16 16" _null_ _null_ _null_ _null_ _null_ boollt _null_ _null_ _null_ ));
DATA(insert OID =  57 (  boolgt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "16 16" _null_ _null_ _null_ _null_ _null_ boolgt _null_ _null_ _null_ ));
DATA(insert OID =  60 (  booleq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "16 16" _null_ _null_ _null_ _null_ _null_ booleq _null_ _null_ _null_ ));
DATA(insert OID =  61 (  chareq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "18 18" _null_ _null_ _null_ _null_ _null_ chareq _null_ _null_ _null_ ));
DATA(insert OID =  62 (  nameeq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "19 19" _null_ _null_ _null_ _null_ _null_ nameeq _null_ _null_ _null_ ));
DATA(insert OID =  63 (  int2eq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 21" _null_ _null_ _null_ _null_ _null_ int2eq _null_ _null_ _null_ ));
DATA(insert OID =  64 (  int2lt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 21" _null_ _null_ _null_ _null_ _null_ int2lt _null_ _null_ _null_ ));
DATA(insert OID =  65 (  int4eq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ int4eq _null_ _null_ _null_ ));
DATA(insert OID =  66 (  int4lt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ int4lt _null_ _null_ _null_ ));
DATA(insert OID =  67 (  texteq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ texteq _null_ _null_ _null_ ));
DATA(insert OID =  68 (  xideq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "28 28" _null_ _null_ _null_ _null_ _null_ xideq _null_ _null_ _null_ ));
DATA(insert OID =  69 (  cideq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "29 29" _null_ _null_ _null_ _null_ _null_ cideq _null_ _null_ _null_ ));
DATA(insert OID =  70 (  charne			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "18 18" _null_ _null_ _null_ _null_ _null_ charne _null_ _null_ _null_ ));
DATA(insert OID = 1246 (  charlt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "18 18" _null_ _null_ _null_ _null_ _null_ charlt _null_ _null_ _null_ ));
DATA(insert OID =  72 (  charle			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "18 18" _null_ _null_ _null_ _null_ _null_ charle _null_ _null_ _null_ ));
DATA(insert OID =  73 (  chargt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "18 18" _null_ _null_ _null_ _null_ _null_ chargt _null_ _null_ _null_ ));
DATA(insert OID =  74 (  charge			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "18 18" _null_ _null_ _null_ _null_ _null_ charge _null_ _null_ _null_ ));
DATA(insert OID =  77 (  int4			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23	"18" _null_ _null_ _null_ _null_ _null_ chartoi4 _null_ _null_ _null_ ));
DESCR("convert char to int4");
DATA(insert OID =  78 (  char			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 18	"23" _null_ _null_ _null_ _null_ _null_ i4tochar _null_ _null_ _null_ ));
DESCR("convert int4 to char");

DATA(insert OID =  79 (  nameregexeq	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ nameregexeq _null_ _null_ _null_ ));
DATA(insert OID = 1252 (  nameregexne	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ nameregexne _null_ _null_ _null_ ));
DATA(insert OID = 1254 (  textregexeq	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ textregexeq _null_ _null_ _null_ ));
DATA(insert OID = 1256 (  textregexne	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ textregexne _null_ _null_ _null_ ));
DATA(insert OID = 1257 (  textlen		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "25" _null_ _null_ _null_ _null_ _null_ textlen _null_ _null_ _null_ ));
DESCR("length");
DATA(insert OID = 1258 (  textcat		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ textcat _null_ _null_ _null_ ));

DATA(insert OID =  84 (  boolne			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "16 16" _null_ _null_ _null_ _null_ _null_ boolne _null_ _null_ _null_ ));
DATA(insert OID =  89 (  version		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 25 "" _null_ _null_ _null_ _null_ _null_ pgsql_version _null_ _null_ _null_ ));
DESCR("PostgreSQL version string");

DATA(insert OID = 86  (  pg_ddl_command_in		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 32 "2275" _null_ _null_ _null_ _null_ _null_ pg_ddl_command_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 87  (  pg_ddl_command_out		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "32" _null_ _null_ _null_ _null_ _null_ pg_ddl_command_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 88  (  pg_ddl_command_recv	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 32 "2281" _null_ _null_ _null_ _null_ _null_ pg_ddl_command_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 90  (  pg_ddl_command_send	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "32" _null_ _null_ _null_ _null_ _null_ pg_ddl_command_send _null_ _null_ _null_ ));
DESCR("I/O");

/* OIDS 100 - 199 */

DATA(insert OID = 101 (  eqsel			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	eqsel _null_ _null_ _null_ ));
DESCR("restriction selectivity of = and related operators");
DATA(insert OID = 102 (  neqsel			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	neqsel _null_ _null_ _null_ ));
DESCR("restriction selectivity of <> and related operators");
DATA(insert OID = 103 (  scalarltsel	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	scalarltsel _null_ _null_ _null_ ));
DESCR("restriction selectivity of < and related operators on scalar datatypes");
DATA(insert OID = 104 (  scalargtsel	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	scalargtsel _null_ _null_ _null_ ));
DESCR("restriction selectivity of > and related operators on scalar datatypes");
DATA(insert OID = 105 (  eqjoinsel		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_ _null_	eqjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of = and related operators");
DATA(insert OID = 106 (  neqjoinsel		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_ _null_	neqjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of <> and related operators");
DATA(insert OID = 107 (  scalarltjoinsel   PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_ _null_	scalarltjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of < and related operators on scalar datatypes");
DATA(insert OID = 108 (  scalargtjoinsel   PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_ _null_	scalargtjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of > and related operators on scalar datatypes");

DATA(insert OID =  109 (  unknownin		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 705 "2275" _null_ _null_ _null_ _null_ _null_ unknownin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  110 (  unknownout	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "705" _null_ _null_ _null_ _null_ _null_	unknownout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 111 (  numeric_fac	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_	numeric_fac _null_ _null_ _null_ ));

DATA(insert OID = 115 (  box_above_eq	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_above_eq _null_ _null_ _null_ ));
DATA(insert OID = 116 (  box_below_eq	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_below_eq _null_ _null_ _null_ ));

DATA(insert OID = 117 (  point_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "2275" _null_ _null_ _null_ _null_ _null_	point_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 118 (  point_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "600" _null_ _null_ _null_ _null_ _null_	point_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 119 (  lseg_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 601 "2275" _null_ _null_ _null_ _null_ _null_	lseg_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 120 (  lseg_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "601" _null_ _null_ _null_ _null_ _null_	lseg_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 121 (  path_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 602 "2275" _null_ _null_ _null_ _null_ _null_	path_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 122 (  path_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "602" _null_ _null_ _null_ _null_ _null_	path_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 123 (  box_in			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 603 "2275" _null_ _null_ _null_ _null_ _null_	box_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 124 (  box_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "603" _null_ _null_ _null_ _null_ _null_	box_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 125 (  box_overlap	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_overlap _null_ _null_ _null_ ));
DATA(insert OID = 126 (  box_ge			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_ge _null_ _null_ _null_ ));
DATA(insert OID = 127 (  box_gt			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_gt _null_ _null_ _null_ ));
DATA(insert OID = 128 (  box_eq			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_eq _null_ _null_ _null_ ));
DATA(insert OID = 129 (  box_lt			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_lt _null_ _null_ _null_ ));
DATA(insert OID = 130 (  box_le			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_le _null_ _null_ _null_ ));
DATA(insert OID = 131 (  point_above	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 600" _null_ _null_ _null_ _null_ _null_ point_above _null_ _null_ _null_ ));
DATA(insert OID = 132 (  point_left		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 600" _null_ _null_ _null_ _null_ _null_ point_left _null_ _null_ _null_ ));
DATA(insert OID = 133 (  point_right	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 600" _null_ _null_ _null_ _null_ _null_ point_right _null_ _null_ _null_ ));
DATA(insert OID = 134 (  point_below	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 600" _null_ _null_ _null_ _null_ _null_ point_below _null_ _null_ _null_ ));
DATA(insert OID = 135 (  point_eq		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 600" _null_ _null_ _null_ _null_ _null_ point_eq _null_ _null_ _null_ ));
DATA(insert OID = 136 (  on_pb			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 603" _null_ _null_ _null_ _null_ _null_ on_pb _null_ _null_ _null_ ));
DATA(insert OID = 137 (  on_ppath		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 602" _null_ _null_ _null_ _null_ _null_ on_ppath _null_ _null_ _null_ ));
DATA(insert OID = 138 (  box_center		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "603" _null_ _null_ _null_ _null_ _null_	box_center _null_ _null_ _null_ ));
DATA(insert OID = 139 (  areasel		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	areasel _null_ _null_ _null_ ));
DESCR("restriction selectivity for area-comparison operators");
DATA(insert OID = 140 (  areajoinsel	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_ _null_	areajoinsel _null_ _null_ _null_ ));
DESCR("join selectivity for area-comparison operators");
DATA(insert OID = 141 (  int4mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4mul _null_ _null_ _null_ ));
DATA(insert OID = 144 (  int4ne			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ int4ne _null_ _null_ _null_ ));
DATA(insert OID = 145 (  int2ne			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 21" _null_ _null_ _null_ _null_ _null_ int2ne _null_ _null_ _null_ ));
DATA(insert OID = 146 (  int2gt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 21" _null_ _null_ _null_ _null_ _null_ int2gt _null_ _null_ _null_ ));
DATA(insert OID = 147 (  int4gt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ int4gt _null_ _null_ _null_ ));
DATA(insert OID = 148 (  int2le			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 21" _null_ _null_ _null_ _null_ _null_ int2le _null_ _null_ _null_ ));
DATA(insert OID = 149 (  int4le			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ int4le _null_ _null_ _null_ ));
DATA(insert OID = 150 (  int4ge			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ int4ge _null_ _null_ _null_ ));
DATA(insert OID = 151 (  int2ge			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 21" _null_ _null_ _null_ _null_ _null_ int2ge _null_ _null_ _null_ ));
DATA(insert OID = 152 (  int2mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2mul _null_ _null_ _null_ ));
DATA(insert OID = 153 (  int2div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2div _null_ _null_ _null_ ));
DATA(insert OID = 154 (  int4div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4div _null_ _null_ _null_ ));
DATA(insert OID = 155 (  int2mod		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2mod _null_ _null_ _null_ ));
DATA(insert OID = 156 (  int4mod		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4mod _null_ _null_ _null_ ));
DATA(insert OID = 157 (  textne			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ textne _null_ _null_ _null_ ));
DATA(insert OID = 158 (  int24eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 23" _null_ _null_ _null_ _null_ _null_ int24eq _null_ _null_ _null_ ));
DATA(insert OID = 159 (  int42eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 21" _null_ _null_ _null_ _null_ _null_ int42eq _null_ _null_ _null_ ));
DATA(insert OID = 160 (  int24lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 23" _null_ _null_ _null_ _null_ _null_ int24lt _null_ _null_ _null_ ));
DATA(insert OID = 161 (  int42lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 21" _null_ _null_ _null_ _null_ _null_ int42lt _null_ _null_ _null_ ));
DATA(insert OID = 162 (  int24gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 23" _null_ _null_ _null_ _null_ _null_ int24gt _null_ _null_ _null_ ));
DATA(insert OID = 163 (  int42gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 21" _null_ _null_ _null_ _null_ _null_ int42gt _null_ _null_ _null_ ));
DATA(insert OID = 164 (  int24ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 23" _null_ _null_ _null_ _null_ _null_ int24ne _null_ _null_ _null_ ));
DATA(insert OID = 165 (  int42ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 21" _null_ _null_ _null_ _null_ _null_ int42ne _null_ _null_ _null_ ));
DATA(insert OID = 166 (  int24le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 23" _null_ _null_ _null_ _null_ _null_ int24le _null_ _null_ _null_ ));
DATA(insert OID = 167 (  int42le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 21" _null_ _null_ _null_ _null_ _null_ int42le _null_ _null_ _null_ ));
DATA(insert OID = 168 (  int24ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 23" _null_ _null_ _null_ _null_ _null_ int24ge _null_ _null_ _null_ ));
DATA(insert OID = 169 (  int42ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 21" _null_ _null_ _null_ _null_ _null_ int42ge _null_ _null_ _null_ ));
DATA(insert OID = 170 (  int24mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "21 23" _null_ _null_ _null_ _null_ _null_ int24mul _null_ _null_ _null_ ));
DATA(insert OID = 171 (  int42mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 21" _null_ _null_ _null_ _null_ _null_ int42mul _null_ _null_ _null_ ));
DATA(insert OID = 172 (  int24div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "21 23" _null_ _null_ _null_ _null_ _null_ int24div _null_ _null_ _null_ ));
DATA(insert OID = 173 (  int42div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 21" _null_ _null_ _null_ _null_ _null_ int42div _null_ _null_ _null_ ));
DATA(insert OID = 176 (  int2pl			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2pl _null_ _null_ _null_ ));
DATA(insert OID = 177 (  int4pl			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4pl _null_ _null_ _null_ ));
DATA(insert OID = 178 (  int24pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "21 23" _null_ _null_ _null_ _null_ _null_ int24pl _null_ _null_ _null_ ));
DATA(insert OID = 179 (  int42pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 21" _null_ _null_ _null_ _null_ _null_ int42pl _null_ _null_ _null_ ));
DATA(insert OID = 180 (  int2mi			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2mi _null_ _null_ _null_ ));
DATA(insert OID = 181 (  int4mi			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4mi _null_ _null_ _null_ ));
DATA(insert OID = 182 (  int24mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "21 23" _null_ _null_ _null_ _null_ _null_ int24mi _null_ _null_ _null_ ));
DATA(insert OID = 183 (  int42mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 21" _null_ _null_ _null_ _null_ _null_ int42mi _null_ _null_ _null_ ));
DATA(insert OID = 184 (  oideq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "26 26" _null_ _null_ _null_ _null_ _null_ oideq _null_ _null_ _null_ ));
DATA(insert OID = 185 (  oidne			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "26 26" _null_ _null_ _null_ _null_ _null_ oidne _null_ _null_ _null_ ));
DATA(insert OID = 186 (  box_same		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_same _null_ _null_ _null_ ));
DATA(insert OID = 187 (  box_contain	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_contain _null_ _null_ _null_ ));
DATA(insert OID = 188 (  box_left		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_left _null_ _null_ _null_ ));
DATA(insert OID = 189 (  box_overleft	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_overleft _null_ _null_ _null_ ));
DATA(insert OID = 190 (  box_overright	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_overright _null_ _null_ _null_ ));
DATA(insert OID = 191 (  box_right		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_right _null_ _null_ _null_ ));
DATA(insert OID = 192 (  box_contained	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_contained _null_ _null_ _null_ ));
DATA(insert OID = 193 (  box_contain_pt    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 600" _null_ _null_ _null_ _null_ _null_ box_contain_pt _null_ _null_ _null_ ));

DATA(insert OID = 195 (  pg_node_tree_in	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 194 "2275" _null_ _null_ _null_ _null_ _null_ pg_node_tree_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 196 (  pg_node_tree_out	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "194" _null_ _null_ _null_ _null_ _null_ pg_node_tree_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 197 (  pg_node_tree_recv	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 194 "2281" _null_ _null_ _null_ _null_ _null_ pg_node_tree_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 198 (  pg_node_tree_send	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "194" _null_ _null_ _null_ _null_ _null_	pg_node_tree_send _null_ _null_ _null_ ));
DESCR("I/O");

/* OIDS 200 - 299 */

DATA(insert OID = 200 (  float4in		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700 "2275" _null_ _null_ _null_ _null_ _null_	float4in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 201 (  float4out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "700" _null_ _null_ _null_ _null_ _null_	float4out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 202 (  float4mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 700 "700 700" _null_ _null_ _null_ _null_ _null_	float4mul _null_ _null_ _null_ ));
DATA(insert OID = 203 (  float4div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 700 "700 700" _null_ _null_ _null_ _null_ _null_	float4div _null_ _null_ _null_ ));
DATA(insert OID = 204 (  float4pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 700 "700 700" _null_ _null_ _null_ _null_ _null_	float4pl _null_ _null_ _null_ ));
DATA(insert OID = 205 (  float4mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 700 "700 700" _null_ _null_ _null_ _null_ _null_	float4mi _null_ _null_ _null_ ));
DATA(insert OID = 206 (  float4um		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700 "700" _null_ _null_ _null_ _null_ _null_	float4um _null_ _null_ _null_ ));
DATA(insert OID = 207 (  float4abs		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700 "700" _null_ _null_ _null_ _null_ _null_	float4abs _null_ _null_ _null_ ));
DATA(insert OID = 208 (  float4_accum	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1022 "1022 700" _null_ _null_ _null_ _null_ _null_ float4_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 209 (  float4larger	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 700 "700 700" _null_ _null_ _null_ _null_ _null_	float4larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 211 (  float4smaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 700 "700 700" _null_ _null_ _null_ _null_ _null_	float4smaller _null_ _null_ _null_ ));
DESCR("smaller of two");

DATA(insert OID = 212 (  int4um			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ int4um _null_ _null_ _null_ ));
DATA(insert OID = 213 (  int2um			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "21" _null_ _null_ _null_ _null_ _null_ int2um _null_ _null_ _null_ ));

DATA(insert OID = 214 (  float8in		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "2275" _null_ _null_ _null_ _null_ _null_	float8in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 215 (  float8out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "701" _null_ _null_ _null_ _null_ _null_	float8out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 216 (  float8mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	float8mul _null_ _null_ _null_ ));
DATA(insert OID = 217 (  float8div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	float8div _null_ _null_ _null_ ));
DATA(insert OID = 218 (  float8pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	float8pl _null_ _null_ _null_ ));
DATA(insert OID = 219 (  float8mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	float8mi _null_ _null_ _null_ ));
DATA(insert OID = 220 (  float8um		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	float8um _null_ _null_ _null_ ));
DATA(insert OID = 221 (  float8abs		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	float8abs _null_ _null_ _null_ ));
DATA(insert OID = 222 (  float8_accum	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1022 "1022 701" _null_ _null_ _null_ _null_ _null_ float8_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 223 (  float8larger	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	float8larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 224 (  float8smaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	float8smaller _null_ _null_ _null_ ));
DESCR("smaller of two");

DATA(insert OID = 225 (  lseg_center	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "601" _null_ _null_ _null_ _null_ _null_	lseg_center _null_ _null_ _null_ ));
DATA(insert OID = 226 (  path_center	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "602" _null_ _null_ _null_ _null_ _null_	path_center _null_ _null_ _null_ ));
DATA(insert OID = 227 (  poly_center	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "604" _null_ _null_ _null_ _null_ _null_	poly_center _null_ _null_ _null_ ));

DATA(insert OID = 228 (  dround			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	dround _null_ _null_ _null_ ));
DESCR("round to nearest integer");
DATA(insert OID = 229 (  dtrunc			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	dtrunc _null_ _null_ _null_ ));
DESCR("truncate to integer");
DATA(insert OID = 2308 ( ceil			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	dceil _null_ _null_ _null_ ));
DESCR("smallest integer >= value");
DATA(insert OID = 2320 ( ceiling		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	dceil _null_ _null_ _null_ ));
DESCR("smallest integer >= value");
DATA(insert OID = 2309 ( floor			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	dfloor _null_ _null_ _null_ ));
DESCR("largest integer <= value");
DATA(insert OID = 2310 ( sign			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	dsign _null_ _null_ _null_ ));
DESCR("sign of value");
DATA(insert OID = 230 (  dsqrt			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	dsqrt _null_ _null_ _null_ ));
DATA(insert OID = 231 (  dcbrt			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	dcbrt _null_ _null_ _null_ ));
DATA(insert OID = 232 (  dpow			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	dpow _null_ _null_ _null_ ));
DATA(insert OID = 233 (  dexp			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	dexp _null_ _null_ _null_ ));
DESCR("natural exponential (e^x)");
DATA(insert OID = 234 (  dlog1			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	dlog1 _null_ _null_ _null_ ));
DESCR("natural logarithm");
DATA(insert OID = 235 (  float8			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "21" _null_ _null_ _null_ _null_ _null_ i2tod _null_ _null_ _null_ ));
DESCR("convert int2 to float8");
DATA(insert OID = 236 (  float4			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700 "21" _null_ _null_ _null_ _null_ _null_ i2tof _null_ _null_ _null_ ));
DESCR("convert int2 to float4");
DATA(insert OID = 237 (  int2			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "701" _null_ _null_ _null_ _null_ _null_ dtoi2 _null_ _null_ _null_ ));
DESCR("convert float8 to int2");
DATA(insert OID = 238 (  int2			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "700" _null_ _null_ _null_ _null_ _null_ ftoi2 _null_ _null_ _null_ ));
DESCR("convert float4 to int2");
DATA(insert OID = 239 (  line_distance	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "628 628" _null_ _null_ _null_ _null_ _null_	line_distance _null_ _null_ _null_ ));

DATA(insert OID = 240 (  abstimein		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 702 "2275" _null_ _null_ _null_ _null_ _null_	abstimein _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 241 (  abstimeout		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "702" _null_ _null_ _null_ _null_ _null_	abstimeout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 242 (  reltimein		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 703 "2275" _null_ _null_ _null_ _null_ _null_	reltimein _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 243 (  reltimeout		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "703" _null_ _null_ _null_ _null_ _null_	reltimeout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 244 (  timepl			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 702 "702 703" _null_ _null_ _null_ _null_ _null_	timepl _null_ _null_ _null_ ));
DATA(insert OID = 245 (  timemi			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 702 "702 703" _null_ _null_ _null_ _null_ _null_	timemi _null_ _null_ _null_ ));
DATA(insert OID = 246 (  tintervalin	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 704 "2275" _null_ _null_ _null_ _null_ _null_	tintervalin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 247 (  tintervalout	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "704" _null_ _null_ _null_ _null_ _null_	tintervalout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 248 (  intinterval	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "702 704" _null_ _null_ _null_ _null_ _null_ intinterval _null_ _null_ _null_ ));
DATA(insert OID = 249 (  tintervalrel	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 703 "704" _null_ _null_ _null_ _null_ _null_	tintervalrel _null_ _null_ _null_ ));
DESCR("tinterval to reltime");
DATA(insert OID = 250 (  timenow		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 702 "" _null_ _null_ _null_ _null_ _null_	timenow _null_ _null_ _null_ ));
DESCR("current date and time (abstime)");
DATA(insert OID = 251 (  abstimeeq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "702 702" _null_ _null_ _null_ _null_ _null_ abstimeeq _null_ _null_ _null_ ));
DATA(insert OID = 252 (  abstimene		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "702 702" _null_ _null_ _null_ _null_ _null_ abstimene _null_ _null_ _null_ ));
DATA(insert OID = 253 (  abstimelt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "702 702" _null_ _null_ _null_ _null_ _null_ abstimelt _null_ _null_ _null_ ));
DATA(insert OID = 254 (  abstimegt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "702 702" _null_ _null_ _null_ _null_ _null_ abstimegt _null_ _null_ _null_ ));
DATA(insert OID = 255 (  abstimele		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "702 702" _null_ _null_ _null_ _null_ _null_ abstimele _null_ _null_ _null_ ));
DATA(insert OID = 256 (  abstimege		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "702 702" _null_ _null_ _null_ _null_ _null_ abstimege _null_ _null_ _null_ ));
DATA(insert OID = 257 (  reltimeeq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "703 703" _null_ _null_ _null_ _null_ _null_ reltimeeq _null_ _null_ _null_ ));
DATA(insert OID = 258 (  reltimene		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "703 703" _null_ _null_ _null_ _null_ _null_ reltimene _null_ _null_ _null_ ));
DATA(insert OID = 259 (  reltimelt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "703 703" _null_ _null_ _null_ _null_ _null_ reltimelt _null_ _null_ _null_ ));
DATA(insert OID = 260 (  reltimegt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "703 703" _null_ _null_ _null_ _null_ _null_ reltimegt _null_ _null_ _null_ ));
DATA(insert OID = 261 (  reltimele		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "703 703" _null_ _null_ _null_ _null_ _null_ reltimele _null_ _null_ _null_ ));
DATA(insert OID = 262 (  reltimege		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "703 703" _null_ _null_ _null_ _null_ _null_ reltimege _null_ _null_ _null_ ));
DATA(insert OID = 263 (  tintervalsame	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "704 704" _null_ _null_ _null_ _null_ _null_ tintervalsame _null_ _null_ _null_ ));
DATA(insert OID = 264 (  tintervalct	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "704 704" _null_ _null_ _null_ _null_ _null_ tintervalct _null_ _null_ _null_ ));
DATA(insert OID = 265 (  tintervalov	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "704 704" _null_ _null_ _null_ _null_ _null_ tintervalov _null_ _null_ _null_ ));
DATA(insert OID = 266 (  tintervalleneq    PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 703" _null_ _null_ _null_ _null_ _null_ tintervalleneq _null_ _null_ _null_ ));
DATA(insert OID = 267 (  tintervallenne    PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 703" _null_ _null_ _null_ _null_ _null_ tintervallenne _null_ _null_ _null_ ));
DATA(insert OID = 268 (  tintervallenlt    PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 703" _null_ _null_ _null_ _null_ _null_ tintervallenlt _null_ _null_ _null_ ));
DATA(insert OID = 269 (  tintervallengt    PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 703" _null_ _null_ _null_ _null_ _null_ tintervallengt _null_ _null_ _null_ ));
DATA(insert OID = 270 (  tintervallenle    PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 703" _null_ _null_ _null_ _null_ _null_ tintervallenle _null_ _null_ _null_ ));
DATA(insert OID = 271 (  tintervallenge    PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 703" _null_ _null_ _null_ _null_ _null_ tintervallenge _null_ _null_ _null_ ));
DATA(insert OID = 272 (  tintervalstart    PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 702 "704" _null_ _null_ _null_ _null_ _null_	tintervalstart _null_ _null_ _null_ ));
DATA(insert OID = 273 (  tintervalend	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 702 "704" _null_ _null_ _null_ _null_ _null_	tintervalend _null_ _null_ _null_ ));
DESCR("end of interval");
DATA(insert OID = 274 (  timeofday		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 25 "" _null_ _null_ _null_ _null_ _null_ timeofday _null_ _null_ _null_ ));
DESCR("current date and time - increments during transactions");
DATA(insert OID = 275 (  isfinite		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "702" _null_ _null_ _null_ _null_ _null_ abstime_finite _null_ _null_ _null_ ));
DESCR("finite abstime?");

DATA(insert OID = 277 (  inter_sl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "601 628" _null_ _null_ _null_ _null_ _null_ inter_sl _null_ _null_ _null_ ));
DATA(insert OID = 278 (  inter_lb		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "628 603" _null_ _null_ _null_ _null_ _null_ inter_lb _null_ _null_ _null_ ));

DATA(insert OID = 279 (  float48mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "700 701" _null_ _null_ _null_ _null_ _null_	float48mul _null_ _null_ _null_ ));
DATA(insert OID = 280 (  float48div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "700 701" _null_ _null_ _null_ _null_ _null_	float48div _null_ _null_ _null_ ));
DATA(insert OID = 281 (  float48pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "700 701" _null_ _null_ _null_ _null_ _null_	float48pl _null_ _null_ _null_ ));
DATA(insert OID = 282 (  float48mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "700 701" _null_ _null_ _null_ _null_ _null_	float48mi _null_ _null_ _null_ ));
DATA(insert OID = 283 (  float84mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 700" _null_ _null_ _null_ _null_ _null_	float84mul _null_ _null_ _null_ ));
DATA(insert OID = 284 (  float84div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 700" _null_ _null_ _null_ _null_ _null_	float84div _null_ _null_ _null_ ));
DATA(insert OID = 285 (  float84pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 700" _null_ _null_ _null_ _null_ _null_	float84pl _null_ _null_ _null_ ));
DATA(insert OID = 286 (  float84mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 700" _null_ _null_ _null_ _null_ _null_	float84mi _null_ _null_ _null_ ));

DATA(insert OID = 287 (  float4eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 700" _null_ _null_ _null_ _null_ _null_ float4eq _null_ _null_ _null_ ));
DATA(insert OID = 288 (  float4ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 700" _null_ _null_ _null_ _null_ _null_ float4ne _null_ _null_ _null_ ));
DATA(insert OID = 289 (  float4lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 700" _null_ _null_ _null_ _null_ _null_ float4lt _null_ _null_ _null_ ));
DATA(insert OID = 290 (  float4le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 700" _null_ _null_ _null_ _null_ _null_ float4le _null_ _null_ _null_ ));
DATA(insert OID = 291 (  float4gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 700" _null_ _null_ _null_ _null_ _null_ float4gt _null_ _null_ _null_ ));
DATA(insert OID = 292 (  float4ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 700" _null_ _null_ _null_ _null_ _null_ float4ge _null_ _null_ _null_ ));

DATA(insert OID = 293 (  float8eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 701" _null_ _null_ _null_ _null_ _null_ float8eq _null_ _null_ _null_ ));
DATA(insert OID = 294 (  float8ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 701" _null_ _null_ _null_ _null_ _null_ float8ne _null_ _null_ _null_ ));
DATA(insert OID = 295 (  float8lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 701" _null_ _null_ _null_ _null_ _null_ float8lt _null_ _null_ _null_ ));
DATA(insert OID = 296 (  float8le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 701" _null_ _null_ _null_ _null_ _null_ float8le _null_ _null_ _null_ ));
DATA(insert OID = 297 (  float8gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 701" _null_ _null_ _null_ _null_ _null_ float8gt _null_ _null_ _null_ ));
DATA(insert OID = 298 (  float8ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 701" _null_ _null_ _null_ _null_ _null_ float8ge _null_ _null_ _null_ ));

DATA(insert OID = 299 (  float48eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 701" _null_ _null_ _null_ _null_ _null_ float48eq _null_ _null_ _null_ ));

/* OIDS 300 - 399 */

DATA(insert OID = 300 (  float48ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 701" _null_ _null_ _null_ _null_ _null_ float48ne _null_ _null_ _null_ ));
DATA(insert OID = 301 (  float48lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 701" _null_ _null_ _null_ _null_ _null_ float48lt _null_ _null_ _null_ ));
DATA(insert OID = 302 (  float48le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 701" _null_ _null_ _null_ _null_ _null_ float48le _null_ _null_ _null_ ));
DATA(insert OID = 303 (  float48gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 701" _null_ _null_ _null_ _null_ _null_ float48gt _null_ _null_ _null_ ));
DATA(insert OID = 304 (  float48ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "700 701" _null_ _null_ _null_ _null_ _null_ float48ge _null_ _null_ _null_ ));
DATA(insert OID = 305 (  float84eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 700" _null_ _null_ _null_ _null_ _null_ float84eq _null_ _null_ _null_ ));
DATA(insert OID = 306 (  float84ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 700" _null_ _null_ _null_ _null_ _null_ float84ne _null_ _null_ _null_ ));
DATA(insert OID = 307 (  float84lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 700" _null_ _null_ _null_ _null_ _null_ float84lt _null_ _null_ _null_ ));
DATA(insert OID = 308 (  float84le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 700" _null_ _null_ _null_ _null_ _null_ float84le _null_ _null_ _null_ ));
DATA(insert OID = 309 (  float84gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 700" _null_ _null_ _null_ _null_ _null_ float84gt _null_ _null_ _null_ ));
DATA(insert OID = 310 (  float84ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "701 700" _null_ _null_ _null_ _null_ _null_ float84ge _null_ _null_ _null_ ));
DATA(insert OID = 320 ( width_bucket	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 23 "701 701 701 23" _null_ _null_ _null_ _null_ _null_ width_bucket_float8 _null_ _null_ _null_ ));
DESCR("bucket number of operand in equal-width histogram");

DATA(insert OID = 311 (  float8			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "700" _null_ _null_ _null_ _null_ _null_	ftod _null_ _null_ _null_ ));
DESCR("convert float4 to float8");
DATA(insert OID = 312 (  float4			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700 "701" _null_ _null_ _null_ _null_ _null_	dtof _null_ _null_ _null_ ));
DESCR("convert float8 to float4");
DATA(insert OID = 313 (  int4			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23	"21" _null_ _null_ _null_ _null_ _null_ i2toi4 _null_ _null_ _null_ ));
DESCR("convert int2 to int4");
DATA(insert OID = 314 (  int2			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21	"23" _null_ _null_ _null_ _null_ _null_ i4toi2 _null_ _null_ _null_ ));
DESCR("convert int4 to int2");
DATA(insert OID = 315 (  int2vectoreq	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "22 22" _null_ _null_ _null_ _null_ _null_ int2vectoreq _null_ _null_ _null_ ));
DATA(insert OID = 316 (  float8			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701  "23" _null_ _null_ _null_ _null_ _null_	i4tod _null_ _null_ _null_ ));
DESCR("convert int4 to float8");
DATA(insert OID = 317 (  int4			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "701" _null_ _null_ _null_ _null_ _null_ dtoi4 _null_ _null_ _null_ ));
DESCR("convert float8 to int4");
DATA(insert OID = 318 (  float4			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700  "23" _null_ _null_ _null_ _null_ _null_	i4tof _null_ _null_ _null_ ));
DESCR("convert int4 to float4");
DATA(insert OID = 319 (  int4			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1  0 23 "700" _null_ _null_ _null_ _null_ _null_	ftoi4 _null_ _null_ _null_ ));
DESCR("convert float4 to int4");

DATA(insert OID = 330 (  btgettuple		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 16 "2281 2281" _null_ _null_ _null_ _null_ _null_	btgettuple _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 636 (  btgetbitmap	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 20 "2281 2281" _null_ _null_ _null_ _null_ _null_	btgetbitmap _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 331 (  btinsert		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 6 0 16 "2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_	btinsert _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 333 (  btbeginscan	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_	btbeginscan _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 334 (  btrescan		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 5 0 2278 "2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ btrescan _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 335 (  btendscan		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btendscan _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 336 (  btmarkpos		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btmarkpos _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 337 (  btrestrpos		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btrestrpos _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 338 (  btbuild		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ btbuild _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 328 (  btbuildempty	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btbuildempty _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 332 (  btbulkdelete	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 4 0 2281 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ btbulkdelete _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 972 (  btvacuumcleanup   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ btvacuumcleanup _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 276 (  btcanreturn	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "2281 23" _null_ _null_ _null_ _null_ _null_ btcanreturn _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 1268 (  btcostestimate   PGNSP PGUID 12 1 0 0 0 f f f f t f v 7 0 2278 "2281 2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ btcostestimate _null_ _null_ _null_ ));
DESCR("btree(internal)");
DATA(insert OID = 2785 (  btoptions		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 17 "1009 16" _null_ _null_ _null_ _null_  _null_ btoptions _null_ _null_ _null_ ));
DESCR("btree(internal)");

DATA(insert OID = 3789 (  bringetbitmap    PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 20 "2281 2281" _null_ _null_ _null_ _null_ _null_	bringetbitmap _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3790 (  brininsert		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 6 0 16 "2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_	brininsert _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3791 (  brinbeginscan    PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_	brinbeginscan _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3792 (  brinrescan		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 5 0 2278 "2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ brinrescan _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3793 (  brinendscan		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ brinendscan _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3794 (  brinmarkpos		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ brinmarkpos _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3795 (  brinrestrpos		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ brinrestrpos _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3796 (  brinbuild		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ brinbuild _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3797 (  brinbuildempty	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ brinbuildempty _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3798 (  brinbulkdelete	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 4 0 2281 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ brinbulkdelete _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3799 (  brinvacuumcleanup   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ brinvacuumcleanup _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3800 (  brincostestimate	 PGNSP PGUID 12 1 0 0 0 f f f f t f v 7 0 2278 "2281 2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ brincostestimate _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3801 (  brinoptions		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 17 "1009 16" _null_ _null_ _null_ _null_ _null_ brinoptions _null_ _null_ _null_ ));
DESCR("brin(internal)");
DATA(insert OID = 3952 (  brin_summarize_new_values PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 23 "2205" _null_ _null_ _null_ _null_ _null_ brin_summarize_new_values _null_ _null_ _null_ ));
DESCR("brin: standalone scan new table pages");

DATA(insert OID = 339 (  poly_same		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_same _null_ _null_ _null_ ));
DATA(insert OID = 340 (  poly_contain	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_contain _null_ _null_ _null_ ));
DATA(insert OID = 341 (  poly_left		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_left _null_ _null_ _null_ ));
DATA(insert OID = 342 (  poly_overleft	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_overleft _null_ _null_ _null_ ));
DATA(insert OID = 343 (  poly_overright    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_overright _null_ _null_ _null_ ));
DATA(insert OID = 344 (  poly_right		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_right _null_ _null_ _null_ ));
DATA(insert OID = 345 (  poly_contained    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_contained _null_ _null_ _null_ ));
DATA(insert OID = 346 (  poly_overlap	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_overlap _null_ _null_ _null_ ));
DATA(insert OID = 347 (  poly_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 604 "2275" _null_ _null_ _null_ _null_ _null_	poly_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 348 (  poly_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "604" _null_ _null_ _null_ _null_ _null_	poly_out _null_ _null_ _null_ ));
DESCR("I/O");

DATA(insert OID = 350 (  btint2cmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "21 21" _null_ _null_ _null_ _null_ _null_ btint2cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3129 ( btint2sortsupport PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btint2sortsupport _null_ _null_ _null_ ));
DESCR("sort support");
DATA(insert OID = 351 (  btint4cmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ btint4cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3130 ( btint4sortsupport PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btint4sortsupport _null_ _null_ _null_ ));
DESCR("sort support");
DATA(insert OID = 842 (  btint8cmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "20 20" _null_ _null_ _null_ _null_ _null_ btint8cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3131 ( btint8sortsupport PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btint8sortsupport _null_ _null_ _null_ ));
DESCR("sort support");
DATA(insert OID = 354 (  btfloat4cmp	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "700 700" _null_ _null_ _null_ _null_ _null_ btfloat4cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3132 ( btfloat4sortsupport PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btfloat4sortsupport _null_ _null_ _null_ ));
DESCR("sort support");
DATA(insert OID = 355 (  btfloat8cmp	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "701 701" _null_ _null_ _null_ _null_ _null_ btfloat8cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3133 ( btfloat8sortsupport PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btfloat8sortsupport _null_ _null_ _null_ ));
DESCR("sort support");
DATA(insert OID = 356 (  btoidcmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "26 26" _null_ _null_ _null_ _null_ _null_ btoidcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3134 ( btoidsortsupport PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btoidsortsupport _null_ _null_ _null_ ));
DESCR("sort support");
DATA(insert OID = 404 (  btoidvectorcmp    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "30 30" _null_ _null_ _null_ _null_ _null_ btoidvectorcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 357 (  btabstimecmp	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "702 702" _null_ _null_ _null_ _null_ _null_ btabstimecmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 358 (  btcharcmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "18 18" _null_ _null_ _null_ _null_ _null_ btcharcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 359 (  btnamecmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "19 19" _null_ _null_ _null_ _null_ _null_ btnamecmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3135 ( btnamesortsupport PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ btnamesortsupport _null_ _null_ _null_ ));
DESCR("sort support");
DATA(insert OID = 360 (  bttextcmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "25 25" _null_ _null_ _null_ _null_ _null_ bttextcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3255 ( bttextsortsupport PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ bttextsortsupport _null_ _null_ _null_ ));
DESCR("sort support");
DATA(insert OID = 377 (  cash_cmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "790 790" _null_ _null_ _null_ _null_ _null_ cash_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 380 (  btreltimecmp	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "703 703" _null_ _null_ _null_ _null_ _null_ btreltimecmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 381 (  bttintervalcmp    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "704 704" _null_ _null_ _null_ _null_ _null_ bttintervalcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 382 (  btarraycmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "2277 2277" _null_ _null_ _null_ _null_ _null_ btarraycmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 361 (  lseg_distance	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "601 601" _null_ _null_ _null_ _null_ _null_	lseg_distance _null_ _null_ _null_ ));
DATA(insert OID = 362 (  lseg_interpt	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "601 601" _null_ _null_ _null_ _null_ _null_	lseg_interpt _null_ _null_ _null_ ));
DATA(insert OID = 363 (  dist_ps		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "600 601" _null_ _null_ _null_ _null_ _null_	dist_ps _null_ _null_ _null_ ));
DATA(insert OID = 364 (  dist_pb		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "600 603" _null_ _null_ _null_ _null_ _null_	dist_pb _null_ _null_ _null_ ));
DATA(insert OID = 365 (  dist_sb		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "601 603" _null_ _null_ _null_ _null_ _null_	dist_sb _null_ _null_ _null_ ));
DATA(insert OID = 366 (  close_ps		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "600 601" _null_ _null_ _null_ _null_ _null_	close_ps _null_ _null_ _null_ ));
DATA(insert OID = 367 (  close_pb		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "600 603" _null_ _null_ _null_ _null_ _null_	close_pb _null_ _null_ _null_ ));
DATA(insert OID = 368 (  close_sb		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "601 603" _null_ _null_ _null_ _null_ _null_	close_sb _null_ _null_ _null_ ));
DATA(insert OID = 369 (  on_ps			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 601" _null_ _null_ _null_ _null_ _null_ on_ps _null_ _null_ _null_ ));
DATA(insert OID = 370 (  path_distance	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "602 602" _null_ _null_ _null_ _null_ _null_	path_distance _null_ _null_ _null_ ));
DATA(insert OID = 371 (  dist_ppath		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "600 602" _null_ _null_ _null_ _null_ _null_	dist_ppath _null_ _null_ _null_ ));
DATA(insert OID = 372 (  on_sb			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "601 603" _null_ _null_ _null_ _null_ _null_ on_sb _null_ _null_ _null_ ));
DATA(insert OID = 373 (  inter_sb		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "601 603" _null_ _null_ _null_ _null_ _null_ inter_sb _null_ _null_ _null_ ));

/* OIDS 400 - 499 */

DATA(insert OID =  401 (  text			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "1042" _null_ _null_ _null_ _null_ _null_	rtrim1 _null_ _null_ _null_ ));
DESCR("convert char(n) to text");
DATA(insert OID =  406 (  text			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "19" _null_ _null_ _null_ _null_ _null_ name_text _null_ _null_ _null_ ));
DESCR("convert name to text");
DATA(insert OID =  407 (  name			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 19 "25" _null_ _null_ _null_ _null_ _null_ text_name _null_ _null_ _null_ ));
DESCR("convert text to name");
DATA(insert OID =  408 (  bpchar		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1042 "19" _null_ _null_ _null_ _null_ _null_ name_bpchar _null_ _null_ _null_ ));
DESCR("convert name to char(n)");
DATA(insert OID =  409 (  name			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 19 "1042" _null_ _null_ _null_ _null_ _null_	bpchar_name _null_ _null_ _null_ ));
DESCR("convert char(n) to name");

DATA(insert OID = 440 (  hashgettuple	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 16 "2281 2281" _null_ _null_ _null_ _null_ _null_	hashgettuple _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 637 (  hashgetbitmap	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 20 "2281 2281" _null_ _null_ _null_ _null_ _null_	hashgetbitmap _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 441 (  hashinsert		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 6 0 16 "2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_	hashinsert _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 443 (  hashbeginscan	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_	hashbeginscan _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 444 (  hashrescan		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 5 0 2278 "2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ hashrescan _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 445 (  hashendscan	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ hashendscan _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 446 (  hashmarkpos	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ hashmarkpos _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 447 (  hashrestrpos	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ hashrestrpos _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 448 (  hashbuild		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ hashbuild _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 327 (  hashbuildempty    PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ hashbuildempty _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 442 (  hashbulkdelete    PGNSP PGUID 12 1 0 0 0 f f f f t f v 4 0 2281 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ hashbulkdelete _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 425 (  hashvacuumcleanup PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ hashvacuumcleanup _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 438 (  hashcostestimate  PGNSP PGUID 12 1 0 0 0 f f f f t f v 7 0 2278 "2281 2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ hashcostestimate _null_ _null_ _null_ ));
DESCR("hash(internal)");
DATA(insert OID = 2786 (  hashoptions	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 17 "1009 16" _null_ _null_ _null_ _null_  _null_ hashoptions _null_ _null_ _null_ ));
DESCR("hash(internal)");

DATA(insert OID = 449 (  hashint2		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "21" _null_ _null_ _null_ _null_ _null_ hashint2 _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 450 (  hashint4		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ hashint4 _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 949 (  hashint8		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "20" _null_ _null_ _null_ _null_ _null_ hashint8 _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 451 (  hashfloat4		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "700" _null_ _null_ _null_ _null_ _null_ hashfloat4 _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 452 (  hashfloat8		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "701" _null_ _null_ _null_ _null_ _null_ hashfloat8 _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 453 (  hashoid		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "26" _null_ _null_ _null_ _null_ _null_ hashoid _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 454 (  hashchar		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "18" _null_ _null_ _null_ _null_ _null_ hashchar _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 455 (  hashname		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "19" _null_ _null_ _null_ _null_ _null_ hashname _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 400 (  hashtext		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "25" _null_ _null_ _null_ _null_ _null_ hashtext _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 456 (  hashvarlena	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "2281" _null_ _null_ _null_ _null_ _null_ hashvarlena _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 457 (  hashoidvector	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "30" _null_ _null_ _null_ _null_ _null_ hashoidvector _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 329 (  hash_aclitem	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1033" _null_ _null_ _null_ _null_ _null_	hash_aclitem _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 398 (  hashint2vector    PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "22" _null_ _null_ _null_ _null_ _null_ hashint2vector _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 399 (  hashmacaddr	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "829" _null_ _null_ _null_ _null_ _null_ hashmacaddr _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 422 (  hashinet		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "869" _null_ _null_ _null_ _null_ _null_ hashinet _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 432 (  hash_numeric	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1700" _null_ _null_ _null_ _null_ _null_ hash_numeric _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 458 (  text_larger	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ text_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 459 (  text_smaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ text_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");

DATA(insert OID = 460 (  int8in			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "2275" _null_ _null_ _null_ _null_ _null_ int8in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 461 (  int8out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "20" _null_ _null_ _null_ _null_ _null_ int8out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 462 (  int8um			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ int8um _null_ _null_ _null_ ));
DATA(insert OID = 463 (  int8pl			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8pl _null_ _null_ _null_ ));
DATA(insert OID = 464 (  int8mi			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8mi _null_ _null_ _null_ ));
DATA(insert OID = 465 (  int8mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8mul _null_ _null_ _null_ ));
DATA(insert OID = 466 (  int8div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8div _null_ _null_ _null_ ));
DATA(insert OID = 467 (  int8eq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 20" _null_ _null_ _null_ _null_ _null_ int8eq _null_ _null_ _null_ ));
DATA(insert OID = 468 (  int8ne			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 20" _null_ _null_ _null_ _null_ _null_ int8ne _null_ _null_ _null_ ));
DATA(insert OID = 469 (  int8lt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 20" _null_ _null_ _null_ _null_ _null_ int8lt _null_ _null_ _null_ ));
DATA(insert OID = 470 (  int8gt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 20" _null_ _null_ _null_ _null_ _null_ int8gt _null_ _null_ _null_ ));
DATA(insert OID = 471 (  int8le			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 20" _null_ _null_ _null_ _null_ _null_ int8le _null_ _null_ _null_ ));
DATA(insert OID = 472 (  int8ge			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 20" _null_ _null_ _null_ _null_ _null_ int8ge _null_ _null_ _null_ ));

DATA(insert OID = 474 (  int84eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 23" _null_ _null_ _null_ _null_ _null_ int84eq _null_ _null_ _null_ ));
DATA(insert OID = 475 (  int84ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 23" _null_ _null_ _null_ _null_ _null_ int84ne _null_ _null_ _null_ ));
DATA(insert OID = 476 (  int84lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 23" _null_ _null_ _null_ _null_ _null_ int84lt _null_ _null_ _null_ ));
DATA(insert OID = 477 (  int84gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 23" _null_ _null_ _null_ _null_ _null_ int84gt _null_ _null_ _null_ ));
DATA(insert OID = 478 (  int84le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 23" _null_ _null_ _null_ _null_ _null_ int84le _null_ _null_ _null_ ));
DATA(insert OID = 479 (  int84ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 23" _null_ _null_ _null_ _null_ _null_ int84ge _null_ _null_ _null_ ));

DATA(insert OID = 480 (  int4			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "20" _null_ _null_ _null_ _null_ _null_ int84 _null_ _null_ _null_ ));
DESCR("convert int8 to int4");
DATA(insert OID = 481 (  int8			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "23" _null_ _null_ _null_ _null_ _null_ int48 _null_ _null_ _null_ ));
DESCR("convert int4 to int8");
DATA(insert OID = 482 (  float8			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "20" _null_ _null_ _null_ _null_ _null_ i8tod _null_ _null_ _null_ ));
DESCR("convert int8 to float8");
DATA(insert OID = 483 (  int8			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "701" _null_ _null_ _null_ _null_ _null_ dtoi8 _null_ _null_ _null_ ));
DESCR("convert float8 to int8");

/* OIDS 500 - 599 */

/* OIDS 600 - 699 */

DATA(insert OID = 626 (  hash_array		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "2277" _null_ _null_ _null_ _null_ _null_ hash_array _null_ _null_ _null_ ));
DESCR("hash");

DATA(insert OID = 652 (  float4			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700 "20" _null_ _null_ _null_ _null_ _null_ i8tof _null_ _null_ _null_ ));
DESCR("convert int8 to float4");
DATA(insert OID = 653 (  int8			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "700" _null_ _null_ _null_ _null_ _null_ ftoi8 _null_ _null_ _null_ ));
DESCR("convert float4 to int8");

DATA(insert OID = 714 (  int2			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "20" _null_ _null_ _null_ _null_ _null_ int82 _null_ _null_ _null_ ));
DESCR("convert int8 to int2");
DATA(insert OID = 754 (  int8			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "21" _null_ _null_ _null_ _null_ _null_ int28 _null_ _null_ _null_ ));
DESCR("convert int2 to int8");

DATA(insert OID = 655 (  namelt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "19 19" _null_ _null_ _null_ _null_ _null_ namelt _null_ _null_ _null_ ));
DATA(insert OID = 656 (  namele			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "19 19" _null_ _null_ _null_ _null_ _null_ namele _null_ _null_ _null_ ));
DATA(insert OID = 657 (  namegt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "19 19" _null_ _null_ _null_ _null_ _null_ namegt _null_ _null_ _null_ ));
DATA(insert OID = 658 (  namege			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "19 19" _null_ _null_ _null_ _null_ _null_ namege _null_ _null_ _null_ ));
DATA(insert OID = 659 (  namene			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "19 19" _null_ _null_ _null_ _null_ _null_ namene _null_ _null_ _null_ ));

DATA(insert OID = 668 (  bpchar			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1042 "1042 23 16" _null_ _null_ _null_ _null_ _null_ bpchar _null_ _null_ _null_ ));
DESCR("adjust char() to typmod length");
DATA(insert OID = 3097 ( varchar_transform PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ varchar_transform _null_ _null_ _null_ ));
DESCR("transform a varchar length coercion");
DATA(insert OID = 669 (  varchar		   PGNSP PGUID 12 1 0 0 varchar_transform f f f f t f i 3 0 1043 "1043 23 16" _null_ _null_ _null_ _null_ _null_ varchar _null_ _null_ _null_ ));
DESCR("adjust varchar() to typmod length");

DATA(insert OID = 676 (  mktinterval	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 704 "702 702" _null_ _null_ _null_ _null_ _null_ mktinterval _null_ _null_ _null_ ));

DATA(insert OID = 619 (  oidvectorne	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "30 30" _null_ _null_ _null_ _null_ _null_ oidvectorne _null_ _null_ _null_ ));
DATA(insert OID = 677 (  oidvectorlt	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "30 30" _null_ _null_ _null_ _null_ _null_ oidvectorlt _null_ _null_ _null_ ));
DATA(insert OID = 678 (  oidvectorle	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "30 30" _null_ _null_ _null_ _null_ _null_ oidvectorle _null_ _null_ _null_ ));
DATA(insert OID = 679 (  oidvectoreq	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "30 30" _null_ _null_ _null_ _null_ _null_ oidvectoreq _null_ _null_ _null_ ));
DATA(insert OID = 680 (  oidvectorge	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "30 30" _null_ _null_ _null_ _null_ _null_ oidvectorge _null_ _null_ _null_ ));
DATA(insert OID = 681 (  oidvectorgt	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "30 30" _null_ _null_ _null_ _null_ _null_ oidvectorgt _null_ _null_ _null_ ));

/* OIDS 700 - 799 */
DATA(insert OID = 710 (  getpgusername	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 19 "" _null_ _null_ _null_ _null_ _null_ current_user _null_ _null_ _null_ ));
DESCR("deprecated, use current_user instead");
DATA(insert OID = 716 (  oidlt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "26 26" _null_ _null_ _null_ _null_ _null_ oidlt _null_ _null_ _null_ ));
DATA(insert OID = 717 (  oidle			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "26 26" _null_ _null_ _null_ _null_ _null_ oidle _null_ _null_ _null_ ));

DATA(insert OID = 720 (  octet_length	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "17" _null_ _null_ _null_ _null_ _null_ byteaoctetlen _null_ _null_ _null_ ));
DESCR("octet length");
DATA(insert OID = 721 (  get_byte		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "17 23" _null_ _null_ _null_ _null_ _null_ byteaGetByte _null_ _null_ _null_ ));
DESCR("get byte");
DATA(insert OID = 722 (  set_byte		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 17 "17 23 23" _null_ _null_ _null_ _null_ _null_	byteaSetByte _null_ _null_ _null_ ));
DESCR("set byte");
DATA(insert OID = 723 (  get_bit		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "17 23" _null_ _null_ _null_ _null_ _null_ byteaGetBit _null_ _null_ _null_ ));
DESCR("get bit");
DATA(insert OID = 724 (  set_bit		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 17 "17 23 23" _null_ _null_ _null_ _null_ _null_	byteaSetBit _null_ _null_ _null_ ));
DESCR("set bit");
DATA(insert OID = 749 (  overlay		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 17 "17 17 23 23" _null_ _null_ _null_ _null_ _null_ byteaoverlay _null_ _null_ _null_ ));
DESCR("substitute portion of string");
DATA(insert OID = 752 (  overlay		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 17 "17 17 23" _null_ _null_ _null_ _null_ _null_	byteaoverlay_no_len _null_ _null_ _null_ ));
DESCR("substitute portion of string");

DATA(insert OID = 725 (  dist_pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "600 628" _null_ _null_ _null_ _null_ _null_	dist_pl _null_ _null_ _null_ ));
DATA(insert OID = 726 (  dist_lb		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "628 603" _null_ _null_ _null_ _null_ _null_	dist_lb _null_ _null_ _null_ ));
DATA(insert OID = 727 (  dist_sl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "601 628" _null_ _null_ _null_ _null_ _null_	dist_sl _null_ _null_ _null_ ));
DATA(insert OID = 728 (  dist_cpoly		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "718 604" _null_ _null_ _null_ _null_ _null_	dist_cpoly _null_ _null_ _null_ ));
DATA(insert OID = 729 (  poly_distance	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "604 604" _null_ _null_ _null_ _null_ _null_	poly_distance _null_ _null_ _null_ ));
DATA(insert OID = 3275 (  dist_ppoly	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "600 604" _null_ _null_ _null_ _null_ _null_	dist_ppoly _null_ _null_ _null_ ));
DATA(insert OID = 3292 (  dist_polyp	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "604 600" _null_ _null_ _null_ _null_ _null_	dist_polyp _null_ _null_ _null_ ));
DATA(insert OID = 3290 (  dist_cpoint	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "718 600" _null_ _null_ _null_ _null_ _null_	dist_cpoint _null_ _null_ _null_ ));

DATA(insert OID = 740 (  text_lt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ text_lt _null_ _null_ _null_ ));
DATA(insert OID = 741 (  text_le		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ text_le _null_ _null_ _null_ ));
DATA(insert OID = 742 (  text_gt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ text_gt _null_ _null_ _null_ ));
DATA(insert OID = 743 (  text_ge		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ text_ge _null_ _null_ _null_ ));

DATA(insert OID = 745 (  current_user	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 19 "" _null_ _null_ _null_ _null_ _null_ current_user _null_ _null_ _null_ ));
DESCR("current user name");
DATA(insert OID = 746 (  session_user	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 19 "" _null_ _null_ _null_ _null_ _null_ session_user _null_ _null_ _null_ ));
DESCR("session user name");

DATA(insert OID = 744 (  array_eq		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2277 2277" _null_ _null_ _null_ _null_ _null_ array_eq _null_ _null_ _null_ ));
DATA(insert OID = 390 (  array_ne		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2277 2277" _null_ _null_ _null_ _null_ _null_ array_ne _null_ _null_ _null_ ));
DATA(insert OID = 391 (  array_lt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2277 2277" _null_ _null_ _null_ _null_ _null_ array_lt _null_ _null_ _null_ ));
DATA(insert OID = 392 (  array_gt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2277 2277" _null_ _null_ _null_ _null_ _null_ array_gt _null_ _null_ _null_ ));
DATA(insert OID = 393 (  array_le		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2277 2277" _null_ _null_ _null_ _null_ _null_ array_le _null_ _null_ _null_ ));
DATA(insert OID = 396 (  array_ge		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2277 2277" _null_ _null_ _null_ _null_ _null_ array_ge _null_ _null_ _null_ ));
DATA(insert OID = 747 (  array_dims		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "2277" _null_ _null_ _null_ _null_ _null_ array_dims _null_ _null_ _null_ ));
DESCR("array dimensions");
DATA(insert OID = 748 (  array_ndims	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "2277" _null_ _null_ _null_ _null_ _null_ array_ndims _null_ _null_ _null_ ));
DESCR("number of array dimensions");
DATA(insert OID = 750 (  array_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 2277 "2275 26 23" _null_ _null_ _null_ _null_ _null_	array_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 751 (  array_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "2277" _null_ _null_ _null_ _null_ _null_ array_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2091 (  array_lower	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "2277 23" _null_ _null_ _null_ _null_ _null_ array_lower _null_ _null_ _null_ ));
DESCR("array lower dimension");
DATA(insert OID = 2092 (  array_upper	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "2277 23" _null_ _null_ _null_ _null_ _null_ array_upper _null_ _null_ _null_ ));
DESCR("array upper dimension");
DATA(insert OID = 2176 (  array_length	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "2277 23" _null_ _null_ _null_ _null_ _null_ array_length _null_ _null_ _null_ ));
DESCR("array length");
DATA(insert OID = 3179 (  cardinality	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "2277" _null_ _null_ _null_ _null_ _null_ array_cardinality _null_ _null_ _null_ ));
DESCR("array cardinality");
DATA(insert OID = 378 (  array_append	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2277 "2277 2283" _null_ _null_ _null_ _null_ _null_ array_append _null_ _null_ _null_ ));
DESCR("append element onto end of array");
DATA(insert OID = 379 (  array_prepend	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2277 "2283 2277" _null_ _null_ _null_ _null_ _null_ array_prepend _null_ _null_ _null_ ));
DESCR("prepend element onto front of array");
DATA(insert OID = 383 (  array_cat		   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2277 "2277 2277" _null_ _null_ _null_ _null_ _null_ array_cat _null_ _null_ _null_ ));
DATA(insert OID = 394 (  string_to_array   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 1009 "25 25" _null_ _null_ _null_ _null_ _null_ text_to_array _null_ _null_ _null_ ));
DESCR("split delimited text into text[]");
DATA(insert OID = 395 (  array_to_string   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "2277 25" _null_ _null_ _null_ _null_ _null_ array_to_text _null_ _null_ _null_ ));
DESCR("concatenate array elements, using delimiter, into text");
DATA(insert OID = 376 (  string_to_array   PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 1009 "25 25 25" _null_ _null_ _null_ _null_ _null_ text_to_array_null _null_ _null_ _null_ ));
DESCR("split delimited text into text[], with null string");
DATA(insert OID = 384 (  array_to_string   PGNSP PGUID 12 1 0 0 0 f f f f f f s 3 0 25 "2277 25 25" _null_ _null_ _null_ _null_ _null_ array_to_text_null _null_ _null_ _null_ ));
DESCR("concatenate array elements, using delimiter and null string, into text");
DATA(insert OID = 515 (  array_larger	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2277 "2277 2277" _null_ _null_ _null_ _null_ _null_ array_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 516 (  array_smaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2277 "2277 2277" _null_ _null_ _null_ _null_ _null_ array_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 3277 (  array_position		   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 23 "2277 2283" _null_ _null_ _null_ _null_ _null_ array_position _null_ _null_ _null_ ));
DESCR("returns an offset of value in array");
DATA(insert OID = 3278 (  array_position		   PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 23 "2277 2283 23" _null_ _null_ _null_ _null_ _null_ array_position_start _null_ _null_ _null_ ));
DESCR("returns an offset of value in array with start index");
DATA(insert OID = 3279 (  array_positions		   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 1007 "2277 2283" _null_ _null_ _null_ _null_ _null_ array_positions _null_ _null_ _null_ ));
DESCR("returns an array of offsets of some value in array");
DATA(insert OID = 1191 (  generate_subscripts PGNSP PGUID 12 1 1000 0 0 f f f f t t i 3 0 23 "2277 23 16" _null_ _null_ _null_ _null_ _null_ generate_subscripts _null_ _null_ _null_ ));
DESCR("array subscripts generator");
DATA(insert OID = 1192 (  generate_subscripts PGNSP PGUID 12 1 1000 0 0 f f f f t t i 2 0 23 "2277 23" _null_ _null_ _null_ _null_ _null_ generate_subscripts_nodir _null_ _null_ _null_ ));
DESCR("array subscripts generator");
DATA(insert OID = 1193 (  array_fill PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2277 "2283 1007" _null_ _null_ _null_ _null_ _null_ array_fill _null_ _null_ _null_ ));
DESCR("array constructor with value");
DATA(insert OID = 1286 (  array_fill PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 2277 "2283 1007 1007" _null_ _null_ _null_ _null_ _null_ array_fill_with_lower_bounds _null_ _null_ _null_ ));
DESCR("array constructor with value");
DATA(insert OID = 2331 (  unnest		   PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 2283 "2277" _null_ _null_ _null_ _null_ _null_ array_unnest _null_ _null_ _null_ ));
DESCR("expand array to set of rows");
DATA(insert OID = 3167 (  array_remove	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2277 "2277 2283" _null_ _null_ _null_ _null_ _null_ array_remove _null_ _null_ _null_ ));
DESCR("remove any occurrences of an element from an array");
DATA(insert OID = 3168 (  array_replace    PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 2277 "2277 2283 2283" _null_ _null_ _null_ _null_ _null_ array_replace _null_ _null_ _null_ ));
DESCR("replace any occurrences of an element in an array");
DATA(insert OID = 2333 (  array_agg_transfn   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 2776" _null_ _null_ _null_ _null_ _null_ array_agg_transfn _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 2334 (  array_agg_finalfn   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2277 "2281 2776" _null_ _null_ _null_ _null_ _null_ array_agg_finalfn _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2335 (  array_agg		   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 2277 "2776" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("concatenate aggregate input into an array");
DATA(insert OID = 4051 (  array_agg_array_transfn	PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 2277" _null_ _null_ _null_ _null_ _null_ array_agg_array_transfn _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 4052 (  array_agg_array_finalfn	PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2277 "2281 2277" _null_ _null_ _null_ _null_ _null_ array_agg_array_finalfn _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 4053 (  array_agg		   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 2277 "2277" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("concatenate aggregate input into an array");
DATA(insert OID = 3218 ( width_bucket	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "2283 2277" _null_ _null_ _null_ _null_ _null_ width_bucket_array _null_ _null_ _null_ ));
DESCR("bucket number of operand given a sorted array of bucket lower bounds");
DATA(insert OID = 3816 (  array_typanalyze PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 16 "2281" _null_ _null_ _null_ _null_ _null_ array_typanalyze _null_ _null_ _null_ ));
DESCR("array typanalyze");
DATA(insert OID = 3817 (  arraycontsel	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_ arraycontsel _null_ _null_ _null_ ));
DESCR("restriction selectivity for array-containment operators");
DATA(insert OID = 3818 (  arraycontjoinsel PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_ _null_ arraycontjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity for array-containment operators");

DATA(insert OID = 760 (  smgrin			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 210 "2275" _null_ _null_ _null_ _null_ _null_	smgrin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 761 (  smgrout		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "210" _null_ _null_ _null_ _null_ _null_	smgrout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 762 (  smgreq			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "210 210" _null_ _null_ _null_ _null_ _null_ smgreq _null_ _null_ _null_ ));
DESCR("storage manager");
DATA(insert OID = 763 (  smgrne			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "210 210" _null_ _null_ _null_ _null_ _null_ smgrne _null_ _null_ _null_ ));
DESCR("storage manager");

DATA(insert OID = 764 (  lo_import		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 26 "25" _null_ _null_ _null_ _null_ _null_ lo_import _null_ _null_ _null_ ));
DESCR("large object import");
DATA(insert OID = 767 (  lo_import		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 26 "25 26" _null_ _null_ _null_ _null_ _null_	lo_import_with_oid _null_ _null_ _null_ ));
DESCR("large object import");
DATA(insert OID = 765 (  lo_export		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 23 "26 25" _null_ _null_ _null_ _null_ _null_ lo_export _null_ _null_ _null_ ));
DESCR("large object export");

DATA(insert OID = 766 (  int4inc		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ int4inc _null_ _null_ _null_ ));
DESCR("increment");
DATA(insert OID = 768 (  int4larger		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 769 (  int4smaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 770 (  int2larger		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 771 (  int2smaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2smaller _null_ _null_ _null_ ));
DESCR("smaller of two");

DATA(insert OID = 774 (  gistgettuple	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 16 "2281 2281" _null_ _null_ _null_ _null_ _null_	gistgettuple _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 638 (  gistgetbitmap	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 20 "2281 2281" _null_ _null_ _null_ _null_ _null_	gistgetbitmap _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 775 (  gistinsert		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 6 0 16 "2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_	gistinsert _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 777 (  gistbeginscan	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_	gistbeginscan _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 778 (  gistrescan		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 5 0 2278 "2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gistrescan _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 779 (  gistendscan	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ gistendscan _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 780 (  gistmarkpos	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ gistmarkpos _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 781 (  gistrestrpos	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ gistrestrpos _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 782 (  gistbuild		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gistbuild _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 326 (  gistbuildempty    PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ gistbuildempty _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 776 (  gistbulkdelete    PGNSP PGUID 12 1 0 0 0 f f f f t f v 4 0 2281 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gistbulkdelete _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 2561 (  gistvacuumcleanup   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ gistvacuumcleanup _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 3280 (  gistcanreturn    PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "2281 23" _null_ _null_ _null_ _null_ _null_ gistcanreturn _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 772 (  gistcostestimate  PGNSP PGUID 12 1 0 0 0 f f f f t f v 7 0 2278 "2281 2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gistcostestimate _null_ _null_ _null_ ));
DESCR("gist(internal)");
DATA(insert OID = 2787 (  gistoptions	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 17 "1009 16" _null_ _null_ _null_ _null_  _null_ gistoptions _null_ _null_ _null_ ));
DESCR("gist(internal)");

DATA(insert OID = 784 (  tintervaleq	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 704" _null_ _null_ _null_ _null_ _null_ tintervaleq _null_ _null_ _null_ ));
DATA(insert OID = 785 (  tintervalne	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 704" _null_ _null_ _null_ _null_ _null_ tintervalne _null_ _null_ _null_ ));
DATA(insert OID = 786 (  tintervallt	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 704" _null_ _null_ _null_ _null_ _null_ tintervallt _null_ _null_ _null_ ));
DATA(insert OID = 787 (  tintervalgt	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 704" _null_ _null_ _null_ _null_ _null_ tintervalgt _null_ _null_ _null_ ));
DATA(insert OID = 788 (  tintervalle	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 704" _null_ _null_ _null_ _null_ _null_ tintervalle _null_ _null_ _null_ ));
DATA(insert OID = 789 (  tintervalge	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "704 704" _null_ _null_ _null_ _null_ _null_ tintervalge _null_ _null_ _null_ ));

/* OIDS 800 - 899 */

DATA(insert OID =  846 (  cash_mul_flt4    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 700" _null_ _null_ _null_ _null_ _null_	cash_mul_flt4 _null_ _null_ _null_ ));
DATA(insert OID =  847 (  cash_div_flt4    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 700" _null_ _null_ _null_ _null_ _null_	cash_div_flt4 _null_ _null_ _null_ ));
DATA(insert OID =  848 (  flt4_mul_cash    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "700 790" _null_ _null_ _null_ _null_ _null_	flt4_mul_cash _null_ _null_ _null_ ));

DATA(insert OID =  849 (  position		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "25 25" _null_ _null_ _null_ _null_ _null_ textpos _null_ _null_ _null_ ));
DESCR("position of substring");
DATA(insert OID =  850 (  textlike		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ textlike _null_ _null_ _null_ ));
DATA(insert OID =  851 (  textnlike		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ textnlike _null_ _null_ _null_ ));

DATA(insert OID =  852 (  int48eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 20" _null_ _null_ _null_ _null_ _null_ int48eq _null_ _null_ _null_ ));
DATA(insert OID =  853 (  int48ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 20" _null_ _null_ _null_ _null_ _null_ int48ne _null_ _null_ _null_ ));
DATA(insert OID =  854 (  int48lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 20" _null_ _null_ _null_ _null_ _null_ int48lt _null_ _null_ _null_ ));
DATA(insert OID =  855 (  int48gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 20" _null_ _null_ _null_ _null_ _null_ int48gt _null_ _null_ _null_ ));
DATA(insert OID =  856 (  int48le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 20" _null_ _null_ _null_ _null_ _null_ int48le _null_ _null_ _null_ ));
DATA(insert OID =  857 (  int48ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "23 20" _null_ _null_ _null_ _null_ _null_ int48ge _null_ _null_ _null_ ));

DATA(insert OID =  858 (  namelike		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ namelike _null_ _null_ _null_ ));
DATA(insert OID =  859 (  namenlike		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ namenlike _null_ _null_ _null_ ));

DATA(insert OID =  860 (  bpchar		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1042 "18" _null_ _null_ _null_ _null_ _null_	char_bpchar _null_ _null_ _null_ ));
DESCR("convert char to char(n)");

DATA(insert OID = 861 ( current_database	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 19 "" _null_ _null_ _null_ _null_ _null_ current_database _null_ _null_ _null_ ));
DESCR("name of the current database");
DATA(insert OID = 817 (  current_query		  PGNSP PGUID 12 1 0 0 0 f f f f f f v 0 0 25 "" _null_ _null_ _null_ _null_  _null_ current_query _null_ _null_ _null_ ));
DESCR("get the currently executing query");

DATA(insert OID =  862 (  int4_mul_cash		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "23 790" _null_ _null_ _null_ _null_ _null_ int4_mul_cash _null_ _null_ _null_ ));
DATA(insert OID =  863 (  int2_mul_cash		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "21 790" _null_ _null_ _null_ _null_ _null_ int2_mul_cash _null_ _null_ _null_ ));
DATA(insert OID =  864 (  cash_mul_int4		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 23" _null_ _null_ _null_ _null_ _null_ cash_mul_int4 _null_ _null_ _null_ ));
DATA(insert OID =  865 (  cash_div_int4		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 23" _null_ _null_ _null_ _null_ _null_ cash_div_int4 _null_ _null_ _null_ ));
DATA(insert OID =  866 (  cash_mul_int2		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 21" _null_ _null_ _null_ _null_ _null_ cash_mul_int2 _null_ _null_ _null_ ));
DATA(insert OID =  867 (  cash_div_int2		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 21" _null_ _null_ _null_ _null_ _null_ cash_div_int2 _null_ _null_ _null_ ));

DATA(insert OID =  886 (  cash_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 790 "2275" _null_ _null_ _null_ _null_ _null_	cash_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  887 (  cash_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "790" _null_ _null_ _null_ _null_ _null_	cash_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  888 (  cash_eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "790 790" _null_ _null_ _null_ _null_ _null_ cash_eq _null_ _null_ _null_ ));
DATA(insert OID =  889 (  cash_ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "790 790" _null_ _null_ _null_ _null_ _null_ cash_ne _null_ _null_ _null_ ));
DATA(insert OID =  890 (  cash_lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "790 790" _null_ _null_ _null_ _null_ _null_ cash_lt _null_ _null_ _null_ ));
DATA(insert OID =  891 (  cash_le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "790 790" _null_ _null_ _null_ _null_ _null_ cash_le _null_ _null_ _null_ ));
DATA(insert OID =  892 (  cash_gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "790 790" _null_ _null_ _null_ _null_ _null_ cash_gt _null_ _null_ _null_ ));
DATA(insert OID =  893 (  cash_ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "790 790" _null_ _null_ _null_ _null_ _null_ cash_ge _null_ _null_ _null_ ));
DATA(insert OID =  894 (  cash_pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 790" _null_ _null_ _null_ _null_ _null_	cash_pl _null_ _null_ _null_ ));
DATA(insert OID =  895 (  cash_mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 790" _null_ _null_ _null_ _null_ _null_	cash_mi _null_ _null_ _null_ ));
DATA(insert OID =  896 (  cash_mul_flt8    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 701" _null_ _null_ _null_ _null_ _null_	cash_mul_flt8 _null_ _null_ _null_ ));
DATA(insert OID =  897 (  cash_div_flt8    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 701" _null_ _null_ _null_ _null_ _null_	cash_div_flt8 _null_ _null_ _null_ ));
DATA(insert OID =  898 (  cashlarger	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 790" _null_ _null_ _null_ _null_ _null_	cashlarger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID =  899 (  cashsmaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "790 790" _null_ _null_ _null_ _null_ _null_	cashsmaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID =  919 (  flt8_mul_cash    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 790 "701 790" _null_ _null_ _null_ _null_ _null_	flt8_mul_cash _null_ _null_ _null_ ));
DATA(insert OID =  935 (  cash_words	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "790" _null_ _null_ _null_ _null_ _null_ cash_words _null_ _null_ _null_ ));
DESCR("output money amount as words");
DATA(insert OID = 3822 (  cash_div_cash    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "790 790" _null_ _null_ _null_ _null_ _null_	cash_div_cash _null_ _null_ _null_ ));
DATA(insert OID = 3823 (  numeric		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1700 "790" _null_ _null_ _null_ _null_ _null_	cash_numeric _null_ _null_ _null_ ));
DESCR("convert money to numeric");
DATA(insert OID = 3824 (  money			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 790 "1700" _null_ _null_ _null_ _null_ _null_	numeric_cash _null_ _null_ _null_ ));
DESCR("convert numeric to money");
DATA(insert OID = 3811 (  money			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 790 "23" _null_ _null_ _null_ _null_ _null_ int4_cash _null_ _null_ _null_ ));
DESCR("convert int4 to money");
DATA(insert OID = 3812 (  money			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 790 "20" _null_ _null_ _null_ _null_ _null_ int8_cash _null_ _null_ _null_ ));
DESCR("convert int8 to money");

/* OIDS 900 - 999 */

DATA(insert OID = 940 (  mod			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2mod _null_ _null_ _null_ ));
DESCR("modulus");
DATA(insert OID = 941 (  mod			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4mod _null_ _null_ _null_ ));
DESCR("modulus");

DATA(insert OID = 945 (  int8mod		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8mod _null_ _null_ _null_ ));
DATA(insert OID = 947 (  mod			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8mod _null_ _null_ _null_ ));
DESCR("modulus");

DATA(insert OID = 944 (  char			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 18 "25" _null_ _null_ _null_ _null_ _null_ text_char _null_ _null_ _null_ ));
DESCR("convert text to char");
DATA(insert OID = 946 (  text			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "18" _null_ _null_ _null_ _null_ _null_ char_text _null_ _null_ _null_ ));
DESCR("convert char to text");

DATA(insert OID = 952 (  lo_open		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 23 "26 23" _null_ _null_ _null_ _null_ _null_ lo_open _null_ _null_ _null_ ));
DESCR("large object open");
DATA(insert OID = 953 (  lo_close		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ lo_close _null_ _null_ _null_ ));
DESCR("large object close");
DATA(insert OID = 954 (  loread			   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 17 "23 23" _null_ _null_ _null_ _null_ _null_ loread _null_ _null_ _null_ ));
DESCR("large object read");
DATA(insert OID = 955 (  lowrite		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 23 "23 17" _null_ _null_ _null_ _null_ _null_ lowrite _null_ _null_ _null_ ));
DESCR("large object write");
DATA(insert OID = 956 (  lo_lseek		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 23 "23 23 23" _null_ _null_ _null_ _null_ _null_	lo_lseek _null_ _null_ _null_ ));
DESCR("large object seek");
DATA(insert OID = 3170 (  lo_lseek64	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 20 "23 20 23" _null_ _null_ _null_ _null_ _null_	lo_lseek64 _null_ _null_ _null_ ));
DESCR("large object seek (64 bit)");
DATA(insert OID = 957 (  lo_creat		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 26 "23" _null_ _null_ _null_ _null_ _null_ lo_creat _null_ _null_ _null_ ));
DESCR("large object create");
DATA(insert OID = 715 (  lo_create		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 26 "26" _null_ _null_ _null_ _null_ _null_ lo_create _null_ _null_ _null_ ));
DESCR("large object create");
DATA(insert OID = 958 (  lo_tell		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ lo_tell _null_ _null_ _null_ ));
DESCR("large object position");
DATA(insert OID = 3171 (  lo_tell64		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "23" _null_ _null_ _null_ _null_ _null_ lo_tell64 _null_ _null_ _null_ ));
DESCR("large object position (64 bit)");
DATA(insert OID = 1004 (  lo_truncate	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ lo_truncate _null_ _null_ _null_ ));
DESCR("truncate large object");
DATA(insert OID = 3172 (  lo_truncate64    PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 23 "23 20" _null_ _null_ _null_ _null_ _null_ lo_truncate64 _null_ _null_ _null_ ));
DESCR("truncate large object (64 bit)");

DATA(insert OID = 3457 (  lo_from_bytea    PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 26 "26 17" _null_ _null_ _null_ _null_ _null_ lo_from_bytea _null_ _null_ _null_ ));
DESCR("create new large object with given content");
DATA(insert OID = 3458 (  lo_get		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 17 "26" _null_ _null_ _null_ _null_ _null_ lo_get _null_ _null_ _null_ ));
DESCR("read entire large object");
DATA(insert OID = 3459 (  lo_get		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 17 "26 20 23" _null_ _null_ _null_ _null_ _null_ lo_get_fragment _null_ _null_ _null_ ));
DESCR("read large object from offset for length");
DATA(insert OID = 3460 (  lo_put		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2278 "26 20 17" _null_ _null_ _null_ _null_ _null_ lo_put _null_ _null_ _null_ ));
DESCR("write data at offset");

DATA(insert OID = 959 (  on_pl			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 628" _null_ _null_ _null_ _null_ _null_ on_pl _null_ _null_ _null_ ));
DATA(insert OID = 960 (  on_sl			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "601 628" _null_ _null_ _null_ _null_ _null_ on_sl _null_ _null_ _null_ ));
DATA(insert OID = 961 (  close_pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "600 628" _null_ _null_ _null_ _null_ _null_	close_pl _null_ _null_ _null_ ));
DATA(insert OID = 962 (  close_sl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "601 628" _null_ _null_ _null_ _null_ _null_	close_sl _null_ _null_ _null_ ));
DATA(insert OID = 963 (  close_lb		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "628 603" _null_ _null_ _null_ _null_ _null_	close_lb _null_ _null_ _null_ ));

DATA(insert OID = 964 (  lo_unlink		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 23 "26" _null_ _null_ _null_ _null_ _null_ lo_unlink _null_ _null_ _null_ ));
DESCR("large object unlink (delete)");

DATA(insert OID = 973 (  path_inter		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "602 602" _null_ _null_ _null_ _null_ _null_ path_inter _null_ _null_ _null_ ));
DATA(insert OID = 975 (  area			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "603" _null_ _null_ _null_ _null_ _null_	box_area _null_ _null_ _null_ ));
DESCR("box area");
DATA(insert OID = 976 (  width			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "603" _null_ _null_ _null_ _null_ _null_	box_width _null_ _null_ _null_ ));
DESCR("box width");
DATA(insert OID = 977 (  height			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "603" _null_ _null_ _null_ _null_ _null_	box_height _null_ _null_ _null_ ));
DESCR("box height");
DATA(insert OID = 978 (  box_distance	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "603 603" _null_ _null_ _null_ _null_ _null_	box_distance _null_ _null_ _null_ ));
DATA(insert OID = 979 (  area			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "602" _null_ _null_ _null_ _null_ _null_	path_area _null_ _null_ _null_ ));
DESCR("area of a closed path");
DATA(insert OID = 980 (  box_intersect	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 603 "603 603" _null_ _null_ _null_ _null_ _null_	box_intersect _null_ _null_ _null_ ));
DATA(insert OID = 4067 ( bound_box		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 603 "603 603" _null_ _null_ _null_ _null_ _null_	boxes_bound_box _null_ _null_ _null_ ));
DESCR("bounding box of two boxes");
DATA(insert OID = 981 (  diagonal		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 601 "603" _null_ _null_ _null_ _null_ _null_	box_diagonal _null_ _null_ _null_ ));
DESCR("box diagonal");
DATA(insert OID = 982 (  path_n_lt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "602 602" _null_ _null_ _null_ _null_ _null_ path_n_lt _null_ _null_ _null_ ));
DATA(insert OID = 983 (  path_n_gt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "602 602" _null_ _null_ _null_ _null_ _null_ path_n_gt _null_ _null_ _null_ ));
DATA(insert OID = 984 (  path_n_eq		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "602 602" _null_ _null_ _null_ _null_ _null_ path_n_eq _null_ _null_ _null_ ));
DATA(insert OID = 985 (  path_n_le		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "602 602" _null_ _null_ _null_ _null_ _null_ path_n_le _null_ _null_ _null_ ));
DATA(insert OID = 986 (  path_n_ge		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "602 602" _null_ _null_ _null_ _null_ _null_ path_n_ge _null_ _null_ _null_ ));
DATA(insert OID = 987 (  path_length	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "602" _null_ _null_ _null_ _null_ _null_	path_length _null_ _null_ _null_ ));
DATA(insert OID = 988 (  point_ne		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 600" _null_ _null_ _null_ _null_ _null_ point_ne _null_ _null_ _null_ ));
DATA(insert OID = 989 (  point_vert		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 600" _null_ _null_ _null_ _null_ _null_ point_vert _null_ _null_ _null_ ));
DATA(insert OID = 990 (  point_horiz	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 600" _null_ _null_ _null_ _null_ _null_ point_horiz _null_ _null_ _null_ ));
DATA(insert OID = 991 (  point_distance    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "600 600" _null_ _null_ _null_ _null_ _null_	point_distance _null_ _null_ _null_ ));
DATA(insert OID = 992 (  slope			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "600 600" _null_ _null_ _null_ _null_ _null_	point_slope _null_ _null_ _null_ ));
DESCR("slope between points");
DATA(insert OID = 993 (  lseg			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 601 "600 600" _null_ _null_ _null_ _null_ _null_	lseg_construct _null_ _null_ _null_ ));
DESCR("convert points to line segment");
DATA(insert OID = 994 (  lseg_intersect    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_ _null_ lseg_intersect _null_ _null_ _null_ ));
DATA(insert OID = 995 (  lseg_parallel	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_ _null_ lseg_parallel _null_ _null_ _null_ ));
DATA(insert OID = 996 (  lseg_perp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_ _null_ lseg_perp _null_ _null_ _null_ ));
DATA(insert OID = 997 (  lseg_vertical	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "601" _null_ _null_ _null_ _null_ _null_ lseg_vertical _null_ _null_ _null_ ));
DATA(insert OID = 998 (  lseg_horizontal   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "601" _null_ _null_ _null_ _null_ _null_ lseg_horizontal _null_ _null_ _null_ ));
DATA(insert OID = 999 (  lseg_eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_ _null_ lseg_eq _null_ _null_ _null_ ));

/* OIDS 1000 - 1999 */

DATA(insert OID = 3994 (  timestamp_izone_transform PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ timestamp_izone_transform _null_ _null_ _null_ ));
DESCR("transform a time zone adjustment");
DATA(insert OID = 1026 (  timezone		   PGNSP PGUID 12 1 0 0 timestamp_izone_transform f f f f t f i 2 0 1114 "1186 1184" _null_ _null_ _null_ _null_ _null_ timestamptz_izone _null_ _null_ _null_ ));
DESCR("adjust timestamp to new time zone");

DATA(insert OID = 1031 (  aclitemin		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1033 "2275" _null_ _null_ _null_ _null_ _null_ aclitemin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1032 (  aclitemout	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "1033" _null_ _null_ _null_ _null_ _null_ aclitemout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1035 (  aclinsert		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1034 "1034 1033" _null_ _null_ _null_ _null_ _null_ aclinsert _null_ _null_ _null_ ));
DESCR("add/update ACL item");
DATA(insert OID = 1036 (  aclremove		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1034 "1034 1033" _null_ _null_ _null_ _null_ _null_ aclremove _null_ _null_ _null_ ));
DESCR("remove ACL item");
DATA(insert OID = 1037 (  aclcontains	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1034 1033" _null_ _null_ _null_ _null_ _null_ aclcontains _null_ _null_ _null_ ));
DESCR("contains");
DATA(insert OID = 1062 (  aclitemeq		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1033 1033" _null_ _null_ _null_ _null_ _null_ aclitem_eq _null_ _null_ _null_ ));
DATA(insert OID = 1365 (  makeaclitem	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 1033 "26 26 25 16" _null_ _null_ _null_ _null_ _null_ makeaclitem _null_ _null_ _null_ ));
DESCR("make ACL item");
DATA(insert OID = 3943 (  acldefault	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1034 "18 26" _null_ _null_ _null_ _null_  _null_ acldefault_sql _null_ _null_ _null_ ));
DESCR("TODO");
DATA(insert OID = 1689 (  aclexplode	PGNSP PGUID 12 1 10 0 0 f f f f t t s 1 0 2249 "1034" "{1034,26,26,25,16}" "{i,o,o,o,o}" "{acl,grantor,grantee,privilege_type,is_grantable}" _null_ _null_ aclexplode _null_ _null_ _null_ ));
DESCR("convert ACL item array to table, for use by information schema");
DATA(insert OID = 1044 (  bpcharin		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1042 "2275 26 23" _null_ _null_ _null_ _null_ _null_ bpcharin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1045 (  bpcharout		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "1042" _null_ _null_ _null_ _null_ _null_ bpcharout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2913 (  bpchartypmodin   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1263" _null_ _null_ _null_ _null_ _null_	bpchartypmodin _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 2914 (  bpchartypmodout  PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_	bpchartypmodout _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 1046 (  varcharin		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1043 "2275 26 23" _null_ _null_ _null_ _null_ _null_ varcharin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1047 (  varcharout	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "1043" _null_ _null_ _null_ _null_ _null_ varcharout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2915 (  varchartypmodin  PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1263" _null_ _null_ _null_ _null_ _null_	varchartypmodin _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 2916 (  varchartypmodout PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_	varchartypmodout _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 1048 (  bpchareq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpchareq _null_ _null_ _null_ ));
DATA(insert OID = 1049 (  bpcharlt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpcharlt _null_ _null_ _null_ ));
DATA(insert OID = 1050 (  bpcharle		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpcharle _null_ _null_ _null_ ));
DATA(insert OID = 1051 (  bpchargt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpchargt _null_ _null_ _null_ ));
DATA(insert OID = 1052 (  bpcharge		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpcharge _null_ _null_ _null_ ));
DATA(insert OID = 1053 (  bpcharne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpcharne _null_ _null_ _null_ ));
DATA(insert OID = 1063 (  bpchar_larger    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1042 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpchar_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 1064 (  bpchar_smaller   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1042 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpchar_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 1078 (  bpcharcmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpcharcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 1080 (  hashbpchar	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1042" _null_ _null_ _null_ _null_ _null_	hashbpchar _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 1081 (  format_type	   PGNSP PGUID 12 1 0 0 0 f f f f f f s 2 0 25 "26 23" _null_ _null_ _null_ _null_ _null_ format_type _null_ _null_ _null_ ));
DESCR("format a type oid and atttypmod to canonical SQL");
DATA(insert OID = 1084 (  date_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1082 "2275" _null_ _null_ _null_ _null_ _null_ date_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1085 (  date_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "1082" _null_ _null_ _null_ _null_ _null_ date_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1086 (  date_eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1082 1082" _null_ _null_ _null_ _null_ _null_ date_eq _null_ _null_ _null_ ));
DATA(insert OID = 1087 (  date_lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1082 1082" _null_ _null_ _null_ _null_ _null_ date_lt _null_ _null_ _null_ ));
DATA(insert OID = 1088 (  date_le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1082 1082" _null_ _null_ _null_ _null_ _null_ date_le _null_ _null_ _null_ ));
DATA(insert OID = 1089 (  date_gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1082 1082" _null_ _null_ _null_ _null_ _null_ date_gt _null_ _null_ _null_ ));
DATA(insert OID = 1090 (  date_ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1082 1082" _null_ _null_ _null_ _null_ _null_ date_ge _null_ _null_ _null_ ));
DATA(insert OID = 1091 (  date_ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1082 1082" _null_ _null_ _null_ _null_ _null_ date_ne _null_ _null_ _null_ ));
DATA(insert OID = 1092 (  date_cmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1082 1082" _null_ _null_ _null_ _null_ _null_ date_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3136 (  date_sortsupport PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ date_sortsupport _null_ _null_ _null_ ));
DESCR("sort support");

/* OIDS 1100 - 1199 */

DATA(insert OID = 1102 (  time_lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1083 1083" _null_ _null_ _null_ _null_ _null_ time_lt _null_ _null_ _null_ ));
DATA(insert OID = 1103 (  time_le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1083 1083" _null_ _null_ _null_ _null_ _null_ time_le _null_ _null_ _null_ ));
DATA(insert OID = 1104 (  time_gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1083 1083" _null_ _null_ _null_ _null_ _null_ time_gt _null_ _null_ _null_ ));
DATA(insert OID = 1105 (  time_ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1083 1083" _null_ _null_ _null_ _null_ _null_ time_ge _null_ _null_ _null_ ));
DATA(insert OID = 1106 (  time_ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1083 1083" _null_ _null_ _null_ _null_ _null_ time_ne _null_ _null_ _null_ ));
DATA(insert OID = 1107 (  time_cmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1083 1083" _null_ _null_ _null_ _null_ _null_ time_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 1138 (  date_larger	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1082 "1082 1082" _null_ _null_ _null_ _null_ _null_ date_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 1139 (  date_smaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1082 "1082 1082" _null_ _null_ _null_ _null_ _null_ date_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 1140 (  date_mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1082 1082" _null_ _null_ _null_ _null_ _null_ date_mi _null_ _null_ _null_ ));
DATA(insert OID = 1141 (  date_pli		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1082 "1082 23" _null_ _null_ _null_ _null_ _null_ date_pli _null_ _null_ _null_ ));
DATA(insert OID = 1142 (  date_mii		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1082 "1082 23" _null_ _null_ _null_ _null_ _null_ date_mii _null_ _null_ _null_ ));
DATA(insert OID = 1143 (  time_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 1083 "2275 26 23" _null_ _null_ _null_ _null_ _null_ time_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1144 (  time_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "1083" _null_ _null_ _null_ _null_ _null_ time_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2909 (  timetypmodin		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1263" _null_ _null_ _null_ _null_ _null_	timetypmodin _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 2910 (  timetypmodout		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_	timetypmodout _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 1145 (  time_eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1083 1083" _null_ _null_ _null_ _null_ _null_ time_eq _null_ _null_ _null_ ));

DATA(insert OID = 1146 (  circle_add_pt    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 718 "718 600" _null_ _null_ _null_ _null_ _null_	circle_add_pt _null_ _null_ _null_ ));
DATA(insert OID = 1147 (  circle_sub_pt    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 718 "718 600" _null_ _null_ _null_ _null_ _null_	circle_sub_pt _null_ _null_ _null_ ));
DATA(insert OID = 1148 (  circle_mul_pt    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 718 "718 600" _null_ _null_ _null_ _null_ _null_	circle_mul_pt _null_ _null_ _null_ ));
DATA(insert OID = 1149 (  circle_div_pt    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 718 "718 600" _null_ _null_ _null_ _null_ _null_	circle_div_pt _null_ _null_ _null_ ));

DATA(insert OID = 1150 (  timestamptz_in   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 1184 "2275 26 23" _null_ _null_ _null_ _null_ _null_ timestamptz_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1151 (  timestamptz_out  PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "1184" _null_ _null_ _null_ _null_ _null_ timestamptz_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2907 (  timestamptztypmodin		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1263" _null_ _null_ _null_ _null_ _null_	timestamptztypmodin _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 2908 (  timestamptztypmodout		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_	timestamptztypmodout _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 1152 (  timestamptz_eq   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1184 1184" _null_ _null_ _null_ _null_ _null_ timestamp_eq _null_ _null_ _null_ ));
DATA(insert OID = 1153 (  timestamptz_ne   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1184 1184" _null_ _null_ _null_ _null_ _null_ timestamp_ne _null_ _null_ _null_ ));
DATA(insert OID = 1154 (  timestamptz_lt   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1184 1184" _null_ _null_ _null_ _null_ _null_ timestamp_lt _null_ _null_ _null_ ));
DATA(insert OID = 1155 (  timestamptz_le   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1184 1184" _null_ _null_ _null_ _null_ _null_ timestamp_le _null_ _null_ _null_ ));
DATA(insert OID = 1156 (  timestamptz_ge   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1184 1184" _null_ _null_ _null_ _null_ _null_ timestamp_ge _null_ _null_ _null_ ));
DATA(insert OID = 1157 (  timestamptz_gt   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1184 1184" _null_ _null_ _null_ _null_ _null_ timestamp_gt _null_ _null_ _null_ ));
DATA(insert OID = 1158 (  to_timestamp	   PGNSP PGUID 14 1 0 0 0 f f f f t f i 1 0 1184 "701" _null_ _null_ _null_ _null_ _null_ "select (''epoch''::pg_catalog.timestamptz + $1 * ''1 second''::pg_catalog.interval)" _null_ _null_ _null_ ));
DESCR("convert UNIX epoch to timestamptz");
DATA(insert OID = 3995 (  timestamp_zone_transform PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ timestamp_zone_transform _null_ _null_ _null_ ));
DESCR("transform a time zone adjustment");
DATA(insert OID = 1159 (  timezone		   PGNSP PGUID 12 1 0 0 timestamp_zone_transform f f f f t f i 2 0 1114 "25 1184" _null_ _null_ _null_ _null_ _null_	timestamptz_zone _null_ _null_ _null_ ));
DESCR("adjust timestamp to new time zone");

DATA(insert OID = 1160 (  interval_in	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 1186 "2275 26 23" _null_ _null_ _null_ _null_ _null_ interval_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1161 (  interval_out	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "1186" _null_ _null_ _null_ _null_ _null_ interval_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2903 (  intervaltypmodin		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1263" _null_ _null_ _null_ _null_ _null_	intervaltypmodin _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 2904 (  intervaltypmodout		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_	intervaltypmodout _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 1162 (  interval_eq	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1186 1186" _null_ _null_ _null_ _null_ _null_ interval_eq _null_ _null_ _null_ ));
DATA(insert OID = 1163 (  interval_ne	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1186 1186" _null_ _null_ _null_ _null_ _null_ interval_ne _null_ _null_ _null_ ));
DATA(insert OID = 1164 (  interval_lt	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1186 1186" _null_ _null_ _null_ _null_ _null_ interval_lt _null_ _null_ _null_ ));
DATA(insert OID = 1165 (  interval_le	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1186 1186" _null_ _null_ _null_ _null_ _null_ interval_le _null_ _null_ _null_ ));
DATA(insert OID = 1166 (  interval_ge	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1186 1186" _null_ _null_ _null_ _null_ _null_ interval_ge _null_ _null_ _null_ ));
DATA(insert OID = 1167 (  interval_gt	   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1186 1186" _null_ _null_ _null_ _null_ _null_ interval_gt _null_ _null_ _null_ ));
DATA(insert OID = 1168 (  interval_um	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1186 "1186" _null_ _null_ _null_ _null_ _null_ interval_um _null_ _null_ _null_ ));
DATA(insert OID = 1169 (  interval_pl	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1186 1186" _null_ _null_ _null_ _null_ _null_ interval_pl _null_ _null_ _null_ ));
DATA(insert OID = 1170 (  interval_mi	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1186 1186" _null_ _null_ _null_ _null_ _null_ interval_mi _null_ _null_ _null_ ));
DATA(insert OID = 1171 (  date_part		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 701 "25 1184" _null_ _null_ _null_ _null_ _null_ timestamptz_part _null_ _null_ _null_ ));
DESCR("extract field from timestamp with time zone");
DATA(insert OID = 1172 (  date_part		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "25 1186" _null_ _null_ _null_ _null_ _null_ interval_part _null_ _null_ _null_ ));
DESCR("extract field from interval");
DATA(insert OID = 1173 (  timestamptz	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1184 "702" _null_ _null_ _null_ _null_ _null_ abstime_timestamptz _null_ _null_ _null_ ));
DESCR("convert abstime to timestamp with time zone");
DATA(insert OID = 1174 (  timestamptz	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1184 "1082" _null_ _null_ _null_ _null_ _null_ date_timestamptz _null_ _null_ _null_ ));
DESCR("convert date to timestamp with time zone");
DATA(insert OID = 2711 (  justify_interval PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1186 "1186" _null_ _null_ _null_ _null_ _null_ interval_justify_interval _null_ _null_ _null_ ));
DESCR("promote groups of 24 hours to numbers of days and promote groups of 30 days to numbers of months");
DATA(insert OID = 1175 (  justify_hours    PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1186 "1186" _null_ _null_ _null_ _null_ _null_ interval_justify_hours _null_ _null_ _null_ ));
DESCR("promote groups of 24 hours to numbers of days");
DATA(insert OID = 1295 (  justify_days	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1186 "1186" _null_ _null_ _null_ _null_ _null_ interval_justify_days _null_ _null_ _null_ ));
DESCR("promote groups of 30 days to numbers of months");
DATA(insert OID = 1176 (  timestamptz	   PGNSP PGUID 14 1 0 0 0 f f f f t f s 2 0 1184 "1082 1083" _null_ _null_ _null_ _null_ _null_ "select cast(($1 + $2) as timestamp with time zone)" _null_ _null_ _null_ ));
DESCR("convert date and time to timestamp with time zone");
DATA(insert OID = 1177 (  interval		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1186 "703" _null_ _null_ _null_ _null_ _null_ reltime_interval _null_ _null_ _null_ ));
DESCR("convert reltime to interval");
DATA(insert OID = 1178 (  date			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1082 "1184" _null_ _null_ _null_ _null_ _null_ timestamptz_date _null_ _null_ _null_ ));
DESCR("convert timestamp with time zone to date");
DATA(insert OID = 1179 (  date			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1082 "702" _null_ _null_ _null_ _null_ _null_ abstime_date _null_ _null_ _null_ ));
DESCR("convert abstime to date");
DATA(insert OID = 1180 (  abstime		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 702 "1184" _null_ _null_ _null_ _null_ _null_	timestamptz_abstime _null_ _null_ _null_ ));
DESCR("convert timestamp with time zone to abstime");
DATA(insert OID = 1181 (  age			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 23 "28" _null_ _null_ _null_ _null_ _null_ xid_age _null_ _null_ _null_ ));
DESCR("age of a transaction ID, in transactions before current transaction");
DATA(insert OID = 3939 (  mxid_age		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 23 "28" _null_ _null_ _null_ _null_ _null_ mxid_age _null_ _null_ _null_ ));
DESCR("age of a multi-transaction ID, in multi-transactions before current multi-transaction");

DATA(insert OID = 1188 (  timestamptz_mi   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1184 1184" _null_ _null_ _null_ _null_ _null_ timestamp_mi _null_ _null_ _null_ ));
DATA(insert OID = 1189 (  timestamptz_pl_interval PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 1184 "1184 1186" _null_ _null_ _null_ _null_ _null_ timestamptz_pl_interval _null_ _null_ _null_ ));
DATA(insert OID = 1190 (  timestamptz_mi_interval PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 1184 "1184 1186" _null_ _null_ _null_ _null_ _null_ timestamptz_mi_interval _null_ _null_ _null_ ));
DATA(insert OID = 1194 (  reltime			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 703 "1186" _null_ _null_ _null_ _null_ _null_ interval_reltime _null_ _null_ _null_ ));
DESCR("convert interval to reltime");
DATA(insert OID = 1195 (  timestamptz_smaller PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1184 "1184 1184" _null_ _null_ _null_ _null_ _null_ timestamp_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 1196 (  timestamptz_larger  PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1184 "1184 1184" _null_ _null_ _null_ _null_ _null_ timestamp_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 1197 (  interval_smaller	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1186 1186" _null_ _null_ _null_ _null_ _null_	interval_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 1198 (  interval_larger	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1186 1186" _null_ _null_ _null_ _null_ _null_	interval_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 1199 (  age				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1184 1184" _null_ _null_ _null_ _null_ _null_	timestamptz_age _null_ _null_ _null_ ));
DESCR("date difference preserving months and years");

/* OIDS 1200 - 1299 */

DATA(insert OID = 3918 (  interval_transform PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ interval_transform _null_ _null_ _null_ ));
DESCR("transform an interval length coercion");
DATA(insert OID = 1200 (  interval			PGNSP PGUID 12 1 0 0 interval_transform f f f f t f i 2 0 1186 "1186 23" _null_ _null_ _null_ _null_ _null_ interval_scale _null_ _null_ _null_ ));
DESCR("adjust interval precision");

DATA(insert OID = 1215 (  obj_description	PGNSP PGUID 14 100 0 0 0 f f f f t f s 2 0 25 "26 19" _null_ _null_ _null_ _null_ _null_ "select description from pg_catalog.pg_description where objoid = $1 and classoid = (select oid from pg_catalog.pg_class where relname = $2 and relnamespace = PGNSP) and objsubid = 0" _null_ _null_ _null_ ));
DESCR("get description for object id and catalog name");
DATA(insert OID = 1216 (  col_description	PGNSP PGUID 14 100 0 0 0 f f f f t f s 2 0 25 "26 23" _null_ _null_ _null_ _null_ _null_ "select description from pg_catalog.pg_description where objoid = $1 and classoid = ''pg_catalog.pg_class''::pg_catalog.regclass and objsubid = $2" _null_ _null_ _null_ ));
DESCR("get description for table column");
DATA(insert OID = 1993 ( shobj_description	PGNSP PGUID 14 100 0 0 0 f f f f t f s 2 0 25 "26 19" _null_ _null_ _null_ _null_ _null_ "select description from pg_catalog.pg_shdescription where objoid = $1 and classoid = (select oid from pg_catalog.pg_class where relname = $2 and relnamespace = PGNSP)" _null_ _null_ _null_ ));
DESCR("get description for object id and shared catalog name");

DATA(insert OID = 1217 (  date_trunc	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 1184 "25 1184" _null_ _null_ _null_ _null_ _null_ timestamptz_trunc _null_ _null_ _null_ ));
DESCR("truncate timestamp with time zone to specified units");
DATA(insert OID = 1218 (  date_trunc	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "25 1186" _null_ _null_ _null_ _null_ _null_ interval_trunc _null_ _null_ _null_ ));
DESCR("truncate interval to specified units");

DATA(insert OID = 1219 (  int8inc		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ int8inc _null_ _null_ _null_ ));
DESCR("increment");
DATA(insert OID = 3546 (  int8dec		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ int8dec _null_ _null_ _null_ ));
DESCR("decrement");
DATA(insert OID = 2804 (  int8inc_any	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 2276" _null_ _null_ _null_ _null_ _null_ int8inc_any _null_ _null_ _null_ ));
DESCR("increment, ignores second argument");
DATA(insert OID = 3547 (  int8dec_any	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 2276" _null_ _null_ _null_ _null_ _null_ int8dec_any _null_ _null_ _null_ ));
DESCR("decrement, ignores second argument");
DATA(insert OID = 1230 (  int8abs		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ int8abs _null_ _null_ _null_ ));

DATA(insert OID = 1236 (  int8larger	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 1237 (  int8smaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8smaller _null_ _null_ _null_ ));
DESCR("smaller of two");

DATA(insert OID = 1238 (  texticregexeq    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ texticregexeq _null_ _null_ _null_ ));
DATA(insert OID = 1239 (  texticregexne    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ texticregexne _null_ _null_ _null_ ));
DATA(insert OID = 1240 (  nameicregexeq    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ nameicregexeq _null_ _null_ _null_ ));
DATA(insert OID = 1241 (  nameicregexne    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ nameicregexne _null_ _null_ _null_ ));

DATA(insert OID = 1251 (  int4abs		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ int4abs _null_ _null_ _null_ ));
DATA(insert OID = 1253 (  int2abs		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "21" _null_ _null_ _null_ _null_ _null_ int2abs _null_ _null_ _null_ ));

DATA(insert OID = 1271 (  overlaps		   PGNSP PGUID 12 1 0 0 0 f f f f f f i 4 0 16 "1266 1266 1266 1266" _null_ _null_ _null_ _null_ _null_ overlaps_timetz _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 1272 (  datetime_pl	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1114 "1082 1083" _null_ _null_ _null_ _null_ _null_ datetime_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 1273 (  date_part		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "25 1266" _null_ _null_ _null_ _null_ _null_ timetz_part _null_ _null_ _null_ ));
DESCR("extract field from time with time zone");
DATA(insert OID = 1274 (  int84pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 23" _null_ _null_ _null_ _null_ _null_ int84pl _null_ _null_ _null_ ));
DATA(insert OID = 1275 (  int84mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 23" _null_ _null_ _null_ _null_ _null_ int84mi _null_ _null_ _null_ ));
DATA(insert OID = 1276 (  int84mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 23" _null_ _null_ _null_ _null_ _null_ int84mul _null_ _null_ _null_ ));
DATA(insert OID = 1277 (  int84div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 23" _null_ _null_ _null_ _null_ _null_ int84div _null_ _null_ _null_ ));
DATA(insert OID = 1278 (  int48pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "23 20" _null_ _null_ _null_ _null_ _null_ int48pl _null_ _null_ _null_ ));
DATA(insert OID = 1279 (  int48mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "23 20" _null_ _null_ _null_ _null_ _null_ int48mi _null_ _null_ _null_ ));
DATA(insert OID = 1280 (  int48mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "23 20" _null_ _null_ _null_ _null_ _null_ int48mul _null_ _null_ _null_ ));
DATA(insert OID = 1281 (  int48div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "23 20" _null_ _null_ _null_ _null_ _null_ int48div _null_ _null_ _null_ ));

DATA(insert OID =  837 (  int82pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 21" _null_ _null_ _null_ _null_ _null_ int82pl _null_ _null_ _null_ ));
DATA(insert OID =  838 (  int82mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 21" _null_ _null_ _null_ _null_ _null_ int82mi _null_ _null_ _null_ ));
DATA(insert OID =  839 (  int82mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 21" _null_ _null_ _null_ _null_ _null_ int82mul _null_ _null_ _null_ ));
DATA(insert OID =  840 (  int82div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 21" _null_ _null_ _null_ _null_ _null_ int82div _null_ _null_ _null_ ));
DATA(insert OID =  841 (  int28pl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "21 20" _null_ _null_ _null_ _null_ _null_ int28pl _null_ _null_ _null_ ));
DATA(insert OID =  942 (  int28mi		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "21 20" _null_ _null_ _null_ _null_ _null_ int28mi _null_ _null_ _null_ ));
DATA(insert OID =  943 (  int28mul		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "21 20" _null_ _null_ _null_ _null_ _null_ int28mul _null_ _null_ _null_ ));
DATA(insert OID =  948 (  int28div		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "21 20" _null_ _null_ _null_ _null_ _null_ int28div _null_ _null_ _null_ ));

DATA(insert OID = 1287 (  oid			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 26 "20" _null_ _null_ _null_ _null_ _null_ i8tooid _null_ _null_ _null_ ));
DESCR("convert int8 to oid");
DATA(insert OID = 1288 (  int8			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ oidtoi8 _null_ _null_ _null_ ));
DESCR("convert oid to int8");

DATA(insert OID = 1291 (  suppress_redundant_updates_trigger	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ suppress_redundant_updates_trigger _null_ _null_ _null_ ));
DESCR("trigger to suppress updates when new and old records match");

DATA(insert OID = 1292 ( tideq			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "27 27" _null_ _null_ _null_ _null_ _null_ tideq _null_ _null_ _null_ ));
DATA(insert OID = 1293 ( currtid		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 27 "26 27" _null_ _null_ _null_ _null_ _null_ currtid_byreloid _null_ _null_ _null_ ));
DESCR("latest tid of a tuple");
DATA(insert OID = 1294 ( currtid2		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 27 "25 27" _null_ _null_ _null_ _null_ _null_ currtid_byrelname _null_ _null_ _null_ ));
DESCR("latest tid of a tuple");
DATA(insert OID = 1265 ( tidne			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "27 27" _null_ _null_ _null_ _null_ _null_ tidne _null_ _null_ _null_ ));
DATA(insert OID = 2790 ( tidgt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "27 27" _null_ _null_ _null_ _null_ _null_ tidgt _null_ _null_ _null_ ));
DATA(insert OID = 2791 ( tidlt			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "27 27" _null_ _null_ _null_ _null_ _null_ tidlt _null_ _null_ _null_ ));
DATA(insert OID = 2792 ( tidge			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "27 27" _null_ _null_ _null_ _null_ _null_ tidge _null_ _null_ _null_ ));
DATA(insert OID = 2793 ( tidle			   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "27 27" _null_ _null_ _null_ _null_ _null_ tidle _null_ _null_ _null_ ));
DATA(insert OID = 2794 ( bttidcmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "27 27" _null_ _null_ _null_ _null_ _null_ bttidcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 2795 ( tidlarger		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 27 "27 27" _null_ _null_ _null_ _null_ _null_ tidlarger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 2796 ( tidsmaller		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 27 "27 27" _null_ _null_ _null_ _null_ _null_ tidsmaller _null_ _null_ _null_ ));
DESCR("smaller of two");

DATA(insert OID = 1296 (  timedate_pl	   PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 1114 "1083 1082" _null_ _null_ _null_ _null_ _null_ "select ($2 + $1)" _null_ _null_ _null_ ));
DATA(insert OID = 1297 (  datetimetz_pl    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1184 "1082 1266" _null_ _null_ _null_ _null_ _null_ datetimetz_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 1298 (  timetzdate_pl    PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 1184 "1266 1082" _null_ _null_ _null_ _null_ _null_ "select ($2 + $1)" _null_ _null_ _null_ ));
DATA(insert OID = 1299 (  now			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 1184 "" _null_ _null_ _null_ _null_ _null_ now _null_ _null_ _null_ ));
DESCR("current transaction time");
DATA(insert OID = 2647 (  transaction_timestamp PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 1184 "" _null_ _null_ _null_ _null_ _null_ now _null_ _null_ _null_ ));
DESCR("current transaction time");
DATA(insert OID = 2648 (  statement_timestamp	PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 1184 "" _null_ _null_ _null_ _null_ _null_ statement_timestamp _null_ _null_ _null_ ));
DESCR("current statement time");
DATA(insert OID = 2649 (  clock_timestamp	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 1184 "" _null_ _null_ _null_ _null_ _null_ clock_timestamp _null_ _null_ _null_ ));
DESCR("current clock time");

/* OIDS 1300 - 1399 */

DATA(insert OID = 1300 (  positionsel		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	positionsel _null_ _null_ _null_ ));
DESCR("restriction selectivity for position-comparison operators");
DATA(insert OID = 1301 (  positionjoinsel	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_ _null_	positionjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity for position-comparison operators");
DATA(insert OID = 1302 (  contsel		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	contsel _null_ _null_ _null_ ));
DESCR("restriction selectivity for containment comparison operators");
DATA(insert OID = 1303 (  contjoinsel	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_ _null_	contjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity for containment comparison operators");

DATA(insert OID = 1304 ( overlaps			 PGNSP PGUID 12 1 0 0 0 f f f f f f i 4 0 16 "1184 1184 1184 1184" _null_ _null_ _null_ _null_ _null_ overlaps_timestamp _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 1305 ( overlaps			 PGNSP PGUID 14 1 0 0 0 f f f f f f s 4 0 16 "1184 1186 1184 1186" _null_ _null_ _null_ _null_ _null_ "select ($1, ($1 + $2)) overlaps ($3, ($3 + $4))" _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 1306 ( overlaps			 PGNSP PGUID 14 1 0 0 0 f f f f f f s 4 0 16 "1184 1184 1184 1186" _null_ _null_ _null_ _null_ _null_ "select ($1, $2) overlaps ($3, ($3 + $4))" _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 1307 ( overlaps			 PGNSP PGUID 14 1 0 0 0 f f f f f f s 4 0 16 "1184 1186 1184 1184" _null_ _null_ _null_ _null_ _null_ "select ($1, ($1 + $2)) overlaps ($3, $4)" _null_ _null_ _null_ ));
DESCR("intervals overlap?");

DATA(insert OID = 1308 ( overlaps			 PGNSP PGUID 12 1 0 0 0 f f f f f f i 4 0 16 "1083 1083 1083 1083" _null_ _null_ _null_ _null_ _null_ overlaps_time _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 1309 ( overlaps			 PGNSP PGUID 14 1 0 0 0 f f f f f f i 4 0 16 "1083 1186 1083 1186" _null_ _null_ _null_ _null_ _null_ "select ($1, ($1 + $2)) overlaps ($3, ($3 + $4))" _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 1310 ( overlaps			 PGNSP PGUID 14 1 0 0 0 f f f f f f i 4 0 16 "1083 1083 1083 1186" _null_ _null_ _null_ _null_ _null_ "select ($1, $2) overlaps ($3, ($3 + $4))" _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 1311 ( overlaps			 PGNSP PGUID 14 1 0 0 0 f f f f f f i 4 0 16 "1083 1186 1083 1083" _null_ _null_ _null_ _null_ _null_ "select ($1, ($1 + $2)) overlaps ($3, $4)" _null_ _null_ _null_ ));
DESCR("intervals overlap?");

DATA(insert OID = 1312 (  timestamp_in		 PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 1114 "2275 26 23" _null_ _null_ _null_ _null_ _null_ timestamp_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1313 (  timestamp_out		 PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "1114" _null_ _null_ _null_ _null_ _null_ timestamp_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2905 (  timestamptypmodin		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1263" _null_ _null_ _null_ _null_ _null_	timestamptypmodin _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 2906 (  timestamptypmodout	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_	timestamptypmodout _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 1314 (  timestamptz_cmp	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1184 1184" _null_ _null_ _null_ _null_ _null_ timestamp_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 1315 (  interval_cmp		 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1186 1186" _null_ _null_ _null_ _null_ _null_ interval_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 1316 (  time				 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1083 "1114" _null_ _null_ _null_ _null_ _null_	timestamp_time _null_ _null_ _null_ ));
DESCR("convert timestamp to time");

DATA(insert OID = 1317 (  length			 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "25" _null_ _null_ _null_ _null_ _null_	textlen _null_ _null_ _null_ ));
DESCR("length");
DATA(insert OID = 1318 (  length			 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1042" _null_ _null_ _null_ _null_ _null_ bpcharlen _null_ _null_ _null_ ));
DESCR("character length");

DATA(insert OID = 1319 (  xideqint4			 PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "28 23" _null_ _null_ _null_ _null_ _null_ xideq _null_ _null_ _null_ ));

DATA(insert OID = 1326 (  interval_div		 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1186 701" _null_ _null_ _null_ _null_ _null_	interval_div _null_ _null_ _null_ ));

DATA(insert OID = 1339 (  dlog10			 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dlog10 _null_ _null_ _null_ ));
DESCR("base 10 logarithm");
DATA(insert OID = 1340 (  log				 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dlog10 _null_ _null_ _null_ ));
DESCR("base 10 logarithm");
DATA(insert OID = 1341 (  ln				 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dlog1 _null_ _null_ _null_ ));
DESCR("natural logarithm");
DATA(insert OID = 1342 (  round				 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dround _null_ _null_ _null_ ));
DESCR("round to nearest integer");
DATA(insert OID = 1343 (  trunc				 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dtrunc _null_ _null_ _null_ ));
DESCR("truncate to integer");
DATA(insert OID = 1344 (  sqrt				 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dsqrt _null_ _null_ _null_ ));
DESCR("square root");
DATA(insert OID = 1345 (  cbrt				 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dcbrt _null_ _null_ _null_ ));
DESCR("cube root");
DATA(insert OID = 1346 (  pow				 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_ dpow _null_ _null_ _null_ ));
DESCR("exponentiation");
DATA(insert OID = 1368 (  power				 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_ dpow _null_ _null_ _null_ ));
DESCR("exponentiation");
DATA(insert OID = 1347 (  exp				 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dexp _null_ _null_ _null_ ));
DESCR("natural exponential (e^x)");

/*
 * This form of obj_description is now deprecated, since it will fail if
 * OIDs are not unique across system catalogs.  Use the other form instead.
 */
DATA(insert OID = 1348 (  obj_description	 PGNSP PGUID 14 100 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ "select description from pg_catalog.pg_description where objoid = $1 and objsubid = 0" _null_ _null_ _null_ ));
DESCR("deprecated, use two-argument form instead");
DATA(insert OID = 1349 (  oidvectortypes	 PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "30" _null_ _null_ _null_ _null_ _null_	oidvectortypes _null_ _null_ _null_ ));
DESCR("print type names of oidvector field");


DATA(insert OID = 1350 (  timetz_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 1266 "2275 26 23" _null_ _null_ _null_ _null_ _null_ timetz_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1351 (  timetz_out	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "1266" _null_ _null_ _null_ _null_ _null_ timetz_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2911 (  timetztypmodin	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1263" _null_ _null_ _null_ _null_ _null_	timetztypmodin _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 2912 (  timetztypmodout	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_	timetztypmodout _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 1352 (  timetz_eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1266 1266" _null_ _null_ _null_ _null_ _null_ timetz_eq _null_ _null_ _null_ ));
DATA(insert OID = 1353 (  timetz_ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1266 1266" _null_ _null_ _null_ _null_ _null_ timetz_ne _null_ _null_ _null_ ));
DATA(insert OID = 1354 (  timetz_lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1266 1266" _null_ _null_ _null_ _null_ _null_ timetz_lt _null_ _null_ _null_ ));
DATA(insert OID = 1355 (  timetz_le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1266 1266" _null_ _null_ _null_ _null_ _null_ timetz_le _null_ _null_ _null_ ));
DATA(insert OID = 1356 (  timetz_ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1266 1266" _null_ _null_ _null_ _null_ _null_ timetz_ge _null_ _null_ _null_ ));
DATA(insert OID = 1357 (  timetz_gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1266 1266" _null_ _null_ _null_ _null_ _null_ timetz_gt _null_ _null_ _null_ ));
DATA(insert OID = 1358 (  timetz_cmp	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1266 1266" _null_ _null_ _null_ _null_ _null_ timetz_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 1359 (  timestamptz	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1184 "1082 1266" _null_ _null_ _null_ _null_ _null_ datetimetz_timestamptz _null_ _null_ _null_ ));
DESCR("convert date and time with time zone to timestamp with time zone");

DATA(insert OID = 1364 (  time			   PGNSP PGUID 14 1 0 0 0 f f f f t f s 1 0 1083 "702" _null_ _null_ _null_ _null_ _null_ "select cast(cast($1 as timestamp without time zone) as pg_catalog.time)" _null_ _null_ _null_ ));
DESCR("convert abstime to time");

DATA(insert OID = 1367 (  character_length	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1042" _null_ _null_ _null_ _null_ _null_	bpcharlen _null_ _null_ _null_ ));
DESCR("character length");
DATA(insert OID = 1369 (  character_length	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "25" _null_ _null_ _null_ _null_ _null_ textlen _null_ _null_ _null_ ));
DESCR("character length");

DATA(insert OID = 1370 (  interval			 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1186 "1083" _null_ _null_ _null_ _null_ _null_	time_interval _null_ _null_ _null_ ));
DESCR("convert time to interval");
DATA(insert OID = 1372 (  char_length		 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1042" _null_ _null_ _null_ _null_ _null_ bpcharlen _null_ _null_ _null_ ));
DESCR("character length");
DATA(insert OID = 1374 (  octet_length			 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "25" _null_ _null_ _null_ _null_ _null_	textoctetlen _null_ _null_ _null_ ));
DESCR("octet length");
DATA(insert OID = 1375 (  octet_length			 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1042" _null_ _null_ _null_ _null_ _null_ bpcharoctetlen _null_ _null_ _null_ ));
DESCR("octet length");

DATA(insert OID = 1377 (  time_larger	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1083 "1083 1083" _null_ _null_ _null_ _null_ _null_ time_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 1378 (  time_smaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1083 "1083 1083" _null_ _null_ _null_ _null_ _null_ time_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 1379 (  timetz_larger    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1266 "1266 1266" _null_ _null_ _null_ _null_ _null_ timetz_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 1380 (  timetz_smaller   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1266 "1266 1266" _null_ _null_ _null_ _null_ _null_ timetz_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");

DATA(insert OID = 1381 (  char_length	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "25" _null_ _null_ _null_ _null_ _null_ textlen _null_ _null_ _null_ ));
DESCR("character length");

DATA(insert OID = 1382 (  date_part    PGNSP PGUID 14 1 0 0 0 f f f f t f s 2 0 701 "25 702" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.date_part($1, cast($2 as timestamp with time zone))" _null_ _null_ _null_ ));
DESCR("extract field from abstime");
DATA(insert OID = 1383 (  date_part    PGNSP PGUID 14 1 0 0 0 f f f f t f s 2 0 701 "25 703" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.date_part($1, cast($2 as pg_catalog.interval))" _null_ _null_ _null_ ));
DESCR("extract field from reltime");
DATA(insert OID = 1384 (  date_part    PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 701 "25 1082" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.date_part($1, cast($2 as timestamp without time zone))" _null_ _null_ _null_ ));
DESCR("extract field from date");
DATA(insert OID = 1385 (  date_part    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "25 1083" _null_ _null_ _null_ _null_  _null_ time_part _null_ _null_ _null_ ));
DESCR("extract field from time");
DATA(insert OID = 1386 (  age		   PGNSP PGUID 14 1 0 0 0 f f f f t f s 1 0 1186 "1184" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.age(cast(current_date as timestamp with time zone), $1)" _null_ _null_ _null_ ));
DESCR("date difference from today preserving months and years");

DATA(insert OID = 1388 (  timetz	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1266 "1184" _null_ _null_ _null_ _null_ _null_ timestamptz_timetz _null_ _null_ _null_ ));
DESCR("convert timestamp with time zone to time with time zone");

DATA(insert OID = 1373 (  isfinite	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "1082" _null_ _null_ _null_ _null_ _null_	date_finite _null_ _null_ _null_ ));
DESCR("finite date?");
DATA(insert OID = 1389 (  isfinite	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "1184" _null_ _null_ _null_ _null_ _null_	timestamp_finite _null_ _null_ _null_ ));
DESCR("finite timestamp?");
DATA(insert OID = 1390 (  isfinite	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "1186" _null_ _null_ _null_ _null_ _null_	interval_finite _null_ _null_ _null_ ));
DESCR("finite interval?");


DATA(insert OID = 1376 (  factorial		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_	numeric_fac _null_ _null_ _null_ ));
DESCR("factorial");
DATA(insert OID = 1394 (  abs			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700 "700" _null_ _null_ _null_ _null_ _null_	float4abs _null_ _null_ _null_ ));
DESCR("absolute value");
DATA(insert OID = 1395 (  abs			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	float8abs _null_ _null_ _null_ ));
DESCR("absolute value");
DATA(insert OID = 1396 (  abs			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ int8abs _null_ _null_ _null_ ));
DESCR("absolute value");
DATA(insert OID = 1397 (  abs			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ int4abs _null_ _null_ _null_ ));
DESCR("absolute value");
DATA(insert OID = 1398 (  abs			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "21" _null_ _null_ _null_ _null_ _null_ int2abs _null_ _null_ _null_ ));
DESCR("absolute value");

/* OIDS 1400 - 1499 */

DATA(insert OID = 1400 (  name		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 19 "1043" _null_ _null_ _null_ _null_ _null_	text_name _null_ _null_ _null_ ));
DESCR("convert varchar to name");
DATA(insert OID = 1401 (  varchar	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1043 "19" _null_ _null_ _null_ _null_ _null_	name_text _null_ _null_ _null_ ));
DESCR("convert name to varchar");

DATA(insert OID = 1402 (  current_schema	PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 19 "" _null_ _null_ _null_ _null_ _null_ current_schema _null_ _null_ _null_ ));
DESCR("current schema name");
DATA(insert OID = 1403 (  current_schemas	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1003 "16" _null_ _null_ _null_ _null_ _null_	current_schemas _null_ _null_ _null_ ));
DESCR("current schema search list");

DATA(insert OID = 1404 (  overlay			PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 25 "25 25 23 23" _null_ _null_ _null_ _null_ _null_	textoverlay _null_ _null_ _null_ ));
DESCR("substitute portion of string");
DATA(insert OID = 1405 (  overlay			PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 25 "25 25 23" _null_ _null_ _null_ _null_ _null_	textoverlay_no_len _null_ _null_ _null_ ));
DESCR("substitute portion of string");

DATA(insert OID = 1406 (  isvertical		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 600" _null_ _null_ _null_ _null_  _null_ point_vert _null_ _null_ _null_ ));
DESCR("vertically aligned");
DATA(insert OID = 1407 (  ishorizontal		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 600" _null_ _null_ _null_ _null_  _null_ point_horiz _null_ _null_ _null_ ));
DESCR("horizontally aligned");
DATA(insert OID = 1408 (  isparallel		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_  _null_ lseg_parallel _null_ _null_ _null_ ));
DESCR("parallel");
DATA(insert OID = 1409 (  isperp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_  _null_ lseg_perp _null_ _null_ _null_ ));
DESCR("perpendicular");
DATA(insert OID = 1410 (  isvertical		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "601" _null_ _null_ _null_ _null_  _null_ lseg_vertical _null_ _null_ _null_ ));
DESCR("vertical");
DATA(insert OID = 1411 (  ishorizontal		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "601" _null_ _null_ _null_ _null_  _null_ lseg_horizontal _null_ _null_ _null_ ));
DESCR("horizontal");
DATA(insert OID = 1412 (  isparallel		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "628 628" _null_ _null_ _null_ _null_  _null_ line_parallel _null_ _null_ _null_ ));
DESCR("parallel");
DATA(insert OID = 1413 (  isperp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "628 628" _null_ _null_ _null_ _null_  _null_ line_perp _null_ _null_ _null_ ));
DESCR("perpendicular");
DATA(insert OID = 1414 (  isvertical		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "628" _null_ _null_ _null_ _null_  _null_ line_vertical _null_ _null_ _null_ ));
DESCR("vertical");
DATA(insert OID = 1415 (  ishorizontal		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "628" _null_ _null_ _null_ _null_  _null_ line_horizontal _null_ _null_ _null_ ));
DESCR("horizontal");
DATA(insert OID = 1416 (  point				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "718" _null_ _null_ _null_ _null_ _null_ circle_center _null_ _null_ _null_ ));
DESCR("center of");

DATA(insert OID = 1419 (  time				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1083 "1186" _null_ _null_ _null_ _null_ _null_ interval_time _null_ _null_ _null_ ));
DESCR("convert interval to time");

DATA(insert OID = 1421 (  box				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 603 "600 600" _null_ _null_ _null_ _null_ _null_ points_box _null_ _null_ _null_ ));
DESCR("convert points to box");
DATA(insert OID = 1422 (  box_add			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 603 "603 600" _null_ _null_ _null_ _null_ _null_ box_add _null_ _null_ _null_ ));
DATA(insert OID = 1423 (  box_sub			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 603 "603 600" _null_ _null_ _null_ _null_ _null_ box_sub _null_ _null_ _null_ ));
DATA(insert OID = 1424 (  box_mul			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 603 "603 600" _null_ _null_ _null_ _null_ _null_ box_mul _null_ _null_ _null_ ));
DATA(insert OID = 1425 (  box_div			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 603 "603 600" _null_ _null_ _null_ _null_ _null_ box_div _null_ _null_ _null_ ));
DATA(insert OID = 1426 (  path_contain_pt	PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 16 "602 600" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.on_ppath($2, $1)" _null_ _null_ _null_ ));
DATA(insert OID = 1428 (  poly_contain_pt	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 600" _null_ _null_ _null_ _null_  _null_ poly_contain_pt _null_ _null_ _null_ ));
DATA(insert OID = 1429 (  pt_contained_poly PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 604" _null_ _null_ _null_ _null_  _null_ pt_contained_poly _null_ _null_ _null_ ));

DATA(insert OID = 1430 (  isclosed			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "602" _null_ _null_ _null_ _null_  _null_ path_isclosed _null_ _null_ _null_ ));
DESCR("path closed?");
DATA(insert OID = 1431 (  isopen			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "602" _null_ _null_ _null_ _null_  _null_ path_isopen _null_ _null_ _null_ ));
DESCR("path open?");
DATA(insert OID = 1432 (  path_npoints		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "602" _null_ _null_ _null_ _null_  _null_ path_npoints _null_ _null_ _null_ ));

/* pclose and popen might better be named close and open, but that crashes initdb.
 * - thomas 97/04/20
 */

DATA(insert OID = 1433 (  pclose			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 602 "602" _null_ _null_ _null_ _null_ _null_ path_close _null_ _null_ _null_ ));
DESCR("close path");
DATA(insert OID = 1434 (  popen				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 602 "602" _null_ _null_ _null_ _null_ _null_ path_open _null_ _null_ _null_ ));
DESCR("open path");
DATA(insert OID = 1435 (  path_add			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 602 "602 602" _null_ _null_ _null_ _null_ _null_ path_add _null_ _null_ _null_ ));
DATA(insert OID = 1436 (  path_add_pt		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 602 "602 600" _null_ _null_ _null_ _null_ _null_ path_add_pt _null_ _null_ _null_ ));
DATA(insert OID = 1437 (  path_sub_pt		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 602 "602 600" _null_ _null_ _null_ _null_ _null_ path_sub_pt _null_ _null_ _null_ ));
DATA(insert OID = 1438 (  path_mul_pt		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 602 "602 600" _null_ _null_ _null_ _null_ _null_ path_mul_pt _null_ _null_ _null_ ));
DATA(insert OID = 1439 (  path_div_pt		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 602 "602 600" _null_ _null_ _null_ _null_ _null_ path_div_pt _null_ _null_ _null_ ));

DATA(insert OID = 1440 (  point				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "701 701" _null_ _null_ _null_ _null_ _null_ construct_point _null_ _null_ _null_ ));
DESCR("convert x, y to point");
DATA(insert OID = 1441 (  point_add			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "600 600" _null_ _null_ _null_ _null_ _null_ point_add _null_ _null_ _null_ ));
DATA(insert OID = 1442 (  point_sub			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "600 600" _null_ _null_ _null_ _null_ _null_ point_sub _null_ _null_ _null_ ));
DATA(insert OID = 1443 (  point_mul			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "600 600" _null_ _null_ _null_ _null_ _null_ point_mul _null_ _null_ _null_ ));
DATA(insert OID = 1444 (  point_div			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "600 600" _null_ _null_ _null_ _null_ _null_ point_div _null_ _null_ _null_ ));

DATA(insert OID = 1445 (  poly_npoints		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "604" _null_ _null_ _null_ _null_  _null_ poly_npoints _null_ _null_ _null_ ));
DATA(insert OID = 1446 (  box				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 603 "604" _null_ _null_ _null_ _null_ _null_ poly_box _null_ _null_ _null_ ));
DESCR("convert polygon to bounding box");
DATA(insert OID = 1447 (  path				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 602 "604" _null_ _null_ _null_ _null_ _null_ poly_path _null_ _null_ _null_ ));
DESCR("convert polygon to path");
DATA(insert OID = 1448 (  polygon			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 604 "603" _null_ _null_ _null_ _null_ _null_ box_poly _null_ _null_ _null_ ));
DESCR("convert box to polygon");
DATA(insert OID = 1449 (  polygon			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 604 "602" _null_ _null_ _null_ _null_ _null_ path_poly _null_ _null_ _null_ ));
DESCR("convert path to polygon");

DATA(insert OID = 1450 (  circle_in			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 718 "2275" _null_ _null_ _null_ _null_ _null_ circle_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1451 (  circle_out		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "718" _null_ _null_ _null_ _null_ _null_ circle_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1452 (  circle_same		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_same _null_ _null_ _null_ ));
DATA(insert OID = 1453 (  circle_contain	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_contain _null_ _null_ _null_ ));
DATA(insert OID = 1454 (  circle_left		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_left _null_ _null_ _null_ ));
DATA(insert OID = 1455 (  circle_overleft	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_overleft _null_ _null_ _null_ ));
DATA(insert OID = 1456 (  circle_overright	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_overright _null_ _null_ _null_ ));
DATA(insert OID = 1457 (  circle_right		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_right _null_ _null_ _null_ ));
DATA(insert OID = 1458 (  circle_contained	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_contained _null_ _null_ _null_ ));
DATA(insert OID = 1459 (  circle_overlap	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_overlap _null_ _null_ _null_ ));
DATA(insert OID = 1460 (  circle_below		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_below _null_ _null_ _null_ ));
DATA(insert OID = 1461 (  circle_above		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_above _null_ _null_ _null_ ));
DATA(insert OID = 1462 (  circle_eq			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_eq _null_ _null_ _null_ ));
DATA(insert OID = 1463 (  circle_ne			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_ne _null_ _null_ _null_ ));
DATA(insert OID = 1464 (  circle_lt			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_lt _null_ _null_ _null_ ));
DATA(insert OID = 1465 (  circle_gt			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_gt _null_ _null_ _null_ ));
DATA(insert OID = 1466 (  circle_le			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_le _null_ _null_ _null_ ));
DATA(insert OID = 1467 (  circle_ge			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_ge _null_ _null_ _null_ ));
DATA(insert OID = 1468 (  area				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "718" _null_ _null_ _null_ _null_ _null_ circle_area _null_ _null_ _null_ ));
DESCR("area of circle");
DATA(insert OID = 1469 (  diameter			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "718" _null_ _null_ _null_ _null_ _null_ circle_diameter _null_ _null_ _null_ ));
DESCR("diameter of circle");
DATA(insert OID = 1470 (  radius			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "718" _null_ _null_ _null_ _null_ _null_ circle_radius _null_ _null_ _null_ ));
DESCR("radius of circle");
DATA(insert OID = 1471 (  circle_distance	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "718 718" _null_ _null_ _null_ _null_ _null_ circle_distance _null_ _null_ _null_ ));
DATA(insert OID = 1472 (  circle_center		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "718" _null_ _null_ _null_ _null_ _null_ circle_center _null_ _null_ _null_ ));
DATA(insert OID = 1473 (  circle			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 718 "600 701" _null_ _null_ _null_ _null_ _null_ cr_circle _null_ _null_ _null_ ));
DESCR("convert point and radius to circle");
DATA(insert OID = 1474 (  circle			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 718 "604" _null_ _null_ _null_ _null_ _null_ poly_circle _null_ _null_ _null_ ));
DESCR("convert polygon to circle");
DATA(insert OID = 1475 (  polygon			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 604 "23 718" _null_ _null_ _null_ _null_ _null_	circle_poly _null_ _null_ _null_ ));
DESCR("convert vertex count and circle to polygon");
DATA(insert OID = 1476 (  dist_pc			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "600 718" _null_ _null_ _null_ _null_ _null_ dist_pc _null_ _null_ _null_ ));
DATA(insert OID = 1477 (  circle_contain_pt PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 600" _null_ _null_ _null_ _null_  _null_ circle_contain_pt _null_ _null_ _null_ ));
DATA(insert OID = 1478 (  pt_contained_circle	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "600 718" _null_ _null_ _null_ _null_  _null_ pt_contained_circle _null_ _null_ _null_ ));
DATA(insert OID = 4091 (  box				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 603 "600" _null_ _null_ _null_ _null_ _null_ point_box _null_ _null_ _null_ ));
DESCR("convert point to empty box");
DATA(insert OID = 1479 (  circle			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 718 "603" _null_ _null_ _null_ _null_ _null_ box_circle _null_ _null_ _null_ ));
DESCR("convert box to circle");
DATA(insert OID = 1480 (  box				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 603 "718" _null_ _null_ _null_ _null_ _null_ circle_box _null_ _null_ _null_ ));
DESCR("convert circle to box");
DATA(insert OID = 1481 (  tinterval			 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 704 "702 702" _null_ _null_ _null_ _null_ _null_ mktinterval _null_ _null_ _null_ ));
DESCR("convert to tinterval");

DATA(insert OID = 1482 (  lseg_ne			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_  _null_ lseg_ne _null_ _null_ _null_ ));
DATA(insert OID = 1483 (  lseg_lt			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_  _null_ lseg_lt _null_ _null_ _null_ ));
DATA(insert OID = 1484 (  lseg_le			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_  _null_ lseg_le _null_ _null_ _null_ ));
DATA(insert OID = 1485 (  lseg_gt			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_  _null_ lseg_gt _null_ _null_ _null_ ));
DATA(insert OID = 1486 (  lseg_ge			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "601 601" _null_ _null_ _null_ _null_  _null_ lseg_ge _null_ _null_ _null_ ));
DATA(insert OID = 1487 (  lseg_length		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "601" _null_ _null_ _null_ _null_ _null_ lseg_length _null_ _null_ _null_ ));
DATA(insert OID = 1488 (  close_ls			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "628 601" _null_ _null_ _null_ _null_ _null_ close_ls _null_ _null_ _null_ ));
DATA(insert OID = 1489 (  close_lseg		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "601 601" _null_ _null_ _null_ _null_ _null_ close_lseg _null_ _null_ _null_ ));

DATA(insert OID = 1490 (  line_in			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 628 "2275" _null_ _null_ _null_ _null_ _null_ line_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1491 (  line_out			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "628" _null_ _null_ _null_ _null_ _null_ line_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1492 (  line_eq			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "628 628" _null_ _null_ _null_ _null_ _null_ line_eq _null_ _null_ _null_ ));
DATA(insert OID = 1493 (  line				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 628 "600 600" _null_ _null_ _null_ _null_ _null_ line_construct_pp _null_ _null_ _null_ ));
DESCR("construct line from points");
DATA(insert OID = 1494 (  line_interpt		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 600 "628 628" _null_ _null_ _null_ _null_ _null_ line_interpt _null_ _null_ _null_ ));
DATA(insert OID = 1495 (  line_intersect	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "628 628" _null_ _null_ _null_ _null_  _null_ line_intersect _null_ _null_ _null_ ));
DATA(insert OID = 1496 (  line_parallel		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "628 628" _null_ _null_ _null_ _null_  _null_ line_parallel _null_ _null_ _null_ ));
DATA(insert OID = 1497 (  line_perp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "628 628" _null_ _null_ _null_ _null_  _null_ line_perp _null_ _null_ _null_ ));
DATA(insert OID = 1498 (  line_vertical		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "628" _null_ _null_ _null_ _null_  _null_ line_vertical _null_ _null_ _null_ ));
DATA(insert OID = 1499 (  line_horizontal	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "628" _null_ _null_ _null_ _null_  _null_ line_horizontal _null_ _null_ _null_ ));

/* OIDS 1500 - 1599 */

DATA(insert OID = 1530 (  length			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "601" _null_ _null_ _null_ _null_ _null_ lseg_length _null_ _null_ _null_ ));
DESCR("distance between endpoints");
DATA(insert OID = 1531 (  length			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "602" _null_ _null_ _null_ _null_ _null_ path_length _null_ _null_ _null_ ));
DESCR("sum of path segments");


DATA(insert OID = 1532 (  point				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "601" _null_ _null_ _null_ _null_ _null_ lseg_center _null_ _null_ _null_ ));
DESCR("center of");
DATA(insert OID = 1533 (  point				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "602" _null_ _null_ _null_ _null_ _null_ path_center _null_ _null_ _null_ ));
DESCR("center of");
DATA(insert OID = 1534 (  point				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "603" _null_ _null_ _null_ _null_ _null_ box_center _null_ _null_ _null_ ));
DESCR("center of");
DATA(insert OID = 1540 (  point				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "604" _null_ _null_ _null_ _null_ _null_ poly_center _null_ _null_ _null_ ));
DESCR("center of");
DATA(insert OID = 1541 (  lseg				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 601 "603" _null_ _null_ _null_ _null_ _null_ box_diagonal _null_ _null_ _null_ ));
DESCR("diagonal of");
DATA(insert OID = 1542 (  center			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "603" _null_ _null_ _null_ _null_ _null_ box_center _null_ _null_ _null_ ));
DESCR("center of");
DATA(insert OID = 1543 (  center			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "718" _null_ _null_ _null_ _null_ _null_ circle_center _null_ _null_ _null_ ));
DESCR("center of");
DATA(insert OID = 1544 (  polygon			PGNSP PGUID 14 1 0 0 0 f f f f t f i 1 0 604 "718" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.polygon(12, $1)" _null_ _null_ _null_ ));
DESCR("convert circle to 12-vertex polygon");
DATA(insert OID = 1545 (  npoints			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "602" _null_ _null_ _null_ _null_  _null_ path_npoints _null_ _null_ _null_ ));
DESCR("number of points");
DATA(insert OID = 1556 (  npoints			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "604" _null_ _null_ _null_ _null_  _null_ poly_npoints _null_ _null_ _null_ ));
DESCR("number of points");

DATA(insert OID = 1564 (  bit_in			PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1560 "2275 26 23" _null_ _null_ _null_ _null_ _null_ bit_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1565 (  bit_out			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "1560" _null_ _null_ _null_ _null_ _null_ bit_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2919 (  bittypmodin		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1263" _null_ _null_ _null_ _null_ _null_	bittypmodin _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 2920 (  bittypmodout		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_	bittypmodout _null_ _null_ _null_ ));
DESCR("I/O typmod");

DATA(insert OID = 1569 (  like				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ textlike _null_ _null_ _null_ ));
DESCR("matches LIKE expression");
DATA(insert OID = 1570 (  notlike			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ textnlike _null_ _null_ _null_ ));
DESCR("does not match LIKE expression");
DATA(insert OID = 1571 (  like				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ namelike _null_ _null_ _null_ ));
DESCR("matches LIKE expression");
DATA(insert OID = 1572 (  notlike			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ namenlike _null_ _null_ _null_ ));
DESCR("does not match LIKE expression");


/* SEQUENCE functions */
DATA(insert OID = 1574 (  nextval			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "2205" _null_ _null_ _null_ _null_ _null_	nextval_oid _null_ _null_ _null_ ));
DESCR("sequence next value");
DATA(insert OID = 1575 (  currval			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "2205" _null_ _null_ _null_ _null_ _null_	currval_oid _null_ _null_ _null_ ));
DESCR("sequence current value");
DATA(insert OID = 1576 (  setval			PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 20 "2205 20" _null_ _null_ _null_ _null_  _null_ setval_oid _null_ _null_ _null_ ));
DESCR("set sequence value");
DATA(insert OID = 1765 (  setval			PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 20 "2205 20 16" _null_ _null_ _null_ _null_ _null_ setval3_oid _null_ _null_ _null_ ));
DESCR("set sequence value and is_called status");
DATA(insert OID = 3078 (  pg_sequence_parameters	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2249 "26" "{26,20,20,20,20,16}" "{i,o,o,o,o,o}" "{sequence_oid,start_value,minimum_value,maximum_value,increment,cycle_option}" _null_ _null_ pg_sequence_parameters _null_ _null_ _null_));
DESCR("sequence parameters, for use by information schema");

DATA(insert OID = 1579 (  varbit_in			PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1562 "2275 26 23" _null_ _null_ _null_ _null_ _null_ varbit_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1580 (  varbit_out		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "1562" _null_ _null_ _null_ _null_ _null_ varbit_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2902 (  varbittypmodin	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1263" _null_ _null_ _null_ _null_ _null_	varbittypmodin _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 2921 (  varbittypmodout	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_	varbittypmodout _null_ _null_ _null_ ));
DESCR("I/O typmod");

DATA(insert OID = 1581 (  biteq				PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1560 1560" _null_ _null_ _null_ _null_ _null_ biteq _null_ _null_ _null_ ));
DATA(insert OID = 1582 (  bitne				PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1560 1560" _null_ _null_ _null_ _null_ _null_ bitne _null_ _null_ _null_ ));
DATA(insert OID = 1592 (  bitge				PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1560 1560" _null_ _null_ _null_ _null_ _null_ bitge _null_ _null_ _null_ ));
DATA(insert OID = 1593 (  bitgt				PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1560 1560" _null_ _null_ _null_ _null_ _null_ bitgt _null_ _null_ _null_ ));
DATA(insert OID = 1594 (  bitle				PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1560 1560" _null_ _null_ _null_ _null_ _null_ bitle _null_ _null_ _null_ ));
DATA(insert OID = 1595 (  bitlt				PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1560 1560" _null_ _null_ _null_ _null_ _null_ bitlt _null_ _null_ _null_ ));
DATA(insert OID = 1596 (  bitcmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1560 1560" _null_ _null_ _null_ _null_ _null_ bitcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 1598 (  random			PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 701 "" _null_ _null_ _null_ _null_ _null_ drandom _null_ _null_ _null_ ));
DESCR("random value");
DATA(insert OID = 1599 (  setseed			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "701" _null_ _null_ _null_ _null_ _null_ setseed _null_ _null_ _null_ ));
DESCR("set random seed");

/* OIDS 1600 - 1699 */

DATA(insert OID = 1600 (  asin				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dasin _null_ _null_ _null_ ));
DESCR("arcsine");
DATA(insert OID = 1601 (  acos				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dacos _null_ _null_ _null_ ));
DESCR("arccosine");
DATA(insert OID = 1602 (  atan				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ datan _null_ _null_ _null_ ));
DESCR("arctangent");
DATA(insert OID = 1603 (  atan2				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_ datan2 _null_ _null_ _null_ ));
DESCR("arctangent, two arguments");
DATA(insert OID = 1604 (  sin				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dsin _null_ _null_ _null_ ));
DESCR("sine");
DATA(insert OID = 1605 (  cos				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dcos _null_ _null_ _null_ ));
DESCR("cosine");
DATA(insert OID = 1606 (  tan				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dtan _null_ _null_ _null_ ));
DESCR("tangent");
DATA(insert OID = 1607 (  cot				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ dcot _null_ _null_ _null_ ));
DESCR("cotangent");
DATA(insert OID = 1608 (  degrees			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ degrees _null_ _null_ _null_ ));
DESCR("radians to degrees");
DATA(insert OID = 1609 (  radians			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ radians _null_ _null_ _null_ ));
DESCR("degrees to radians");
DATA(insert OID = 1610 (  pi				PGNSP PGUID 12 1 0 0 0 f f f f t f i 0 0 701 "" _null_ _null_ _null_ _null_ _null_ dpi _null_ _null_ _null_ ));
DESCR("PI");

DATA(insert OID = 1618 (  interval_mul		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1186 701" _null_ _null_ _null_ _null_ _null_ interval_mul _null_ _null_ _null_ ));

DATA(insert OID = 1620 (  ascii				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "25" _null_ _null_ _null_ _null_ _null_ ascii _null_ _null_ _null_ ));
DESCR("convert first char to int4");
DATA(insert OID = 1621 (  chr				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "23" _null_ _null_ _null_ _null_ _null_ chr _null_ _null_ _null_ ));
DESCR("convert int4 to char");
DATA(insert OID = 1622 (  repeat			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 23" _null_ _null_ _null_ _null_ _null_ repeat _null_ _null_ _null_ ));
DESCR("replicate string n times");

DATA(insert OID = 1623 (  similar_escape	PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ similar_escape _null_ _null_ _null_ ));
DESCR("convert SQL99 regexp pattern to POSIX style");

DATA(insert OID = 1624 (  mul_d_interval	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "701 1186" _null_ _null_ _null_ _null_ _null_ mul_d_interval _null_ _null_ _null_ ));

DATA(insert OID = 1631 (  bpcharlike	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 25" _null_ _null_ _null_ _null_ _null_ textlike _null_ _null_ _null_ ));
DATA(insert OID = 1632 (  bpcharnlike	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 25" _null_ _null_ _null_ _null_ _null_ textnlike _null_ _null_ _null_ ));

DATA(insert OID = 1633 (  texticlike		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ texticlike _null_ _null_ _null_ ));
DATA(insert OID = 1634 (  texticnlike		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ texticnlike _null_ _null_ _null_ ));
DATA(insert OID = 1635 (  nameiclike		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ nameiclike _null_ _null_ _null_ ));
DATA(insert OID = 1636 (  nameicnlike		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ nameicnlike _null_ _null_ _null_ ));
DATA(insert OID = 1637 (  like_escape		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ like_escape _null_ _null_ _null_ ));
DESCR("convert LIKE pattern to use backslash escapes");

DATA(insert OID = 1656 (  bpcharicregexeq	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 25" _null_ _null_ _null_ _null_ _null_ texticregexeq _null_ _null_ _null_ ));
DATA(insert OID = 1657 (  bpcharicregexne	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 25" _null_ _null_ _null_ _null_ _null_ texticregexne _null_ _null_ _null_ ));
DATA(insert OID = 1658 (  bpcharregexeq    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 25" _null_ _null_ _null_ _null_ _null_ textregexeq _null_ _null_ _null_ ));
DATA(insert OID = 1659 (  bpcharregexne    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 25" _null_ _null_ _null_ _null_ _null_ textregexne _null_ _null_ _null_ ));
DATA(insert OID = 1660 (  bpchariclike		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 25" _null_ _null_ _null_ _null_ _null_ texticlike _null_ _null_ _null_ ));
DATA(insert OID = 1661 (  bpcharicnlike		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 25" _null_ _null_ _null_ _null_ _null_ texticnlike _null_ _null_ _null_ ));

/* Oracle Compatibility Related Functions - By Edmund Mergl <E.Mergl@bawue.de> */
DATA(insert OID =  868 (  strpos	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "25 25" _null_ _null_ _null_ _null_ _null_ textpos _null_ _null_ _null_ ));
DESCR("position of substring");
DATA(insert OID =  870 (  lower		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ lower _null_ _null_ _null_ ));
DESCR("lowercase");
DATA(insert OID =  871 (  upper		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ upper _null_ _null_ _null_ ));
DESCR("uppercase");
DATA(insert OID =  872 (  initcap	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ initcap _null_ _null_ _null_ ));
DESCR("capitalize each word");
DATA(insert OID =  873 (  lpad		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 25 "25 23 25" _null_ _null_ _null_ _null_ _null_	lpad _null_ _null_ _null_ ));
DESCR("left-pad string to length");
DATA(insert OID =  874 (  rpad		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 25 "25 23 25" _null_ _null_ _null_ _null_ _null_	rpad _null_ _null_ _null_ ));
DESCR("right-pad string to length");
DATA(insert OID =  875 (  ltrim		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ ltrim _null_ _null_ _null_ ));
DESCR("trim selected characters from left end of string");
DATA(insert OID =  876 (  rtrim		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ rtrim _null_ _null_ _null_ ));
DESCR("trim selected characters from right end of string");
DATA(insert OID =  877 (  substr	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 25 "25 23 23" _null_ _null_ _null_ _null_ _null_	text_substr _null_ _null_ _null_ ));
DESCR("extract portion of string");
DATA(insert OID =  878 (  translate    PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 25 "25 25 25" _null_ _null_ _null_ _null_ _null_	translate _null_ _null_ _null_ ));
DESCR("map a set of characters appearing in string");
DATA(insert OID =  879 (  lpad		   PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 25 "25 23" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.lpad($1, $2, '' '')" _null_ _null_ _null_ ));
DESCR("left-pad string to length");
DATA(insert OID =  880 (  rpad		   PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 25 "25 23" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.rpad($1, $2, '' '')" _null_ _null_ _null_ ));
DESCR("right-pad string to length");
DATA(insert OID =  881 (  ltrim		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ ltrim1 _null_ _null_ _null_ ));
DESCR("trim spaces from left end of string");
DATA(insert OID =  882 (  rtrim		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ rtrim1 _null_ _null_ _null_ ));
DESCR("trim spaces from right end of string");
DATA(insert OID =  883 (  substr	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 23" _null_ _null_ _null_ _null_ _null_ text_substr_no_len _null_ _null_ _null_ ));
DESCR("extract portion of string");
DATA(insert OID =  884 (  btrim		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ btrim _null_ _null_ _null_ ));
DESCR("trim selected characters from both ends of string");
DATA(insert OID =  885 (  btrim		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ btrim1 _null_ _null_ _null_ ));
DESCR("trim spaces from both ends of string");

DATA(insert OID =  936 (  substring    PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 25 "25 23 23" _null_ _null_ _null_ _null_ _null_	text_substr _null_ _null_ _null_ ));
DESCR("extract portion of string");
DATA(insert OID =  937 (  substring    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 23" _null_ _null_ _null_ _null_ _null_ text_substr_no_len _null_ _null_ _null_ ));
DESCR("extract portion of string");
DATA(insert OID =  2087 ( replace	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 25 "25 25 25" _null_ _null_ _null_ _null_ _null_	replace_text _null_ _null_ _null_ ));
DESCR("replace all occurrences in string of old_substr with new_substr");
DATA(insert OID =  2284 ( regexp_replace	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 25 "25 25 25" _null_ _null_ _null_ _null_ _null_	textregexreplace_noopt _null_ _null_ _null_ ));
DESCR("replace text using regexp");
DATA(insert OID =  2285 ( regexp_replace	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 25 "25 25 25 25" _null_ _null_ _null_ _null_ _null_ textregexreplace _null_ _null_ _null_ ));
DESCR("replace text using regexp");
DATA(insert OID =  2763 ( regexp_matches   PGNSP PGUID 12 1 1 0 0 f f f f t t i 2 0 1009 "25 25" _null_ _null_ _null_ _null_ _null_ regexp_matches_no_flags _null_ _null_ _null_ ));
DESCR("find all match groups for regexp");
DATA(insert OID =  2764 ( regexp_matches   PGNSP PGUID 12 1 10 0 0 f f f f t t i 3 0 1009 "25 25 25" _null_ _null_ _null_ _null_ _null_ regexp_matches _null_ _null_ _null_ ));
DESCR("find all match groups for regexp");
DATA(insert OID =  2088 ( split_part   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 25 "25 25 23" _null_ _null_ _null_ _null_ _null_	split_text _null_ _null_ _null_ ));
DESCR("split string by field_sep and return field_num");
DATA(insert OID =  2765 ( regexp_split_to_table PGNSP PGUID 12 1 1000 0 0 f f f f t t i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_	regexp_split_to_table_no_flags _null_ _null_ _null_ ));
DESCR("split string by pattern");
DATA(insert OID =  2766 ( regexp_split_to_table PGNSP PGUID 12 1 1000 0 0 f f f f t t i 3 0 25 "25 25 25" _null_ _null_ _null_ _null_ _null_	regexp_split_to_table _null_ _null_ _null_ ));
DESCR("split string by pattern");
DATA(insert OID =  2767 ( regexp_split_to_array PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1009 "25 25" _null_ _null_ _null_ _null_ _null_	regexp_split_to_array_no_flags _null_ _null_ _null_ ));
DESCR("split string by pattern");
DATA(insert OID =  2768 ( regexp_split_to_array PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1009 "25 25 25" _null_ _null_ _null_ _null_ _null_ regexp_split_to_array _null_ _null_ _null_ ));
DESCR("split string by pattern");
DATA(insert OID =  2089 ( to_hex	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "23" _null_ _null_ _null_ _null_ _null_ to_hex32 _null_ _null_ _null_ ));
DESCR("convert int4 number to hex");
DATA(insert OID =  2090 ( to_hex	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "20" _null_ _null_ _null_ _null_ _null_ to_hex64 _null_ _null_ _null_ ));
DESCR("convert int8 number to hex");

/* for character set encoding support */

/* return database encoding name */
DATA(insert OID = 1039 (  getdatabaseencoding	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 19 "" _null_ _null_ _null_ _null_ _null_ getdatabaseencoding _null_ _null_ _null_ ));
DESCR("encoding name of current database");

/* return client encoding name i.e. session encoding */
DATA(insert OID = 810 (  pg_client_encoding    PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 19 "" _null_ _null_ _null_ _null_ _null_ pg_client_encoding _null_ _null_ _null_ ));
DESCR("encoding name of current database");

DATA(insert OID = 1713 (  length		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 23 "17 19" _null_ _null_ _null_ _null_ _null_ length_in_encoding _null_ _null_ _null_ ));
DESCR("length of string in specified encoding");

DATA(insert OID = 1714 (  convert_from		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "17 19" _null_ _null_ _null_ _null_ _null_ pg_convert_from _null_ _null_ _null_ ));
DESCR("convert string with specified source encoding name");

DATA(insert OID = 1717 (  convert_to		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 17 "25 19" _null_ _null_ _null_ _null_ _null_ pg_convert_to _null_ _null_ _null_ ));
DESCR("convert string with specified destination encoding name");

DATA(insert OID = 1813 (  convert		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 17 "17 19 19" _null_ _null_ _null_ _null_ _null_ pg_convert _null_ _null_ _null_ ));
DESCR("convert string with specified encoding names");

DATA(insert OID = 1264 (  pg_char_to_encoding	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 23 "19" _null_ _null_ _null_ _null_ _null_ PG_char_to_encoding _null_ _null_ _null_ ));
DESCR("convert encoding name to encoding id");

DATA(insert OID = 1597 (  pg_encoding_to_char	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 19 "23" _null_ _null_ _null_ _null_ _null_ PG_encoding_to_char _null_ _null_ _null_ ));
DESCR("convert encoding id to encoding name");

DATA(insert OID = 2319 (  pg_encoding_max_length   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ pg_encoding_max_length_sql _null_ _null_ _null_ ));
DESCR("maximum octet length of a character in given encoding");

DATA(insert OID = 1638 (  oidgt				   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "26 26" _null_ _null_ _null_ _null_ _null_ oidgt _null_ _null_ _null_ ));
DATA(insert OID = 1639 (  oidge				   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "26 26" _null_ _null_ _null_ _null_ _null_ oidge _null_ _null_ _null_ ));

/* System-view support functions */
DATA(insert OID = 1573 (  pg_get_ruledef	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ pg_get_ruledef _null_ _null_ _null_ ));
DESCR("source text of a rule");
DATA(insert OID = 1640 (  pg_get_viewdef	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ pg_get_viewdef_name _null_ _null_ _null_ ));
DESCR("select statement of a view");
DATA(insert OID = 1641 (  pg_get_viewdef	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ pg_get_viewdef _null_ _null_ _null_ ));
DESCR("select statement of a view");
DATA(insert OID = 1642 (  pg_get_userbyid	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 19 "26" _null_ _null_ _null_ _null_ _null_ pg_get_userbyid _null_ _null_ _null_ ));
DESCR("role name by OID (with fallback)");
DATA(insert OID = 1643 (  pg_get_indexdef	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ pg_get_indexdef _null_ _null_ _null_ ));
DESCR("index description");
DATA(insert OID = 1662 (  pg_get_triggerdef    PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ pg_get_triggerdef _null_ _null_ _null_ ));
DESCR("trigger description");
DATA(insert OID = 1387 (  pg_get_constraintdef PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ pg_get_constraintdef _null_ _null_ _null_ ));
DESCR("constraint description");
DATA(insert OID = 1716 (  pg_get_expr		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "194 26" _null_ _null_ _null_ _null_ _null_ pg_get_expr _null_ _null_ _null_ ));
DESCR("deparse an encoded expression");
DATA(insert OID = 1665 (  pg_get_serial_sequence	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ pg_get_serial_sequence _null_ _null_ _null_ ));
DESCR("name of sequence for a serial column");
DATA(insert OID = 2098 (  pg_get_functiondef	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ pg_get_functiondef _null_ _null_ _null_ ));
DESCR("definition of a function");
DATA(insert OID = 2162 (  pg_get_function_arguments    PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ pg_get_function_arguments _null_ _null_ _null_ ));
DESCR("argument list of a function");
DATA(insert OID = 2232 (  pg_get_function_identity_arguments	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ pg_get_function_identity_arguments _null_ _null_ _null_ ));
DESCR("identity argument list of a function");
DATA(insert OID = 2165 (  pg_get_function_result	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ pg_get_function_result _null_ _null_ _null_ ));
DESCR("result type of a function");
DATA(insert OID = 3808 (  pg_get_function_arg_default	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "26 23" _null_ _null_ _null_ _null_ _null_ pg_get_function_arg_default _null_ _null_ _null_ ));
DESCR("function argument default");

DATA(insert OID = 1686 (  pg_get_keywords		PGNSP PGUID 12 10 400 0 0 f f f f t t s 0 0 2249 "" "{25,18,25}" "{o,o,o}" "{word,catcode,catdesc}" _null_ _null_ pg_get_keywords _null_ _null_ _null_ ));
DESCR("list of SQL keywords");

DATA(insert OID = 2289 (  pg_options_to_table		PGNSP PGUID 12 1 3 0 0 f f f f t t s 1 0 2249 "1009" "{1009,25,25}" "{i,o,o}" "{options_array,option_name,option_value}" _null_ _null_ pg_options_to_table _null_ _null_ _null_ ));
DESCR("convert generic options array to name/value table");

DATA(insert OID = 1619 (  pg_typeof				PGNSP PGUID 12 1 0 0 0 f f f f f f s 1 0 2206 "2276" _null_ _null_ _null_ _null_  _null_ pg_typeof _null_ _null_ _null_ ));
DESCR("type of the argument");
DATA(insert OID = 3162 (  pg_collation_for		PGNSP PGUID 12 1 0 0 0 f f f f f f s 1 0   25 "2276" _null_ _null_ _null_ _null_  _null_ pg_collation_for _null_ _null_ _null_ ));
DESCR("collation of the argument; implementation of the COLLATION FOR expression");

DATA(insert OID = 3842 (  pg_relation_is_updatable	PGNSP PGUID 12 10 0 0 0 f f f f t f s 2 0 23 "2205 16" _null_ _null_ _null_ _null_ _null_ pg_relation_is_updatable _null_ _null_ _null_ ));
DESCR("is a relation insertable/updatable/deletable");
DATA(insert OID = 3843 (  pg_column_is_updatable	PGNSP PGUID 12 10 0 0 0 f f f f t f s 3 0 16 "2205 21 16" _null_ _null_ _null_ _null_ _null_ pg_column_is_updatable _null_ _null_ _null_ ));
DESCR("is a column updatable");

/* Deferrable unique constraint trigger */
DATA(insert OID = 1250 (  unique_key_recheck	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ unique_key_recheck _null_ _null_ _null_ ));
DESCR("deferred UNIQUE constraint check");

/* Generic referential integrity constraint triggers */
DATA(insert OID = 1644 (  RI_FKey_check_ins		PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_check_ins _null_ _null_ _null_ ));
DESCR("referential integrity FOREIGN KEY ... REFERENCES");
DATA(insert OID = 1645 (  RI_FKey_check_upd		PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_check_upd _null_ _null_ _null_ ));
DESCR("referential integrity FOREIGN KEY ... REFERENCES");
DATA(insert OID = 1646 (  RI_FKey_cascade_del	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_cascade_del _null_ _null_ _null_ ));
DESCR("referential integrity ON DELETE CASCADE");
DATA(insert OID = 1647 (  RI_FKey_cascade_upd	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_cascade_upd _null_ _null_ _null_ ));
DESCR("referential integrity ON UPDATE CASCADE");
DATA(insert OID = 1648 (  RI_FKey_restrict_del	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_restrict_del _null_ _null_ _null_ ));
DESCR("referential integrity ON DELETE RESTRICT");
DATA(insert OID = 1649 (  RI_FKey_restrict_upd	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_restrict_upd _null_ _null_ _null_ ));
DESCR("referential integrity ON UPDATE RESTRICT");
DATA(insert OID = 1650 (  RI_FKey_setnull_del	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_setnull_del _null_ _null_ _null_ ));
DESCR("referential integrity ON DELETE SET NULL");
DATA(insert OID = 1651 (  RI_FKey_setnull_upd	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_setnull_upd _null_ _null_ _null_ ));
DESCR("referential integrity ON UPDATE SET NULL");
DATA(insert OID = 1652 (  RI_FKey_setdefault_del PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_setdefault_del _null_ _null_ _null_ ));
DESCR("referential integrity ON DELETE SET DEFAULT");
DATA(insert OID = 1653 (  RI_FKey_setdefault_upd PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_setdefault_upd _null_ _null_ _null_ ));
DESCR("referential integrity ON UPDATE SET DEFAULT");
DATA(insert OID = 1654 (  RI_FKey_noaction_del PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_noaction_del _null_ _null_ _null_ ));
DESCR("referential integrity ON DELETE NO ACTION");
DATA(insert OID = 1655 (  RI_FKey_noaction_upd PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ RI_FKey_noaction_upd _null_ _null_ _null_ ));
DESCR("referential integrity ON UPDATE NO ACTION");

DATA(insert OID = 1666 (  varbiteq			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1562 1562" _null_ _null_ _null_ _null_ _null_ biteq _null_ _null_ _null_ ));
DATA(insert OID = 1667 (  varbitne			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1562 1562" _null_ _null_ _null_ _null_ _null_ bitne _null_ _null_ _null_ ));
DATA(insert OID = 1668 (  varbitge			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1562 1562" _null_ _null_ _null_ _null_ _null_ bitge _null_ _null_ _null_ ));
DATA(insert OID = 1669 (  varbitgt			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1562 1562" _null_ _null_ _null_ _null_ _null_ bitgt _null_ _null_ _null_ ));
DATA(insert OID = 1670 (  varbitle			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1562 1562" _null_ _null_ _null_ _null_ _null_ bitle _null_ _null_ _null_ ));
DATA(insert OID = 1671 (  varbitlt			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1562 1562" _null_ _null_ _null_ _null_ _null_ bitlt _null_ _null_ _null_ ));
DATA(insert OID = 1672 (  varbitcmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1562 1562" _null_ _null_ _null_ _null_ _null_ bitcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 1673 (  bitand			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1560 "1560 1560" _null_ _null_ _null_ _null_ _null_	bit_and _null_ _null_ _null_ ));
DATA(insert OID = 1674 (  bitor				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1560 "1560 1560" _null_ _null_ _null_ _null_ _null_	bit_or _null_ _null_ _null_ ));
DATA(insert OID = 1675 (  bitxor			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1560 "1560 1560" _null_ _null_ _null_ _null_ _null_	bitxor _null_ _null_ _null_ ));
DATA(insert OID = 1676 (  bitnot			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1560 "1560" _null_ _null_ _null_ _null_ _null_ bitnot _null_ _null_ _null_ ));
DATA(insert OID = 1677 (  bitshiftleft		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1560 "1560 23" _null_ _null_ _null_ _null_ _null_ bitshiftleft _null_ _null_ _null_ ));
DATA(insert OID = 1678 (  bitshiftright		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1560 "1560 23" _null_ _null_ _null_ _null_ _null_ bitshiftright _null_ _null_ _null_ ));
DATA(insert OID = 1679 (  bitcat			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1562 "1562 1562" _null_ _null_ _null_ _null_ _null_	bitcat _null_ _null_ _null_ ));
DATA(insert OID = 1680 (  substring			PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1560 "1560 23 23" _null_ _null_ _null_ _null_ _null_ bitsubstr _null_ _null_ _null_ ));
DESCR("extract portion of bitstring");
DATA(insert OID = 1681 (  length			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1560" _null_ _null_ _null_ _null_ _null_ bitlength _null_ _null_ _null_ ));
DESCR("bitstring length");
DATA(insert OID = 1682 (  octet_length		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1560" _null_ _null_ _null_ _null_ _null_ bitoctetlength _null_ _null_ _null_ ));
DESCR("octet length");
DATA(insert OID = 1683 (  bit				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1560 "23 23" _null_ _null_ _null_ _null_ _null_	bitfromint4 _null_ _null_ _null_ ));
DESCR("convert int4 to bitstring");
DATA(insert OID = 1684 (  int4				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1560" _null_ _null_ _null_ _null_ _null_ bittoint4 _null_ _null_ _null_ ));
DESCR("convert bitstring to int4");

DATA(insert OID = 1685 (  bit			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1560 "1560 23 16" _null_ _null_ _null_ _null_ _null_ bit _null_ _null_ _null_ ));
DESCR("adjust bit() to typmod length");
DATA(insert OID = 3158 ( varbit_transform  PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ varbit_transform _null_ _null_ _null_ ));
DESCR("transform a varbit length coercion");
DATA(insert OID = 1687 (  varbit		   PGNSP PGUID 12 1 0 0 varbit_transform f f f f t f i 3 0 1562 "1562 23 16" _null_ _null_ _null_ _null_ _null_ varbit _null_ _null_ _null_ ));
DESCR("adjust varbit() to typmod length");

DATA(insert OID = 1698 (  position		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1560 1560" _null_ _null_ _null_ _null_ _null_ bitposition _null_ _null_ _null_ ));
DESCR("position of sub-bitstring");
DATA(insert OID = 1699 (  substring		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1560 "1560 23" _null_ _null_ _null_ _null_ _null_ bitsubstr_no_len _null_ _null_ _null_ ));
DESCR("extract portion of bitstring");

DATA(insert OID = 3030 (  overlay		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 1560 "1560 1560 23 23" _null_ _null_ _null_ _null_ _null_	bitoverlay _null_ _null_ _null_ ));
DESCR("substitute portion of bitstring");
DATA(insert OID = 3031 (  overlay		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1560 "1560 1560 23" _null_ _null_ _null_ _null_ _null_ bitoverlay_no_len _null_ _null_ _null_ ));
DESCR("substitute portion of bitstring");
DATA(insert OID = 3032 (  get_bit		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1560 23" _null_ _null_ _null_ _null_ _null_ bitgetbit _null_ _null_ _null_ ));
DESCR("get bit");
DATA(insert OID = 3033 (  set_bit		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1560 "1560 23 23" _null_ _null_ _null_ _null_ _null_ bitsetbit _null_ _null_ _null_ ));
DESCR("set bit");

/* for mac type support */
DATA(insert OID = 436 (  macaddr_in			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 829 "2275" _null_ _null_ _null_ _null_ _null_ macaddr_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 437 (  macaddr_out		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "829" _null_ _null_ _null_ _null_ _null_ macaddr_out _null_ _null_ _null_ ));
DESCR("I/O");

DATA(insert OID = 753 (  trunc				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 829 "829" _null_ _null_ _null_ _null_ _null_ macaddr_trunc _null_ _null_ _null_ ));
DESCR("MAC manufacturer fields");

DATA(insert OID = 830 (  macaddr_eq			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "829 829" _null_ _null_ _null_ _null_ _null_	macaddr_eq _null_ _null_ _null_ ));
DATA(insert OID = 831 (  macaddr_lt			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "829 829" _null_ _null_ _null_ _null_ _null_	macaddr_lt _null_ _null_ _null_ ));
DATA(insert OID = 832 (  macaddr_le			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "829 829" _null_ _null_ _null_ _null_ _null_	macaddr_le _null_ _null_ _null_ ));
DATA(insert OID = 833 (  macaddr_gt			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "829 829" _null_ _null_ _null_ _null_ _null_	macaddr_gt _null_ _null_ _null_ ));
DATA(insert OID = 834 (  macaddr_ge			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "829 829" _null_ _null_ _null_ _null_ _null_	macaddr_ge _null_ _null_ _null_ ));
DATA(insert OID = 835 (  macaddr_ne			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "829 829" _null_ _null_ _null_ _null_ _null_	macaddr_ne _null_ _null_ _null_ ));
DATA(insert OID = 836 (  macaddr_cmp		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "829 829" _null_ _null_ _null_ _null_ _null_	macaddr_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3144 (  macaddr_not		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 829 "829" _null_ _null_ _null_ _null_ _null_	macaddr_not _null_ _null_ _null_ ));
DATA(insert OID = 3145 (  macaddr_and		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 829 "829 829" _null_ _null_ _null_ _null_ _null_	macaddr_and _null_ _null_ _null_ ));
DATA(insert OID = 3146 (  macaddr_or		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 829 "829 829" _null_ _null_ _null_ _null_ _null_	macaddr_or _null_ _null_ _null_ ));

/* for inet type support */
DATA(insert OID = 910 (  inet_in			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 869 "2275" _null_ _null_ _null_ _null_ _null_ inet_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 911 (  inet_out			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "869" _null_ _null_ _null_ _null_ _null_ inet_out _null_ _null_ _null_ ));
DESCR("I/O");

/* for cidr type support */
DATA(insert OID = 1267 (  cidr_in			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 650 "2275" _null_ _null_ _null_ _null_ _null_ cidr_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1427 (  cidr_out			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "650" _null_ _null_ _null_ _null_ _null_ cidr_out _null_ _null_ _null_ ));
DESCR("I/O");

/* these are used for both inet and cidr */
DATA(insert OID = 920 (  network_eq			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_eq _null_ _null_ _null_ ));
DATA(insert OID = 921 (  network_lt			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_lt _null_ _null_ _null_ ));
DATA(insert OID = 922 (  network_le			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_le _null_ _null_ _null_ ));
DATA(insert OID = 923 (  network_gt			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_gt _null_ _null_ _null_ ));
DATA(insert OID = 924 (  network_ge			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_ge _null_ _null_ _null_ ));
DATA(insert OID = 925 (  network_ne			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_ne _null_ _null_ _null_ ));
DATA(insert OID = 3562 (  network_larger	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 869 "869 869" _null_ _null_ _null_ _null_ _null_	network_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 3563 (  network_smaller	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 869 "869 869" _null_ _null_ _null_ _null_ _null_	network_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 926 (  network_cmp		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "869 869" _null_ _null_ _null_ _null_ _null_	network_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 927 (  network_sub		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_sub _null_ _null_ _null_ ));
DATA(insert OID = 928 (  network_subeq		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_subeq _null_ _null_ _null_ ));
DATA(insert OID = 929 (  network_sup		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_sup _null_ _null_ _null_ ));
DATA(insert OID = 930 (  network_supeq		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_supeq _null_ _null_ _null_ ));
DATA(insert OID = 3551 (  network_overlap	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_	network_overlap _null_ _null_ _null_ ));

/* inet/cidr functions */
DATA(insert OID = 598 (  abbrev				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "869" _null_ _null_ _null_ _null_ _null_	inet_abbrev _null_ _null_ _null_ ));
DESCR("abbreviated display of inet value");
DATA(insert OID = 599 (  abbrev				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "650" _null_ _null_ _null_ _null_ _null_	cidr_abbrev _null_ _null_ _null_ ));
DESCR("abbreviated display of cidr value");
DATA(insert OID = 605 (  set_masklen		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 869 "869 23" _null_ _null_ _null_ _null_ _null_	inet_set_masklen _null_ _null_ _null_ ));
DESCR("change netmask of inet");
DATA(insert OID = 635 (  set_masklen		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 650 "650 23" _null_ _null_ _null_ _null_ _null_	cidr_set_masklen _null_ _null_ _null_ ));
DESCR("change netmask of cidr");
DATA(insert OID = 711 (  family				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "869" _null_ _null_ _null_ _null_ _null_	network_family _null_ _null_ _null_ ));
DESCR("address family (4 for IPv4, 6 for IPv6)");
DATA(insert OID = 683 (  network			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 650 "869" _null_ _null_ _null_ _null_ _null_ network_network _null_ _null_ _null_ ));
DESCR("network part of address");
DATA(insert OID = 696 (  netmask			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 869 "869" _null_ _null_ _null_ _null_ _null_ network_netmask _null_ _null_ _null_ ));
DESCR("netmask of address");
DATA(insert OID = 697 (  masklen			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "869" _null_ _null_ _null_ _null_ _null_	network_masklen _null_ _null_ _null_ ));
DESCR("netmask length");
DATA(insert OID = 698 (  broadcast			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 869 "869" _null_ _null_ _null_ _null_ _null_ network_broadcast _null_ _null_ _null_ ));
DESCR("broadcast address of network");
DATA(insert OID = 699 (  host				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "869" _null_ _null_ _null_ _null_ _null_	network_host _null_ _null_ _null_ ));
DESCR("show address octets only");
DATA(insert OID = 730 (  text				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "869" _null_ _null_ _null_ _null_ _null_	network_show _null_ _null_ _null_ ));
DESCR("show all parts of inet/cidr value");
DATA(insert OID = 1362 (  hostmask			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 869 "869" _null_ _null_ _null_ _null_ _null_ network_hostmask _null_ _null_ _null_ ));
DESCR("hostmask of address");
DATA(insert OID = 1715 (  cidr				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 650 "869" _null_ _null_ _null_ _null_ _null_	inet_to_cidr _null_ _null_ _null_ ));
DESCR("convert inet to cidr");

DATA(insert OID = 2196 (  inet_client_addr		PGNSP PGUID 12 1 0 0 0 f f f f f f s 0 0 869 "" _null_ _null_ _null_ _null_ _null_ inet_client_addr _null_ _null_ _null_ ));
DESCR("inet address of the client");
DATA(insert OID = 2197 (  inet_client_port		PGNSP PGUID 12 1 0 0 0 f f f f f f s 0 0 23 "" _null_ _null_ _null_ _null_ _null_	inet_client_port _null_ _null_ _null_ ));
DESCR("client's port number for this connection");
DATA(insert OID = 2198 (  inet_server_addr		PGNSP PGUID 12 1 0 0 0 f f f f f f s 0 0 869 "" _null_ _null_ _null_ _null_ _null_ inet_server_addr _null_ _null_ _null_ ));
DESCR("inet address of the server");
DATA(insert OID = 2199 (  inet_server_port		PGNSP PGUID 12 1 0 0 0 f f f f f f s 0 0 23 "" _null_ _null_ _null_ _null_ _null_	inet_server_port _null_ _null_ _null_ ));
DESCR("server's port number for this connection");

DATA(insert OID = 2627 (  inetnot			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 869 "869" _null_ _null_ _null_ _null_ _null_	inetnot _null_ _null_ _null_ ));
DATA(insert OID = 2628 (  inetand			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 869 "869 869" _null_ _null_ _null_ _null_ _null_	inetand _null_ _null_ _null_ ));
DATA(insert OID = 2629 (  inetor			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 869 "869 869" _null_ _null_ _null_ _null_ _null_	inetor _null_ _null_ _null_ ));
DATA(insert OID = 2630 (  inetpl			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 869 "869 20" _null_ _null_ _null_ _null_ _null_	inetpl _null_ _null_ _null_ ));
DATA(insert OID = 2631 (  int8pl_inet		PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 869 "20 869" _null_ _null_ _null_ _null_ _null_ "select $2 + $1" _null_ _null_ _null_ ));
DATA(insert OID = 2632 (  inetmi_int8		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 869 "869 20" _null_ _null_ _null_ _null_ _null_	inetmi_int8 _null_ _null_ _null_ ));
DATA(insert OID = 2633 (  inetmi			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "869 869" _null_ _null_ _null_ _null_ _null_	inetmi _null_ _null_ _null_ ));
DATA(insert OID = 4071 (  inet_same_family	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "869 869" _null_ _null_ _null_ _null_ _null_ inet_same_family _null_ _null_ _null_ ));
DESCR("are the addresses from the same family?");
DATA(insert OID = 4063 (  inet_merge		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 650 "869 869" _null_ _null_ _null_ _null_ _null_ inet_merge _null_ _null_ _null_ ));
DESCR("the smallest network which includes both of the given networks");

/* GiST support for inet and cidr */
DATA(insert OID = 3553 (  inet_gist_consistent	PGNSP PGUID 12 1 0 0 0 f f f f t f i 5 0 16 "2281 869 23 26 2281" _null_ _null_ _null_ _null_ _null_ inet_gist_consistent _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3554 (  inet_gist_union		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ inet_gist_union _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3555 (  inet_gist_compress	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ inet_gist_compress _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3556 (  inet_gist_decompress	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ inet_gist_decompress _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3573 (  inet_gist_fetch		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ inet_gist_fetch _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3557 (  inet_gist_penalty		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ inet_gist_penalty _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3558 (  inet_gist_picksplit	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ inet_gist_picksplit _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3559 (  inet_gist_same		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "869 869 2281" _null_ _null_ _null_ _null_ _null_ inet_gist_same _null_ _null_ _null_ ));
DESCR("GiST support");

/* Selectivity estimation for inet and cidr */
DATA(insert OID = 3560 (  networksel		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	networksel _null_ _null_ _null_ ));
DESCR("restriction selectivity for network operators");
DATA(insert OID = 3561 (  networkjoinsel	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_ _null_	networkjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity for network operators");

DATA(insert OID = 1690 ( time_mi_time		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1083 1083" _null_ _null_ _null_ _null_ _null_	time_mi_time _null_ _null_ _null_ ));

DATA(insert OID = 1691 (  boolle			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "16 16" _null_ _null_ _null_ _null_ _null_ boolle _null_ _null_ _null_ ));
DATA(insert OID = 1692 (  boolge			PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "16 16" _null_ _null_ _null_ _null_ _null_ boolge _null_ _null_ _null_ ));
DATA(insert OID = 1693 (  btboolcmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "16 16" _null_ _null_ _null_ _null_ _null_ btboolcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 1688 (  time_hash			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1083" _null_ _null_ _null_ _null_ _null_ time_hash _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 1696 (  timetz_hash		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1266" _null_ _null_ _null_ _null_ _null_ timetz_hash _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 1697 (  interval_hash		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1186" _null_ _null_ _null_ _null_ _null_ interval_hash _null_ _null_ _null_ ));
DESCR("hash");


/* OID's 1700 - 1799 NUMERIC data type */
DATA(insert OID = 1701 ( numeric_in				PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1700 "2275 26 23" _null_ _null_ _null_ _null_ _null_	numeric_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1702 ( numeric_out			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "1700" _null_ _null_ _null_ _null_ _null_ numeric_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2917 (  numerictypmodin		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1263" _null_ _null_ _null_ _null_ _null_	numerictypmodin _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 2918 (  numerictypmodout		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "23" _null_ _null_ _null_ _null_ _null_	numerictypmodout _null_ _null_ _null_ ));
DESCR("I/O typmod");
DATA(insert OID = 3157 ( numeric_transform		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ numeric_transform _null_ _null_ _null_ ));
DESCR("transform a numeric length coercion");
DATA(insert OID = 1703 ( numeric				PGNSP PGUID 12 1 0 0 numeric_transform f f f f t f i 2 0 1700 "1700 23" _null_ _null_ _null_ _null_ _null_ numeric _null_ _null_ _null_ ));
DESCR("adjust numeric to typmod precision/scale");
DATA(insert OID = 1704 ( numeric_abs			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_abs _null_ _null_ _null_ ));
DATA(insert OID = 1705 ( abs					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_abs _null_ _null_ _null_ ));
DESCR("absolute value");
DATA(insert OID = 1706 ( sign					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_sign _null_ _null_ _null_ ));
DESCR("sign of value");
DATA(insert OID = 1707 ( round					PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 23" _null_ _null_ _null_ _null_ _null_ numeric_round _null_ _null_ _null_ ));
DESCR("value rounded to 'scale'");
DATA(insert OID = 1708 ( round					PGNSP PGUID 14 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.round($1,0)" _null_ _null_ _null_ ));
DESCR("value rounded to 'scale' of zero");
DATA(insert OID = 1709 ( trunc					PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 23" _null_ _null_ _null_ _null_ _null_ numeric_trunc _null_ _null_ _null_ ));
DESCR("value truncated to 'scale'");
DATA(insert OID = 1710 ( trunc					PGNSP PGUID 14 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.trunc($1,0)" _null_ _null_ _null_ ));
DESCR("value truncated to 'scale' of zero");
DATA(insert OID = 1711 ( ceil					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_ceil _null_ _null_ _null_ ));
DESCR("smallest integer >= value");
DATA(insert OID = 2167 ( ceiling				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_ceil _null_ _null_ _null_ ));
DESCR("smallest integer >= value");
DATA(insert OID = 1712 ( floor					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_floor _null_ _null_ _null_ ));
DESCR("largest integer <= value");
DATA(insert OID = 1718 ( numeric_eq				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1700 1700" _null_ _null_ _null_ _null_ _null_ numeric_eq _null_ _null_ _null_ ));
DATA(insert OID = 1719 ( numeric_ne				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1700 1700" _null_ _null_ _null_ _null_ _null_ numeric_ne _null_ _null_ _null_ ));
DATA(insert OID = 1720 ( numeric_gt				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1700 1700" _null_ _null_ _null_ _null_ _null_ numeric_gt _null_ _null_ _null_ ));
DATA(insert OID = 1721 ( numeric_ge				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1700 1700" _null_ _null_ _null_ _null_ _null_ numeric_ge _null_ _null_ _null_ ));
DATA(insert OID = 1722 ( numeric_lt				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1700 1700" _null_ _null_ _null_ _null_ _null_ numeric_lt _null_ _null_ _null_ ));
DATA(insert OID = 1723 ( numeric_le				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1700 1700" _null_ _null_ _null_ _null_ _null_ numeric_le _null_ _null_ _null_ ));
DATA(insert OID = 1724 ( numeric_add			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_add _null_ _null_ _null_ ));
DATA(insert OID = 1725 ( numeric_sub			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_sub _null_ _null_ _null_ ));
DATA(insert OID = 1726 ( numeric_mul			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_mul _null_ _null_ _null_ ));
DATA(insert OID = 1727 ( numeric_div			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_div _null_ _null_ _null_ ));
DATA(insert OID = 1728 ( mod					PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_mod _null_ _null_ _null_ ));
DESCR("modulus");
DATA(insert OID = 1729 ( numeric_mod			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_mod _null_ _null_ _null_ ));
DATA(insert OID = 1730 ( sqrt					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_sqrt _null_ _null_ _null_ ));
DESCR("square root");
DATA(insert OID = 1731 ( numeric_sqrt			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_sqrt _null_ _null_ _null_ ));
DESCR("square root");
DATA(insert OID = 1732 ( exp					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_exp _null_ _null_ _null_ ));
DESCR("natural exponential (e^x)");
DATA(insert OID = 1733 ( numeric_exp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_exp _null_ _null_ _null_ ));
DESCR("natural exponential (e^x)");
DATA(insert OID = 1734 ( ln						PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_ln _null_ _null_ _null_ ));
DESCR("natural logarithm");
DATA(insert OID = 1735 ( numeric_ln				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_ln _null_ _null_ _null_ ));
DESCR("natural logarithm");
DATA(insert OID = 1736 ( log					PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_log _null_ _null_ _null_ ));
DESCR("logarithm base m of n");
DATA(insert OID = 1737 ( numeric_log			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_log _null_ _null_ _null_ ));
DESCR("logarithm base m of n");
DATA(insert OID = 1738 ( pow					PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_power _null_ _null_ _null_ ));
DESCR("exponentiation");
DATA(insert OID = 2169 ( power					PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_power _null_ _null_ _null_ ));
DESCR("exponentiation");
DATA(insert OID = 1739 ( numeric_power			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_power _null_ _null_ _null_ ));
DATA(insert OID = 1740 ( numeric				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "23" _null_ _null_ _null_ _null_ _null_ int4_numeric _null_ _null_ _null_ ));
DESCR("convert int4 to numeric");
DATA(insert OID = 1741 ( log					PGNSP PGUID 14 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.log(10, $1)" _null_ _null_ _null_ ));
DESCR("base 10 logarithm");
DATA(insert OID = 1742 ( numeric				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "700" _null_ _null_ _null_ _null_ _null_ float4_numeric _null_ _null_ _null_ ));
DESCR("convert float4 to numeric");
DATA(insert OID = 1743 ( numeric				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "701" _null_ _null_ _null_ _null_ _null_ float8_numeric _null_ _null_ _null_ ));
DESCR("convert float8 to numeric");
DATA(insert OID = 1744 ( int4					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1700" _null_ _null_ _null_ _null_ _null_ numeric_int4 _null_ _null_ _null_ ));
DESCR("convert numeric to int4");
DATA(insert OID = 1745 ( float4					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_float4 _null_ _null_ _null_ ));
DESCR("convert numeric to float4");
DATA(insert OID = 1746 ( float8					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1700" _null_ _null_ _null_ _null_ _null_ numeric_float8 _null_ _null_ _null_ ));
DESCR("convert numeric to float8");
DATA(insert OID = 1973 ( div					PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_div_trunc _null_ _null_ _null_ ));
DESCR("trunc(x/y)");
DATA(insert OID = 1980 ( numeric_div_trunc		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_div_trunc _null_ _null_ _null_ ));
DESCR("trunc(x/y)");
DATA(insert OID = 2170 ( width_bucket			PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 23 "1700 1700 1700 23" _null_ _null_ _null_ _null_ _null_ width_bucket_numeric _null_ _null_ _null_ ));
DESCR("bucket number of operand in equal-width histogram");

DATA(insert OID = 1747 ( time_pl_interval		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1083 "1083 1186" _null_ _null_ _null_ _null_ _null_	time_pl_interval _null_ _null_ _null_ ));
DATA(insert OID = 1748 ( time_mi_interval		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1083 "1083 1186" _null_ _null_ _null_ _null_ _null_	time_mi_interval _null_ _null_ _null_ ));
DATA(insert OID = 1749 ( timetz_pl_interval		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1266 "1266 1186" _null_ _null_ _null_ _null_ _null_	timetz_pl_interval _null_ _null_ _null_ ));
DATA(insert OID = 1750 ( timetz_mi_interval		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1266 "1266 1186" _null_ _null_ _null_ _null_ _null_	timetz_mi_interval _null_ _null_ _null_ ));

DATA(insert OID = 1764 ( numeric_inc			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_inc _null_ _null_ _null_ ));
DESCR("increment by one");
DATA(insert OID = 1766 ( numeric_smaller		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 1767 ( numeric_larger			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_	numeric_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 1769 ( numeric_cmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1700 1700" _null_ _null_ _null_ _null_ _null_ numeric_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3283 ( numeric_sortsupport	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ numeric_sortsupport _null_ _null_ _null_ ));
DESCR("sort support");
DATA(insert OID = 1771 ( numeric_uminus			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_uminus _null_ _null_ _null_ ));
DATA(insert OID = 1779 ( int8					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "1700" _null_ _null_ _null_ _null_ _null_ numeric_int8 _null_ _null_ _null_ ));
DESCR("convert numeric to int8");
DATA(insert OID = 1781 ( numeric				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_ int8_numeric _null_ _null_ _null_ ));
DESCR("convert int8 to numeric");
DATA(insert OID = 1782 ( numeric				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "21" _null_ _null_ _null_ _null_ _null_ int2_numeric _null_ _null_ _null_ ));
DESCR("convert int2 to numeric");
DATA(insert OID = 1783 ( int2					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "1700" _null_ _null_ _null_ _null_ _null_ numeric_int2 _null_ _null_ _null_ ));
DESCR("convert numeric to int2");

/* formatting */
DATA(insert OID = 1770 ( to_char			PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "1184 25" _null_ _null_ _null_ _null_  _null_ timestamptz_to_char _null_ _null_ _null_ ));
DESCR("format timestamp with time zone to text");
DATA(insert OID = 1772 ( to_char			PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "1700 25" _null_ _null_ _null_ _null_  _null_ numeric_to_char _null_ _null_ _null_ ));
DESCR("format numeric to text");
DATA(insert OID = 1773 ( to_char			PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "23 25" _null_ _null_ _null_ _null_ _null_ int4_to_char _null_ _null_ _null_ ));
DESCR("format int4 to text");
DATA(insert OID = 1774 ( to_char			PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "20 25" _null_ _null_ _null_ _null_ _null_ int8_to_char _null_ _null_ _null_ ));
DESCR("format int8 to text");
DATA(insert OID = 1775 ( to_char			PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "700 25" _null_ _null_ _null_ _null_ _null_ float4_to_char _null_ _null_ _null_ ));
DESCR("format float4 to text");
DATA(insert OID = 1776 ( to_char			PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "701 25" _null_ _null_ _null_ _null_ _null_ float8_to_char _null_ _null_ _null_ ));
DESCR("format float8 to text");
DATA(insert OID = 1777 ( to_number			PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 1700 "25 25" _null_ _null_ _null_ _null_  _null_ numeric_to_number _null_ _null_ _null_ ));
DESCR("convert text to numeric");
DATA(insert OID = 1778 ( to_timestamp		PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 1184 "25 25" _null_ _null_ _null_ _null_  _null_ to_timestamp _null_ _null_ _null_ ));
DESCR("convert text to timestamp with time zone");
DATA(insert OID = 1780 ( to_date			PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 1082 "25 25" _null_ _null_ _null_ _null_  _null_ to_date _null_ _null_ _null_ ));
DESCR("convert text to date");
DATA(insert OID = 1768 ( to_char			PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "1186 25" _null_ _null_ _null_ _null_  _null_ interval_to_char _null_ _null_ _null_ ));
DESCR("format interval to text");

DATA(insert OID =  1282 ( quote_ident	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ quote_ident _null_ _null_ _null_ ));
DESCR("quote an identifier for usage in a querystring");
DATA(insert OID =  1283 ( quote_literal    PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ quote_literal _null_ _null_ _null_ ));
DESCR("quote a literal for usage in a querystring");
DATA(insert OID =  1285 ( quote_literal    PGNSP PGUID 14 1 0 0 0 f f f f t f s 1 0 25 "2283" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.quote_literal($1::pg_catalog.text)" _null_ _null_ _null_ ));
DESCR("quote a data value for usage in a querystring");
DATA(insert OID =  1289 ( quote_nullable   PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ quote_nullable _null_ _null_ _null_ ));
DESCR("quote a possibly-null literal for usage in a querystring");
DATA(insert OID =  1290 ( quote_nullable   PGNSP PGUID 14 1 0 0 0 f f f f f f s 1 0 25 "2283" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.quote_nullable($1::pg_catalog.text)" _null_ _null_ _null_ ));
DESCR("quote a possibly-null data value for usage in a querystring");

DATA(insert OID = 1798 (  oidin			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 26 "2275" _null_ _null_ _null_ _null_ _null_ oidin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 1799 (  oidout		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "26" _null_ _null_ _null_ _null_ _null_ oidout _null_ _null_ _null_ ));
DESCR("I/O");

DATA(insert OID = 3058 ( concat		PGNSP PGUID 12 1 0 2276 0 f f f f f f s 1 0 25 "2276" "{2276}" "{v}" _null_ _null_ _null_	text_concat _null_ _null_ _null_ ));
DESCR("concatenate values");
DATA(insert OID = 3059 ( concat_ws	PGNSP PGUID 12 1 0 2276 0 f f f f f f s 2 0 25 "25 2276" "{25,2276}" "{i,v}" _null_ _null_ _null_	text_concat_ws _null_ _null_ _null_ ));
DESCR("concatenate values with separators");
DATA(insert OID = 3060 ( left		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 23" _null_ _null_ _null_ _null_  _null_ text_left _null_ _null_ _null_ ));
DESCR("extract the first n characters");
DATA(insert OID = 3061 ( right		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 23" _null_ _null_ _null_ _null_  _null_ text_right _null_ _null_ _null_ ));
DESCR("extract the last n characters");
DATA(insert OID = 3062 ( reverse	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_  text_reverse	_null_ _null_ _null_ ));
DESCR("reverse text");
DATA(insert OID = 3539 ( format		PGNSP PGUID 12 1 0 2276 0 f f f f f f s 2 0 25 "25 2276" "{25,2276}" "{i,v}" _null_ _null_ _null_	text_format _null_ _null_ _null_ ));
DESCR("format text message");
DATA(insert OID = 3540 ( format		PGNSP PGUID 12 1 0 0 0 f f f f f f s 1 0 25 "25" _null_ _null_ _null_ _null_  _null_ text_format_nv _null_ _null_ _null_ ));
DESCR("format text message");

DATA(insert OID = 1810 (  bit_length	   PGNSP PGUID 14 1 0 0 0 f f f f t f i 1 0 23 "17" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.octet_length($1) * 8" _null_ _null_ _null_ ));
DESCR("length in bits");
DATA(insert OID = 1811 (  bit_length	   PGNSP PGUID 14 1 0 0 0 f f f f t f i 1 0 23 "25" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.octet_length($1) * 8" _null_ _null_ _null_ ));
DESCR("length in bits");
DATA(insert OID = 1812 (  bit_length	   PGNSP PGUID 14 1 0 0 0 f f f f t f i 1 0 23 "1560" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.length($1)" _null_ _null_ _null_ ));
DESCR("length in bits");

/* Selectivity estimators for LIKE and related operators */
DATA(insert OID = 1814 ( iclikesel			PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	iclikesel _null_ _null_ _null_ ));
DESCR("restriction selectivity of ILIKE");
DATA(insert OID = 1815 ( icnlikesel			PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	icnlikesel _null_ _null_ _null_ ));
DESCR("restriction selectivity of NOT ILIKE");
DATA(insert OID = 1816 ( iclikejoinsel		PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_  _null_ iclikejoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of ILIKE");
DATA(insert OID = 1817 ( icnlikejoinsel		PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_  _null_ icnlikejoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of NOT ILIKE");
DATA(insert OID = 1818 ( regexeqsel			PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	regexeqsel _null_ _null_ _null_ ));
DESCR("restriction selectivity of regex match");
DATA(insert OID = 1819 ( likesel			PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	likesel _null_ _null_ _null_ ));
DESCR("restriction selectivity of LIKE");
DATA(insert OID = 1820 ( icregexeqsel		PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	icregexeqsel _null_ _null_ _null_ ));
DESCR("restriction selectivity of case-insensitive regex match");
DATA(insert OID = 1821 ( regexnesel			PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	regexnesel _null_ _null_ _null_ ));
DESCR("restriction selectivity of regex non-match");
DATA(insert OID = 1822 ( nlikesel			PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	nlikesel _null_ _null_ _null_ ));
DESCR("restriction selectivity of NOT LIKE");
DATA(insert OID = 1823 ( icregexnesel		PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_	icregexnesel _null_ _null_ _null_ ));
DESCR("restriction selectivity of case-insensitive regex non-match");
DATA(insert OID = 1824 ( regexeqjoinsel		PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_  _null_ regexeqjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of regex match");
DATA(insert OID = 1825 ( likejoinsel		PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_  _null_ likejoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of LIKE");
DATA(insert OID = 1826 ( icregexeqjoinsel	PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_  _null_ icregexeqjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of case-insensitive regex match");
DATA(insert OID = 1827 ( regexnejoinsel		PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_  _null_ regexnejoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of regex non-match");
DATA(insert OID = 1828 ( nlikejoinsel		PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_  _null_ nlikejoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of NOT LIKE");
DATA(insert OID = 1829 ( icregexnejoinsel	PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_  _null_ icregexnejoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of case-insensitive regex non-match");

/* Aggregate-related functions */
DATA(insert OID = 1830 (  float8_avg	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_avg _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2512 (  float8_var_pop   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_var_pop _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 1831 (  float8_var_samp  PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_var_samp _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2513 (  float8_stddev_pop PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_stddev_pop _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 1832 (  float8_stddev_samp	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_stddev_samp _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 1833 (  numeric_accum    PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 1700" _null_ _null_ _null_ _null_ _null_ numeric_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 2858 (  numeric_avg_accum    PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 1700" _null_ _null_ _null_ _null_ _null_ numeric_avg_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3548 (  numeric_accum_inv    PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 1700" _null_ _null_ _null_ _null_ _null_ numeric_accum_inv _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 1834 (  int2_accum	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 21" _null_ _null_ _null_ _null_ _null_ int2_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 1835 (  int4_accum	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 23" _null_ _null_ _null_ _null_ _null_ int4_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 1836 (  int8_accum	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 20" _null_ _null_ _null_ _null_ _null_ int8_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 2746 (  int8_avg_accum	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 20" _null_ _null_ _null_ _null_ _null_ int8_avg_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3567 (  int2_accum_inv   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 21" _null_ _null_ _null_ _null_ _null_ int2_accum_inv _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3568 (  int4_accum_inv   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 23" _null_ _null_ _null_ _null_ _null_ int4_accum_inv _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3569 (  int8_accum_inv   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 20" _null_ _null_ _null_ _null_ _null_ int8_accum_inv _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3387 (  int8_avg_accum_inv   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 20" _null_ _null_ _null_ _null_ _null_ int8_avg_accum_inv _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3178 (  numeric_sum	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_sum _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 1837 (  numeric_avg	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_avg _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2514 (  numeric_var_pop  PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_var_pop _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 1838 (  numeric_var_samp PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_var_samp _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2596 (  numeric_stddev_pop PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_	numeric_stddev_pop _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 1839 (  numeric_stddev_samp	PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_stddev_samp _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 1840 (  int2_sum		   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 20 "20 21" _null_ _null_ _null_ _null_ _null_ int2_sum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 1841 (  int4_sum		   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 20 "20 23" _null_ _null_ _null_ _null_ _null_ int4_sum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 1842 (  int8_sum		   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 1700 "1700 20" _null_ _null_ _null_ _null_ _null_ int8_sum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3388 (  numeric_poly_sum	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_poly_sum _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3389 (  numeric_poly_avg	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_poly_avg _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3390 (  numeric_poly_var_pop	PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_poly_var_pop _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3391 (  numeric_poly_var_samp PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_poly_var_samp _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3392 (  numeric_poly_stddev_pop PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_poly_stddev_pop _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3393 (  numeric_poly_stddev_samp	PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 1700 "2281" _null_ _null_ _null_ _null_ _null_ numeric_poly_stddev_samp _null_ _null_ _null_ ));
DESCR("aggregate final function");

DATA(insert OID = 1843 (  interval_accum   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1187 "1187 1186" _null_ _null_ _null_ _null_ _null_ interval_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3549 (  interval_accum_inv   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1187 "1187 1186" _null_ _null_ _null_ _null_ _null_ interval_accum_inv _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 1844 (  interval_avg	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1186 "1187" _null_ _null_ _null_ _null_ _null_ interval_avg _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 1962 (  int2_avg_accum   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1016 "1016 21" _null_ _null_ _null_ _null_ _null_ int2_avg_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 1963 (  int4_avg_accum   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1016 "1016 23" _null_ _null_ _null_ _null_ _null_ int4_avg_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3570 (  int2_avg_accum_inv   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1016 "1016 21" _null_ _null_ _null_ _null_ _null_ int2_avg_accum_inv _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3571 (  int4_avg_accum_inv   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1016 "1016 23" _null_ _null_ _null_ _null_ _null_ int4_avg_accum_inv _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 1964 (  int8_avg		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1016" _null_ _null_ _null_ _null_ _null_ int8_avg _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3572 (  int2int4_sum	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "1016" _null_ _null_ _null_ _null_ _null_ int2int4_sum _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2805 (  int8inc_float8_float8		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 20 "20 701 701" _null_ _null_ _null_ _null_ _null_ int8inc_float8_float8 _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 2806 (  float8_regr_accum			PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1022 "1022 701 701" _null_ _null_ _null_ _null_ _null_ float8_regr_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 2807 (  float8_regr_sxx			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_regr_sxx _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2808 (  float8_regr_syy			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_regr_syy _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2809 (  float8_regr_sxy			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_regr_sxy _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2810 (  float8_regr_avgx			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_regr_avgx _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2811 (  float8_regr_avgy			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_regr_avgy _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2812 (  float8_regr_r2			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_regr_r2 _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2813 (  float8_regr_slope			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_regr_slope _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2814 (  float8_regr_intercept		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_regr_intercept _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2815 (  float8_covar_pop			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_covar_pop _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2816 (  float8_covar_samp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_covar_samp _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2817 (  float8_corr				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "1022" _null_ _null_ _null_ _null_ _null_ float8_corr _null_ _null_ _null_ ));
DESCR("aggregate final function");

DATA(insert OID = 3535 (  string_agg_transfn		PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 2281 "2281 25 25" _null_ _null_ _null_ _null_ _null_ string_agg_transfn _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3536 (  string_agg_finalfn		PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 25 "2281" _null_ _null_ _null_ _null_ _null_ string_agg_finalfn _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3538 (  string_agg				PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("concatenate aggregate input into a string");
DATA(insert OID = 3543 (  bytea_string_agg_transfn	PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 2281 "2281 17 17" _null_ _null_ _null_ _null_ _null_ bytea_string_agg_transfn _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3544 (  bytea_string_agg_finalfn	PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 17 "2281" _null_ _null_ _null_ _null_ _null_ bytea_string_agg_finalfn _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3545 (  string_agg				PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 17 "17 17" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("concatenate aggregate input into a bytea");

/* To ASCII conversion */
DATA(insert OID = 1845 ( to_ascii	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ to_ascii_default _null_ _null_ _null_ ));
DESCR("encode text from DB encoding to ASCII text");
DATA(insert OID = 1846 ( to_ascii	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 23" _null_ _null_ _null_ _null_ _null_ to_ascii_enc _null_ _null_ _null_ ));
DESCR("encode text from encoding to ASCII text");
DATA(insert OID = 1847 ( to_ascii	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 19" _null_ _null_ _null_ _null_ _null_ to_ascii_encname _null_ _null_ _null_ ));
DESCR("encode text from encoding to ASCII text");

DATA(insert OID = 1848 ( interval_pl_time	PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 1083 "1186 1083" _null_ _null_ _null_ _null_ _null_ "select $2 + $1" _null_ _null_ _null_ ));

DATA(insert OID = 1850 (  int28eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 20" _null_ _null_ _null_ _null_ _null_ int28eq _null_ _null_ _null_ ));
DATA(insert OID = 1851 (  int28ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 20" _null_ _null_ _null_ _null_ _null_ int28ne _null_ _null_ _null_ ));
DATA(insert OID = 1852 (  int28lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 20" _null_ _null_ _null_ _null_ _null_ int28lt _null_ _null_ _null_ ));
DATA(insert OID = 1853 (  int28gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 20" _null_ _null_ _null_ _null_ _null_ int28gt _null_ _null_ _null_ ));
DATA(insert OID = 1854 (  int28le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 20" _null_ _null_ _null_ _null_ _null_ int28le _null_ _null_ _null_ ));
DATA(insert OID = 1855 (  int28ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "21 20" _null_ _null_ _null_ _null_ _null_ int28ge _null_ _null_ _null_ ));

DATA(insert OID = 1856 (  int82eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 21" _null_ _null_ _null_ _null_ _null_ int82eq _null_ _null_ _null_ ));
DATA(insert OID = 1857 (  int82ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 21" _null_ _null_ _null_ _null_ _null_ int82ne _null_ _null_ _null_ ));
DATA(insert OID = 1858 (  int82lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 21" _null_ _null_ _null_ _null_ _null_ int82lt _null_ _null_ _null_ ));
DATA(insert OID = 1859 (  int82gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 21" _null_ _null_ _null_ _null_ _null_ int82gt _null_ _null_ _null_ ));
DATA(insert OID = 1860 (  int82le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 21" _null_ _null_ _null_ _null_ _null_ int82le _null_ _null_ _null_ ));
DATA(insert OID = 1861 (  int82ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "20 21" _null_ _null_ _null_ _null_ _null_ int82ge _null_ _null_ _null_ ));

DATA(insert OID = 1892 (  int2and		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2and _null_ _null_ _null_ ));
DATA(insert OID = 1893 (  int2or		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2or _null_ _null_ _null_ ));
DATA(insert OID = 1894 (  int2xor		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 21" _null_ _null_ _null_ _null_ _null_ int2xor _null_ _null_ _null_ ));
DATA(insert OID = 1895 (  int2not		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "21" _null_ _null_ _null_ _null_ _null_ int2not _null_ _null_ _null_ ));
DATA(insert OID = 1896 (  int2shl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 23" _null_ _null_ _null_ _null_ _null_ int2shl _null_ _null_ _null_ ));
DATA(insert OID = 1897 (  int2shr		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 21 "21 23" _null_ _null_ _null_ _null_ _null_ int2shr _null_ _null_ _null_ ));

DATA(insert OID = 1898 (  int4and		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4and _null_ _null_ _null_ ));
DATA(insert OID = 1899 (  int4or		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4or _null_ _null_ _null_ ));
DATA(insert OID = 1900 (  int4xor		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4xor _null_ _null_ _null_ ));
DATA(insert OID = 1901 (  int4not		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ int4not _null_ _null_ _null_ ));
DATA(insert OID = 1902 (  int4shl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4shl _null_ _null_ _null_ ));
DATA(insert OID = 1903 (  int4shr		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ int4shr _null_ _null_ _null_ ));

DATA(insert OID = 1904 (  int8and		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8and _null_ _null_ _null_ ));
DATA(insert OID = 1905 (  int8or		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8or _null_ _null_ _null_ ));
DATA(insert OID = 1906 (  int8xor		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ int8xor _null_ _null_ _null_ ));
DATA(insert OID = 1907 (  int8not		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ int8not _null_ _null_ _null_ ));
DATA(insert OID = 1908 (  int8shl		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 23" _null_ _null_ _null_ _null_ _null_ int8shl _null_ _null_ _null_ ));
DATA(insert OID = 1909 (  int8shr		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 20 "20 23" _null_ _null_ _null_ _null_ _null_ int8shr _null_ _null_ _null_ ));

DATA(insert OID = 1910 (  int8up		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ int8up _null_ _null_ _null_ ));
DATA(insert OID = 1911 (  int2up		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "21" _null_ _null_ _null_ _null_ _null_ int2up _null_ _null_ _null_ ));
DATA(insert OID = 1912 (  int4up		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ int4up _null_ _null_ _null_ ));
DATA(insert OID = 1913 (  float4up		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700 "700" _null_ _null_ _null_ _null_ _null_ float4up _null_ _null_ _null_ ));
DATA(insert OID = 1914 (  float8up		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_ float8up _null_ _null_ _null_ ));
DATA(insert OID = 1915 (  numeric_uplus    PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ numeric_uplus _null_ _null_ _null_ ));

DATA(insert OID = 1922 (  has_table_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_table_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on relation by username, rel name");
DATA(insert OID = 1923 (  has_table_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_table_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on relation by username, rel oid");
DATA(insert OID = 1924 (  has_table_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_table_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on relation by user oid, rel name");
DATA(insert OID = 1925 (  has_table_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_table_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on relation by user oid, rel oid");
DATA(insert OID = 1926 (  has_table_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_table_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on relation by rel name");
DATA(insert OID = 1927 (  has_table_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_table_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on relation by rel oid");

DATA(insert OID = 2181 (  has_sequence_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_sequence_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on sequence by username, seq name");
DATA(insert OID = 2182 (  has_sequence_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_sequence_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on sequence by username, seq oid");
DATA(insert OID = 2183 (  has_sequence_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_sequence_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on sequence by user oid, seq name");
DATA(insert OID = 2184 (  has_sequence_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_sequence_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on sequence by user oid, seq oid");
DATA(insert OID = 2185 (  has_sequence_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_sequence_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on sequence by seq name");
DATA(insert OID = 2186 (  has_sequence_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_sequence_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on sequence by seq oid");

DATA(insert OID = 3012 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 16 "19 25 25 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_name_name_name _null_ _null_ _null_ ));
DESCR("user privilege on column by username, rel name, col name");
DATA(insert OID = 3013 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 16 "19 25 21 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_name_name_attnum _null_ _null_ _null_ ));
DESCR("user privilege on column by username, rel name, col attnum");
DATA(insert OID = 3014 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 16 "19 26 25 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_name_id_name _null_ _null_ _null_ ));
DESCR("user privilege on column by username, rel oid, col name");
DATA(insert OID = 3015 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 16 "19 26 21 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_name_id_attnum _null_ _null_ _null_ ));
DESCR("user privilege on column by username, rel oid, col attnum");
DATA(insert OID = 3016 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 16 "26 25 25 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_id_name_name _null_ _null_ _null_ ));
DESCR("user privilege on column by user oid, rel name, col name");
DATA(insert OID = 3017 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 16 "26 25 21 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_id_name_attnum _null_ _null_ _null_ ));
DESCR("user privilege on column by user oid, rel name, col attnum");
DATA(insert OID = 3018 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 16 "26 26 25 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_id_id_name _null_ _null_ _null_ ));
DESCR("user privilege on column by user oid, rel oid, col name");
DATA(insert OID = 3019 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 16 "26 26 21 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_id_id_attnum _null_ _null_ _null_ ));
DESCR("user privilege on column by user oid, rel oid, col attnum");
DATA(insert OID = 3020 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "25 25 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_name_name _null_ _null_ _null_ ));
DESCR("current user privilege on column by rel name, col name");
DATA(insert OID = 3021 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "25 21 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_name_attnum _null_ _null_ _null_ ));
DESCR("current user privilege on column by rel name, col attnum");
DATA(insert OID = 3022 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_id_name _null_ _null_ _null_ ));
DESCR("current user privilege on column by rel oid, col name");
DATA(insert OID = 3023 (  has_column_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 21 25" _null_ _null_ _null_ _null_ _null_ has_column_privilege_id_attnum _null_ _null_ _null_ ));
DESCR("current user privilege on column by rel oid, col attnum");

DATA(insert OID = 3024 (  has_any_column_privilege	   PGNSP PGUID 12 10 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_any_column_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on any column by username, rel name");
DATA(insert OID = 3025 (  has_any_column_privilege	   PGNSP PGUID 12 10 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_any_column_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on any column by username, rel oid");
DATA(insert OID = 3026 (  has_any_column_privilege	   PGNSP PGUID 12 10 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_any_column_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on any column by user oid, rel name");
DATA(insert OID = 3027 (  has_any_column_privilege	   PGNSP PGUID 12 10 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_any_column_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on any column by user oid, rel oid");
DATA(insert OID = 3028 (  has_any_column_privilege	   PGNSP PGUID 12 10 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_any_column_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on any column by rel name");
DATA(insert OID = 3029 (  has_any_column_privilege	   PGNSP PGUID 12 10 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_any_column_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on any column by rel oid");

DATA(insert OID = 1928 (  pg_stat_get_numscans			PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_numscans _null_ _null_ _null_ ));
DESCR("statistics: number of scans done for table/index");
DATA(insert OID = 1929 (  pg_stat_get_tuples_returned	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_tuples_returned _null_ _null_ _null_ ));
DESCR("statistics: number of tuples read by seqscan");
DATA(insert OID = 1930 (  pg_stat_get_tuples_fetched	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_tuples_fetched _null_ _null_ _null_ ));
DESCR("statistics: number of tuples fetched by idxscan");
DATA(insert OID = 1931 (  pg_stat_get_tuples_inserted	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_tuples_inserted _null_ _null_ _null_ ));
DESCR("statistics: number of tuples inserted");
DATA(insert OID = 1932 (  pg_stat_get_tuples_updated	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_tuples_updated _null_ _null_ _null_ ));
DESCR("statistics: number of tuples updated");
DATA(insert OID = 1933 (  pg_stat_get_tuples_deleted	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_tuples_deleted _null_ _null_ _null_ ));
DESCR("statistics: number of tuples deleted");
DATA(insert OID = 1972 (  pg_stat_get_tuples_hot_updated PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_tuples_hot_updated _null_ _null_ _null_ ));
DESCR("statistics: number of tuples hot updated");
DATA(insert OID = 2878 (  pg_stat_get_live_tuples	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_live_tuples _null_ _null_ _null_ ));
DESCR("statistics: number of live tuples");
DATA(insert OID = 2879 (  pg_stat_get_dead_tuples	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_dead_tuples _null_ _null_ _null_ ));
DESCR("statistics: number of dead tuples");
DATA(insert OID = 3177 (  pg_stat_get_mod_since_analyze PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_mod_since_analyze _null_ _null_ _null_ ));
DESCR("statistics: number of tuples changed since last analyze");
DATA(insert OID = 1934 (  pg_stat_get_blocks_fetched	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_blocks_fetched _null_ _null_ _null_ ));
DESCR("statistics: number of blocks fetched");
DATA(insert OID = 1935 (  pg_stat_get_blocks_hit		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_blocks_hit _null_ _null_ _null_ ));
DESCR("statistics: number of blocks found in cache");
DATA(insert OID = 2781 (  pg_stat_get_last_vacuum_time PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1184 "26" _null_ _null_ _null_ _null_ _null_	pg_stat_get_last_vacuum_time _null_ _null_ _null_ ));
DESCR("statistics: last manual vacuum time for a table");
DATA(insert OID = 2782 (  pg_stat_get_last_autovacuum_time PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1184 "26" _null_ _null_ _null_ _null_ _null_	pg_stat_get_last_autovacuum_time _null_ _null_ _null_ ));
DESCR("statistics: last auto vacuum time for a table");
DATA(insert OID = 2783 (  pg_stat_get_last_analyze_time PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1184 "26" _null_ _null_ _null_ _null_ _null_	pg_stat_get_last_analyze_time _null_ _null_ _null_ ));
DESCR("statistics: last manual analyze time for a table");
DATA(insert OID = 2784 (  pg_stat_get_last_autoanalyze_time PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1184 "26" _null_ _null_ _null_ _null_ _null_	pg_stat_get_last_autoanalyze_time _null_ _null_ _null_ ));
DESCR("statistics: last auto analyze time for a table");
DATA(insert OID = 3054 ( pg_stat_get_vacuum_count PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_vacuum_count _null_ _null_ _null_ ));
DESCR("statistics: number of manual vacuums for a table");
DATA(insert OID = 3055 ( pg_stat_get_autovacuum_count PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_autovacuum_count _null_ _null_ _null_ ));
DESCR("statistics: number of auto vacuums for a table");
DATA(insert OID = 3056 ( pg_stat_get_analyze_count PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_analyze_count _null_ _null_ _null_ ));
DESCR("statistics: number of manual analyzes for a table");
DATA(insert OID = 3057 ( pg_stat_get_autoanalyze_count PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_autoanalyze_count _null_ _null_ _null_ ));
DESCR("statistics: number of auto analyzes for a table");
DATA(insert OID = 1936 (  pg_stat_get_backend_idset		PGNSP PGUID 12 1 100 0 0 f f f f t t s 0 0 23 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_idset _null_ _null_ _null_ ));
DESCR("statistics: currently active backend IDs");
DATA(insert OID = 2022 (  pg_stat_get_activity			PGNSP PGUID 12 1 100 0 0 f f f f f t s 1 0 2249 "23" "{23,26,23,26,25,25,25,16,1184,1184,1184,1184,869,25,23,28,28,16,25,25,23,16,25}" "{i,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o}" "{pid,datid,pid,usesysid,application_name,state,query,waiting,xact_start,query_start,backend_start,state_change,client_addr,client_hostname,client_port,backend_xid,backend_xmin,ssl,sslversion,sslcipher,sslbits,sslcompression,sslclientdn}" _null_ _null_ pg_stat_get_activity _null_ _null_ _null_ ));
DESCR("statistics: information about currently active backends");
DATA(insert OID = 3099 (  pg_stat_get_wal_senders	PGNSP PGUID 12 1 10 0 0 f f f f f t s 0 0 2249 "" "{23,25,3220,3220,3220,3220,23,25}" "{o,o,o,o,o,o,o,o}" "{pid,state,sent_location,write_location,flush_location,replay_location,sync_priority,sync_state}" _null_ _null_ pg_stat_get_wal_senders _null_ _null_ _null_ ));
DESCR("statistics: information about currently active replication");
DATA(insert OID = 2026 (  pg_backend_pid				PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 23 "" _null_ _null_ _null_ _null_ _null_ pg_backend_pid _null_ _null_ _null_ ));
DESCR("statistics: current backend PID");
DATA(insert OID = 1937 (  pg_stat_get_backend_pid		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_pid _null_ _null_ _null_ ));
DESCR("statistics: PID of backend");
DATA(insert OID = 1938 (  pg_stat_get_backend_dbid		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 26 "23" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_dbid _null_ _null_ _null_ ));
DESCR("statistics: database ID of backend");
DATA(insert OID = 1939 (  pg_stat_get_backend_userid	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 26 "23" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_userid _null_ _null_ _null_ ));
DESCR("statistics: user ID of backend");
DATA(insert OID = 1940 (  pg_stat_get_backend_activity	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "23" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_activity _null_ _null_ _null_ ));
DESCR("statistics: current query of backend");
DATA(insert OID = 2853 (  pg_stat_get_backend_waiting	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 16 "23" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_waiting _null_ _null_ _null_ ));
DESCR("statistics: is backend currently waiting for a lock");
DATA(insert OID = 2094 (  pg_stat_get_backend_activity_start PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1184 "23" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_activity_start _null_ _null_ _null_ ));
DESCR("statistics: start time for current query of backend");
DATA(insert OID = 2857 (  pg_stat_get_backend_xact_start PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1184 "23" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_xact_start _null_ _null_ _null_ ));
DESCR("statistics: start time for backend's current transaction");
DATA(insert OID = 1391 ( pg_stat_get_backend_start PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1184 "23" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_start _null_ _null_ _null_ ));
DESCR("statistics: start time for current backend session");
DATA(insert OID = 1392 ( pg_stat_get_backend_client_addr PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 869 "23" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_client_addr _null_ _null_ _null_ ));
DESCR("statistics: address of client connected to backend");
DATA(insert OID = 1393 ( pg_stat_get_backend_client_port PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ pg_stat_get_backend_client_port _null_ _null_ _null_ ));
DESCR("statistics: port number of client connected to backend");
DATA(insert OID = 1941 (  pg_stat_get_db_numbackends	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 23 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_numbackends _null_ _null_ _null_ ));
DESCR("statistics: number of backends in database");
DATA(insert OID = 1942 (  pg_stat_get_db_xact_commit	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_xact_commit _null_ _null_ _null_ ));
DESCR("statistics: transactions committed");
DATA(insert OID = 1943 (  pg_stat_get_db_xact_rollback	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_xact_rollback _null_ _null_ _null_ ));
DESCR("statistics: transactions rolled back");
DATA(insert OID = 1944 (  pg_stat_get_db_blocks_fetched PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_blocks_fetched _null_ _null_ _null_ ));
DESCR("statistics: blocks fetched for database");
DATA(insert OID = 1945 (  pg_stat_get_db_blocks_hit		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_blocks_hit _null_ _null_ _null_ ));
DESCR("statistics: blocks found in cache for database");
DATA(insert OID = 2758 (  pg_stat_get_db_tuples_returned PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_tuples_returned _null_ _null_ _null_ ));
DESCR("statistics: tuples returned for database");
DATA(insert OID = 2759 (  pg_stat_get_db_tuples_fetched PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_tuples_fetched _null_ _null_ _null_ ));
DESCR("statistics: tuples fetched for database");
DATA(insert OID = 2760 (  pg_stat_get_db_tuples_inserted PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_tuples_inserted _null_ _null_ _null_ ));
DESCR("statistics: tuples inserted in database");
DATA(insert OID = 2761 (  pg_stat_get_db_tuples_updated PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_tuples_updated _null_ _null_ _null_ ));
DESCR("statistics: tuples updated in database");
DATA(insert OID = 2762 (  pg_stat_get_db_tuples_deleted PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_tuples_deleted _null_ _null_ _null_ ));
DESCR("statistics: tuples deleted in database");
DATA(insert OID = 3065 (  pg_stat_get_db_conflict_tablespace PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_conflict_tablespace _null_ _null_ _null_ ));
DESCR("statistics: recovery conflicts in database caused by drop tablespace");
DATA(insert OID = 3066 (  pg_stat_get_db_conflict_lock PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_conflict_lock _null_ _null_ _null_ ));
DESCR("statistics: recovery conflicts in database caused by relation lock");
DATA(insert OID = 3067 (  pg_stat_get_db_conflict_snapshot PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_conflict_snapshot _null_ _null_ _null_ ));
DESCR("statistics: recovery conflicts in database caused by snapshot expiry");
DATA(insert OID = 3068 (  pg_stat_get_db_conflict_bufferpin PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_conflict_bufferpin _null_ _null_ _null_ ));
DESCR("statistics: recovery conflicts in database caused by shared buffer pin");
DATA(insert OID = 3069 (  pg_stat_get_db_conflict_startup_deadlock PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_conflict_startup_deadlock _null_ _null_ _null_ ));
DESCR("statistics: recovery conflicts in database caused by buffer deadlock");
DATA(insert OID = 3070 (  pg_stat_get_db_conflict_all PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_conflict_all _null_ _null_ _null_ ));
DESCR("statistics: recovery conflicts in database");
DATA(insert OID = 3152 (  pg_stat_get_db_deadlocks PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_deadlocks _null_ _null_ _null_ ));
DESCR("statistics: deadlocks detected in database");
DATA(insert OID = 3074 (  pg_stat_get_db_stat_reset_time PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1184 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_stat_reset_time _null_ _null_ _null_ ));
DESCR("statistics: last reset for a database");
DATA(insert OID = 3150 (  pg_stat_get_db_temp_files PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_temp_files _null_ _null_ _null_ ));
DESCR("statistics: number of temporary files written");
DATA(insert OID = 3151 (  pg_stat_get_db_temp_bytes PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_temp_bytes _null_ _null_ _null_ ));
DESCR("statistics: number of bytes in temporary files written");
DATA(insert OID = 2844 (  pg_stat_get_db_blk_read_time	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 701 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_blk_read_time _null_ _null_ _null_ ));
DESCR("statistics: block read time, in msec");
DATA(insert OID = 2845 (  pg_stat_get_db_blk_write_time PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 701 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_db_blk_write_time _null_ _null_ _null_ ));
DESCR("statistics: block write time, in msec");
DATA(insert OID = 3195 (  pg_stat_get_archiver		PGNSP PGUID 12 1 0 0 0 f f f f f f s 0 0 2249 "" "{20,25,1184,20,25,1184,1184}" "{o,o,o,o,o,o,o}" "{archived_count,last_archived_wal,last_archived_time,failed_count,last_failed_wal,last_failed_time,stats_reset}" _null_ _null_ pg_stat_get_archiver _null_ _null_ _null_ ));
DESCR("statistics: information about WAL archiver");
DATA(insert OID = 2769 ( pg_stat_get_bgwriter_timed_checkpoints PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 20 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_bgwriter_timed_checkpoints _null_ _null_ _null_ ));
DESCR("statistics: number of timed checkpoints started by the bgwriter");
DATA(insert OID = 2770 ( pg_stat_get_bgwriter_requested_checkpoints PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 20 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_bgwriter_requested_checkpoints _null_ _null_ _null_ ));
DESCR("statistics: number of backend requested checkpoints started by the bgwriter");
DATA(insert OID = 2771 ( pg_stat_get_bgwriter_buf_written_checkpoints PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 20 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_bgwriter_buf_written_checkpoints _null_ _null_ _null_ ));
DESCR("statistics: number of buffers written by the bgwriter during checkpoints");
DATA(insert OID = 2772 ( pg_stat_get_bgwriter_buf_written_clean PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 20 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_bgwriter_buf_written_clean _null_ _null_ _null_ ));
DESCR("statistics: number of buffers written by the bgwriter for cleaning dirty buffers");
DATA(insert OID = 2773 ( pg_stat_get_bgwriter_maxwritten_clean PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 20 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_bgwriter_maxwritten_clean _null_ _null_ _null_ ));
DESCR("statistics: number of times the bgwriter stopped processing when it had written too many buffers while cleaning");
DATA(insert OID = 3075 ( pg_stat_get_bgwriter_stat_reset_time PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 1184 "" _null_ _null_ _null_ _null_ _null_	pg_stat_get_bgwriter_stat_reset_time _null_ _null_ _null_ ));
DESCR("statistics: last reset for the bgwriter");
DATA(insert OID = 3160 ( pg_stat_get_checkpoint_write_time PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 701 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_checkpoint_write_time _null_ _null_ _null_ ));
DESCR("statistics: checkpoint time spent writing buffers to disk, in msec");
DATA(insert OID = 3161 ( pg_stat_get_checkpoint_sync_time PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 701 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_checkpoint_sync_time _null_ _null_ _null_ ));
DESCR("statistics: checkpoint time spent synchronizing buffers to disk, in msec");
DATA(insert OID = 2775 ( pg_stat_get_buf_written_backend PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 20 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_buf_written_backend _null_ _null_ _null_ ));
DESCR("statistics: number of buffers written by backends");
DATA(insert OID = 3063 ( pg_stat_get_buf_fsync_backend PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 20 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_buf_fsync_backend _null_ _null_ _null_ ));
DESCR("statistics: number of backend buffer writes that did their own fsync");
DATA(insert OID = 2859 ( pg_stat_get_buf_alloc			PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 20 "" _null_ _null_ _null_ _null_ _null_ pg_stat_get_buf_alloc _null_ _null_ _null_ ));
DESCR("statistics: number of buffer allocations");

DATA(insert OID = 2978 (  pg_stat_get_function_calls		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_function_calls _null_ _null_ _null_ ));
DESCR("statistics: number of function calls");
DATA(insert OID = 2979 (  pg_stat_get_function_total_time	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 701 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_function_total_time _null_ _null_ _null_ ));
DESCR("statistics: total execution time of function, in msec");
DATA(insert OID = 2980 (  pg_stat_get_function_self_time	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 701 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_function_self_time _null_ _null_ _null_ ));
DESCR("statistics: self execution time of function, in msec");

DATA(insert OID = 3037 (  pg_stat_get_xact_numscans				PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_numscans _null_ _null_ _null_ ));
DESCR("statistics: number of scans done for table/index in current transaction");
DATA(insert OID = 3038 (  pg_stat_get_xact_tuples_returned		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_tuples_returned _null_ _null_ _null_ ));
DESCR("statistics: number of tuples read by seqscan in current transaction");
DATA(insert OID = 3039 (  pg_stat_get_xact_tuples_fetched		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_tuples_fetched _null_ _null_ _null_ ));
DESCR("statistics: number of tuples fetched by idxscan in current transaction");
DATA(insert OID = 3040 (  pg_stat_get_xact_tuples_inserted		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_tuples_inserted _null_ _null_ _null_ ));
DESCR("statistics: number of tuples inserted in current transaction");
DATA(insert OID = 3041 (  pg_stat_get_xact_tuples_updated		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_tuples_updated _null_ _null_ _null_ ));
DESCR("statistics: number of tuples updated in current transaction");
DATA(insert OID = 3042 (  pg_stat_get_xact_tuples_deleted		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_tuples_deleted _null_ _null_ _null_ ));
DESCR("statistics: number of tuples deleted in current transaction");
DATA(insert OID = 3043 (  pg_stat_get_xact_tuples_hot_updated	PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_tuples_hot_updated _null_ _null_ _null_ ));
DESCR("statistics: number of tuples hot updated in current transaction");
DATA(insert OID = 3044 (  pg_stat_get_xact_blocks_fetched		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_blocks_fetched _null_ _null_ _null_ ));
DESCR("statistics: number of blocks fetched in current transaction");
DATA(insert OID = 3045 (  pg_stat_get_xact_blocks_hit			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_blocks_hit _null_ _null_ _null_ ));
DESCR("statistics: number of blocks found in cache in current transaction");
DATA(insert OID = 3046 (  pg_stat_get_xact_function_calls		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_function_calls _null_ _null_ _null_ ));
DESCR("statistics: number of function calls in current transaction");
DATA(insert OID = 3047 (  pg_stat_get_xact_function_total_time	PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 701 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_function_total_time _null_ _null_ _null_ ));
DESCR("statistics: total execution time of function in current transaction, in msec");
DATA(insert OID = 3048 (  pg_stat_get_xact_function_self_time	PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 701 "26" _null_ _null_ _null_ _null_ _null_ pg_stat_get_xact_function_self_time _null_ _null_ _null_ ));
DESCR("statistics: self execution time of function in current transaction, in msec");

DATA(insert OID = 3788 (  pg_stat_get_snapshot_timestamp PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 1184 "" _null_ _null_ _null_ _null_ _null_	pg_stat_get_snapshot_timestamp _null_ _null_ _null_ ));
DESCR("statistics: timestamp of the current statistics snapshot");
DATA(insert OID = 2230 (  pg_stat_clear_snapshot		PGNSP PGUID 12 1 0 0 0 f f f f f f v 0 0 2278 "" _null_ _null_ _null_ _null_ _null_ pg_stat_clear_snapshot _null_ _null_ _null_ ));
DESCR("statistics: discard current transaction's statistics snapshot");
DATA(insert OID = 2274 (  pg_stat_reset					PGNSP PGUID 12 1 0 0 0 f f f f f f v 0 0 2278 "" _null_ _null_ _null_ _null_ _null_ pg_stat_reset _null_ _null_ _null_ ));
DESCR("statistics: reset collected statistics for current database");
DATA(insert OID = 3775 (  pg_stat_reset_shared			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "25" _null_ _null_ _null_ _null_ _null_	pg_stat_reset_shared _null_ _null_ _null_ ));
DESCR("statistics: reset collected statistics shared across the cluster");
DATA(insert OID = 3776 (  pg_stat_reset_single_table_counters	PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_	pg_stat_reset_single_table_counters _null_ _null_ _null_ ));
DESCR("statistics: reset collected statistics for a single table or index in the current database");
DATA(insert OID = 3777 (  pg_stat_reset_single_function_counters	PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_	pg_stat_reset_single_function_counters _null_ _null_ _null_ ));
DESCR("statistics: reset collected statistics for a single function in the current database");

DATA(insert OID = 3163 (  pg_trigger_depth				PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 23 "" _null_ _null_ _null_ _null_ _null_ pg_trigger_depth _null_ _null_ _null_ ));
DESCR("current trigger depth");

DATA(insert OID = 3778 ( pg_tablespace_location PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "26" _null_ _null_ _null_ _null_ _null_ pg_tablespace_location _null_ _null_ _null_ ));
DESCR("tablespace location");

DATA(insert OID = 1946 (  encode						PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "17 25" _null_ _null_ _null_ _null_ _null_ binary_encode _null_ _null_ _null_ ));
DESCR("convert bytea value into some ascii-only text string");
DATA(insert OID = 1947 (  decode						PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 17 "25 25" _null_ _null_ _null_ _null_ _null_ binary_decode _null_ _null_ _null_ ));
DESCR("convert ascii-encoded text string into bytea value");

DATA(insert OID = 1948 (  byteaeq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "17 17" _null_ _null_ _null_ _null_ _null_ byteaeq _null_ _null_ _null_ ));
DATA(insert OID = 1949 (  bytealt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "17 17" _null_ _null_ _null_ _null_ _null_ bytealt _null_ _null_ _null_ ));
DATA(insert OID = 1950 (  byteale		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "17 17" _null_ _null_ _null_ _null_ _null_ byteale _null_ _null_ _null_ ));
DATA(insert OID = 1951 (  byteagt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "17 17" _null_ _null_ _null_ _null_ _null_ byteagt _null_ _null_ _null_ ));
DATA(insert OID = 1952 (  byteage		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "17 17" _null_ _null_ _null_ _null_ _null_ byteage _null_ _null_ _null_ ));
DATA(insert OID = 1953 (  byteane		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "17 17" _null_ _null_ _null_ _null_ _null_ byteane _null_ _null_ _null_ ));
DATA(insert OID = 1954 (  byteacmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "17 17" _null_ _null_ _null_ _null_ _null_ byteacmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 3917 (  timestamp_transform PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ timestamp_transform _null_ _null_ _null_ ));
DESCR("transform a timestamp length coercion");
DATA(insert OID = 3944 (  time_transform   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ time_transform _null_ _null_ _null_ ));
DESCR("transform a time length coercion");

DATA(insert OID = 1961 (  timestamp		   PGNSP PGUID 12 1 0 0 timestamp_transform f f f f t f i 2 0 1114 "1114 23" _null_ _null_ _null_ _null_ _null_ timestamp_scale _null_ _null_ _null_ ));
DESCR("adjust timestamp precision");

DATA(insert OID = 1965 (  oidlarger		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 26 "26 26" _null_ _null_ _null_ _null_ _null_ oidlarger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 1966 (  oidsmaller	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 26 "26 26" _null_ _null_ _null_ _null_ _null_ oidsmaller _null_ _null_ _null_ ));
DESCR("smaller of two");

DATA(insert OID = 1967 (  timestamptz	   PGNSP PGUID 12 1 0 0 timestamp_transform f f f f t f i 2 0 1184 "1184 23" _null_ _null_ _null_ _null_ _null_ timestamptz_scale _null_ _null_ _null_ ));
DESCR("adjust timestamptz precision");
DATA(insert OID = 1968 (  time			   PGNSP PGUID 12 1 0 0 time_transform f f f f t f i 2 0 1083 "1083 23" _null_ _null_ _null_ _null_ _null_ time_scale _null_ _null_ _null_ ));
DESCR("adjust time precision");
DATA(insert OID = 1969 (  timetz		   PGNSP PGUID 12 1 0 0 time_transform f f f f t f i 2 0 1266 "1266 23" _null_ _null_ _null_ _null_ _null_ timetz_scale _null_ _null_ _null_ ));
DESCR("adjust time with time zone precision");

DATA(insert OID = 2003 (  textanycat	   PGNSP PGUID 14 1 0 0 0 f f f f t f s 2 0 25 "25 2776" _null_ _null_ _null_ _null_ _null_ "select $1 || $2::pg_catalog.text" _null_ _null_ _null_ ));
DATA(insert OID = 2004 (  anytextcat	   PGNSP PGUID 14 1 0 0 0 f f f f t f s 2 0 25 "2776 25" _null_ _null_ _null_ _null_ _null_ "select $1::pg_catalog.text || $2" _null_ _null_ _null_ ));

DATA(insert OID = 2005 (  bytealike		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "17 17" _null_ _null_ _null_ _null_ _null_ bytealike _null_ _null_ _null_ ));
DATA(insert OID = 2006 (  byteanlike	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "17 17" _null_ _null_ _null_ _null_ _null_ byteanlike _null_ _null_ _null_ ));
DATA(insert OID = 2007 (  like			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "17 17" _null_ _null_ _null_ _null_ _null_ bytealike _null_ _null_ _null_ ));
DESCR("matches LIKE expression");
DATA(insert OID = 2008 (  notlike		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "17 17" _null_ _null_ _null_ _null_ _null_ byteanlike _null_ _null_ _null_ ));
DESCR("does not match LIKE expression");
DATA(insert OID = 2009 (  like_escape	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 17 "17 17" _null_ _null_ _null_ _null_ _null_ like_escape_bytea _null_ _null_ _null_ ));
DESCR("convert LIKE pattern to use backslash escapes");
DATA(insert OID = 2010 (  length		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "17" _null_ _null_ _null_ _null_ _null_ byteaoctetlen _null_ _null_ _null_ ));
DESCR("octet length");
DATA(insert OID = 2011 (  byteacat		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 17 "17 17" _null_ _null_ _null_ _null_ _null_ byteacat _null_ _null_ _null_ ));
DATA(insert OID = 2012 (  substring		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 17 "17 23 23" _null_ _null_ _null_ _null_ _null_	bytea_substr _null_ _null_ _null_ ));
DESCR("extract portion of string");
DATA(insert OID = 2013 (  substring		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 17 "17 23" _null_ _null_ _null_ _null_ _null_ bytea_substr_no_len _null_ _null_ _null_ ));
DESCR("extract portion of string");
DATA(insert OID = 2085 (  substr		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 17 "17 23 23" _null_ _null_ _null_ _null_ _null_	bytea_substr _null_ _null_ _null_ ));
DESCR("extract portion of string");
DATA(insert OID = 2086 (  substr		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 17 "17 23" _null_ _null_ _null_ _null_ _null_ bytea_substr_no_len _null_ _null_ _null_ ));
DESCR("extract portion of string");
DATA(insert OID = 2014 (  position		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "17 17" _null_ _null_ _null_ _null_ _null_ byteapos _null_ _null_ _null_ ));
DESCR("position of substring");
DATA(insert OID = 2015 (  btrim			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 17 "17 17" _null_ _null_ _null_ _null_ _null_ byteatrim _null_ _null_ _null_ ));
DESCR("trim both ends of string");

DATA(insert OID = 2019 (  time				PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1083 "1184" _null_ _null_ _null_ _null_ _null_ timestamptz_time _null_ _null_ _null_ ));
DESCR("convert timestamp with time zone to time");
DATA(insert OID = 2020 (  date_trunc		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1114 "25 1114" _null_ _null_ _null_ _null_ _null_ timestamp_trunc _null_ _null_ _null_ ));
DESCR("truncate timestamp to specified units");
DATA(insert OID = 2021 (  date_part			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "25 1114" _null_ _null_ _null_ _null_ _null_	timestamp_part _null_ _null_ _null_ ));
DESCR("extract field from timestamp");
DATA(insert OID = 2023 (  timestamp			PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1114 "702" _null_ _null_ _null_ _null_ _null_ abstime_timestamp _null_ _null_ _null_ ));
DESCR("convert abstime to timestamp");
DATA(insert OID = 2024 (  timestamp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1114 "1082" _null_ _null_ _null_ _null_ _null_ date_timestamp _null_ _null_ _null_ ));
DESCR("convert date to timestamp");
DATA(insert OID = 2025 (  timestamp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1114 "1082 1083" _null_ _null_ _null_ _null_ _null_	datetime_timestamp _null_ _null_ _null_ ));
DESCR("convert date and time to timestamp");
DATA(insert OID = 2027 (  timestamp			PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1114 "1184" _null_ _null_ _null_ _null_ _null_ timestamptz_timestamp _null_ _null_ _null_ ));
DESCR("convert timestamp with time zone to timestamp");
DATA(insert OID = 2028 (  timestamptz		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1184 "1114" _null_ _null_ _null_ _null_ _null_ timestamp_timestamptz _null_ _null_ _null_ ));
DESCR("convert timestamp to timestamp with time zone");
DATA(insert OID = 2029 (  date				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1082 "1114" _null_ _null_ _null_ _null_ _null_ timestamp_date _null_ _null_ _null_ ));
DESCR("convert timestamp to date");
DATA(insert OID = 2030 (  abstime			PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 702 "1114" _null_ _null_ _null_ _null_ _null_ timestamp_abstime _null_ _null_ _null_ ));
DESCR("convert timestamp to abstime");
DATA(insert OID = 2031 (  timestamp_mi		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1114 1114" _null_ _null_ _null_ _null_ _null_	timestamp_mi _null_ _null_ _null_ ));
DATA(insert OID = 2032 (  timestamp_pl_interval PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1114 "1114 1186" _null_ _null_ _null_ _null_ _null_	timestamp_pl_interval _null_ _null_ _null_ ));
DATA(insert OID = 2033 (  timestamp_mi_interval PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1114 "1114 1186" _null_ _null_ _null_ _null_ _null_	timestamp_mi_interval _null_ _null_ _null_ ));
DATA(insert OID = 2035 (  timestamp_smaller PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1114 "1114 1114" _null_ _null_ _null_ _null_ _null_	timestamp_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 2036 (  timestamp_larger	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1114 "1114 1114" _null_ _null_ _null_ _null_ _null_	timestamp_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 2037 (  timezone			PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 1266 "25 1266" _null_ _null_ _null_ _null_ _null_ timetz_zone _null_ _null_ _null_ ));
DESCR("adjust time with time zone to new zone");
DATA(insert OID = 2038 (  timezone			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1266 "1186 1266" _null_ _null_ _null_ _null_ _null_	timetz_izone _null_ _null_ _null_ ));
DESCR("adjust time with time zone to new zone");
DATA(insert OID = 2039 (  timestamp_hash	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "1114" _null_ _null_ _null_ _null_ _null_ timestamp_hash _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 2041 ( overlaps			PGNSP PGUID 12 1 0 0 0 f f f f f f i 4 0 16 "1114 1114 1114 1114" _null_ _null_ _null_ _null_ _null_	overlaps_timestamp _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 2042 ( overlaps			PGNSP PGUID 14 1 0 0 0 f f f f f f i 4 0 16 "1114 1186 1114 1186" _null_ _null_ _null_ _null_	_null_ "select ($1, ($1 + $2)) overlaps ($3, ($3 + $4))" _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 2043 ( overlaps			PGNSP PGUID 14 1 0 0 0 f f f f f f i 4 0 16 "1114 1114 1114 1186" _null_ _null_ _null_ _null_	_null_ "select ($1, $2) overlaps ($3, ($3 + $4))" _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 2044 ( overlaps			PGNSP PGUID 14 1 0 0 0 f f f f f f i 4 0 16 "1114 1186 1114 1114" _null_ _null_ _null_ _null_	_null_ "select ($1, ($1 + $2)) overlaps ($3, $4)" _null_ _null_ _null_ ));
DESCR("intervals overlap?");
DATA(insert OID = 2045 (  timestamp_cmp		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1114 1114" _null_ _null_ _null_ _null_ _null_ timestamp_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3137 (  timestamp_sortsupport PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ timestamp_sortsupport _null_ _null_ _null_ ));
DESCR("sort support");
DATA(insert OID = 2046 (  time				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1083 "1266" _null_ _null_ _null_ _null_ _null_ timetz_time _null_ _null_ _null_ ));
DESCR("convert time with time zone to time");
DATA(insert OID = 2047 (  timetz			PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 1266 "1083" _null_ _null_ _null_ _null_ _null_ time_timetz _null_ _null_ _null_ ));
DESCR("convert time to time with time zone");
DATA(insert OID = 2048 (  isfinite			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "1114" _null_ _null_ _null_ _null_ _null_ timestamp_finite _null_ _null_ _null_ ));
DESCR("finite timestamp?");
DATA(insert OID = 2049 ( to_char			PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "1114 25" _null_ _null_ _null_ _null_  _null_ timestamp_to_char _null_ _null_ _null_ ));
DESCR("format timestamp to text");
DATA(insert OID = 2052 (  timestamp_eq		PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1114 1114" _null_ _null_ _null_ _null_ _null_ timestamp_eq _null_ _null_ _null_ ));
DATA(insert OID = 2053 (  timestamp_ne		PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1114 1114" _null_ _null_ _null_ _null_ _null_ timestamp_ne _null_ _null_ _null_ ));
DATA(insert OID = 2054 (  timestamp_lt		PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1114 1114" _null_ _null_ _null_ _null_ _null_ timestamp_lt _null_ _null_ _null_ ));
DATA(insert OID = 2055 (  timestamp_le		PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1114 1114" _null_ _null_ _null_ _null_ _null_ timestamp_le _null_ _null_ _null_ ));
DATA(insert OID = 2056 (  timestamp_ge		PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1114 1114" _null_ _null_ _null_ _null_ _null_ timestamp_ge _null_ _null_ _null_ ));
DATA(insert OID = 2057 (  timestamp_gt		PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "1114 1114" _null_ _null_ _null_ _null_ _null_ timestamp_gt _null_ _null_ _null_ ));
DATA(insert OID = 2058 (  age				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1186 "1114 1114" _null_ _null_ _null_ _null_ _null_	timestamp_age _null_ _null_ _null_ ));
DESCR("date difference preserving months and years");
DATA(insert OID = 2059 (  age				PGNSP PGUID 14 1 0 0 0 f f f f t f s 1 0 1186 "1114" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.age(cast(current_date as timestamp without time zone), $1)" _null_ _null_ _null_ ));
DESCR("date difference from today preserving months and years");

DATA(insert OID = 2069 (  timezone			PGNSP PGUID 12 1 0 0 timestamp_zone_transform f f f f t f i 2 0 1184 "25 1114" _null_ _null_ _null_ _null_ _null_ timestamp_zone _null_ _null_ _null_ ));
DESCR("adjust timestamp to new time zone");
DATA(insert OID = 2070 (  timezone			PGNSP PGUID 12 1 0 0 timestamp_izone_transform f f f f t f i 2 0 1184 "1186 1114" _null_ _null_ _null_ _null_ _null_	timestamp_izone _null_ _null_ _null_ ));
DESCR("adjust timestamp to new time zone");
DATA(insert OID = 2071 (  date_pl_interval	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1114 "1082 1186" _null_ _null_ _null_ _null_ _null_	date_pl_interval _null_ _null_ _null_ ));
DATA(insert OID = 2072 (  date_mi_interval	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1114 "1082 1186" _null_ _null_ _null_ _null_ _null_	date_mi_interval _null_ _null_ _null_ ));

DATA(insert OID = 2073 (  substring			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25 "25 25" _null_ _null_ _null_ _null_ _null_ textregexsubstr _null_ _null_ _null_ ));
DESCR("extract text matching regular expression");
DATA(insert OID = 2074 (  substring			PGNSP PGUID 14 1 0 0 0 f f f f t f i 3 0 25 "25 25 25" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.substring($1, pg_catalog.similar_escape($2, $3))" _null_ _null_ _null_ ));
DESCR("extract text matching SQL99 regular expression");

DATA(insert OID = 2075 (  bit				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1560 "20 23" _null_ _null_ _null_ _null_ _null_	bitfromint8 _null_ _null_ _null_ ));
DESCR("convert int8 to bitstring");
DATA(insert OID = 2076 (  int8				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "1560" _null_ _null_ _null_ _null_ _null_ bittoint8 _null_ _null_ _null_ ));
DESCR("convert bitstring to int8");

DATA(insert OID = 2077 (  current_setting	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ show_config_by_name _null_ _null_ _null_ ));
DESCR("SHOW X as a function");
DATA(insert OID = 2078 (  set_config		PGNSP PGUID 12 1 0 0 0 f f f f f f v 3 0 25 "25 25 16" _null_ _null_ _null_ _null_ _null_ set_config_by_name _null_ _null_ _null_ ));
DESCR("SET X as a function");
DATA(insert OID = 2084 (  pg_show_all_settings	PGNSP PGUID 12 1 1000 0 0 f f f f t t s 0 0 2249 "" "{25,25,25,25,25,25,25,25,25,25,25,1009,25,25,25,23,16}" "{o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o,o}" "{name,setting,unit,category,short_desc,extra_desc,context,vartype,source,min_val,max_val,enumvals,boot_val,reset_val,sourcefile,sourceline,pending_restart}" _null_ _null_ show_all_settings _null_ _null_ _null_ ));
DESCR("SHOW ALL as a function");
DATA(insert OID = 3329 (  pg_show_all_file_settings PGNSP PGUID 12 1 1000 0 0 f f f f t t v 0 0 2249 "" "{25,23,23,25,25,16,25}" "{o,o,o,o,o,o,o}" "{sourcefile,sourceline,seqno,name,setting,applied,error}" _null_ _null_ show_all_file_settings _null_ _null_ _null_ ));
DESCR("show config file settings");
DATA(insert OID = 1371 (  pg_lock_status   PGNSP PGUID 12 1 1000 0 0 f f f f t t v 0 0 2249 "" "{25,26,26,23,21,25,28,26,26,21,25,23,25,16,16}" "{o,o,o,o,o,o,o,o,o,o,o,o,o,o,o}" "{locktype,database,relation,page,tuple,virtualxid,transactionid,classid,objid,objsubid,virtualtransaction,pid,mode,granted,fastpath}" _null_ _null_ pg_lock_status _null_ _null_ _null_ ));
DESCR("view system lock information");
DATA(insert OID = 1065 (  pg_prepared_xact PGNSP PGUID 12 1 1000 0 0 f f f f t t v 0 0 2249 "" "{28,25,1184,26,26}" "{o,o,o,o,o}" "{transaction,gid,prepared,ownerid,dbid}" _null_ _null_ pg_prepared_xact _null_ _null_ _null_ ));
DESCR("view two-phase transactions");
DATA(insert OID = 3819 (  pg_get_multixact_members PGNSP PGUID 12 1 1000 0 0 f f f f t t v 1 0 2249 "28" "{28,28,25}" "{i,o,o}" "{multixid,xid,mode}" _null_ _null_ pg_get_multixact_members _null_ _null_ _null_ ));
DESCR("view members of a multixactid");

DATA(insert OID = 3581 ( pg_xact_commit_timestamp PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 1184 "28" _null_ _null_ _null_ _null_ _null_ pg_xact_commit_timestamp _null_ _null_ _null_ ));
DESCR("get commit timestamp of a transaction");

DATA(insert OID = 3583 ( pg_last_committed_xact PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2249 "" "{28,1184}" "{o,o}" "{xid,timestamp}" _null_ _null_ pg_last_committed_xact _null_ _null_ _null_ ));
DESCR("get transaction Id and commit timestamp of latest transaction commit");

DATA(insert OID = 3537 (  pg_describe_object		PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 25 "26 26 23" _null_ _null_ _null_ _null_ _null_ pg_describe_object _null_ _null_ _null_ ));
DESCR("get identification of SQL object");

DATA(insert OID = 3839 (  pg_identify_object		PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 2249 "26 26 23" "{26,26,23,25,25,25,25}" "{i,i,i,o,o,o,o}" "{classid,objid,subobjid,type,schema,name,identity}" _null_ _null_ pg_identify_object _null_ _null_ _null_ ));
DESCR("get machine-parseable identification of SQL object");

DATA(insert OID = 3382 (  pg_identify_object_as_address PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 2249 "26 26 23" "{26,26,23,25,1009,1009}" "{i,i,i,o,o,o}" "{classid,objid,subobjid,type,object_names,object_args}" _null_ _null_ pg_identify_object_as_address _null_ _null_ _null_ ));
DESCR("get identification of SQL object for pg_get_object_address()");

DATA(insert OID = 3954 (  pg_get_object_address    PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 2249 "25 1009 1009" "{25,1009,1009,26,26,23}" "{i,i,i,o,o,o}" "{type,name,args,classid,objid,subobjid}" _null_ _null_ pg_get_object_address _null_ _null_ _null_ ));
DESCR("get OID-based object address from name/args arrays");

DATA(insert OID = 2079 (  pg_table_is_visible		PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_table_is_visible _null_ _null_ _null_ ));
DESCR("is table visible in search path?");
DATA(insert OID = 2080 (  pg_type_is_visible		PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_type_is_visible _null_ _null_ _null_ ));
DESCR("is type visible in search path?");
DATA(insert OID = 2081 (  pg_function_is_visible	PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_function_is_visible _null_ _null_ _null_ ));
DESCR("is function visible in search path?");
DATA(insert OID = 2082 (  pg_operator_is_visible	PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_operator_is_visible _null_ _null_ _null_ ));
DESCR("is operator visible in search path?");
DATA(insert OID = 2083 (  pg_opclass_is_visible		PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_opclass_is_visible _null_ _null_ _null_ ));
DESCR("is opclass visible in search path?");
DATA(insert OID = 3829 (  pg_opfamily_is_visible	PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_opfamily_is_visible _null_ _null_ _null_ ));
DESCR("is opfamily visible in search path?");
DATA(insert OID = 2093 (  pg_conversion_is_visible	PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_conversion_is_visible _null_ _null_ _null_ ));
DESCR("is conversion visible in search path?");
DATA(insert OID = 3756 (  pg_ts_parser_is_visible	PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_ts_parser_is_visible _null_ _null_ _null_ ));
DESCR("is text search parser visible in search path?");
DATA(insert OID = 3757 (  pg_ts_dict_is_visible		PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_ts_dict_is_visible _null_ _null_ _null_ ));
DESCR("is text search dictionary visible in search path?");
DATA(insert OID = 3768 (  pg_ts_template_is_visible PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_ts_template_is_visible _null_ _null_ _null_ ));
DESCR("is text search template visible in search path?");
DATA(insert OID = 3758 (  pg_ts_config_is_visible	PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_ts_config_is_visible _null_ _null_ _null_ ));
DESCR("is text search configuration visible in search path?");
DATA(insert OID = 3815 (  pg_collation_is_visible	PGNSP PGUID 12 10 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_collation_is_visible _null_ _null_ _null_ ));
DESCR("is collation visible in search path?");

DATA(insert OID = 2854 (  pg_my_temp_schema			PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 26 "" _null_ _null_ _null_ _null_ _null_ pg_my_temp_schema _null_ _null_ _null_ ));
DESCR("get OID of current session's temp schema, if any");
DATA(insert OID = 2855 (  pg_is_other_temp_schema	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_ pg_is_other_temp_schema _null_ _null_ _null_ ));
DESCR("is schema another session's temp schema?");

DATA(insert OID = 2171 ( pg_cancel_backend		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 16 "23" _null_ _null_ _null_ _null_ _null_ pg_cancel_backend _null_ _null_ _null_ ));
DESCR("cancel a server process' current query");
DATA(insert OID = 2096 ( pg_terminate_backend		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 16 "23" _null_ _null_ _null_ _null_ _null_ pg_terminate_backend _null_ _null_ _null_ ));
DESCR("terminate a server process");
DATA(insert OID = 2172 ( pg_start_backup		PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 3220 "25 16" _null_ _null_ _null_ _null_ _null_ pg_start_backup _null_ _null_ _null_ ));
DESCR("prepare for taking an online backup");
DATA(insert OID = 2173 ( pg_stop_backup			PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 3220 "" _null_ _null_ _null_ _null_ _null_ pg_stop_backup _null_ _null_ _null_ ));
DESCR("finish taking an online backup");
DATA(insert OID = 3813 ( pg_is_in_backup		PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 16 "" _null_ _null_ _null_ _null_ _null_ pg_is_in_backup _null_ _null_ _null_ ));
DESCR("true if server is in online backup");
DATA(insert OID = 3814 ( pg_backup_start_time		PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 1184 "" _null_ _null_ _null_ _null_ _null_ pg_backup_start_time _null_ _null_ _null_ ));
DESCR("start time of an online backup");
DATA(insert OID = 2848 ( pg_switch_xlog			PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 3220 "" _null_ _null_ _null_ _null_ _null_ pg_switch_xlog _null_ _null_ _null_ ));
DESCR("switch to new xlog file");
DATA(insert OID = 3098 ( pg_create_restore_point	PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 3220 "25" _null_ _null_ _null_ _null_ _null_ pg_create_restore_point _null_ _null_ _null_ ));
DESCR("create a named restore point");
DATA(insert OID = 2849 ( pg_current_xlog_location	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 3220 "" _null_ _null_ _null_ _null_ _null_ pg_current_xlog_location _null_ _null_ _null_ ));
DESCR("current xlog write location");
DATA(insert OID = 2852 ( pg_current_xlog_insert_location	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 3220 "" _null_ _null_ _null_ _null_ _null_ pg_current_xlog_insert_location _null_ _null_ _null_ ));
DESCR("current xlog insert location");
DATA(insert OID = 2850 ( pg_xlogfile_name_offset	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2249 "3220" "{3220,25,23}" "{i,o,o}" "{wal_location,file_name,file_offset}" _null_ _null_ pg_xlogfile_name_offset _null_ _null_ _null_ ));
DESCR("xlog filename and byte offset, given an xlog location");
DATA(insert OID = 2851 ( pg_xlogfile_name			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "3220" _null_ _null_ _null_ _null_ _null_ pg_xlogfile_name _null_ _null_ _null_ ));
DESCR("xlog filename, given an xlog location");

DATA(insert OID = 3165 ( pg_xlog_location_diff		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "3220 3220" _null_ _null_ _null_ _null_ _null_ pg_xlog_location_diff _null_ _null_ _null_ ));
DESCR("difference in bytes, given two xlog locations");

DATA(insert OID = 3809 ( pg_export_snapshot		PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 25 "" _null_ _null_ _null_ _null_ _null_ pg_export_snapshot _null_ _null_ _null_ ));
DESCR("export a snapshot");

DATA(insert OID = 3810 (  pg_is_in_recovery		PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 16 "" _null_ _null_ _null_ _null_ _null_ pg_is_in_recovery _null_ _null_ _null_ ));
DESCR("true if server is in recovery");

DATA(insert OID = 3820 ( pg_last_xlog_receive_location	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 3220 "" _null_ _null_ _null_ _null_ _null_ pg_last_xlog_receive_location _null_ _null_ _null_ ));
DESCR("current xlog flush location");
DATA(insert OID = 3821 ( pg_last_xlog_replay_location	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 3220 "" _null_ _null_ _null_ _null_ _null_ pg_last_xlog_replay_location _null_ _null_ _null_ ));
DESCR("last xlog replay location");
DATA(insert OID = 3830 ( pg_last_xact_replay_timestamp	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 1184 "" _null_ _null_ _null_ _null_ _null_ pg_last_xact_replay_timestamp _null_ _null_ _null_ ));
DESCR("timestamp of last replay xact");

DATA(insert OID = 3071 ( pg_xlog_replay_pause		PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2278 "" _null_ _null_ _null_ _null_ _null_ pg_xlog_replay_pause _null_ _null_ _null_ ));
DESCR("pause xlog replay");
DATA(insert OID = 3072 ( pg_xlog_replay_resume		PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2278 "" _null_ _null_ _null_ _null_ _null_ pg_xlog_replay_resume _null_ _null_ _null_ ));
DESCR("resume xlog replay, if it was paused");
DATA(insert OID = 3073 ( pg_is_xlog_replay_paused	PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 16 "" _null_ _null_ _null_ _null_ _null_ pg_is_xlog_replay_paused _null_ _null_ _null_ ));
DESCR("true if xlog replay is paused");

DATA(insert OID = 2621 ( pg_reload_conf			PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 16 "" _null_ _null_ _null_ _null_ _null_ pg_reload_conf _null_ _null_ _null_ ));
DESCR("reload configuration files");
DATA(insert OID = 2622 ( pg_rotate_logfile		PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 16 "" _null_ _null_ _null_ _null_ _null_ pg_rotate_logfile _null_ _null_ _null_ ));
DESCR("rotate log file");

DATA(insert OID = 2623 ( pg_stat_file		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2249 "25" "{25,20,1184,1184,1184,1184,16}" "{i,o,o,o,o,o,o}" "{filename,size,access,modification,change,creation,isdir}" _null_ _null_ pg_stat_file_1arg _null_ _null_ _null_ ));
DESCR("get information about file");
DATA(insert OID = 3307 ( pg_stat_file		PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2249 "25 16" "{25,16,20,1184,1184,1184,1184,16}" "{i,i,o,o,o,o,o,o}" "{filename,missing_ok,size,access,modification,change,creation,isdir}" _null_ _null_ pg_stat_file _null_ _null_ _null_ ));
DESCR("get information about file");
DATA(insert OID = 2624 ( pg_read_file		PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 25 "25 20 20" _null_ _null_ _null_ _null_ _null_ pg_read_file_off_len _null_ _null_ _null_ ));
DESCR("read text from a file");
DATA(insert OID = 3293 ( pg_read_file		PGNSP PGUID 12 1 0 0 0 f f f f t f v 4 0 25 "25 20 20 16" _null_ _null_ _null_ _null_ _null_ pg_read_file _null_ _null_ _null_ ));
DESCR("read text from a file");
DATA(insert OID = 3826 ( pg_read_file		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ pg_read_file_all _null_ _null_ _null_ ));
DESCR("read text from a file");
DATA(insert OID = 3827 ( pg_read_binary_file	PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 17 "25 20 20" _null_ _null_ _null_ _null_ _null_ pg_read_binary_file_off_len _null_ _null_ _null_ ));
DESCR("read bytea from a file");
DATA(insert OID = 3295 ( pg_read_binary_file	PGNSP PGUID 12 1 0 0 0 f f f f t f v 4 0 17 "25 20 20 16" _null_ _null_ _null_ _null_ _null_ pg_read_binary_file _null_ _null_ _null_ ));
DESCR("read bytea from a file");
DATA(insert OID = 3828 ( pg_read_binary_file	PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 17 "25" _null_ _null_ _null_ _null_ _null_ pg_read_binary_file_all _null_ _null_ _null_ ));
DESCR("read bytea from a file");
DATA(insert OID = 2625 ( pg_ls_dir			PGNSP PGUID 12 1 1000 0 0 f f f f t t v 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ pg_ls_dir_1arg _null_ _null_ _null_ ));
DESCR("list all files in a directory");
DATA(insert OID = 3297 ( pg_ls_dir			PGNSP PGUID 12 1 1000 0 0 f f f f t t v 3 0 25 "25 16 16" _null_ _null_ _null_ _null_ _null_ pg_ls_dir _null_ _null_ _null_ ));
DESCR("list all files in a directory");
DATA(insert OID = 2626 ( pg_sleep			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "701" _null_ _null_ _null_ _null_ _null_ pg_sleep _null_ _null_ _null_ ));
DESCR("sleep for the specified time in seconds");
DATA(insert OID = 3935 ( pg_sleep_for			PGNSP PGUID 14 1 0 0 0 f f f f t f v 1 0 2278 "1186" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.pg_sleep(extract(epoch from pg_catalog.clock_timestamp() operator(pg_catalog.+) $1) operator(pg_catalog.-) extract(epoch from pg_catalog.clock_timestamp()))" _null_ _null_ _null_ ));
DESCR("sleep for the specified interval");
DATA(insert OID = 3936 ( pg_sleep_until			PGNSP PGUID 14 1 0 0 0 f f f f t f v 1 0 2278 "1184" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.pg_sleep(extract(epoch from $1) operator(pg_catalog.-) extract(epoch from pg_catalog.clock_timestamp()))" _null_ _null_ _null_ ));
DESCR("sleep until the specified time");

DATA(insert OID = 2971 (  text				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "16" _null_ _null_ _null_ _null_ _null_ booltext _null_ _null_ _null_ ));
DESCR("convert boolean to text");

/* Aggregates (moved here from pg_aggregate for 7.3) */

DATA(insert OID = 2100 (  avg				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("the average (arithmetic mean) as numeric of all bigint values");
DATA(insert OID = 2101 (  avg				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "23" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("the average (arithmetic mean) as numeric of all integer values");
DATA(insert OID = 2102 (  avg				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "21" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("the average (arithmetic mean) as numeric of all smallint values");
DATA(insert OID = 2103 (  avg				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("the average (arithmetic mean) as numeric of all numeric values");
DATA(insert OID = 2104 (  avg				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "700" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("the average (arithmetic mean) as float8 of all float4 values");
DATA(insert OID = 2105 (  avg				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("the average (arithmetic mean) as float8 of all float8 values");
DATA(insert OID = 2106 (  avg				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1186 "1186" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("the average (arithmetic mean) as interval of all interval values");

DATA(insert OID = 2107 (  sum				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum as numeric across all bigint input values");
DATA(insert OID = 2108 (  sum				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 20 "23" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum as bigint across all integer input values");
DATA(insert OID = 2109 (  sum				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 20 "21" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum as bigint across all smallint input values");
DATA(insert OID = 2110 (  sum				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 700 "700" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum as float4 across all float4 input values");
DATA(insert OID = 2111 (  sum				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum as float8 across all float8 input values");
DATA(insert OID = 2112 (  sum				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 790 "790" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum as money across all money input values");
DATA(insert OID = 2113 (  sum				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1186 "1186" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum as interval across all interval input values");
DATA(insert OID = 2114 (  sum				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum as numeric across all numeric input values");

DATA(insert OID = 2115 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all bigint input values");
DATA(insert OID = 2116 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all integer input values");
DATA(insert OID = 2117 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 21 "21" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all smallint input values");
DATA(insert OID = 2118 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 26 "26" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all oid input values");
DATA(insert OID = 2119 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 700 "700" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all float4 input values");
DATA(insert OID = 2120 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all float8 input values");
DATA(insert OID = 2121 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 702 "702" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all abstime input values");
DATA(insert OID = 2122 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1082 "1082" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all date input values");
DATA(insert OID = 2123 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1083 "1083" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all time input values");
DATA(insert OID = 2124 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1266 "1266" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all time with time zone input values");
DATA(insert OID = 2125 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 790 "790" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all money input values");
DATA(insert OID = 2126 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1114 "1114" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all timestamp input values");
DATA(insert OID = 2127 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1184 "1184" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all timestamp with time zone input values");
DATA(insert OID = 2128 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1186 "1186" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all interval input values");
DATA(insert OID = 2129 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all text input values");
DATA(insert OID = 2130 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all numeric input values");
DATA(insert OID = 2050 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 2277 "2277" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all anyarray input values");
DATA(insert OID = 2244 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1042 "1042" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all bpchar input values");
DATA(insert OID = 2797 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 27 "27" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all tid input values");
DATA(insert OID = 3564 (  max				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 869 "869" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all inet input values");

DATA(insert OID = 2131 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all bigint input values");
DATA(insert OID = 2132 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all integer input values");
DATA(insert OID = 2133 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 21 "21" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all smallint input values");
DATA(insert OID = 2134 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 26 "26" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all oid input values");
DATA(insert OID = 2135 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 700 "700" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all float4 input values");
DATA(insert OID = 2136 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all float8 input values");
DATA(insert OID = 2137 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 702 "702" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all abstime input values");
DATA(insert OID = 2138 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1082 "1082" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all date input values");
DATA(insert OID = 2139 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1083 "1083" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all time input values");
DATA(insert OID = 2140 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1266 "1266" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all time with time zone input values");
DATA(insert OID = 2141 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 790 "790" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all money input values");
DATA(insert OID = 2142 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1114 "1114" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all timestamp input values");
DATA(insert OID = 2143 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1184 "1184" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all timestamp with time zone input values");
DATA(insert OID = 2144 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1186 "1186" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all interval input values");
DATA(insert OID = 2145 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all text values");
DATA(insert OID = 2146 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all numeric input values");
DATA(insert OID = 2051 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 2277 "2277" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all anyarray input values");
DATA(insert OID = 2245 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1042 "1042" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all bpchar input values");
DATA(insert OID = 2798 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 27 "27" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all tid input values");
DATA(insert OID = 3565 (  min				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 869 "869" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all inet input values");

/* count has two forms: count(any) and count(*) */
DATA(insert OID = 2147 (  count				PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 20 "2276" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("number of input rows for which the input expression is not null");
DATA(insert OID = 2803 (  count				PGNSP PGUID 12 1 0 0 0 t f f f f f i 0 0 20 "" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("number of input rows");

DATA(insert OID = 2718 (  var_pop			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population variance of bigint input values (square of the population standard deviation)");
DATA(insert OID = 2719 (  var_pop			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "23" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population variance of integer input values (square of the population standard deviation)");
DATA(insert OID = 2720 (  var_pop			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "21" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population variance of smallint input values (square of the population standard deviation)");
DATA(insert OID = 2721 (  var_pop			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "700" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population variance of float4 input values (square of the population standard deviation)");
DATA(insert OID = 2722 (  var_pop			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population variance of float8 input values (square of the population standard deviation)");
DATA(insert OID = 2723 (  var_pop			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("population variance of numeric input values (square of the population standard deviation)");

DATA(insert OID = 2641 (  var_samp			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample variance of bigint input values (square of the sample standard deviation)");
DATA(insert OID = 2642 (  var_samp			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "23" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample variance of integer input values (square of the sample standard deviation)");
DATA(insert OID = 2643 (  var_samp			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "21" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample variance of smallint input values (square of the sample standard deviation)");
DATA(insert OID = 2644 (  var_samp			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "700" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample variance of float4 input values (square of the sample standard deviation)");

DATA(insert OID = 2645 (  var_samp			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample variance of float8 input values (square of the sample standard deviation)");
DATA(insert OID = 2646 (  var_samp			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample variance of numeric input values (square of the sample standard deviation)");

DATA(insert OID = 2148 (  variance			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for var_samp");
DATA(insert OID = 2149 (  variance			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "23" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for var_samp");
DATA(insert OID = 2150 (  variance			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "21" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for var_samp");
DATA(insert OID = 2151 (  variance			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "700" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for var_samp");
DATA(insert OID = 2152 (  variance			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for var_samp");
DATA(insert OID = 2153 (  variance			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for var_samp");

DATA(insert OID = 2724 (  stddev_pop		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population standard deviation of bigint input values");
DATA(insert OID = 2725 (  stddev_pop		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "23" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population standard deviation of integer input values");
DATA(insert OID = 2726 (  stddev_pop		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "21" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population standard deviation of smallint input values");
DATA(insert OID = 2727 (  stddev_pop		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "700" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population standard deviation of float4 input values");
DATA(insert OID = 2728 (  stddev_pop		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population standard deviation of float8 input values");
DATA(insert OID = 2729 (  stddev_pop		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("population standard deviation of numeric input values");

DATA(insert OID = 2712 (  stddev_samp		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample standard deviation of bigint input values");
DATA(insert OID = 2713 (  stddev_samp		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "23" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample standard deviation of integer input values");
DATA(insert OID = 2714 (  stddev_samp		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "21" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample standard deviation of smallint input values");
DATA(insert OID = 2715 (  stddev_samp		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "700" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample standard deviation of float4 input values");
DATA(insert OID = 2716 (  stddev_samp		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample standard deviation of float8 input values");
DATA(insert OID = 2717 (  stddev_samp		PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample standard deviation of numeric input values");

DATA(insert OID = 2154 (  stddev			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "20" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for stddev_samp");
DATA(insert OID = 2155 (  stddev			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "23" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for stddev_samp");
DATA(insert OID = 2156 (  stddev			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "21" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for stddev_samp");
DATA(insert OID = 2157 (  stddev			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "700" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for stddev_samp");
DATA(insert OID = 2158 (  stddev			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 701 "701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for stddev_samp");
DATA(insert OID = 2159 (  stddev			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1700 "1700" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("historical alias for stddev_samp");

DATA(insert OID = 2818 (  regr_count		PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 20 "701 701" _null_ _null_ _null_ _null_  _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("number of input rows in which both expressions are not null");
DATA(insert OID = 2819 (  regr_sxx			PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum of squares of the independent variable (sum(X^2) - sum(X)^2/N)");
DATA(insert OID = 2820 (  regr_syy			PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum of squares of the dependent variable (sum(Y^2) - sum(Y)^2/N)");
DATA(insert OID = 2821 (  regr_sxy			PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sum of products of independent times dependent variable (sum(X*Y) - sum(X) * sum(Y)/N)");
DATA(insert OID = 2822 (  regr_avgx			PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("average of the independent variable (sum(X)/N)");
DATA(insert OID = 2823 (  regr_avgy			PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("average of the dependent variable (sum(Y)/N)");
DATA(insert OID = 2824 (  regr_r2			PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("square of the correlation coefficient");
DATA(insert OID = 2825 (  regr_slope		PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("slope of the least-squares-fit linear equation determined by the (X, Y) pairs");
DATA(insert OID = 2826 (  regr_intercept	PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("y-intercept of the least-squares-fit linear equation determined by the (X, Y) pairs");

DATA(insert OID = 2827 (  covar_pop			PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("population covariance");
DATA(insert OID = 2828 (  covar_samp		PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("sample covariance");
DATA(insert OID = 2829 (  corr				PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("correlation coefficient");

DATA(insert OID = 2160 ( text_pattern_lt	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ text_pattern_lt _null_ _null_ _null_ ));
DATA(insert OID = 2161 ( text_pattern_le	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ text_pattern_le _null_ _null_ _null_ ));
DATA(insert OID = 2163 ( text_pattern_ge	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ text_pattern_ge _null_ _null_ _null_ ));
DATA(insert OID = 2164 ( text_pattern_gt	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ text_pattern_gt _null_ _null_ _null_ ));
DATA(insert OID = 2166 ( bttext_pattern_cmp  PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "25 25" _null_ _null_ _null_ _null_ _null_ bttext_pattern_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 2174 ( bpchar_pattern_lt	  PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpchar_pattern_lt _null_ _null_ _null_ ));
DATA(insert OID = 2175 ( bpchar_pattern_le	  PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpchar_pattern_le _null_ _null_ _null_ ));
DATA(insert OID = 2177 ( bpchar_pattern_ge	  PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpchar_pattern_ge _null_ _null_ _null_ ));
DATA(insert OID = 2178 ( bpchar_pattern_gt	  PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1042 1042" _null_ _null_ _null_ _null_ _null_ bpchar_pattern_gt _null_ _null_ _null_ ));
DATA(insert OID = 2180 ( btbpchar_pattern_cmp PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1042 1042" _null_ _null_ _null_ _null_ _null_ btbpchar_pattern_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 2188 ( btint48cmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 20" _null_ _null_ _null_ _null_ _null_ btint48cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 2189 ( btint84cmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "20 23" _null_ _null_ _null_ _null_ _null_ btint84cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 2190 ( btint24cmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "21 23" _null_ _null_ _null_ _null_ _null_ btint24cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 2191 ( btint42cmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "23 21" _null_ _null_ _null_ _null_ _null_ btint42cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 2192 ( btint28cmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "21 20" _null_ _null_ _null_ _null_ _null_ btint28cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 2193 ( btint82cmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "20 21" _null_ _null_ _null_ _null_ _null_ btint82cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 2194 ( btfloat48cmp		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "700 701" _null_ _null_ _null_ _null_ _null_ btfloat48cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 2195 ( btfloat84cmp		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "701 700" _null_ _null_ _null_ _null_ _null_ btfloat84cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 2212 (  regprocedurein	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2202 "2275" _null_ _null_ _null_ _null_ _null_ regprocedurein _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2213 (  regprocedureout	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "2202" _null_ _null_ _null_ _null_ _null_ regprocedureout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2214 (  regoperin			PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2203 "2275" _null_ _null_ _null_ _null_ _null_ regoperin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2215 (  regoperout		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "2203" _null_ _null_ _null_ _null_ _null_ regoperout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3492 (  to_regoper		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2203 "2275" _null_ _null_ _null_ _null_ _null_ to_regoper _null_ _null_ _null_ ));
DESCR("convert operator name to regoper");
DATA(insert OID = 3476 (  to_regoperator	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2204 "2275" _null_ _null_ _null_ _null_ _null_ to_regoperator _null_ _null_ _null_ ));
DESCR("convert operator name to regoperator");
DATA(insert OID = 2216 (  regoperatorin		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2204 "2275" _null_ _null_ _null_ _null_ _null_ regoperatorin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2217 (  regoperatorout	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "2204" _null_ _null_ _null_ _null_ _null_ regoperatorout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2218 (  regclassin		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2205 "2275" _null_ _null_ _null_ _null_ _null_ regclassin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2219 (  regclassout		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "2205" _null_ _null_ _null_ _null_ _null_ regclassout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3495 (  to_regclass		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2205 "2275" _null_ _null_ _null_ _null_ _null_ to_regclass _null_ _null_ _null_ ));
DESCR("convert classname to regclass");
DATA(insert OID = 2220 (  regtypein			PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2206 "2275" _null_ _null_ _null_ _null_ _null_ regtypein _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2221 (  regtypeout		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "2206" _null_ _null_ _null_ _null_ _null_ regtypeout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3493 (  to_regtype		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2206 "2275" _null_ _null_ _null_ _null_ _null_ to_regtype _null_ _null_ _null_ ));
DESCR("convert type name to regtype");
DATA(insert OID = 1079 (  regclass			PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2205 "25" _null_ _null_ _null_ _null_ _null_	text_regclass _null_ _null_ _null_ ));
DESCR("convert text to regclass");

DATA(insert OID = 4098 (  regrolein			PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 4096 "2275" _null_ _null_ _null_ _null_ _null_ regrolein _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 4092 (  regroleout		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "4096" _null_ _null_ _null_ _null_ _null_ regroleout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 4093 (  to_regrole		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 4096 "2275" _null_ _null_ _null_ _null_ _null_ to_regrole _null_ _null_ _null_ ));
DESCR("convert role name to regrole");

DATA(insert OID = 4084 (  regnamespacein	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 4089 "2275" _null_ _null_ _null_ _null_ _null_ regnamespacein _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 4085 (  regnamespaceout	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "4089" _null_ _null_ _null_ _null_ _null_ regnamespaceout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 4086 (  to_regnamespace	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 4089 "2275" _null_ _null_ _null_ _null_ _null_ to_regnamespace _null_ _null_ _null_ ));
DESCR("convert namespace name to regnamespace");

DATA(insert OID = 2246 ( fmgr_internal_validator PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ fmgr_internal_validator _null_ _null_ _null_ ));
DESCR("(internal)");
DATA(insert OID = 2247 ( fmgr_c_validator	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ fmgr_c_validator _null_ _null_ _null_ ));
DESCR("(internal)");
DATA(insert OID = 2248 ( fmgr_sql_validator PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ fmgr_sql_validator _null_ _null_ _null_ ));
DESCR("(internal)");

DATA(insert OID = 2250 (  has_database_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_database_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on database by username, database name");
DATA(insert OID = 2251 (  has_database_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_database_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on database by username, database oid");
DATA(insert OID = 2252 (  has_database_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_database_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on database by user oid, database name");
DATA(insert OID = 2253 (  has_database_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_database_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on database by user oid, database oid");
DATA(insert OID = 2254 (  has_database_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_database_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on database by database name");
DATA(insert OID = 2255 (  has_database_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_database_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on database by database oid");

DATA(insert OID = 2256 (  has_function_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_function_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on function by username, function name");
DATA(insert OID = 2257 (  has_function_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_function_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on function by username, function oid");
DATA(insert OID = 2258 (  has_function_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_function_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on function by user oid, function name");
DATA(insert OID = 2259 (  has_function_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_function_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on function by user oid, function oid");
DATA(insert OID = 2260 (  has_function_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_function_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on function by function name");
DATA(insert OID = 2261 (  has_function_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_function_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on function by function oid");

DATA(insert OID = 2262 (  has_language_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_language_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on language by username, language name");
DATA(insert OID = 2263 (  has_language_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_language_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on language by username, language oid");
DATA(insert OID = 2264 (  has_language_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_language_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on language by user oid, language name");
DATA(insert OID = 2265 (  has_language_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_language_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on language by user oid, language oid");
DATA(insert OID = 2266 (  has_language_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_language_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on language by language name");
DATA(insert OID = 2267 (  has_language_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_language_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on language by language oid");

DATA(insert OID = 2268 (  has_schema_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_schema_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on schema by username, schema name");
DATA(insert OID = 2269 (  has_schema_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_schema_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on schema by username, schema oid");
DATA(insert OID = 2270 (  has_schema_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_schema_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on schema by user oid, schema name");
DATA(insert OID = 2271 (  has_schema_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_schema_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on schema by user oid, schema oid");
DATA(insert OID = 2272 (  has_schema_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_schema_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on schema by schema name");
DATA(insert OID = 2273 (  has_schema_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_schema_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on schema by schema oid");

DATA(insert OID = 2390 (  has_tablespace_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_tablespace_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on tablespace by username, tablespace name");
DATA(insert OID = 2391 (  has_tablespace_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_tablespace_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on tablespace by username, tablespace oid");
DATA(insert OID = 2392 (  has_tablespace_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_tablespace_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on tablespace by user oid, tablespace name");
DATA(insert OID = 2393 (  has_tablespace_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_tablespace_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on tablespace by user oid, tablespace oid");
DATA(insert OID = 2394 (  has_tablespace_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_tablespace_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on tablespace by tablespace name");
DATA(insert OID = 2395 (  has_tablespace_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_tablespace_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on tablespace by tablespace oid");

DATA(insert OID = 3000 (  has_foreign_data_wrapper_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_foreign_data_wrapper_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on foreign data wrapper by username, foreign data wrapper name");
DATA(insert OID = 3001 (  has_foreign_data_wrapper_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_foreign_data_wrapper_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on foreign data wrapper by username, foreign data wrapper oid");
DATA(insert OID = 3002 (  has_foreign_data_wrapper_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_foreign_data_wrapper_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on foreign data wrapper by user oid, foreign data wrapper name");
DATA(insert OID = 3003 (  has_foreign_data_wrapper_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_foreign_data_wrapper_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on foreign data wrapper by user oid, foreign data wrapper oid");
DATA(insert OID = 3004 (  has_foreign_data_wrapper_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_foreign_data_wrapper_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on foreign data wrapper by foreign data wrapper name");
DATA(insert OID = 3005 (  has_foreign_data_wrapper_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_foreign_data_wrapper_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on foreign data wrapper by foreign data wrapper oid");

DATA(insert OID = 3006 (  has_server_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_server_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on server by username, server name");
DATA(insert OID = 3007 (  has_server_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_server_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on server by username, server oid");
DATA(insert OID = 3008 (  has_server_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_server_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on server by user oid, server name");
DATA(insert OID = 3009 (  has_server_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_server_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on server by user oid, server oid");
DATA(insert OID = 3010 (  has_server_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_server_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on server by server name");
DATA(insert OID = 3011 (  has_server_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_server_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on server by server oid");

DATA(insert OID = 3138 (  has_type_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 25 25" _null_ _null_ _null_ _null_ _null_	has_type_privilege_name_name _null_ _null_ _null_ ));
DESCR("user privilege on type by username, type name");
DATA(insert OID = 3139 (  has_type_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	has_type_privilege_name_id _null_ _null_ _null_ ));
DESCR("user privilege on type by username, type oid");
DATA(insert OID = 3140 (  has_type_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 25 25" _null_ _null_ _null_ _null_ _null_	has_type_privilege_id_name _null_ _null_ _null_ ));
DESCR("user privilege on type by user oid, type name");
DATA(insert OID = 3141 (  has_type_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	has_type_privilege_id_id _null_ _null_ _null_ ));
DESCR("user privilege on type by user oid, type oid");
DATA(insert OID = 3142 (  has_type_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ has_type_privilege_name _null_ _null_ _null_ ));
DESCR("current user privilege on type by type name");
DATA(insert OID = 3143 (  has_type_privilege		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ has_type_privilege_id _null_ _null_ _null_ ));
DESCR("current user privilege on type by type oid");

DATA(insert OID = 2705 (  pg_has_role		PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 19 25" _null_ _null_ _null_ _null_ _null_	pg_has_role_name_name _null_ _null_ _null_ ));
DESCR("user privilege on role by username, role name");
DATA(insert OID = 2706 (  pg_has_role		PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "19 26 25" _null_ _null_ _null_ _null_ _null_	pg_has_role_name_id _null_ _null_ _null_ ));
DESCR("user privilege on role by username, role oid");
DATA(insert OID = 2707 (  pg_has_role		PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 19 25" _null_ _null_ _null_ _null_ _null_	pg_has_role_id_name _null_ _null_ _null_ ));
DESCR("user privilege on role by user oid, role name");
DATA(insert OID = 2708 (  pg_has_role		PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 16 "26 26 25" _null_ _null_ _null_ _null_ _null_	pg_has_role_id_id _null_ _null_ _null_ ));
DESCR("user privilege on role by user oid, role oid");
DATA(insert OID = 2709 (  pg_has_role		PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "19 25" _null_ _null_ _null_ _null_ _null_ pg_has_role_name _null_ _null_ _null_ ));
DESCR("current user privilege on role by role name");
DATA(insert OID = 2710 (  pg_has_role		PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "26 25" _null_ _null_ _null_ _null_ _null_ pg_has_role_id _null_ _null_ _null_ ));
DESCR("current user privilege on role by role oid");

DATA(insert OID = 1269 (  pg_column_size		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 23 "2276" _null_ _null_ _null_ _null_ _null_	pg_column_size _null_ _null_ _null_ ));
DESCR("bytes required to store the value, perhaps with compression");
DATA(insert OID = 2322 ( pg_tablespace_size		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_tablespace_size_oid _null_ _null_ _null_ ));
DESCR("total disk space usage for the specified tablespace");
DATA(insert OID = 2323 ( pg_tablespace_size		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "19" _null_ _null_ _null_ _null_ _null_ pg_tablespace_size_name _null_ _null_ _null_ ));
DESCR("total disk space usage for the specified tablespace");
DATA(insert OID = 2324 ( pg_database_size		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "26" _null_ _null_ _null_ _null_ _null_ pg_database_size_oid _null_ _null_ _null_ ));
DESCR("total disk space usage for the specified database");
DATA(insert OID = 2168 ( pg_database_size		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "19" _null_ _null_ _null_ _null_ _null_ pg_database_size_name _null_ _null_ _null_ ));
DESCR("total disk space usage for the specified database");
DATA(insert OID = 2325 ( pg_relation_size		PGNSP PGUID 14 1 0 0 0 f f f f t f v 1 0 20 "2205" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.pg_relation_size($1, ''main'')" _null_ _null_ _null_ ));
DESCR("disk space usage for the main fork of the specified table or index");
DATA(insert OID = 2332 ( pg_relation_size		PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 20 "2205 25" _null_ _null_ _null_ _null_ _null_ pg_relation_size _null_ _null_ _null_ ));
DESCR("disk space usage for the specified fork of a table or index");
DATA(insert OID = 2286 ( pg_total_relation_size PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "2205" _null_ _null_ _null_ _null_ _null_ pg_total_relation_size _null_ _null_ _null_ ));
DESCR("total disk space usage for the specified table and associated indexes");
DATA(insert OID = 2288 ( pg_size_pretty			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 25 "20" _null_ _null_ _null_ _null_ _null_ pg_size_pretty _null_ _null_ _null_ ));
DESCR("convert a long int to a human readable text using size units");
DATA(insert OID = 3166 ( pg_size_pretty			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 25 "1700" _null_ _null_ _null_ _null_ _null_ pg_size_pretty_numeric _null_ _null_ _null_ ));
DESCR("convert a numeric to a human readable text using size units");
DATA(insert OID = 2997 ( pg_table_size			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "2205" _null_ _null_ _null_ _null_ _null_ pg_table_size _null_ _null_ _null_ ));
DESCR("disk space usage for the specified table, including TOAST, free space and visibility map");
DATA(insert OID = 2998 ( pg_indexes_size		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 20 "2205" _null_ _null_ _null_ _null_ _null_ pg_indexes_size _null_ _null_ _null_ ));
DESCR("disk space usage for all indexes attached to the specified table");
DATA(insert OID = 2999 ( pg_relation_filenode	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 26 "2205" _null_ _null_ _null_ _null_ _null_ pg_relation_filenode _null_ _null_ _null_ ));
DESCR("filenode identifier of relation");
DATA(insert OID = 3454 ( pg_filenode_relation PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 2205 "26 26" _null_ _null_ _null_ _null_ _null_ pg_filenode_relation _null_ _null_ _null_ ));
DESCR("relation OID for filenode and tablespace");
DATA(insert OID = 3034 ( pg_relation_filepath	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "2205" _null_ _null_ _null_ _null_ _null_ pg_relation_filepath _null_ _null_ _null_ ));
DESCR("file path of relation");

DATA(insert OID = 2316 ( postgresql_fdw_validator PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1009 26" _null_ _null_ _null_ _null_ _null_ postgresql_fdw_validator _null_ _null_ _null_));
DESCR("(internal)");

DATA(insert OID = 2290 (  record_in			PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 2249 "2275 26 23" _null_ _null_ _null_ _null_ _null_	record_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2291 (  record_out		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "2249" _null_ _null_ _null_ _null_ _null_ record_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2292 (  cstring_in		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2275" _null_ _null_ _null_ _null_ _null_ cstring_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2293 (  cstring_out		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2275" _null_ _null_ _null_ _null_ _null_ cstring_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2294 (  any_in			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2276 "2275" _null_ _null_ _null_ _null_ _null_ any_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2295 (  any_out			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2276" _null_ _null_ _null_ _null_ _null_ any_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2296 (  anyarray_in		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2277 "2275" _null_ _null_ _null_ _null_ _null_ anyarray_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2297 (  anyarray_out		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "2277" _null_ _null_ _null_ _null_ _null_ anyarray_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2298 (  void_in			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2275" _null_ _null_ _null_ _null_ _null_ void_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2299 (  void_out			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2278" _null_ _null_ _null_ _null_ _null_ void_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2300 (  trigger_in		PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 2279 "2275" _null_ _null_ _null_ _null_ _null_ trigger_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2301 (  trigger_out		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2279" _null_ _null_ _null_ _null_ _null_ trigger_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3594 (  event_trigger_in	PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 3838 "2275" _null_ _null_ _null_ _null_ _null_ event_trigger_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3595 (  event_trigger_out PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "3838" _null_ _null_ _null_ _null_ _null_ event_trigger_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2302 (  language_handler_in	PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 2280 "2275" _null_ _null_ _null_ _null_ _null_ language_handler_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2303 (  language_handler_out	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2280" _null_ _null_ _null_ _null_ _null_ language_handler_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2304 (  internal_in		PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 2281 "2275" _null_ _null_ _null_ _null_ _null_ internal_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2305 (  internal_out		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2281" _null_ _null_ _null_ _null_ _null_ internal_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2306 (  opaque_in			PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 2282 "2275" _null_ _null_ _null_ _null_ _null_ opaque_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2307 (  opaque_out		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2282" _null_ _null_ _null_ _null_ _null_ opaque_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2312 (  anyelement_in		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2283 "2275" _null_ _null_ _null_ _null_ _null_ anyelement_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2313 (  anyelement_out	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2283" _null_ _null_ _null_ _null_ _null_ anyelement_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2398 (  shell_in			PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 2282 "2275" _null_ _null_ _null_ _null_ _null_ shell_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2399 (  shell_out			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2282" _null_ _null_ _null_ _null_ _null_ shell_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2597 (  domain_in			PGNSP PGUID 12 1 0 0 0 f f f f f f s 3 0 2276 "2275 26 23" _null_ _null_ _null_ _null_ _null_ domain_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2598 (  domain_recv		PGNSP PGUID 12 1 0 0 0 f f f f f f s 3 0 2276 "2281 26 23" _null_ _null_ _null_ _null_ _null_ domain_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2777 (  anynonarray_in	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2776 "2275" _null_ _null_ _null_ _null_ _null_ anynonarray_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2778 (  anynonarray_out	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2776" _null_ _null_ _null_ _null_ _null_ anynonarray_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3116 (  fdw_handler_in	PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 3115 "2275" _null_ _null_ _null_ _null_ _null_ fdw_handler_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3117 (  fdw_handler_out	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "3115" _null_ _null_ _null_ _null_ _null_ fdw_handler_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3311 (  tsm_handler_in	PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 3310 "2275" _null_ _null_ _null_ _null_ _null_ tsm_handler_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3312 (  tsm_handler_out	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "3310" _null_ _null_ _null_ _null_ _null_ tsm_handler_out _null_ _null_ _null_ ));
DESCR("I/O");

/* tablesample method handlers */
DATA(insert OID = 3313 (  bernoulli			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 3310 "2281" _null_ _null_ _null_ _null_ _null_ tsm_bernoulli_handler _null_ _null_ _null_ ));
DESCR("BERNOULLI tablesample method handler");
DATA(insert OID = 3314 (  system			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 3310 "2281" _null_ _null_ _null_ _null_ _null_ tsm_system_handler _null_ _null_ _null_ ));
DESCR("SYSTEM tablesample method handler");

/* cryptographic */
DATA(insert OID =  2311 (  md5	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "25" _null_ _null_ _null_ _null_ _null_ md5_text _null_ _null_ _null_ ));
DESCR("MD5 hash");
DATA(insert OID =  2321 (  md5	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "17" _null_ _null_ _null_ _null_ _null_ md5_bytea _null_ _null_ _null_ ));
DESCR("MD5 hash");

/* crosstype operations for date vs. timestamp and timestamptz */
DATA(insert OID = 2338 (  date_lt_timestamp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1082 1114" _null_ _null_ _null_ _null_ _null_ date_lt_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2339 (  date_le_timestamp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1082 1114" _null_ _null_ _null_ _null_ _null_ date_le_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2340 (  date_eq_timestamp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1082 1114" _null_ _null_ _null_ _null_ _null_ date_eq_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2341 (  date_gt_timestamp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1082 1114" _null_ _null_ _null_ _null_ _null_ date_gt_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2342 (  date_ge_timestamp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1082 1114" _null_ _null_ _null_ _null_ _null_ date_ge_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2343 (  date_ne_timestamp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1082 1114" _null_ _null_ _null_ _null_ _null_ date_ne_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2344 (  date_cmp_timestamp	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1082 1114" _null_ _null_ _null_ _null_ _null_ date_cmp_timestamp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 2351 (  date_lt_timestamptz	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1082 1184" _null_ _null_ _null_ _null_ _null_ date_lt_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2352 (  date_le_timestamptz	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1082 1184" _null_ _null_ _null_ _null_ _null_ date_le_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2353 (  date_eq_timestamptz	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1082 1184" _null_ _null_ _null_ _null_ _null_ date_eq_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2354 (  date_gt_timestamptz	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1082 1184" _null_ _null_ _null_ _null_ _null_ date_gt_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2355 (  date_ge_timestamptz	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1082 1184" _null_ _null_ _null_ _null_ _null_ date_ge_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2356 (  date_ne_timestamptz	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1082 1184" _null_ _null_ _null_ _null_ _null_ date_ne_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2357 (  date_cmp_timestamptz	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 23 "1082 1184" _null_ _null_ _null_ _null_ _null_ date_cmp_timestamptz _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 2364 (  timestamp_lt_date		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1114 1082" _null_ _null_ _null_ _null_ _null_ timestamp_lt_date _null_ _null_ _null_ ));
DATA(insert OID = 2365 (  timestamp_le_date		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1114 1082" _null_ _null_ _null_ _null_ _null_ timestamp_le_date _null_ _null_ _null_ ));
DATA(insert OID = 2366 (  timestamp_eq_date		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1114 1082" _null_ _null_ _null_ _null_ _null_ timestamp_eq_date _null_ _null_ _null_ ));
DATA(insert OID = 2367 (  timestamp_gt_date		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1114 1082" _null_ _null_ _null_ _null_ _null_ timestamp_gt_date _null_ _null_ _null_ ));
DATA(insert OID = 2368 (  timestamp_ge_date		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1114 1082" _null_ _null_ _null_ _null_ _null_ timestamp_ge_date _null_ _null_ _null_ ));
DATA(insert OID = 2369 (  timestamp_ne_date		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "1114 1082" _null_ _null_ _null_ _null_ _null_ timestamp_ne_date _null_ _null_ _null_ ));
DATA(insert OID = 2370 (  timestamp_cmp_date	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "1114 1082" _null_ _null_ _null_ _null_ _null_ timestamp_cmp_date _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 2377 (  timestamptz_lt_date	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1082" _null_ _null_ _null_ _null_ _null_ timestamptz_lt_date _null_ _null_ _null_ ));
DATA(insert OID = 2378 (  timestamptz_le_date	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1082" _null_ _null_ _null_ _null_ _null_ timestamptz_le_date _null_ _null_ _null_ ));
DATA(insert OID = 2379 (  timestamptz_eq_date	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1082" _null_ _null_ _null_ _null_ _null_ timestamptz_eq_date _null_ _null_ _null_ ));
DATA(insert OID = 2380 (  timestamptz_gt_date	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1082" _null_ _null_ _null_ _null_ _null_ timestamptz_gt_date _null_ _null_ _null_ ));
DATA(insert OID = 2381 (  timestamptz_ge_date	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1082" _null_ _null_ _null_ _null_ _null_ timestamptz_ge_date _null_ _null_ _null_ ));
DATA(insert OID = 2382 (  timestamptz_ne_date	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1082" _null_ _null_ _null_ _null_ _null_ timestamptz_ne_date _null_ _null_ _null_ ));
DATA(insert OID = 2383 (  timestamptz_cmp_date	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 23 "1184 1082" _null_ _null_ _null_ _null_ _null_ timestamptz_cmp_date _null_ _null_ _null_ ));
DESCR("less-equal-greater");

/* crosstype operations for timestamp vs. timestamptz */
DATA(insert OID = 2520 (  timestamp_lt_timestamptz	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1114 1184" _null_ _null_ _null_ _null_ _null_ timestamp_lt_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2521 (  timestamp_le_timestamptz	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1114 1184" _null_ _null_ _null_ _null_ _null_ timestamp_le_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2522 (  timestamp_eq_timestamptz	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1114 1184" _null_ _null_ _null_ _null_ _null_ timestamp_eq_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2523 (  timestamp_gt_timestamptz	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1114 1184" _null_ _null_ _null_ _null_ _null_ timestamp_gt_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2524 (  timestamp_ge_timestamptz	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1114 1184" _null_ _null_ _null_ _null_ _null_ timestamp_ge_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2525 (  timestamp_ne_timestamptz	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1114 1184" _null_ _null_ _null_ _null_ _null_ timestamp_ne_timestamptz _null_ _null_ _null_ ));
DATA(insert OID = 2526 (  timestamp_cmp_timestamptz PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 23 "1114 1184" _null_ _null_ _null_ _null_ _null_ timestamp_cmp_timestamptz _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 2527 (  timestamptz_lt_timestamp	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1114" _null_ _null_ _null_ _null_ _null_ timestamptz_lt_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2528 (  timestamptz_le_timestamp	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1114" _null_ _null_ _null_ _null_ _null_ timestamptz_le_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2529 (  timestamptz_eq_timestamp	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1114" _null_ _null_ _null_ _null_ _null_ timestamptz_eq_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2530 (  timestamptz_gt_timestamp	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1114" _null_ _null_ _null_ _null_ _null_ timestamptz_gt_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2531 (  timestamptz_ge_timestamp	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1114" _null_ _null_ _null_ _null_ _null_ timestamptz_ge_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2532 (  timestamptz_ne_timestamp	PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "1184 1114" _null_ _null_ _null_ _null_ _null_ timestamptz_ne_timestamp _null_ _null_ _null_ ));
DATA(insert OID = 2533 (  timestamptz_cmp_timestamp PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 23 "1184 1114" _null_ _null_ _null_ _null_ _null_ timestamptz_cmp_timestamp _null_ _null_ _null_ ));
DESCR("less-equal-greater");


/* send/receive functions */
DATA(insert OID = 2400 (  array_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 2277 "2281 26 23" _null_ _null_ _null_ _null_  _null_ array_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2401 (  array_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "2277" _null_ _null_ _null_ _null_ _null_	array_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2402 (  record_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 2249 "2281 26 23" _null_ _null_ _null_ _null_  _null_ record_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2403 (  record_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "2249" _null_ _null_ _null_ _null_  _null_ record_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2404 (  int2recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 21 "2281" _null_ _null_ _null_ _null_ _null_	int2recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2405 (  int2send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "21" _null_ _null_ _null_ _null_ _null_ int2send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2406 (  int4recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "2281" _null_ _null_ _null_ _null_ _null_	int4recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2407 (  int4send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "23" _null_ _null_ _null_ _null_ _null_ int4send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2408 (  int8recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 20 "2281" _null_ _null_ _null_ _null_ _null_	int8recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2409 (  int8send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "20" _null_ _null_ _null_ _null_ _null_ int8send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2410 (  int2vectorrecv	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 22 "2281" _null_ _null_ _null_ _null_ _null_	int2vectorrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2411 (  int2vectorsend	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "22" _null_ _null_ _null_ _null_ _null_ int2vectorsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2412 (  bytearecv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "2281" _null_ _null_ _null_ _null_ _null_	bytearecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2413 (  byteasend			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "17" _null_ _null_ _null_ _null_ _null_ byteasend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2414 (  textrecv			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 25 "2281" _null_ _null_ _null_ _null_ _null_	textrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2415 (  textsend			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "25" _null_ _null_ _null_ _null_ _null_ textsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2416 (  unknownrecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 705 "2281" _null_ _null_ _null_ _null_ _null_	unknownrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2417 (  unknownsend		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "705" _null_ _null_ _null_ _null_ _null_ unknownsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2418 (  oidrecv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 26 "2281" _null_ _null_ _null_ _null_ _null_	oidrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2419 (  oidsend			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "26" _null_ _null_ _null_ _null_ _null_ oidsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2420 (  oidvectorrecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 30 "2281" _null_ _null_ _null_ _null_ _null_	oidvectorrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2421 (  oidvectorsend		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "30" _null_ _null_ _null_ _null_ _null_ oidvectorsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2422 (  namerecv			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 19 "2281" _null_ _null_ _null_ _null_ _null_	namerecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2423 (  namesend			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "19" _null_ _null_ _null_ _null_ _null_ namesend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2424 (  float4recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 700 "2281" _null_ _null_ _null_ _null_ _null_	float4recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2425 (  float4send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "700" _null_ _null_ _null_ _null_ _null_ float4send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2426 (  float8recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 701 "2281" _null_ _null_ _null_ _null_ _null_	float8recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2427 (  float8send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "701" _null_ _null_ _null_ _null_ _null_ float8send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2428 (  point_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 600 "2281" _null_ _null_ _null_ _null_ _null_	point_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2429 (  point_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "600" _null_ _null_ _null_ _null_ _null_ point_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2430 (  bpcharrecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 1042 "2281 26 23" _null_ _null_ _null_ _null_  _null_ bpcharrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2431 (  bpcharsend		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "1042" _null_ _null_ _null_ _null_ _null_	bpcharsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2432 (  varcharrecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 1043 "2281 26 23" _null_ _null_ _null_ _null_  _null_ varcharrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2433 (  varcharsend		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "1043" _null_ _null_ _null_ _null_ _null_	varcharsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2434 (  charrecv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 18 "2281" _null_ _null_ _null_ _null_ _null_	charrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2435 (  charsend			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "18" _null_ _null_ _null_ _null_ _null_ charsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2436 (  boolrecv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "2281" _null_ _null_ _null_ _null_ _null_	boolrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2437 (  boolsend			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "16" _null_ _null_ _null_ _null_ _null_ boolsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2438 (  tidrecv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 27 "2281" _null_ _null_ _null_ _null_ _null_	tidrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2439 (  tidsend			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "27" _null_ _null_ _null_ _null_ _null_ tidsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2440 (  xidrecv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 28 "2281" _null_ _null_ _null_ _null_ _null_	xidrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2441 (  xidsend			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "28" _null_ _null_ _null_ _null_ _null_ xidsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2442 (  cidrecv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 29 "2281" _null_ _null_ _null_ _null_ _null_	cidrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2443 (  cidsend			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "29" _null_ _null_ _null_ _null_ _null_ cidsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2444 (  regprocrecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 24 "2281" _null_ _null_ _null_ _null_ _null_	regprocrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2445 (  regprocsend		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "24" _null_ _null_ _null_ _null_ _null_ regprocsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2446 (  regprocedurerecv	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2202 "2281" _null_ _null_ _null_ _null_ _null_ regprocedurerecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2447 (  regproceduresend	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "2202" _null_ _null_ _null_ _null_ _null_	regproceduresend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2448 (  regoperrecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2203 "2281" _null_ _null_ _null_ _null_ _null_ regoperrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2449 (  regopersend		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "2203" _null_ _null_ _null_ _null_ _null_	regopersend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2450 (  regoperatorrecv	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2204 "2281" _null_ _null_ _null_ _null_ _null_ regoperatorrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2451 (  regoperatorsend	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "2204" _null_ _null_ _null_ _null_ _null_	regoperatorsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2452 (  regclassrecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2205 "2281" _null_ _null_ _null_ _null_ _null_ regclassrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2453 (  regclasssend		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "2205" _null_ _null_ _null_ _null_ _null_	regclasssend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2454 (  regtyperecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2206 "2281" _null_ _null_ _null_ _null_ _null_ regtyperecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2455 (  regtypesend		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "2206" _null_ _null_ _null_ _null_ _null_	regtypesend _null_ _null_ _null_ ));
DESCR("I/O");

DATA(insert OID = 4094 (  regrolerecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 4096 "2281" _null_ _null_ _null_ _null_ _null_	regrolerecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 4095 (  regrolesend		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "4096" _null_ _null_ _null_ _null_ _null_	regrolesend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 4087 (  regnamespacerecv	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 4089 "2281" _null_ _null_ _null_ _null_ _null_ regnamespacerecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 4088 (  regnamespacesend	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "4089" _null_ _null_ _null_ _null_ _null_	regnamespacesend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2456 (  bit_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1560 "2281 26 23" _null_ _null_ _null_ _null_  _null_ bit_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2457 (  bit_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "1560" _null_ _null_ _null_ _null_ _null_	bit_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2458 (  varbit_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1562 "2281 26 23" _null_ _null_ _null_ _null_  _null_ varbit_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2459 (  varbit_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "1562" _null_ _null_ _null_ _null_ _null_	varbit_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2460 (  numeric_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1700 "2281 26 23" _null_ _null_ _null_ _null_  _null_ numeric_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2461 (  numeric_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "1700" _null_ _null_ _null_ _null_ _null_	numeric_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2462 (  abstimerecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 702 "2281" _null_ _null_ _null_ _null_ _null_	abstimerecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2463 (  abstimesend		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "702" _null_ _null_ _null_ _null_ _null_ abstimesend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2464 (  reltimerecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 703 "2281" _null_ _null_ _null_ _null_ _null_	reltimerecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2465 (  reltimesend		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "703" _null_ _null_ _null_ _null_ _null_ reltimesend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2466 (  tintervalrecv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 704 "2281" _null_ _null_ _null_ _null_ _null_	tintervalrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2467 (  tintervalsend		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "704" _null_ _null_ _null_ _null_ _null_ tintervalsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2468 (  date_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 1082 "2281" _null_ _null_ _null_ _null_ _null_ date_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2469 (  date_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "1082" _null_ _null_ _null_ _null_ _null_	date_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2470 (  time_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1083 "2281 26 23" _null_ _null_ _null_ _null_  _null_ time_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2471 (  time_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "1083" _null_ _null_ _null_ _null_ _null_	time_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2472 (  timetz_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1266 "2281 26 23" _null_ _null_ _null_ _null_  _null_ timetz_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2473 (  timetz_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "1266" _null_ _null_ _null_ _null_ _null_	timetz_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2474 (  timestamp_recv	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1114 "2281 26 23" _null_ _null_ _null_ _null_  _null_ timestamp_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2475 (  timestamp_send	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "1114" _null_ _null_ _null_ _null_ _null_	timestamp_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2476 (  timestamptz_recv	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1184 "2281 26 23" _null_ _null_ _null_ _null_  _null_ timestamptz_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2477 (  timestamptz_send	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "1184" _null_ _null_ _null_ _null_ _null_	timestamptz_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2478 (  interval_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1186 "2281 26 23" _null_ _null_ _null_ _null_  _null_ interval_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2479 (  interval_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "1186" _null_ _null_ _null_ _null_ _null_	interval_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2480 (  lseg_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 601 "2281" _null_ _null_ _null_ _null_ _null_	lseg_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2481 (  lseg_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "601" _null_ _null_ _null_ _null_ _null_ lseg_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2482 (  path_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 602 "2281" _null_ _null_ _null_ _null_ _null_	path_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2483 (  path_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "602" _null_ _null_ _null_ _null_ _null_ path_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2484 (  box_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 603 "2281" _null_ _null_ _null_ _null_ _null_	box_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2485 (  box_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "603" _null_ _null_ _null_ _null_ _null_ box_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2486 (  poly_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 604 "2281" _null_ _null_ _null_ _null_ _null_	poly_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2487 (  poly_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "604" _null_ _null_ _null_ _null_ _null_ poly_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2488 (  line_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 628 "2281" _null_ _null_ _null_ _null_ _null_	line_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2489 (  line_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "628" _null_ _null_ _null_ _null_ _null_ line_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2490 (  circle_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 718 "2281" _null_ _null_ _null_ _null_ _null_	circle_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2491 (  circle_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "718" _null_ _null_ _null_ _null_ _null_ circle_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2492 (  cash_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 790 "2281" _null_ _null_ _null_ _null_ _null_	cash_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2493 (  cash_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "790" _null_ _null_ _null_ _null_ _null_ cash_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2494 (  macaddr_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 829 "2281" _null_ _null_ _null_ _null_ _null_	macaddr_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2495 (  macaddr_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "829" _null_ _null_ _null_ _null_ _null_ macaddr_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2496 (  inet_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 869 "2281" _null_ _null_ _null_ _null_ _null_	inet_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2497 (  inet_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "869" _null_ _null_ _null_ _null_ _null_ inet_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2498 (  cidr_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 650 "2281" _null_ _null_ _null_ _null_ _null_	cidr_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2499 (  cidr_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "650" _null_ _null_ _null_ _null_ _null_ cidr_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2500 (  cstring_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "2281" _null_ _null_ _null_ _null_ _null_ cstring_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2501 (  cstring_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "2275" _null_ _null_ _null_ _null_ _null_	cstring_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2502 (  anyarray_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2277 "2281" _null_ _null_ _null_ _null_ _null_ anyarray_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2503 (  anyarray_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "2277" _null_ _null_ _null_ _null_ _null_	anyarray_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3120 (  void_recv			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ void_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3121 (  void_send			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "2278" _null_ _null_ _null_ _null_ _null_	void_send _null_ _null_ _null_ ));
DESCR("I/O");

/* System-view support functions with pretty-print option */
DATA(insert OID = 2504 (  pg_get_ruledef	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "26 16" _null_ _null_ _null_ _null_ _null_	pg_get_ruledef_ext _null_ _null_ _null_ ));
DESCR("source text of a rule with pretty-print option");
DATA(insert OID = 2505 (  pg_get_viewdef	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "25 16" _null_ _null_ _null_ _null_ _null_	pg_get_viewdef_name_ext _null_ _null_ _null_ ));
DESCR("select statement of a view with pretty-print option");
DATA(insert OID = 2506 (  pg_get_viewdef	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "26 16" _null_ _null_ _null_ _null_ _null_	pg_get_viewdef_ext _null_ _null_ _null_ ));
DESCR("select statement of a view with pretty-print option");
DATA(insert OID = 3159 (  pg_get_viewdef	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "26 23" _null_ _null_ _null_ _null_ _null_	pg_get_viewdef_wrap _null_ _null_ _null_ ));
DESCR("select statement of a view with pretty-printing and specified line wrapping");
DATA(insert OID = 2507 (  pg_get_indexdef	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 25 "26 23 16" _null_ _null_ _null_ _null_ _null_	pg_get_indexdef_ext _null_ _null_ _null_ ));
DESCR("index description (full create statement or single expression) with pretty-print option");
DATA(insert OID = 2508 (  pg_get_constraintdef PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "26 16" _null_ _null_ _null_ _null_ _null_	pg_get_constraintdef_ext _null_ _null_ _null_ ));
DESCR("constraint description with pretty-print option");
DATA(insert OID = 2509 (  pg_get_expr		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 25 "194 26 16" _null_ _null_ _null_ _null_ _null_ pg_get_expr_ext _null_ _null_ _null_ ));
DESCR("deparse an encoded expression with pretty-print option");
DATA(insert OID = 2510 (  pg_prepared_statement PGNSP PGUID 12 1 1000 0 0 f f f f t t s 0 0 2249 "" "{25,25,1184,2211,16}" "{o,o,o,o,o}" "{name,statement,prepare_time,parameter_types,from_sql}" _null_ _null_ pg_prepared_statement _null_ _null_ _null_ ));
DESCR("get the prepared statements for this session");
DATA(insert OID = 2511 (  pg_cursor PGNSP PGUID 12 1 1000 0 0 f f f f t t s 0 0 2249 "" "{25,25,16,16,16,1184}" "{o,o,o,o,o,o}" "{name,statement,is_holdable,is_binary,is_scrollable,creation_time}" _null_ _null_ pg_cursor _null_ _null_ _null_ ));
DESCR("get the open cursors for this session");
DATA(insert OID = 2599 (  pg_timezone_abbrevs	PGNSP PGUID 12 1 1000 0 0 f f f f t t s 0 0 2249 "" "{25,1186,16}" "{o,o,o}" "{abbrev,utc_offset,is_dst}" _null_ _null_ pg_timezone_abbrevs _null_ _null_ _null_ ));
DESCR("get the available time zone abbreviations");
DATA(insert OID = 2856 (  pg_timezone_names		PGNSP PGUID 12 1 1000 0 0 f f f f t t s 0 0 2249 "" "{25,25,1186,16}" "{o,o,o,o}" "{name,abbrev,utc_offset,is_dst}" _null_ _null_ pg_timezone_names _null_ _null_ _null_ ));
DESCR("get the available time zone names");
DATA(insert OID = 2730 (  pg_get_triggerdef		PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 25 "26 16" _null_ _null_ _null_ _null_ _null_ pg_get_triggerdef_ext _null_ _null_ _null_ ));
DESCR("trigger description with pretty-print option");
DATA(insert OID = 3035 (  pg_listening_channels PGNSP PGUID 12 1 10 0 0 f f f f t t s 0 0 25 "" _null_ _null_ _null_ _null_ _null_ pg_listening_channels _null_ _null_ _null_ ));
DESCR("get the channels that the current backend listens to");
DATA(insert OID = 3036 (  pg_notify				PGNSP PGUID 12 1 0 0 0 f f f f f f v 2 0 2278 "25 25" _null_ _null_ _null_ _null_ _null_ pg_notify _null_ _null_ _null_ ));
DESCR("send a notification event");

/* non-persistent series generator */
DATA(insert OID = 1066 (  generate_series PGNSP PGUID 12 1 1000 0 0 f f f f t t i 3 0 23 "23 23 23" _null_ _null_ _null_ _null_ _null_ generate_series_step_int4 _null_ _null_ _null_ ));
DESCR("non-persistent series generator");
DATA(insert OID = 1067 (  generate_series PGNSP PGUID 12 1 1000 0 0 f f f f t t i 2 0 23 "23 23" _null_ _null_ _null_ _null_ _null_ generate_series_int4 _null_ _null_ _null_ ));
DESCR("non-persistent series generator");
DATA(insert OID = 1068 (  generate_series PGNSP PGUID 12 1 1000 0 0 f f f f t t i 3 0 20 "20 20 20" _null_ _null_ _null_ _null_ _null_ generate_series_step_int8 _null_ _null_ _null_ ));
DESCR("non-persistent series generator");
DATA(insert OID = 1069 (  generate_series PGNSP PGUID 12 1 1000 0 0 f f f f t t i 2 0 20 "20 20" _null_ _null_ _null_ _null_ _null_ generate_series_int8 _null_ _null_ _null_ ));
DESCR("non-persistent series generator");
DATA(insert OID = 3259 (  generate_series PGNSP PGUID 12 1 1000 0 0 f f f f t t i 3 0 1700 "1700 1700 1700" _null_ _null_ _null_ _null_ _null_ generate_series_step_numeric _null_ _null_ _null_ ));
DESCR("non-persistent series generator");
DATA(insert OID = 3260 (  generate_series PGNSP PGUID 12 1 1000 0 0 f f f f t t i 2 0 1700 "1700 1700" _null_ _null_ _null_ _null_ _null_ generate_series_numeric _null_ _null_ _null_ ));
DESCR("non-persistent series generator");
DATA(insert OID = 938  (  generate_series PGNSP PGUID 12 1 1000 0 0 f f f f t t i 3 0 1114 "1114 1114 1186" _null_ _null_ _null_ _null_ _null_ generate_series_timestamp _null_ _null_ _null_ ));
DESCR("non-persistent series generator");
DATA(insert OID = 939  (  generate_series PGNSP PGUID 12 1 1000 0 0 f f f f t t s 3 0 1184 "1184 1184 1186" _null_ _null_ _null_ _null_ _null_ generate_series_timestamptz _null_ _null_ _null_ ));
DESCR("non-persistent series generator");

/* boolean aggregates */
DATA(insert OID = 2515 ( booland_statefunc			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "16 16" _null_ _null_ _null_ _null_ _null_ booland_statefunc _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 2516 ( boolor_statefunc			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "16 16" _null_ _null_ _null_ _null_ _null_ boolor_statefunc _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3496 ( bool_accum					   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 16" _null_ _null_ _null_ _null_ _null_ bool_accum _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3497 ( bool_accum_inv				   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 16" _null_ _null_ _null_ _null_ _null_ bool_accum_inv _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3498 ( bool_alltrue				   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "2281" _null_ _null_ _null_ _null_ _null_ bool_alltrue _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3499 ( bool_anytrue				   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "2281" _null_ _null_ _null_ _null_ _null_ bool_anytrue _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 2517 ( bool_and					   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 16 "16" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("boolean-and aggregate");
/* ANY, SOME? These names conflict with subquery operators. See doc. */
DATA(insert OID = 2518 ( bool_or					   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 16 "16" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("boolean-or aggregate");
DATA(insert OID = 2519 ( every						   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 16 "16" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("boolean-and aggregate");

/* bitwise integer aggregates */
DATA(insert OID = 2236 ( bit_and					   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 21 "21" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("bitwise-and smallint aggregate");
DATA(insert OID = 2237 ( bit_or						   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 21 "21" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("bitwise-or smallint aggregate");
DATA(insert OID = 2238 ( bit_and					   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("bitwise-and integer aggregate");
DATA(insert OID = 2239 ( bit_or						   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("bitwise-or integer aggregate");
DATA(insert OID = 2240 ( bit_and					   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("bitwise-and bigint aggregate");
DATA(insert OID = 2241 ( bit_or						   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 20 "20" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("bitwise-or bigint aggregate");
DATA(insert OID = 2242 ( bit_and					   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1560 "1560" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("bitwise-and bit aggregate");
DATA(insert OID = 2243 ( bit_or						   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 1560 "1560" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("bitwise-or bit aggregate");

/* formerly-missing interval + datetime operators */
DATA(insert OID = 2546 ( interval_pl_date			PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 1114 "1186 1082" _null_ _null_ _null_ _null_	_null_ "select $2 + $1" _null_ _null_ _null_ ));
DATA(insert OID = 2547 ( interval_pl_timetz			PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 1266 "1186 1266" _null_ _null_ _null_ _null_	_null_ "select $2 + $1" _null_ _null_ _null_ ));
DATA(insert OID = 2548 ( interval_pl_timestamp		PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 1114 "1186 1114" _null_ _null_ _null_ _null_	_null_ "select $2 + $1" _null_ _null_ _null_ ));
DATA(insert OID = 2549 ( interval_pl_timestamptz	PGNSP PGUID 14 1 0 0 0 f f f f t f s 2 0 1184 "1186 1184" _null_ _null_ _null_ _null_	_null_ "select $2 + $1" _null_ _null_ _null_ ));
DATA(insert OID = 2550 ( integer_pl_date			PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 1082 "23 1082" _null_ _null_ _null_ _null_ _null_ "select $2 + $1" _null_ _null_ _null_ ));

DATA(insert OID = 2556 ( pg_tablespace_databases	PGNSP PGUID 12 1 1000 0 0 f f f f t t s 1 0 26 "26" _null_ _null_ _null_ _null_ _null_ pg_tablespace_databases _null_ _null_ _null_ ));
DESCR("get OIDs of databases in a tablespace");

DATA(insert OID = 2557 ( bool				   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "23" _null_ _null_ _null_ _null_ _null_ int4_bool _null_ _null_ _null_ ));
DESCR("convert int4 to boolean");
DATA(insert OID = 2558 ( int4				   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "16" _null_ _null_ _null_ _null_ _null_ bool_int4 _null_ _null_ _null_ ));
DESCR("convert boolean to int4");
DATA(insert OID = 2559 ( lastval			   PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 20 "" _null_ _null_ _null_ _null_ _null_	lastval _null_ _null_ _null_ ));
DESCR("current value from last used sequence");

/* start time function */
DATA(insert OID = 2560 (  pg_postmaster_start_time	PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 1184 "" _null_ _null_ _null_ _null_ _null_ pg_postmaster_start_time _null_ _null_ _null_ ));
DESCR("postmaster start time");
/* config reload time function */
DATA(insert OID = 2034 (  pg_conf_load_time			PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 1184 "" _null_ _null_ _null_ _null_ _null_ pg_conf_load_time _null_ _null_ _null_ ));
DESCR("configuration load time");

/* new functions for Y-direction rtree opclasses */
DATA(insert OID = 2562 (  box_below		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_below _null_ _null_ _null_ ));
DATA(insert OID = 2563 (  box_overbelow    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_overbelow _null_ _null_ _null_ ));
DATA(insert OID = 2564 (  box_overabove    PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_overabove _null_ _null_ _null_ ));
DATA(insert OID = 2565 (  box_above		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "603 603" _null_ _null_ _null_ _null_ _null_ box_above _null_ _null_ _null_ ));
DATA(insert OID = 2566 (  poly_below	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_below _null_ _null_ _null_ ));
DATA(insert OID = 2567 (  poly_overbelow   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_overbelow _null_ _null_ _null_ ));
DATA(insert OID = 2568 (  poly_overabove   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_overabove _null_ _null_ _null_ ));
DATA(insert OID = 2569 (  poly_above	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "604 604" _null_ _null_ _null_ _null_ _null_ poly_above _null_ _null_ _null_ ));
DATA(insert OID = 2587 (  circle_overbelow		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_overbelow _null_ _null_ _null_ ));
DATA(insert OID = 2588 (  circle_overabove		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "718 718" _null_ _null_ _null_ _null_  _null_ circle_overabove _null_ _null_ _null_ ));

/* support functions for GiST r-tree emulation */
DATA(insert OID = 2578 (  gist_box_consistent	PGNSP PGUID 12 1 0 0 0 f f f f t f i 5 0 16 "2281 603 23 26 2281" _null_ _null_ _null_ _null_ _null_	gist_box_consistent _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2579 (  gist_box_compress		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gist_box_compress _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2580 (  gist_box_decompress	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gist_box_decompress _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3281 (  gist_box_fetch	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gist_box_fetch _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2581 (  gist_box_penalty		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_	gist_box_penalty _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2582 (  gist_box_picksplit	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_	gist_box_picksplit _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2583 (  gist_box_union		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 603 "2281 2281" _null_ _null_ _null_ _null_ _null_ gist_box_union _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2584 (  gist_box_same			PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "603 603 2281" _null_ _null_ _null_ _null_ _null_ gist_box_same _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2585 (  gist_poly_consistent	PGNSP PGUID 12 1 0 0 0 f f f f t f i 5 0 16 "2281 604 23 26 2281" _null_ _null_ _null_ _null_ _null_	gist_poly_consistent _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2586 (  gist_poly_compress	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gist_poly_compress _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2591 (  gist_circle_consistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 5 0 16 "2281 718 23 26 2281" _null_ _null_ _null_ _null_ _null_	gist_circle_consistent _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2592 (  gist_circle_compress	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gist_circle_compress _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 1030 (  gist_point_compress	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gist_point_compress _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3282 (  gist_point_fetch	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gist_point_fetch _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 2179 (  gist_point_consistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 5 0 16 "2281 600 23 26 2281" _null_ _null_ _null_ _null_ _null_	gist_point_consistent _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3064 (  gist_point_distance	PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 701 "2281 600 23 26" _null_ _null_ _null_ _null_ _null_	gist_point_distance _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3288 (  gist_bbox_distance	PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 701 "2281 600 23 26" _null_ _null_ _null_ _null_ _null_	gist_bbox_distance _null_ _null_ _null_ ));
DESCR("GiST support");

/* GIN */
DATA(insert OID = 2731 (  gingetbitmap	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 20 "2281 2281" _null_ _null_ _null_ _null_ _null_	gingetbitmap _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2732 (  gininsert		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 6 0 16 "2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_	gininsert _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2733 (  ginbeginscan	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_	ginbeginscan _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2734 (  ginrescan		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 5 0 2278 "2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ ginrescan _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2735 (  ginendscan	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ ginendscan _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2736 (  ginmarkpos	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ ginmarkpos _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2737 (  ginrestrpos	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ ginrestrpos _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2738 (  ginbuild		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ ginbuild _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 325 (  ginbuildempty	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ ginbuildempty _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2739 (  ginbulkdelete    PGNSP PGUID 12 1 0 0 0 f f f f t f v 4 0 2281 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ ginbulkdelete _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2740 (  ginvacuumcleanup PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ ginvacuumcleanup _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2741 (  gincostestimate  PGNSP PGUID 12 1 0 0 0 f f f f t f v 7 0 2278 "2281 2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gincostestimate _null_ _null_ _null_ ));
DESCR("gin(internal)");
DATA(insert OID = 2788 (  ginoptions	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 17 "1009 16" _null_ _null_ _null_ _null_  _null_ ginoptions _null_ _null_ _null_ ));
DESCR("gin(internal)");

/* GIN array support */
DATA(insert OID = 2743 (  ginarrayextract	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "2277 2281 2281" _null_ _null_ _null_ _null_ _null_ ginarrayextract _null_ _null_ _null_ ));
DESCR("GIN array support");
DATA(insert OID = 2774 (  ginqueryarrayextract	PGNSP PGUID 12 1 0 0 0 f f f f t f i 7 0 2281 "2277 2281 21 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ ginqueryarrayextract _null_ _null_ _null_ ));
DESCR("GIN array support");
DATA(insert OID = 2744 (  ginarrayconsistent	PGNSP PGUID 12 1 0 0 0 f f f f t f i 8 0 16 "2281 21 2277 23 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ ginarrayconsistent _null_ _null_ _null_ ));
DESCR("GIN array support");
DATA(insert OID = 3920 (  ginarraytriconsistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 7 0 18 "2281 21 2277 23 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ ginarraytriconsistent _null_ _null_ _null_ ));
DESCR("GIN array support");
DATA(insert OID = 3076 (  ginarrayextract	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2277 2281" _null_ _null_ _null_ _null_ _null_	ginarrayextract_2args _null_ _null_ _null_ ));
DESCR("GIN array support (obsolete)");

/* overlap/contains/contained */
DATA(insert OID = 2747 (  arrayoverlap		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2277 2277" _null_ _null_ _null_ _null_ _null_ arrayoverlap _null_ _null_ _null_ ));
DATA(insert OID = 2748 (  arraycontains		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2277 2277" _null_ _null_ _null_ _null_ _null_ arraycontains _null_ _null_ _null_ ));
DATA(insert OID = 2749 (  arraycontained	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2277 2277" _null_ _null_ _null_ _null_ _null_ arraycontained _null_ _null_ _null_ ));

/* BRIN minmax */
DATA(insert OID = 3383 ( brin_minmax_opcinfo	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ brin_minmax_opcinfo _null_ _null_ _null_ ));
DESCR("BRIN minmax support");
DATA(insert OID = 3384 ( brin_minmax_add_value	PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 16 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ brin_minmax_add_value _null_ _null_ _null_ ));
DESCR("BRIN minmax support");
DATA(insert OID = 3385 ( brin_minmax_consistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 16 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ brin_minmax_consistent _null_ _null_ _null_ ));
DESCR("BRIN minmax support");
DATA(insert OID = 3386 ( brin_minmax_union		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 16 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ brin_minmax_union _null_ _null_ _null_ ));
DESCR("BRIN minmax support");

/* BRIN inclusion */
DATA(insert OID = 4105 ( brin_inclusion_opcinfo PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ brin_inclusion_opcinfo _null_ _null_ _null_ ));
DESCR("BRIN inclusion support");
DATA(insert OID = 4106 ( brin_inclusion_add_value PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 16 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ brin_inclusion_add_value _null_ _null_ _null_ ));
DESCR("BRIN inclusion support");
DATA(insert OID = 4107 ( brin_inclusion_consistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 16 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ brin_inclusion_consistent _null_ _null_ _null_ ));
DESCR("BRIN inclusion support");
DATA(insert OID = 4108 ( brin_inclusion_union	PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 16 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ brin_inclusion_union _null_ _null_ _null_ ));
DESCR("BRIN inclusion support");

/* userlock replacements */
DATA(insert OID = 2880 (  pg_advisory_lock				PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "20" _null_ _null_ _null_ _null_ _null_ pg_advisory_lock_int8 _null_ _null_ _null_ ));
DESCR("obtain exclusive advisory lock");
DATA(insert OID = 3089 (  pg_advisory_xact_lock				PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "20" _null_ _null_ _null_ _null_ _null_ pg_advisory_xact_lock_int8 _null_ _null_ _null_ ));
DESCR("obtain exclusive advisory lock");
DATA(insert OID = 2881 (  pg_advisory_lock_shared		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "20" _null_ _null_ _null_ _null_ _null_ pg_advisory_lock_shared_int8 _null_ _null_ _null_ ));
DESCR("obtain shared advisory lock");
DATA(insert OID = 3090 (  pg_advisory_xact_lock_shared		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "20" _null_ _null_ _null_ _null_ _null_ pg_advisory_xact_lock_shared_int8 _null_ _null_ _null_ ));
DESCR("obtain shared advisory lock");
DATA(insert OID = 2882 (  pg_try_advisory_lock			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 16 "20" _null_ _null_ _null_ _null_ _null_ pg_try_advisory_lock_int8 _null_ _null_ _null_ ));
DESCR("obtain exclusive advisory lock if available");
DATA(insert OID = 3091 (  pg_try_advisory_xact_lock			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 16 "20" _null_ _null_ _null_ _null_ _null_ pg_try_advisory_xact_lock_int8 _null_ _null_ _null_ ));
DESCR("obtain exclusive advisory lock if available");
DATA(insert OID = 2883 (  pg_try_advisory_lock_shared	PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 16 "20" _null_ _null_ _null_ _null_ _null_ pg_try_advisory_lock_shared_int8 _null_ _null_ _null_ ));
DESCR("obtain shared advisory lock if available");
DATA(insert OID = 3092 (  pg_try_advisory_xact_lock_shared	PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 16 "20" _null_ _null_ _null_ _null_ _null_ pg_try_advisory_xact_lock_shared_int8 _null_ _null_ _null_ ));
DESCR("obtain shared advisory lock if available");
DATA(insert OID = 2884 (  pg_advisory_unlock			PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 16 "20" _null_ _null_ _null_ _null_ _null_ pg_advisory_unlock_int8 _null_ _null_ _null_ ));
DESCR("release exclusive advisory lock");
DATA(insert OID = 2885 (  pg_advisory_unlock_shared		PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 16 "20" _null_ _null_ _null_ _null_ _null_ pg_advisory_unlock_shared_int8 _null_ _null_ _null_ ));
DESCR("release shared advisory lock");
DATA(insert OID = 2886 (  pg_advisory_lock				PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2278 "23 23" _null_ _null_ _null_ _null_ _null_ pg_advisory_lock_int4 _null_ _null_ _null_ ));
DESCR("obtain exclusive advisory lock");
DATA(insert OID = 3093 (  pg_advisory_xact_lock				PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2278 "23 23" _null_ _null_ _null_ _null_ _null_ pg_advisory_xact_lock_int4 _null_ _null_ _null_ ));
DESCR("obtain exclusive advisory lock");
DATA(insert OID = 2887 (  pg_advisory_lock_shared		PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2278 "23 23" _null_ _null_ _null_ _null_ _null_ pg_advisory_lock_shared_int4 _null_ _null_ _null_ ));
DESCR("obtain shared advisory lock");
DATA(insert OID = 3094 (  pg_advisory_xact_lock_shared		PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2278 "23 23" _null_ _null_ _null_ _null_ _null_ pg_advisory_xact_lock_shared_int4 _null_ _null_ _null_ ));
DESCR("obtain shared advisory lock");
DATA(insert OID = 2888 (  pg_try_advisory_lock			PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ pg_try_advisory_lock_int4 _null_ _null_ _null_ ));
DESCR("obtain exclusive advisory lock if available");
DATA(insert OID = 3095 (  pg_try_advisory_xact_lock			PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ pg_try_advisory_xact_lock_int4 _null_ _null_ _null_ ));
DESCR("obtain exclusive advisory lock if available");
DATA(insert OID = 2889 (  pg_try_advisory_lock_shared	PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ pg_try_advisory_lock_shared_int4 _null_ _null_ _null_ ));
DESCR("obtain shared advisory lock if available");
DATA(insert OID = 3096 (  pg_try_advisory_xact_lock_shared	PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ pg_try_advisory_xact_lock_shared_int4 _null_ _null_ _null_ ));
DESCR("obtain shared advisory lock if available");
DATA(insert OID = 2890 (  pg_advisory_unlock			PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ pg_advisory_unlock_int4 _null_ _null_ _null_ ));
DESCR("release exclusive advisory lock");
DATA(insert OID = 2891 (  pg_advisory_unlock_shared		PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 16 "23 23" _null_ _null_ _null_ _null_ _null_ pg_advisory_unlock_shared_int4 _null_ _null_ _null_ ));
DESCR("release shared advisory lock");
DATA(insert OID = 2892 (  pg_advisory_unlock_all		PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2278 "" _null_ _null_ _null_ _null_ _null_ pg_advisory_unlock_all _null_ _null_ _null_ ));
DESCR("release all advisory locks");

/* XML support */
DATA(insert OID = 2893 (  xml_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 142 "2275" _null_ _null_ _null_ _null_ _null_ xml_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2894 (  xml_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "142" _null_ _null_ _null_ _null_ _null_ xml_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2895 (  xmlcomment	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 142 "25" _null_ _null_ _null_ _null_ _null_ xmlcomment _null_ _null_ _null_ ));
DESCR("generate XML comment");
DATA(insert OID = 2896 (  xml			   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 142 "25" _null_ _null_ _null_ _null_ _null_ texttoxml _null_ _null_ _null_ ));
DESCR("perform a non-validating parse of a character string to produce an XML value");
DATA(insert OID = 2897 (  xmlvalidate	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "142 25" _null_ _null_ _null_ _null_ _null_ xmlvalidate _null_ _null_ _null_ ));
DESCR("validate an XML value");
DATA(insert OID = 2898 (  xml_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 142 "2281" _null_ _null_ _null_ _null_ _null_	xml_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2899 (  xml_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "142" _null_ _null_ _null_ _null_ _null_ xml_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2900 (  xmlconcat2	   PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 142 "142 142" _null_ _null_ _null_ _null_ _null_ xmlconcat2 _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 2901 (  xmlagg		   PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 142 "142" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("concatenate XML values");
DATA(insert OID = 2922 (  text			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "142" _null_ _null_ _null_ _null_ _null_ xmltotext _null_ _null_ _null_ ));
DESCR("serialize an XML value to a character string");

DATA(insert OID = 2923 (  table_to_xml				  PGNSP PGUID 12 100 0 0 0 f f f f t f s 4 0 142 "2205 16 16 25" _null_ _null_ "{tbl,nulls,tableforest,targetns}" _null_ _null_ table_to_xml _null_ _null_ _null_ ));
DESCR("map table contents to XML");
DATA(insert OID = 2924 (  query_to_xml				  PGNSP PGUID 12 100 0 0 0 f f f f t f s 4 0 142 "25 16 16 25" _null_ _null_ "{query,nulls,tableforest,targetns}" _null_ _null_ query_to_xml _null_ _null_ _null_ ));
DESCR("map query result to XML");
DATA(insert OID = 2925 (  cursor_to_xml				  PGNSP PGUID 12 100 0 0 0 f f f f t f s 5 0 142 "1790 23 16 16 25" _null_ _null_ "{cursor,count,nulls,tableforest,targetns}" _null_ _null_ cursor_to_xml _null_ _null_ _null_ ));
DESCR("map rows from cursor to XML");
DATA(insert OID = 2926 (  table_to_xmlschema		  PGNSP PGUID 12 100 0 0 0 f f f f t f s 4 0 142 "2205 16 16 25" _null_ _null_ "{tbl,nulls,tableforest,targetns}" _null_ _null_ table_to_xmlschema _null_ _null_ _null_ ));
DESCR("map table structure to XML Schema");
DATA(insert OID = 2927 (  query_to_xmlschema		  PGNSP PGUID 12 100 0 0 0 f f f f t f s 4 0 142 "25 16 16 25" _null_ _null_ "{query,nulls,tableforest,targetns}" _null_ _null_ query_to_xmlschema _null_ _null_ _null_ ));
DESCR("map query result structure to XML Schema");
DATA(insert OID = 2928 (  cursor_to_xmlschema		  PGNSP PGUID 12 100 0 0 0 f f f f t f s 4 0 142 "1790 16 16 25" _null_ _null_ "{cursor,nulls,tableforest,targetns}" _null_ _null_ cursor_to_xmlschema _null_ _null_ _null_ ));
DESCR("map cursor structure to XML Schema");
DATA(insert OID = 2929 (  table_to_xml_and_xmlschema  PGNSP PGUID 12 100 0 0 0 f f f f t f s 4 0 142 "2205 16 16 25" _null_ _null_ "{tbl,nulls,tableforest,targetns}" _null_ _null_ table_to_xml_and_xmlschema _null_ _null_ _null_ ));
DESCR("map table contents and structure to XML and XML Schema");
DATA(insert OID = 2930 (  query_to_xml_and_xmlschema  PGNSP PGUID 12 100 0 0 0 f f f f t f s 4 0 142 "25 16 16 25" _null_ _null_ "{query,nulls,tableforest,targetns}" _null_ _null_ query_to_xml_and_xmlschema _null_ _null_ _null_ ));
DESCR("map query result and structure to XML and XML Schema");

DATA(insert OID = 2933 (  schema_to_xml				  PGNSP PGUID 12 100 0 0 0 f f f f t f s 4 0 142 "19 16 16 25" _null_ _null_ "{schema,nulls,tableforest,targetns}" _null_ _null_ schema_to_xml _null_ _null_ _null_ ));
DESCR("map schema contents to XML");
DATA(insert OID = 2934 (  schema_to_xmlschema		  PGNSP PGUID 12 100 0 0 0 f f f f t f s 4 0 142 "19 16 16 25" _null_ _null_ "{schema,nulls,tableforest,targetns}" _null_ _null_ schema_to_xmlschema _null_ _null_ _null_ ));
DESCR("map schema structure to XML Schema");
DATA(insert OID = 2935 (  schema_to_xml_and_xmlschema PGNSP PGUID 12 100 0 0 0 f f f f t f s 4 0 142 "19 16 16 25" _null_ _null_ "{schema,nulls,tableforest,targetns}" _null_ _null_ schema_to_xml_and_xmlschema _null_ _null_ _null_ ));
DESCR("map schema contents and structure to XML and XML Schema");

DATA(insert OID = 2936 (  database_to_xml			  PGNSP PGUID 12 100 0 0 0 f f f f t f s 3 0 142 "16 16 25" _null_ _null_ "{nulls,tableforest,targetns}" _null_ _null_ database_to_xml _null_ _null_ _null_ ));
DESCR("map database contents to XML");
DATA(insert OID = 2937 (  database_to_xmlschema		  PGNSP PGUID 12 100 0 0 0 f f f f t f s 3 0 142 "16 16 25" _null_ _null_ "{nulls,tableforest,targetns}" _null_ _null_ database_to_xmlschema _null_ _null_ _null_ ));
DESCR("map database structure to XML Schema");
DATA(insert OID = 2938 (  database_to_xml_and_xmlschema PGNSP PGUID 12 100 0 0 0 f f f f t f s 3 0 142 "16 16 25" _null_ _null_ "{nulls,tableforest,targetns}" _null_ _null_ database_to_xml_and_xmlschema _null_ _null_ _null_ ));
DESCR("map database contents and structure to XML and XML Schema");

DATA(insert OID = 2931 (  xpath		 PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 143 "25 142 1009" _null_ _null_ _null_ _null_ _null_ xpath _null_ _null_ _null_ ));
DESCR("evaluate XPath expression, with namespaces support");
DATA(insert OID = 2932 (  xpath		 PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 143 "25 142" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.xpath($1, $2, ''{}''::pg_catalog.text[])" _null_ _null_ _null_ ));
DESCR("evaluate XPath expression");

DATA(insert OID = 2614 (  xmlexists  PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "25 142" _null_ _null_ _null_ _null_ _null_ xmlexists _null_ _null_ _null_ ));
DESCR("test XML value against XPath expression");

DATA(insert OID = 3049 (  xpath_exists	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 16 "25 142 1009" _null_ _null_ _null_ _null_ _null_ xpath_exists _null_ _null_ _null_ ));
DESCR("test XML value against XPath expression, with namespace support");
DATA(insert OID = 3050 (  xpath_exists	 PGNSP PGUID 14 1 0 0 0 f f f f t f i 2 0 16 "25 142" _null_ _null_ _null_ _null_ _null_ "select pg_catalog.xpath_exists($1, $2, ''{}''::pg_catalog.text[])" _null_ _null_ _null_ ));
DESCR("test XML value against XPath expression");
DATA(insert OID = 3051 (  xml_is_well_formed			 PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 16 "25" _null_ _null_ _null_ _null_ _null_ xml_is_well_formed _null_ _null_ _null_ ));
DESCR("determine if a string is well formed XML");
DATA(insert OID = 3052 (  xml_is_well_formed_document	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "25" _null_ _null_ _null_ _null_ _null_ xml_is_well_formed_document _null_ _null_ _null_ ));
DESCR("determine if a string is well formed XML document");
DATA(insert OID = 3053 (  xml_is_well_formed_content	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "25" _null_ _null_ _null_ _null_ _null_ xml_is_well_formed_content _null_ _null_ _null_ ));
DESCR("determine if a string is well formed XML content");

/* json */
DATA(insert OID = 321 (  json_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 114 "2275" _null_ _null_ _null_ _null_ _null_ json_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 322 (  json_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "114" _null_ _null_ _null_ _null_ _null_ json_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 323 (  json_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 114 "2281" _null_ _null_ _null_ _null_ _null_	json_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 324 (  json_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "114" _null_ _null_ _null_ _null_ _null_ json_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3153 (  array_to_json    PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 114 "2277" _null_ _null_ _null_ _null_ _null_ array_to_json _null_ _null_ _null_ ));
DESCR("map array to json");
DATA(insert OID = 3154 (  array_to_json    PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 114 "2277 16" _null_ _null_ _null_ _null_ _null_ array_to_json_pretty _null_ _null_ _null_ ));
DESCR("map array to json with optional pretty printing");
DATA(insert OID = 3155 (  row_to_json	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 114 "2249" _null_ _null_ _null_ _null_ _null_ row_to_json _null_ _null_ _null_ ));
DESCR("map row to json");
DATA(insert OID = 3156 (  row_to_json	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 114 "2249 16" _null_ _null_ _null_ _null_ _null_ row_to_json_pretty _null_ _null_ _null_ ));
DESCR("map row to json with optional pretty printing");
DATA(insert OID = 3173 (  json_agg_transfn	 PGNSP PGUID 12 1 0 0 0 f f f f f f s 2 0 2281 "2281 2283" _null_ _null_ _null_ _null_ _null_ json_agg_transfn _null_ _null_ _null_ ));
DESCR("json aggregate transition function");
DATA(insert OID = 3174 (  json_agg_finalfn	 PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 114 "2281" _null_ _null_ _null_ _null_ _null_ json_agg_finalfn _null_ _null_ _null_ ));
DESCR("json aggregate final function");
DATA(insert OID = 3175 (  json_agg		   PGNSP PGUID 12 1 0 0 0 t f f f f f s 1 0 114 "2283" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("aggregate input into json");
DATA(insert OID = 3180 (  json_object_agg_transfn	 PGNSP PGUID 12 1 0 0 0 f f f f f f s 3 0 2281 "2281 2276 2276" _null_ _null_ _null_ _null_ _null_ json_object_agg_transfn _null_ _null_ _null_ ));
DESCR("json object aggregate transition function");
DATA(insert OID = 3196 (  json_object_agg_finalfn	 PGNSP PGUID 12 1 0 0 0 f f f f f f i 1 0 114 "2281" _null_ _null_ _null_ _null_ _null_ json_object_agg_finalfn _null_ _null_ _null_ ));
DESCR("json object aggregate final function");
DATA(insert OID = 3197 (  json_object_agg		   PGNSP PGUID 12 1 0 0 0 t f f f f f s 2 0 114 "2276 2276" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("aggregate input into a json object");
DATA(insert OID = 3198 (  json_build_array	   PGNSP PGUID 12 1 0 2276 0 f f f f f f s 1 0 114 "2276" "{2276}" "{v}" _null_ _null_ _null_ json_build_array _null_ _null_ _null_ ));
DESCR("build a json array from any inputs");
DATA(insert OID = 3199 (  json_build_array	   PGNSP PGUID 12 1 0 0 0 f f f f f f s 0 0 114  "" _null_ _null_ _null_ _null_ _null_ json_build_array_noargs _null_ _null_ _null_ ));
DESCR("build an empty json array");
DATA(insert OID = 3200 (  json_build_object    PGNSP PGUID 12 1 0 2276 0 f f f f f f s 1 0 114 "2276" "{2276}" "{v}" _null_ _null_ _null_ json_build_object _null_ _null_ _null_ ));
DESCR("build a json object from pairwise key/value inputs");
DATA(insert OID = 3201 (  json_build_object    PGNSP PGUID 12 1 0 0 0 f f f f f f s 0 0 114  "" _null_ _null_ _null_ _null_ _null_ json_build_object_noargs _null_ _null_ _null_ ));
DESCR("build an empty json object");
DATA(insert OID = 3202 (  json_object	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 114 "1009" _null_ _null_ _null_ _null_ _null_ json_object _null_ _null_ _null_ ));
DESCR("map text array of key value pairs to json object");
DATA(insert OID = 3203 (  json_object	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 114 "1009 1009" _null_ _null_ _null_ _null_ _null_ json_object_two_arg _null_ _null_ _null_ ));
DESCR("map text arrays of keys and values to json object");
DATA(insert OID = 3176 (  to_json	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 114 "2283" _null_ _null_ _null_ _null_ _null_ to_json _null_ _null_ _null_ ));
DESCR("map input to json");
DATA(insert OID = 3261 (  json_strip_nulls	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 114 "114" _null_ _null_ _null_ _null_ _null_ json_strip_nulls _null_ _null_ _null_ ));
DESCR("remove object fields with null values from json");

DATA(insert OID = 3947 (  json_object_field			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 114 "114 25" _null_ _null_ "{from_json, field_name}" _null_ _null_ json_object_field _null_ _null_ _null_ ));
DATA(insert OID = 3948 (  json_object_field_text	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25  "114 25" _null_ _null_ "{from_json, field_name}" _null_ _null_ json_object_field_text _null_ _null_ _null_ ));
DATA(insert OID = 3949 (  json_array_element		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 114 "114 23" _null_ _null_ "{from_json, element_index}" _null_ _null_ json_array_element _null_ _null_ _null_ ));
DATA(insert OID = 3950 (  json_array_element_text	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25  "114 23" _null_ _null_ "{from_json, element_index}" _null_ _null_ json_array_element_text _null_ _null_ _null_ ));
DATA(insert OID = 3951 (  json_extract_path			PGNSP PGUID 12 1 0 25 0 f f f f t f i 2 0 114 "114 1009" "{114,1009}" "{i,v}" "{from_json,path_elems}" _null_ _null_ json_extract_path _null_ _null_ _null_ ));
DESCR("get value from json with path elements");
DATA(insert OID = 3953 (  json_extract_path_text	PGNSP PGUID 12 1 0 25 0 f f f f t f i 2 0 25 "114 1009" "{114,1009}" "{i,v}" "{from_json,path_elems}" _null_ _null_ json_extract_path_text _null_ _null_ _null_ ));
DESCR("get value from json as text with path elements");
DATA(insert OID = 3955 (  json_array_elements		PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 114 "114" "{114,114}" "{i,o}" "{from_json,value}" _null_ _null_ json_array_elements _null_ _null_ _null_ ));
DESCR("key value pairs of a json object");
DATA(insert OID = 3969 (  json_array_elements_text	PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 25 "114" "{114,25}" "{i,o}" "{from_json,value}" _null_ _null_ json_array_elements_text _null_ _null_ _null_ ));
DESCR("elements of json array");
DATA(insert OID = 3956 (  json_array_length			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "114" _null_ _null_ _null_ _null_ _null_ json_array_length _null_ _null_ _null_ ));
DESCR("length of json array");
DATA(insert OID = 3957 (  json_object_keys			PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 25 "114" _null_ _null_ _null_ _null_ _null_ json_object_keys _null_ _null_ _null_ ));
DESCR("get json object keys");
DATA(insert OID = 3958 (  json_each				   PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 2249 "114" "{114,25,114}" "{i,o,o}" "{from_json,key,value}" _null_ _null_ json_each _null_ _null_ _null_ ));
DESCR("key value pairs of a json object");
DATA(insert OID = 3959 (  json_each_text		   PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 2249 "114" "{114,25,25}" "{i,o,o}" "{from_json,key,value}" _null_ _null_ json_each_text _null_ _null_ _null_ ));
DESCR("key value pairs of a json object");
DATA(insert OID = 3960 (  json_populate_record	   PGNSP PGUID 12 1 0 0 0 f f f f f f s 3 0 2283 "2283 114 16" _null_ _null_ _null_ _null_ _null_ json_populate_record _null_ _null_ _null_ ));
DESCR("get record fields from a json object");
DATA(insert OID = 3961 (  json_populate_recordset  PGNSP PGUID 12 1 100 0 0 f f f f f t s 3 0 2283 "2283 114 16" _null_ _null_ _null_ _null_ _null_ json_populate_recordset _null_ _null_ _null_ ));
DESCR("get set of records with fields from a json array of objects");
DATA(insert OID = 3204 (  json_to_record		   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2249 "114" _null_ _null_ _null_ _null_ _null_ json_to_record _null_ _null_ _null_ ));
DESCR("get record fields from a json object");
DATA(insert OID = 3205 (  json_to_recordset		   PGNSP PGUID 12 1 100 0 0 f f f f f t s 1 0 2249 "114" _null_ _null_ _null_ _null_ _null_ json_to_recordset _null_ _null_ _null_ ));
DESCR("get set of records with fields from a json array of objects");
DATA(insert OID = 3968 (  json_typeof			   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "114" _null_ _null_ _null_ _null_ _null_ json_typeof _null_ _null_ _null_ ));
DESCR("get the type of a json value");

/* uuid */
DATA(insert OID = 2952 (  uuid_in		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2950 "2275" _null_ _null_ _null_ _null_ _null_ uuid_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2953 (  uuid_out		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "2950" _null_ _null_ _null_ _null_ _null_ uuid_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2954 (  uuid_lt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "2950 2950" _null_ _null_ _null_ _null_ _null_ uuid_lt _null_ _null_ _null_ ));
DATA(insert OID = 2955 (  uuid_le		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "2950 2950" _null_ _null_ _null_ _null_ _null_ uuid_le _null_ _null_ _null_ ));
DATA(insert OID = 2956 (  uuid_eq		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "2950 2950" _null_ _null_ _null_ _null_ _null_ uuid_eq _null_ _null_ _null_ ));
DATA(insert OID = 2957 (  uuid_ge		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "2950 2950" _null_ _null_ _null_ _null_ _null_ uuid_ge _null_ _null_ _null_ ));
DATA(insert OID = 2958 (  uuid_gt		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "2950 2950" _null_ _null_ _null_ _null_ _null_ uuid_gt _null_ _null_ _null_ ));
DATA(insert OID = 2959 (  uuid_ne		   PGNSP PGUID 12 1 0 0 0 f f f t t f i 2 0 16 "2950 2950" _null_ _null_ _null_ _null_ _null_ uuid_ne _null_ _null_ _null_ ));
DATA(insert OID = 2960 (  uuid_cmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "2950 2950" _null_ _null_ _null_ _null_ _null_ uuid_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 2961 (  uuid_recv		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2950 "2281" _null_ _null_ _null_ _null_ _null_ uuid_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2962 (  uuid_send		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "2950" _null_ _null_ _null_ _null_ _null_ uuid_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2963 (  uuid_hash		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "2950" _null_ _null_ _null_ _null_ _null_ uuid_hash _null_ _null_ _null_ ));
DESCR("hash");

/* pg_lsn */
DATA(insert OID = 3229 (  pg_lsn_in		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3220 "2275" _null_ _null_ _null_ _null_ _null_ pg_lsn_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3230 (  pg_lsn_out		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3231 (  pg_lsn_lt		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3220 3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_lt _null_ _null_ _null_ ));
DATA(insert OID = 3232 (  pg_lsn_le		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3220 3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_le _null_ _null_ _null_ ));
DATA(insert OID = 3233 (  pg_lsn_eq		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3220 3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_eq _null_ _null_ _null_ ));
DATA(insert OID = 3234 (  pg_lsn_ge		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3220 3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_ge _null_ _null_ _null_ ));
DATA(insert OID = 3235 (  pg_lsn_gt		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3220 3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_gt _null_ _null_ _null_ ));
DATA(insert OID = 3236 (  pg_lsn_ne		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3220 3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_ne _null_ _null_ _null_ ));
DATA(insert OID = 3237 (  pg_lsn_mi		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1700 "3220 3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_mi _null_ _null_ _null_ ));
DATA(insert OID = 3238 (  pg_lsn_recv	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3220 "2281" _null_ _null_ _null_ _null_ _null_ pg_lsn_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3239 (  pg_lsn_send	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3251 (  pg_lsn_cmp	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "3220 3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3252 (  pg_lsn_hash	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "3220" _null_ _null_ _null_ _null_ _null_ pg_lsn_hash _null_ _null_ _null_ ));
DESCR("hash");

/* enum related procs */
DATA(insert OID = 3504 (  anyenum_in	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3500 "2275" _null_ _null_ _null_ _null_ _null_ anyenum_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3505 (  anyenum_out	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "3500" _null_ _null_ _null_ _null_ _null_ anyenum_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3506 (  enum_in		PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 3500 "2275 26" _null_ _null_ _null_ _null_ _null_ enum_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3507 (  enum_out		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "3500" _null_ _null_ _null_ _null_ _null_ enum_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3508 (  enum_eq		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3500 3500" _null_ _null_ _null_ _null_ _null_ enum_eq _null_ _null_ _null_ ));
DATA(insert OID = 3509 (  enum_ne		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3500 3500" _null_ _null_ _null_ _null_ _null_ enum_ne _null_ _null_ _null_ ));
DATA(insert OID = 3510 (  enum_lt		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3500 3500" _null_ _null_ _null_ _null_ _null_ enum_lt _null_ _null_ _null_ ));
DATA(insert OID = 3511 (  enum_gt		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3500 3500" _null_ _null_ _null_ _null_ _null_ enum_gt _null_ _null_ _null_ ));
DATA(insert OID = 3512 (  enum_le		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3500 3500" _null_ _null_ _null_ _null_ _null_ enum_le _null_ _null_ _null_ ));
DATA(insert OID = 3513 (  enum_ge		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3500 3500" _null_ _null_ _null_ _null_ _null_ enum_ge _null_ _null_ _null_ ));
DATA(insert OID = 3514 (  enum_cmp		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "3500 3500" _null_ _null_ _null_ _null_ _null_ enum_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3515 (  hashenum		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "3500" _null_ _null_ _null_ _null_ _null_ hashenum _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 3524 (  enum_smaller	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3500 "3500 3500" _null_ _null_ _null_ _null_ _null_ enum_smaller _null_ _null_ _null_ ));
DESCR("smaller of two");
DATA(insert OID = 3525 (  enum_larger	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3500 "3500 3500" _null_ _null_ _null_ _null_ _null_ enum_larger _null_ _null_ _null_ ));
DESCR("larger of two");
DATA(insert OID = 3526 (  max			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 3500 "3500" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("maximum value of all enum input values");
DATA(insert OID = 3527 (  min			PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 3500 "3500" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("minimum value of all enum input values");
DATA(insert OID = 3528 (  enum_first	PGNSP PGUID 12 1 0 0 0 f f f f f f s 1 0 3500 "3500" _null_ _null_ _null_ _null_ _null_ enum_first _null_ _null_ _null_ ));
DESCR("first value of the input enum type");
DATA(insert OID = 3529 (  enum_last		PGNSP PGUID 12 1 0 0 0 f f f f f f s 1 0 3500 "3500" _null_ _null_ _null_ _null_ _null_ enum_last _null_ _null_ _null_ ));
DESCR("last value of the input enum type");
DATA(insert OID = 3530 (  enum_range	PGNSP PGUID 12 1 0 0 0 f f f f f f s 2 0 2277 "3500 3500" _null_ _null_ _null_ _null_ _null_ enum_range_bounds _null_ _null_ _null_ ));
DESCR("range between the two given enum values, as an ordered array");
DATA(insert OID = 3531 (  enum_range	PGNSP PGUID 12 1 0 0 0 f f f f f f s 1 0 2277 "3500" _null_ _null_ _null_ _null_ _null_ enum_range_all _null_ _null_ _null_ ));
DESCR("range of the given enum type, as an ordered array");
DATA(insert OID = 3532 (  enum_recv		PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 3500 "2281 26" _null_ _null_ _null_ _null_ _null_ enum_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3533 (  enum_send		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "3500" _null_ _null_ _null_ _null_ _null_ enum_send _null_ _null_ _null_ ));
DESCR("I/O");

/* text search stuff */
DATA(insert OID =  3610 (  tsvectorin			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3614 "2275" _null_ _null_ _null_ _null_ _null_ tsvectorin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3639 (  tsvectorrecv			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3614 "2281" _null_ _null_ _null_ _null_ _null_ tsvectorrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3611 (  tsvectorout			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "3614" _null_ _null_ _null_ _null_ _null_ tsvectorout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3638 (  tsvectorsend			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "3614" _null_ _null_ _null_ _null_ _null_	tsvectorsend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3612 (  tsqueryin			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3615 "2275" _null_ _null_ _null_ _null_ _null_ tsqueryin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3641 (  tsqueryrecv			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3615 "2281" _null_ _null_ _null_ _null_ _null_ tsqueryrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3613 (  tsqueryout			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "3615" _null_ _null_ _null_ _null_ _null_ tsqueryout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3640 (  tsquerysend			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "3615" _null_ _null_ _null_ _null_ _null_	tsquerysend _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3646 (  gtsvectorin			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3642 "2275" _null_ _null_ _null_ _null_ _null_ gtsvectorin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3647 (  gtsvectorout			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "3642" _null_ _null_ _null_ _null_ _null_ gtsvectorout _null_ _null_ _null_ ));
DESCR("I/O");

DATA(insert OID = 3616 (  tsvector_lt			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3614 3614" _null_ _null_ _null_ _null_ _null_ tsvector_lt _null_ _null_ _null_ ));
DATA(insert OID = 3617 (  tsvector_le			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3614 3614" _null_ _null_ _null_ _null_ _null_ tsvector_le _null_ _null_ _null_ ));
DATA(insert OID = 3618 (  tsvector_eq			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3614 3614" _null_ _null_ _null_ _null_ _null_ tsvector_eq _null_ _null_ _null_ ));
DATA(insert OID = 3619 (  tsvector_ne			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3614 3614" _null_ _null_ _null_ _null_ _null_ tsvector_ne _null_ _null_ _null_ ));
DATA(insert OID = 3620 (  tsvector_ge			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3614 3614" _null_ _null_ _null_ _null_ _null_ tsvector_ge _null_ _null_ _null_ ));
DATA(insert OID = 3621 (  tsvector_gt			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3614 3614" _null_ _null_ _null_ _null_ _null_ tsvector_gt _null_ _null_ _null_ ));
DATA(insert OID = 3622 (  tsvector_cmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "3614 3614" _null_ _null_ _null_ _null_ _null_ tsvector_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 3711 (  length				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "3614" _null_ _null_ _null_ _null_ _null_ tsvector_length _null_ _null_ _null_ ));
DESCR("number of lexemes");
DATA(insert OID = 3623 (  strip					PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3614 "3614" _null_ _null_ _null_ _null_ _null_ tsvector_strip _null_ _null_ _null_ ));
DESCR("strip position information");
DATA(insert OID = 3624 (  setweight				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3614 "3614 18" _null_ _null_ _null_ _null_ _null_ tsvector_setweight _null_ _null_ _null_ ));
DESCR("set weight of lexeme's entries");
DATA(insert OID = 3625 (  tsvector_concat		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3614 "3614 3614" _null_ _null_ _null_ _null_ _null_ tsvector_concat _null_ _null_ _null_ ));

DATA(insert OID = 3634 (  ts_match_vq			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3614 3615" _null_ _null_ _null_ _null_ _null_ ts_match_vq _null_ _null_ _null_ ));
DATA(insert OID = 3635 (  ts_match_qv			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3615 3614" _null_ _null_ _null_ _null_ _null_ ts_match_qv _null_ _null_ _null_ ));
DATA(insert OID = 3760 (  ts_match_tt			PGNSP PGUID 12 100 0 0 0 f f f f t f s 2 0 16 "25 25" _null_ _null_ _null_ _null_ _null_ ts_match_tt _null_ _null_ _null_ ));
DATA(insert OID = 3761 (  ts_match_tq			PGNSP PGUID 12 100 0 0 0 f f f f t f s 2 0 16 "25 3615" _null_ _null_ _null_ _null_ _null_ ts_match_tq _null_ _null_ _null_ ));

DATA(insert OID = 3648 (  gtsvector_compress	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gtsvector_compress _null_ _null_ _null_ ));
DESCR("GiST tsvector support");
DATA(insert OID = 3649 (  gtsvector_decompress	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gtsvector_decompress _null_ _null_ _null_ ));
DESCR("GiST tsvector support");
DATA(insert OID = 3650 (  gtsvector_picksplit	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ gtsvector_picksplit _null_ _null_ _null_ ));
DESCR("GiST tsvector support");
DATA(insert OID = 3651 (  gtsvector_union		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ gtsvector_union _null_ _null_ _null_ ));
DESCR("GiST tsvector support");
DATA(insert OID = 3652 (  gtsvector_same		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "3642 3642 2281" _null_ _null_ _null_ _null_ _null_ gtsvector_same _null_ _null_ _null_ ));
DESCR("GiST tsvector support");
DATA(insert OID = 3653 (  gtsvector_penalty		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gtsvector_penalty _null_ _null_ _null_ ));
DESCR("GiST tsvector support");
DATA(insert OID = 3654 (  gtsvector_consistent	PGNSP PGUID 12 1 0 0 0 f f f f t f i 5 0 16 "2281 3642 23 26 2281" _null_ _null_ _null_ _null_ _null_ gtsvector_consistent _null_ _null_ _null_ ));
DESCR("GiST tsvector support");

DATA(insert OID = 3656 (  gin_extract_tsvector	PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "3614 2281 2281" _null_ _null_ _null_ _null_ _null_	gin_extract_tsvector _null_ _null_ _null_ ));
DESCR("GIN tsvector support");
DATA(insert OID = 3657 (  gin_extract_tsquery	PGNSP PGUID 12 1 0 0 0 f f f f t f i 7 0 2281 "3615 2281 21 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_extract_tsquery _null_ _null_ _null_ ));
DESCR("GIN tsvector support");
DATA(insert OID = 3658 (  gin_tsquery_consistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 8 0 16 "2281 21 3615 23 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_	gin_tsquery_consistent _null_ _null_ _null_ ));
DESCR("GIN tsvector support");
DATA(insert OID = 3921 (  gin_tsquery_triconsistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 7 0 18 "2281 21 3615 23 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_tsquery_triconsistent _null_ _null_ _null_ ));
DESCR("GIN tsvector support");
DATA(insert OID = 3724 (  gin_cmp_tslexeme		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "25 25" _null_ _null_ _null_ _null_ _null_ gin_cmp_tslexeme _null_ _null_ _null_ ));
DESCR("GIN tsvector support");
DATA(insert OID = 2700 (  gin_cmp_prefix		PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 23 "25 25 21 2281" _null_ _null_ _null_ _null_ _null_ gin_cmp_prefix _null_ _null_ _null_ ));
DESCR("GIN tsvector support");
DATA(insert OID = 3077 (  gin_extract_tsvector	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "3614 2281" _null_ _null_ _null_ _null_ _null_	gin_extract_tsvector_2args _null_ _null_ _null_ ));
DESCR("GIN tsvector support (obsolete)");
DATA(insert OID = 3087 (  gin_extract_tsquery	PGNSP PGUID 12 1 0 0 0 f f f f t f i 5 0 2281 "3615 2281 21 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_extract_tsquery_5args _null_ _null_ _null_ ));
DESCR("GIN tsvector support (obsolete)");
DATA(insert OID = 3088 (  gin_tsquery_consistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 6 0 16 "2281 21 3615 23 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_tsquery_consistent_6args _null_ _null_ _null_ ));
DESCR("GIN tsvector support (obsolete)");

DATA(insert OID = 3662 (  tsquery_lt			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsquery_lt _null_ _null_ _null_ ));
DATA(insert OID = 3663 (  tsquery_le			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsquery_le _null_ _null_ _null_ ));
DATA(insert OID = 3664 (  tsquery_eq			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsquery_eq _null_ _null_ _null_ ));
DATA(insert OID = 3665 (  tsquery_ne			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsquery_ne _null_ _null_ _null_ ));
DATA(insert OID = 3666 (  tsquery_ge			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsquery_ge _null_ _null_ _null_ ));
DATA(insert OID = 3667 (  tsquery_gt			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsquery_gt _null_ _null_ _null_ ));
DATA(insert OID = 3668 (  tsquery_cmp			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsquery_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

DATA(insert OID = 3669 (  tsquery_and		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3615 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsquery_and _null_ _null_ _null_ ));
DATA(insert OID = 3670 (  tsquery_or		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3615 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsquery_or _null_ _null_ _null_ ));
DATA(insert OID = 3671 (  tsquery_not		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3615 "3615" _null_ _null_ _null_ _null_ _null_ tsquery_not _null_ _null_ _null_ ));

DATA(insert OID = 3691 (  tsq_mcontains		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsq_mcontains _null_ _null_ _null_ ));
DATA(insert OID = 3692 (  tsq_mcontained	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3615 3615" _null_ _null_ _null_ _null_ _null_ tsq_mcontained _null_ _null_ _null_ ));

DATA(insert OID = 3672 (  numnode			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "3615" _null_ _null_ _null_ _null_ _null_ tsquery_numnode _null_ _null_ _null_ ));
DESCR("number of nodes");
DATA(insert OID = 3673 (  querytree			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "3615" _null_ _null_ _null_ _null_ _null_ tsquerytree _null_ _null_ _null_ ));
DESCR("show real useful query for GiST index");

DATA(insert OID = 3684 (  ts_rewrite		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 3615 "3615 3615 3615" _null_ _null_ _null_ _null_ _null_ tsquery_rewrite _null_ _null_ _null_ ));
DESCR("rewrite tsquery");
DATA(insert OID = 3685 (  ts_rewrite		PGNSP PGUID 12 100 0 0 0 f f f f t f v 2 0 3615 "3615 25" _null_ _null_ _null_ _null_ _null_ tsquery_rewrite_query _null_ _null_ _null_ ));
DESCR("rewrite tsquery");

DATA(insert OID = 3695 (  gtsquery_compress				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gtsquery_compress _null_ _null_ _null_ ));
DESCR("GiST tsquery support");
DATA(insert OID = 3696 (  gtsquery_decompress			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ gtsquery_decompress _null_ _null_ _null_ ));
DESCR("GiST tsquery support");
DATA(insert OID = 3697 (  gtsquery_picksplit			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ gtsquery_picksplit _null_ _null_ _null_ ));
DESCR("GiST tsquery support");
DATA(insert OID = 3698 (  gtsquery_union				PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ gtsquery_union _null_ _null_ _null_ ));
DESCR("GiST tsquery support");
DATA(insert OID = 3699 (  gtsquery_same					PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "20 20 2281" _null_ _null_ _null_ _null_ _null_ gtsquery_same _null_ _null_ _null_ ));
DESCR("GiST tsquery support");
DATA(insert OID = 3700 (  gtsquery_penalty				PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gtsquery_penalty _null_ _null_ _null_ ));
DESCR("GiST tsquery support");
DATA(insert OID = 3701 (  gtsquery_consistent			PGNSP PGUID 12 1 0 0 0 f f f f t f i 5 0 16 "2281 2281 23 26 2281" _null_ _null_ _null_ _null_ _null_ gtsquery_consistent _null_ _null_ _null_ ));
DESCR("GiST tsquery support");

DATA(insert OID = 3686 (  tsmatchsel		PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_ tsmatchsel _null_ _null_ _null_ ));
DESCR("restriction selectivity of tsvector @@ tsquery");
DATA(insert OID = 3687 (  tsmatchjoinsel	PGNSP PGUID 12 1 0 0 0 f f f f t f s 5 0 701 "2281 26 2281 21 2281" _null_ _null_ _null_ _null_ _null_ tsmatchjoinsel _null_ _null_ _null_ ));
DESCR("join selectivity of tsvector @@ tsquery");
DATA(insert OID = 3688 (  ts_typanalyze		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 16 "2281" _null_ _null_ _null_ _null_ _null_ ts_typanalyze _null_ _null_ _null_ ));
DESCR("tsvector typanalyze");

DATA(insert OID = 3689 (  ts_stat		PGNSP PGUID 12 10 10000 0 0 f f f f t t v 1 0 2249 "25" "{25,25,23,23}" "{i,o,o,o}" "{query,word,ndoc,nentry}" _null_ _null_ ts_stat1 _null_ _null_ _null_ ));
DESCR("statistics of tsvector column");
DATA(insert OID = 3690 (  ts_stat		PGNSP PGUID 12 10 10000 0 0 f f f f t t v 2 0 2249 "25 25" "{25,25,25,23,23}" "{i,i,o,o,o}" "{query,weights,word,ndoc,nentry}" _null_ _null_ ts_stat2 _null_ _null_ _null_ ));
DESCR("statistics of tsvector column");

DATA(insert OID = 3703 (  ts_rank		PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 700 "1021 3614 3615 23" _null_ _null_ _null_ _null_ _null_ ts_rank_wttf _null_ _null_ _null_ ));
DESCR("relevance");
DATA(insert OID = 3704 (  ts_rank		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 700 "1021 3614 3615" _null_ _null_ _null_ _null_ _null_ ts_rank_wtt _null_ _null_ _null_ ));
DESCR("relevance");
DATA(insert OID = 3705 (  ts_rank		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 700 "3614 3615 23" _null_ _null_ _null_ _null_ _null_ ts_rank_ttf _null_ _null_ _null_ ));
DESCR("relevance");
DATA(insert OID = 3706 (  ts_rank		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 700 "3614 3615" _null_ _null_ _null_ _null_ _null_ ts_rank_tt _null_ _null_ _null_ ));
DESCR("relevance");
DATA(insert OID = 3707 (  ts_rank_cd	PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 700 "1021 3614 3615 23" _null_ _null_ _null_ _null_ _null_ ts_rankcd_wttf _null_ _null_ _null_ ));
DESCR("relevance");
DATA(insert OID = 3708 (  ts_rank_cd	PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 700 "1021 3614 3615" _null_ _null_ _null_ _null_ _null_ ts_rankcd_wtt _null_ _null_ _null_ ));
DESCR("relevance");
DATA(insert OID = 3709 (  ts_rank_cd	PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 700 "3614 3615 23" _null_ _null_ _null_ _null_ _null_ ts_rankcd_ttf _null_ _null_ _null_ ));
DESCR("relevance");
DATA(insert OID = 3710 (  ts_rank_cd	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 700 "3614 3615" _null_ _null_ _null_ _null_ _null_ ts_rankcd_tt _null_ _null_ _null_ ));
DESCR("relevance");

DATA(insert OID = 3713 (  ts_token_type PGNSP PGUID 12 1 16 0 0 f f f f t t i 1 0 2249 "26" "{26,23,25,25}" "{i,o,o,o}" "{parser_oid,tokid,alias,description}" _null_ _null_ ts_token_type_byid _null_ _null_ _null_ ));
DESCR("get parser's token types");
DATA(insert OID = 3714 (  ts_token_type PGNSP PGUID 12 1 16 0 0 f f f f t t s 1 0 2249 "25" "{25,23,25,25}" "{i,o,o,o}" "{parser_name,tokid,alias,description}" _null_ _null_ ts_token_type_byname _null_ _null_ _null_ ));
DESCR("get parser's token types");
DATA(insert OID = 3715 (  ts_parse		PGNSP PGUID 12 1 1000 0 0 f f f f t t i 2 0 2249 "26 25" "{26,25,23,25}" "{i,i,o,o}" "{parser_oid,txt,tokid,token}" _null_ _null_ ts_parse_byid _null_ _null_ _null_ ));
DESCR("parse text to tokens");
DATA(insert OID = 3716 (  ts_parse		PGNSP PGUID 12 1 1000 0 0 f f f f t t s 2 0 2249 "25 25" "{25,25,23,25}" "{i,i,o,o}" "{parser_name,txt,tokid,token}" _null_ _null_ ts_parse_byname _null_ _null_ _null_ ));
DESCR("parse text to tokens");

DATA(insert OID = 3717 (  prsd_start		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2281 23" _null_ _null_ _null_ _null_ _null_ prsd_start _null_ _null_ _null_ ));
DESCR("(internal)");
DATA(insert OID = 3718 (  prsd_nexttoken	PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ prsd_nexttoken _null_ _null_ _null_ ));
DESCR("(internal)");
DATA(insert OID = 3719 (  prsd_end			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ prsd_end _null_ _null_ _null_ ));
DESCR("(internal)");
DATA(insert OID = 3720 (  prsd_headline		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "2281 2281 3615" _null_ _null_ _null_ _null_ _null_ prsd_headline _null_ _null_ _null_ ));
DESCR("(internal)");
DATA(insert OID = 3721 (  prsd_lextype		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ prsd_lextype _null_ _null_ _null_ ));
DESCR("(internal)");

DATA(insert OID = 3723 (  ts_lexize			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 1009 "3769 25" _null_ _null_ _null_ _null_ _null_ ts_lexize _null_ _null_ _null_ ));
DESCR("normalize one word by dictionary");

DATA(insert OID = 3725 (  dsimple_init		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ dsimple_init _null_ _null_ _null_ ));
DESCR("(internal)");
DATA(insert OID = 3726 (  dsimple_lexize	PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 2281 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ dsimple_lexize _null_ _null_ _null_ ));
DESCR("(internal)");

DATA(insert OID = 3728 (  dsynonym_init		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ dsynonym_init _null_ _null_ _null_ ));
DESCR("(internal)");
DATA(insert OID = 3729 (  dsynonym_lexize	PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 2281 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ dsynonym_lexize _null_ _null_ _null_ ));
DESCR("(internal)");

DATA(insert OID = 3731 (  dispell_init		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ dispell_init _null_ _null_ _null_ ));
DESCR("(internal)");
DATA(insert OID = 3732 (  dispell_lexize	PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 2281 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ dispell_lexize _null_ _null_ _null_ ));
DESCR("(internal)");

DATA(insert OID = 3740 (  thesaurus_init	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ thesaurus_init _null_ _null_ _null_ ));
DESCR("(internal)");
DATA(insert OID = 3741 (  thesaurus_lexize	PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 2281 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ thesaurus_lexize _null_ _null_ _null_ ));
DESCR("(internal)");

DATA(insert OID = 3743 (  ts_headline	PGNSP PGUID 12 100 0 0 0 f f f f t f i 4 0 25 "3734 25 3615 25" _null_ _null_ _null_ _null_ _null_ ts_headline_byid_opt _null_ _null_ _null_ ));
DESCR("generate headline");
DATA(insert OID = 3744 (  ts_headline	PGNSP PGUID 12 100 0 0 0 f f f f t f i 3 0 25 "3734 25 3615" _null_ _null_ _null_ _null_ _null_ ts_headline_byid _null_ _null_ _null_ ));
DESCR("generate headline");
DATA(insert OID = 3754 (  ts_headline	PGNSP PGUID 12 100 0 0 0 f f f f t f s 3 0 25 "25 3615 25" _null_ _null_ _null_ _null_ _null_ ts_headline_opt _null_ _null_ _null_ ));
DESCR("generate headline");
DATA(insert OID = 3755 (  ts_headline	PGNSP PGUID 12 100 0 0 0 f f f f t f s 2 0 25 "25 3615" _null_ _null_ _null_ _null_ _null_ ts_headline _null_ _null_ _null_ ));
DESCR("generate headline");

DATA(insert OID = 3745 (  to_tsvector		PGNSP PGUID 12 100 0 0 0 f f f f t f i 2 0 3614 "3734 25" _null_ _null_ _null_ _null_ _null_ to_tsvector_byid _null_ _null_ _null_ ));
DESCR("transform to tsvector");
DATA(insert OID = 3746 (  to_tsquery		PGNSP PGUID 12 100 0 0 0 f f f f t f i 2 0 3615 "3734 25" _null_ _null_ _null_ _null_ _null_ to_tsquery_byid _null_ _null_ _null_ ));
DESCR("make tsquery");
DATA(insert OID = 3747 (  plainto_tsquery	PGNSP PGUID 12 100 0 0 0 f f f f t f i 2 0 3615 "3734 25" _null_ _null_ _null_ _null_ _null_ plainto_tsquery_byid _null_ _null_ _null_ ));
DESCR("transform to tsquery");
DATA(insert OID = 3749 (  to_tsvector		PGNSP PGUID 12 100 0 0 0 f f f f t f s 1 0 3614 "25" _null_ _null_ _null_ _null_ _null_ to_tsvector _null_ _null_ _null_ ));
DESCR("transform to tsvector");
DATA(insert OID = 3750 (  to_tsquery		PGNSP PGUID 12 100 0 0 0 f f f f t f s 1 0 3615 "25" _null_ _null_ _null_ _null_ _null_ to_tsquery _null_ _null_ _null_ ));
DESCR("make tsquery");
DATA(insert OID = 3751 (  plainto_tsquery	PGNSP PGUID 12 100 0 0 0 f f f f t f s 1 0 3615 "25" _null_ _null_ _null_ _null_ _null_ plainto_tsquery _null_ _null_ _null_ ));
DESCR("transform to tsquery");

DATA(insert OID = 3752 (  tsvector_update_trigger			PGNSP PGUID 12 1 0 0 0 f f f f f f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ tsvector_update_trigger_byid _null_ _null_ _null_ ));
DESCR("trigger for automatic update of tsvector column");
DATA(insert OID = 3753 (  tsvector_update_trigger_column	PGNSP PGUID 12 1 0 0 0 f f f f f f v 0 0 2279 "" _null_ _null_ _null_ _null_ _null_ tsvector_update_trigger_bycolumn _null_ _null_ _null_ ));
DESCR("trigger for automatic update of tsvector column");

DATA(insert OID = 3759 (  get_current_ts_config PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 3734 "" _null_ _null_ _null_ _null_ _null_ get_current_ts_config _null_ _null_ _null_ ));
DESCR("get current tsearch configuration");

DATA(insert OID = 3736 (  regconfigin		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 3734 "2275" _null_ _null_ _null_ _null_ _null_ regconfigin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3737 (  regconfigout		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "3734" _null_ _null_ _null_ _null_ _null_ regconfigout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3738 (  regconfigrecv		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3734 "2281" _null_ _null_ _null_ _null_ _null_ regconfigrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3739 (  regconfigsend		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "3734" _null_ _null_ _null_ _null_ _null_ regconfigsend _null_ _null_ _null_ ));
DESCR("I/O");

DATA(insert OID = 3771 (  regdictionaryin	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 3769 "2275" _null_ _null_ _null_ _null_ _null_ regdictionaryin _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3772 (  regdictionaryout	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "3769" _null_ _null_ _null_ _null_ _null_ regdictionaryout _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3773 (  regdictionaryrecv PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3769 "2281" _null_ _null_ _null_ _null_ _null_ regdictionaryrecv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3774 (  regdictionarysend PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "3769" _null_ _null_ _null_ _null_ _null_ regdictionarysend _null_ _null_ _null_ ));
DESCR("I/O");

/* jsonb */
DATA(insert OID =  3806 (  jsonb_in			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3802 "2275" _null_ _null_ _null_ _null_ _null_ jsonb_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3805 (  jsonb_recv		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3802 "2281" _null_ _null_ _null_ _null_ _null_ jsonb_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3804 (  jsonb_out		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2275 "3802" _null_ _null_ _null_ _null_ _null_ jsonb_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID =  3803 (  jsonb_send		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 17 "3802" _null_ _null_ _null_ _null_ _null_	jsonb_send _null_ _null_ _null_ ));
DESCR("I/O");

DATA(insert OID = 3263 (  jsonb_object	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3802 "1009" _null_ _null_ _null_ _null_ _null_ jsonb_object _null_ _null_ _null_ ));
DESCR("map text array of key value pairs to jsonb object");
DATA(insert OID = 3264 (  jsonb_object	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3802 "1009 1009" _null_ _null_ _null_ _null_ _null_ jsonb_object_two_arg _null_ _null_ _null_ ));
DESCR("map text array of key value pairs to jsonb object");
DATA(insert OID = 3787 (  to_jsonb	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 3802 "2283" _null_ _null_ _null_ _null_ _null_ to_jsonb _null_ _null_ _null_ ));
DESCR("map input to jsonb");
DATA(insert OID = 3265 (  jsonb_agg_transfn  PGNSP PGUID 12 1 0 0 0 f f f f f f s 2 0 2281 "2281 2283" _null_ _null_ _null_ _null_ _null_ jsonb_agg_transfn _null_ _null_ _null_ ));
DESCR("jsonb aggregate transition function");
DATA(insert OID = 3266 (  jsonb_agg_finalfn  PGNSP PGUID 12 1 0 0 0 f f f f f f s 1 0 3802 "2281" _null_ _null_ _null_ _null_ _null_ jsonb_agg_finalfn _null_ _null_ _null_ ));
DESCR("jsonb aggregate final function");
DATA(insert OID = 3267 (  jsonb_agg		   PGNSP PGUID 12 1 0 0 0 t f f f f f s 1 0 3802 "2283" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("aggregate input into jsonb");
DATA(insert OID = 3268 (  jsonb_object_agg_transfn	 PGNSP PGUID 12 1 0 0 0 f f f f f f s 3 0 2281 "2281 2276 2276" _null_ _null_ _null_ _null_ _null_ jsonb_object_agg_transfn _null_ _null_ _null_ ));
DESCR("jsonb object aggregate transition function");
DATA(insert OID = 3269 (  jsonb_object_agg_finalfn	 PGNSP PGUID 12 1 0 0 0 f f f f f f s 1 0 3802 "2281" _null_ _null_ _null_ _null_ _null_ jsonb_object_agg_finalfn _null_ _null_ _null_ ));
DESCR("jsonb object aggregate final function");
DATA(insert OID = 3270 (  jsonb_object_agg		   PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 3802 "2276 2276" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("aggregate inputs into jsonb object");
DATA(insert OID = 3271 (  jsonb_build_array    PGNSP PGUID 12 1 0 2276 0 f f f f f f s 1 0 3802 "2276" "{2276}" "{v}" _null_ _null_ _null_ jsonb_build_array _null_ _null_ _null_ ));
DESCR("build a jsonb array from any inputs");
DATA(insert OID = 3272 (  jsonb_build_array    PGNSP PGUID 12 1 0 0 0 f f f f f f s 0 0 3802  "" _null_ _null_ _null_ _null_ _null_ jsonb_build_array_noargs _null_ _null_ _null_ ));
DESCR("build an empty jsonb array");
DATA(insert OID = 3273 (  jsonb_build_object	PGNSP PGUID 12 1 0 2276 0 f f f f f f s 1 0 3802 "2276" "{2276}" "{v}" _null_ _null_ _null_ jsonb_build_object _null_ _null_ _null_ ));
DESCR("build a jsonb object from pairwise key/value inputs");
DATA(insert OID = 3274 (  jsonb_build_object	PGNSP PGUID 12 1 0 0 0 f f f f f f s 0 0 3802  "" _null_ _null_ _null_ _null_ _null_ jsonb_build_object_noargs _null_ _null_ _null_ ));
DESCR("build an empty jsonb object");
DATA(insert OID = 3262 (  jsonb_strip_nulls    PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3802 "3802" _null_ _null_ _null_ _null_ _null_ jsonb_strip_nulls _null_ _null_ _null_ ));
DESCR("remove object fields with null values from jsonb");

DATA(insert OID = 3478 (  jsonb_object_field			PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3802 "3802 25" _null_ _null_ "{from_json, field_name}" _null_ _null_ jsonb_object_field _null_ _null_ _null_ ));
DATA(insert OID = 3214 (  jsonb_object_field_text	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25  "3802 25" _null_ _null_ "{from_json, field_name}" _null_ _null_ jsonb_object_field_text _null_ _null_ _null_ ));
DATA(insert OID = 3215 (  jsonb_array_element		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3802 "3802 23" _null_ _null_ "{from_json, element_index}" _null_ _null_ jsonb_array_element _null_ _null_ _null_ ));
DATA(insert OID = 3216 (  jsonb_array_element_text	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 25  "3802 23" _null_ _null_ "{from_json, element_index}" _null_ _null_ jsonb_array_element_text _null_ _null_ _null_ ));
DATA(insert OID = 3217 (  jsonb_extract_path			PGNSP PGUID 12 1 0 25 0 f f f f t f i 2 0 3802 "3802 1009" "{3802,1009}" "{i,v}" "{from_json,path_elems}" _null_ _null_ jsonb_extract_path _null_ _null_ _null_ ));
DESCR("get value from jsonb with path elements");
DATA(insert OID = 3940 (  jsonb_extract_path_text	PGNSP PGUID 12 1 0 25 0 f f f f t f i 2 0 25 "3802 1009" "{3802,1009}" "{i,v}" "{from_json,path_elems}" _null_ _null_ jsonb_extract_path_text _null_ _null_ _null_ ));
DESCR("get value from jsonb as text with path elements");
DATA(insert OID = 3219 (  jsonb_array_elements		PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 3802 "3802" "{3802,3802}" "{i,o}" "{from_json,value}" _null_ _null_ jsonb_array_elements _null_ _null_ _null_ ));
DESCR("elements of a jsonb array");
DATA(insert OID = 3465 (  jsonb_array_elements_text PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 25 "3802" "{3802,25}" "{i,o}" "{from_json,value}" _null_ _null_ jsonb_array_elements_text _null_ _null_ _null_ ));
DESCR("elements of jsonb array");
DATA(insert OID = 3207 (  jsonb_array_length			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "3802" _null_ _null_ _null_ _null_ _null_ jsonb_array_length _null_ _null_ _null_ ));
DESCR("length of jsonb array");
DATA(insert OID = 3931 (  jsonb_object_keys			PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 25 "3802" _null_ _null_ _null_ _null_ _null_ jsonb_object_keys _null_ _null_ _null_ ));
DESCR("get jsonb object keys");
DATA(insert OID = 3208 (  jsonb_each				   PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 2249 "3802" "{3802,25,3802}" "{i,o,o}" "{from_json,key,value}" _null_ _null_ jsonb_each _null_ _null_ _null_ ));
DESCR("key value pairs of a jsonb object");
DATA(insert OID = 3932 (  jsonb_each_text		   PGNSP PGUID 12 1 100 0 0 f f f f t t i 1 0 2249 "3802" "{3802,25,25}" "{i,o,o}" "{from_json,key,value}" _null_ _null_ jsonb_each_text _null_ _null_ _null_ ));
DESCR("key value pairs of a jsonb object");
DATA(insert OID = 3209 (  jsonb_populate_record    PGNSP PGUID 12 1 0 0 0 f f f f f f s 2 0 2283 "2283 3802" _null_ _null_ _null_ _null_ _null_ jsonb_populate_record _null_ _null_ _null_ ));
DESCR("get record fields from a jsonb object");
DATA(insert OID = 3475 (  jsonb_populate_recordset	PGNSP PGUID 12 1 100 0 0 f f f f f t s 2 0 2283 "2283 3802" _null_ _null_ _null_ _null_ _null_ jsonb_populate_recordset _null_ _null_ _null_ ));
DESCR("get set of records with fields from a jsonb array of objects");
DATA(insert OID = 3490 (  jsonb_to_record			PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2249 "3802" _null_ _null_ _null_ _null_ _null_ jsonb_to_record _null_ _null_ _null_ ));
DESCR("get record fields from a jsonb object");
DATA(insert OID = 3491 (  jsonb_to_recordset		PGNSP PGUID 12 1 100 0 0 f f f f f t s 1 0 2249 "3802" _null_ _null_ _null_ _null_ _null_ jsonb_to_recordset _null_ _null_ _null_ ));
DESCR("get set of records with fields from a jsonb array of objects");
DATA(insert OID = 3210 (  jsonb_typeof				PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "3802" _null_ _null_ _null_ _null_ _null_ jsonb_typeof _null_ _null_ _null_ ));
DESCR("get the type of a jsonb value");
DATA(insert OID = 4038 (  jsonb_ne		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 3802" _null_ _null_ _null_ _null_ _null_ jsonb_ne _null_ _null_ _null_ ));
DATA(insert OID = 4039 (  jsonb_lt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 3802" _null_ _null_ _null_ _null_ _null_ jsonb_lt _null_ _null_ _null_ ));
DATA(insert OID = 4040 (  jsonb_gt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 3802" _null_ _null_ _null_ _null_ _null_ jsonb_gt _null_ _null_ _null_ ));
DATA(insert OID = 4041 (  jsonb_le		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 3802" _null_ _null_ _null_ _null_ _null_ jsonb_le _null_ _null_ _null_ ));
DATA(insert OID = 4042 (  jsonb_ge		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 3802" _null_ _null_ _null_ _null_ _null_ jsonb_ge _null_ _null_ _null_ ));
DATA(insert OID = 4043 (  jsonb_eq		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 3802" _null_ _null_ _null_ _null_ _null_ jsonb_eq _null_ _null_ _null_ ));
DATA(insert OID = 4044 (  jsonb_cmp		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "3802 3802" _null_ _null_ _null_ _null_ _null_ jsonb_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 4045 (  jsonb_hash	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "3802" _null_ _null_ _null_ _null_ _null_ jsonb_hash _null_ _null_ _null_ ));
DESCR("hash");
DATA(insert OID = 4046 (  jsonb_contains   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 3802" _null_ _null_ _null_ _null_ _null_ jsonb_contains _null_ _null_ _null_ ));
DESCR("implementation of @> operator");
DATA(insert OID = 4047 (  jsonb_exists	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 25" _null_ _null_ _null_ _null_ _null_ jsonb_exists _null_ _null_ _null_ ));
DESCR("implementation of ? operator");
DATA(insert OID = 4048 (  jsonb_exists_any	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 1009" _null_ _null_ _null_ _null_ _null_ jsonb_exists_any _null_ _null_ _null_ ));
DESCR("implementation of ?| operator");
DATA(insert OID = 4049 (  jsonb_exists_all	 PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 1009" _null_ _null_ _null_ _null_ _null_ jsonb_exists_all _null_ _null_ _null_ ));
DESCR("implementation of ?& operator");
DATA(insert OID = 4050 (  jsonb_contained	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3802 3802" _null_ _null_ _null_ _null_ _null_ jsonb_contained _null_ _null_ _null_ ));
DESCR("implementation of <@ operator");
DATA(insert OID = 3480 (  gin_compare_jsonb  PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "25 25" _null_ _null_ _null_ _null_ _null_ gin_compare_jsonb _null_ _null_ _null_ ));
DESCR("GIN support");
DATA(insert OID = 3482 (  gin_extract_jsonb  PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_extract_jsonb _null_ _null_ _null_ ));
DESCR("GIN support");
DATA(insert OID = 3483 (  gin_extract_jsonb_query  PGNSP PGUID 12 1 0 0 0 f f f f t f i 7 0 2281 "2277 2281 21 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_extract_jsonb_query _null_ _null_ _null_ ));
DESCR("GIN support");
DATA(insert OID = 3484 (  gin_consistent_jsonb	PGNSP PGUID 12 1 0 0 0 f f f f t f i 8 0 16 "2281 21 2277 23 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_consistent_jsonb _null_ _null_ _null_ ));
DESCR("GIN support");
DATA(insert OID = 3488 (  gin_triconsistent_jsonb	PGNSP PGUID 12 1 0 0 0 f f f f t f i 7 0 18 "2281 21 2277 23 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_triconsistent_jsonb _null_ _null_ _null_ ));
DESCR("GIN support");
DATA(insert OID = 3485 (  gin_extract_jsonb_path  PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_extract_jsonb_path _null_ _null_ _null_ ));
DESCR("GIN support");
DATA(insert OID = 3486 (  gin_extract_jsonb_query_path	PGNSP PGUID 12 1 0 0 0 f f f f t f i 7 0 2281 "2277 2281 21 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_extract_jsonb_query_path _null_ _null_ _null_ ));
DESCR("GIN support");
DATA(insert OID = 3487 (  gin_consistent_jsonb_path  PGNSP PGUID 12 1 0 0 0 f f f f t f i 8 0 16 "2281 21 2277 23 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_consistent_jsonb_path _null_ _null_ _null_ ));
DESCR("GIN support");
DATA(insert OID = 3489 (  gin_triconsistent_jsonb_path	PGNSP PGUID 12 1 0 0 0 f f f f t f i 7 0 18 "2281 21 2277 23 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ gin_triconsistent_jsonb_path _null_ _null_ _null_ ));
DESCR("GIN support");
DATA(insert OID = 3301 (  jsonb_concat	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3802 "3802 3802" _null_ _null_ _null_ _null_ _null_ jsonb_concat _null_ _null_ _null_ ));
DATA(insert OID = 3302 (  jsonb_delete	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3802 "3802 25" _null_ _null_ _null_ _null_ _null_ jsonb_delete _null_ _null_ _null_ ));
DATA(insert OID = 3303 (  jsonb_delete	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3802 "3802 23" _null_ _null_ _null_ _null_ _null_ jsonb_delete_idx _null_ _null_ _null_ ));
DATA(insert OID = 3304 (  jsonb_delete_path	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3802 "3802 1009" _null_ _null_ _null_ _null_ _null_ jsonb_delete_path _null_ _null_ _null_ ));
DATA(insert OID = 3305 (  jsonb_set	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 4 0 3802 "3802 1009 3802 16" _null_ _null_ _null_ _null_ _null_ jsonb_set _null_ _null_ _null_ ));
DESCR("Set part of a jsonb");
DATA(insert OID = 3306 (  jsonb_pretty	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 25 "3802" _null_ _null_ _null_ _null_ _null_ jsonb_pretty _null_ _null_ _null_ ));
DESCR("Indented text from jsonb");
/* txid */
DATA(insert OID = 2939 (  txid_snapshot_in			PGNSP PGUID 12 1  0 0 0 f f f f t f i 1 0 2970 "2275" _null_ _null_ _null_ _null_ _null_ txid_snapshot_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2940 (  txid_snapshot_out			PGNSP PGUID 12 1  0 0 0 f f f f t f i 1 0 2275 "2970" _null_ _null_ _null_ _null_ _null_ txid_snapshot_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2941 (  txid_snapshot_recv		PGNSP PGUID 12 1  0 0 0 f f f f t f i 1 0 2970 "2281" _null_ _null_ _null_ _null_ _null_ txid_snapshot_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2942 (  txid_snapshot_send		PGNSP PGUID 12 1  0 0 0 f f f f t f i 1 0 17 "2970" _null_ _null_ _null_ _null_ _null_ txid_snapshot_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 2943 (  txid_current				PGNSP PGUID 12 1  0 0 0 f f f f t f s 0 0 20 "" _null_ _null_ _null_ _null_ _null_ txid_current _null_ _null_ _null_ ));
DESCR("get current transaction ID");
DATA(insert OID = 2944 (  txid_current_snapshot		PGNSP PGUID 12 1  0 0 0 f f f f t f s 0 0 2970 "" _null_ _null_ _null_ _null_ _null_ txid_current_snapshot _null_ _null_ _null_ ));
DESCR("get current snapshot");
DATA(insert OID = 2945 (  txid_snapshot_xmin		PGNSP PGUID 12 1  0 0 0 f f f f t f i 1 0 20 "2970" _null_ _null_ _null_ _null_ _null_ txid_snapshot_xmin _null_ _null_ _null_ ));
DESCR("get xmin of snapshot");
DATA(insert OID = 2946 (  txid_snapshot_xmax		PGNSP PGUID 12 1  0 0 0 f f f f t f i 1 0 20 "2970" _null_ _null_ _null_ _null_ _null_ txid_snapshot_xmax _null_ _null_ _null_ ));
DESCR("get xmax of snapshot");
DATA(insert OID = 2947 (  txid_snapshot_xip			PGNSP PGUID 12 1 50 0 0 f f f f t t i 1 0 20 "2970" _null_ _null_ _null_ _null_ _null_ txid_snapshot_xip _null_ _null_ _null_ ));
DESCR("get set of in-progress txids in snapshot");
DATA(insert OID = 2948 (  txid_visible_in_snapshot	PGNSP PGUID 12 1  0 0 0 f f f f t f i 2 0 16 "20 2970" _null_ _null_ _null_ _null_ _null_ txid_visible_in_snapshot _null_ _null_ _null_ ));
DESCR("is txid visible in snapshot?");

/* record comparison using normal comparison rules */
DATA(insert OID = 2981 (  record_eq		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_eq _null_ _null_ _null_ ));
DATA(insert OID = 2982 (  record_ne		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_ne _null_ _null_ _null_ ));
DATA(insert OID = 2983 (  record_lt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_lt _null_ _null_ _null_ ));
DATA(insert OID = 2984 (  record_gt		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_gt _null_ _null_ _null_ ));
DATA(insert OID = 2985 (  record_le		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_le _null_ _null_ _null_ ));
DATA(insert OID = 2986 (  record_ge		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_ge _null_ _null_ _null_ ));
DATA(insert OID = 2987 (  btrecordcmp	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "2249 2249" _null_ _null_ _null_ _null_ _null_ btrecordcmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");

/* record comparison using raw byte images */
DATA(insert OID = 3181 (  record_image_eq	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_image_eq _null_ _null_ _null_ ));
DATA(insert OID = 3182 (  record_image_ne	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_image_ne _null_ _null_ _null_ ));
DATA(insert OID = 3183 (  record_image_lt	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_image_lt _null_ _null_ _null_ ));
DATA(insert OID = 3184 (  record_image_gt	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_image_gt _null_ _null_ _null_ ));
DATA(insert OID = 3185 (  record_image_le	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_image_le _null_ _null_ _null_ ));
DATA(insert OID = 3186 (  record_image_ge	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2249 2249" _null_ _null_ _null_ _null_ _null_ record_image_ge _null_ _null_ _null_ ));
DATA(insert OID = 3187 (  btrecordimagecmp	   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "2249 2249" _null_ _null_ _null_ _null_ _null_ btrecordimagecmp _null_ _null_ _null_ ));
DESCR("less-equal-greater based on byte images");

/* Extensions */
DATA(insert OID = 3082 (  pg_available_extensions		PGNSP PGUID 12 10 100 0 0 f f f f t t s 0 0 2249 "" "{19,25,25}" "{o,o,o}" "{name,default_version,comment}" _null_ _null_ pg_available_extensions _null_ _null_ _null_ ));
DESCR("list available extensions");
DATA(insert OID = 3083 (  pg_available_extension_versions	PGNSP PGUID 12 10 100 0 0 f f f f t t s 0 0 2249 "" "{19,25,16,16,19,1003,25}" "{o,o,o,o,o,o,o}" "{name,version,superuser,relocatable,schema,requires,comment}" _null_ _null_ pg_available_extension_versions _null_ _null_ _null_ ));
DESCR("list available extension versions");
DATA(insert OID = 3084 (  pg_extension_update_paths		PGNSP PGUID 12 10 100 0 0 f f f f t t s 1 0 2249 "19" "{19,25,25,25}" "{i,o,o,o}" "{name,source,target,path}" _null_ _null_ pg_extension_update_paths _null_ _null_ _null_ ));
DESCR("list an extension's version update paths");
DATA(insert OID = 3086 (  pg_extension_config_dump		PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2278 "2205 25" _null_ _null_ _null_ _null_ _null_ pg_extension_config_dump _null_ _null_ _null_ ));
DESCR("flag an extension's table contents to be emitted by pg_dump");

/* SQL-spec window functions */
DATA(insert OID = 3100 (  row_number	PGNSP PGUID 12 1 0 0 0 f t f f f f i 0 0 20 "" _null_ _null_ _null_ _null_ _null_ window_row_number _null_ _null_ _null_ ));
DESCR("row number within partition");
DATA(insert OID = 3101 (  rank			PGNSP PGUID 12 1 0 0 0 f t f f f f i 0 0 20 "" _null_ _null_ _null_ _null_ _null_ window_rank _null_ _null_ _null_ ));
DESCR("integer rank with gaps");
DATA(insert OID = 3102 (  dense_rank	PGNSP PGUID 12 1 0 0 0 f t f f f f i 0 0 20 "" _null_ _null_ _null_ _null_ _null_ window_dense_rank _null_ _null_ _null_ ));
DESCR("integer rank without gaps");
DATA(insert OID = 3103 (  percent_rank	PGNSP PGUID 12 1 0 0 0 f t f f f f i 0 0 701 "" _null_ _null_ _null_ _null_ _null_ window_percent_rank _null_ _null_ _null_ ));
DESCR("fractional rank within partition");
DATA(insert OID = 3104 (  cume_dist		PGNSP PGUID 12 1 0 0 0 f t f f f f i 0 0 701 "" _null_ _null_ _null_ _null_ _null_ window_cume_dist _null_ _null_ _null_ ));
DESCR("fractional row number within partition");
DATA(insert OID = 3105 (  ntile			PGNSP PGUID 12 1 0 0 0 f t f f t f i 1 0 23 "23" _null_ _null_ _null_ _null_ _null_ window_ntile _null_ _null_ _null_ ));
DESCR("split rows into N groups");
DATA(insert OID = 3106 (  lag			PGNSP PGUID 12 1 0 0 0 f t f f t f i 1 0 2283 "2283" _null_ _null_ _null_ _null_ _null_ window_lag _null_ _null_ _null_ ));
DESCR("fetch the preceding row value");
DATA(insert OID = 3107 (  lag			PGNSP PGUID 12 1 0 0 0 f t f f t f i 2 0 2283 "2283 23" _null_ _null_ _null_ _null_ _null_ window_lag_with_offset _null_ _null_ _null_ ));
DESCR("fetch the Nth preceding row value");
DATA(insert OID = 3108 (  lag			PGNSP PGUID 12 1 0 0 0 f t f f t f i 3 0 2283 "2283 23 2283" _null_ _null_ _null_ _null_ _null_ window_lag_with_offset_and_default _null_ _null_ _null_ ));
DESCR("fetch the Nth preceding row value with default");
DATA(insert OID = 3109 (  lead			PGNSP PGUID 12 1 0 0 0 f t f f t f i 1 0 2283 "2283" _null_ _null_ _null_ _null_ _null_ window_lead _null_ _null_ _null_ ));
DESCR("fetch the following row value");
DATA(insert OID = 3110 (  lead			PGNSP PGUID 12 1 0 0 0 f t f f t f i 2 0 2283 "2283 23" _null_ _null_ _null_ _null_ _null_ window_lead_with_offset _null_ _null_ _null_ ));
DESCR("fetch the Nth following row value");
DATA(insert OID = 3111 (  lead			PGNSP PGUID 12 1 0 0 0 f t f f t f i 3 0 2283 "2283 23 2283" _null_ _null_ _null_ _null_ _null_ window_lead_with_offset_and_default _null_ _null_ _null_ ));
DESCR("fetch the Nth following row value with default");
DATA(insert OID = 3112 (  first_value	PGNSP PGUID 12 1 0 0 0 f t f f t f i 1 0 2283 "2283" _null_ _null_ _null_ _null_ _null_ window_first_value _null_ _null_ _null_ ));
DESCR("fetch the first row value");
DATA(insert OID = 3113 (  last_value	PGNSP PGUID 12 1 0 0 0 f t f f t f i 1 0 2283 "2283" _null_ _null_ _null_ _null_ _null_ window_last_value _null_ _null_ _null_ ));
DESCR("fetch the last row value");
DATA(insert OID = 3114 (  nth_value		PGNSP PGUID 12 1 0 0 0 f t f f t f i 2 0 2283 "2283 23" _null_ _null_ _null_ _null_ _null_ window_nth_value _null_ _null_ _null_ ));
DESCR("fetch the Nth row value");

/* functions for range types */
DATA(insert OID = 3832 (  anyrange_in	PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 3831 "2275 26 23" _null_ _null_ _null_ _null_ _null_ anyrange_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3833 (  anyrange_out	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "3831" _null_ _null_ _null_ _null_ _null_ anyrange_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3834 (  range_in		PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 3831 "2275 26 23" _null_ _null_ _null_ _null_ _null_ range_in _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3835 (  range_out		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 2275 "3831" _null_ _null_ _null_ _null_ _null_ range_out _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3836 (  range_recv	PGNSP PGUID 12 1 0 0 0 f f f f t f s 3 0 3831 "2281 26 23" _null_ _null_ _null_ _null_ _null_ range_recv _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3837 (  range_send	PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 17 "3831" _null_ _null_ _null_ _null_ _null_ range_send _null_ _null_ _null_ ));
DESCR("I/O");
DATA(insert OID = 3848 (  lower		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2283 "3831" _null_ _null_ _null_ _null_ _null_ range_lower _null_ _null_ _null_ ));
DESCR("lower bound of range");
DATA(insert OID = 3849 (  upper		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2283 "3831" _null_ _null_ _null_ _null_ _null_ range_upper _null_ _null_ _null_ ));
DESCR("upper bound of range");
DATA(insert OID = 3850 (  isempty	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "3831" _null_ _null_ _null_ _null_ _null_ range_empty _null_ _null_ _null_ ));
DESCR("is the range empty?");
DATA(insert OID = 3851 (  lower_inc PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "3831" _null_ _null_ _null_ _null_ _null_ range_lower_inc _null_ _null_ _null_ ));
DESCR("is the range's lower bound inclusive?");
DATA(insert OID = 3852 (  upper_inc PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "3831" _null_ _null_ _null_ _null_ _null_ range_upper_inc _null_ _null_ _null_ ));
DESCR("is the range's upper bound inclusive?");
DATA(insert OID = 3853 (  lower_inf PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "3831" _null_ _null_ _null_ _null_ _null_ range_lower_inf _null_ _null_ _null_ ));
DESCR("is the range's lower bound infinite?");
DATA(insert OID = 3854 (  upper_inf PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 16 "3831" _null_ _null_ _null_ _null_ _null_ range_upper_inf _null_ _null_ _null_ ));
DESCR("is the range's upper bound infinite?");
DATA(insert OID = 3855 (  range_eq	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_eq _null_ _null_ _null_ ));
DESCR("implementation of = operator");
DATA(insert OID = 3856 (  range_ne	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_ne _null_ _null_ _null_ ));
DESCR("implementation of <> operator");
DATA(insert OID = 3857 (  range_overlaps		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_overlaps _null_ _null_ _null_ ));
DESCR("implementation of && operator");
DATA(insert OID = 3858 (  range_contains_elem	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 2283" _null_ _null_ _null_ _null_ _null_ range_contains_elem _null_ _null_ _null_ ));
DESCR("implementation of @> operator");
DATA(insert OID = 3859 (  range_contains		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_contains _null_ _null_ _null_ ));
DESCR("implementation of @> operator");
DATA(insert OID = 3860 (  elem_contained_by_range	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2283 3831" _null_ _null_ _null_ _null_ _null_ elem_contained_by_range _null_ _null_ _null_ ));
DESCR("implementation of <@ operator");
DATA(insert OID = 3861 (  range_contained_by	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_contained_by _null_ _null_ _null_ ));
DESCR("implementation of <@ operator");
DATA(insert OID = 3862 (  range_adjacent		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_adjacent _null_ _null_ _null_ ));
DESCR("implementation of -|- operator");
DATA(insert OID = 3863 (  range_before		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_before _null_ _null_ _null_ ));
DESCR("implementation of << operator");
DATA(insert OID = 3864 (  range_after		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_after _null_ _null_ _null_ ));
DESCR("implementation of >> operator");
DATA(insert OID = 3865 (  range_overleft	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_overleft _null_ _null_ _null_ ));
DESCR("implementation of &< operator");
DATA(insert OID = 3866 (  range_overright	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_overright _null_ _null_ _null_ ));
DESCR("implementation of &> operator");
DATA(insert OID = 3867 (  range_union		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3831 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_union _null_ _null_ _null_ ));
DESCR("implementation of + operator");
DATA(insert OID = 4057 (  range_merge		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3831 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_merge _null_ _null_ _null_ ));
DESCR("the smallest range which includes both of the given ranges");
DATA(insert OID = 3868 (  range_intersect	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3831 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_intersect _null_ _null_ _null_ ));
DESCR("implementation of * operator");
DATA(insert OID = 3869 (  range_minus		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 3831 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_minus _null_ _null_ _null_ ));
DESCR("implementation of - operator");
DATA(insert OID = 3870 (  range_cmp PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 23 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_cmp _null_ _null_ _null_ ));
DESCR("less-equal-greater");
DATA(insert OID = 3871 (  range_lt	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_lt _null_ _null_ _null_ ));
DATA(insert OID = 3872 (  range_le	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_le _null_ _null_ _null_ ));
DATA(insert OID = 3873 (  range_ge	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_ge _null_ _null_ _null_ ));
DATA(insert OID = 3874 (  range_gt	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "3831 3831" _null_ _null_ _null_ _null_ _null_ range_gt _null_ _null_ _null_ ));
DATA(insert OID = 3875 (  range_gist_consistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 5 0 16 "2281 3831 23 26 2281" _null_ _null_ _null_ _null_ _null_ range_gist_consistent _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3876 (  range_gist_union		PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ range_gist_union _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3877 (  range_gist_compress	PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ range_gist_compress _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3878 (  range_gist_decompress PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ range_gist_decompress _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3996 (  range_gist_fetch		PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 2281 "2281" _null_ _null_ _null_ _null_ _null_ range_gist_fetch _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3879 (  range_gist_penalty	PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ range_gist_penalty _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3880 (  range_gist_picksplit	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ range_gist_picksplit _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3881 (  range_gist_same		PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 2281 "3831 3831 2281" _null_ _null_ _null_ _null_ _null_ range_gist_same _null_ _null_ _null_ ));
DESCR("GiST support");
DATA(insert OID = 3902 (  hash_range			PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 23 "3831" _null_ _null_ _null_ _null_ _null_ hash_range _null_ _null_ _null_ ));
DESCR("hash a range");
DATA(insert OID = 3916 (  range_typanalyze		PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 16 "2281" _null_ _null_ _null_ _null_ _null_ range_typanalyze _null_ _null_ _null_ ));
DESCR("range typanalyze");
DATA(insert OID = 3169 (  rangesel				PGNSP PGUID 12 1 0 0 0 f f f f t f s 4 0 701 "2281 26 2281 23" _null_ _null_ _null_ _null_ _null_ rangesel _null_ _null_ _null_ ));
DESCR("restriction selectivity for range operators");

DATA(insert OID = 3914 (  int4range_canonical		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3904 "3904" _null_ _null_ _null_ _null_ _null_ int4range_canonical _null_ _null_ _null_ ));
DESCR("convert an int4 range to canonical form");
DATA(insert OID = 3928 (  int8range_canonical		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3926 "3926" _null_ _null_ _null_ _null_ _null_ int8range_canonical _null_ _null_ _null_ ));
DESCR("convert an int8 range to canonical form");
DATA(insert OID = 3915 (  daterange_canonical		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 1 0 3912 "3912" _null_ _null_ _null_ _null_ _null_ daterange_canonical _null_ _null_ _null_ ));
DESCR("convert a date range to canonical form");
DATA(insert OID = 3922 (  int4range_subdiff		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "23 23" _null_ _null_ _null_ _null_ _null_ int4range_subdiff _null_ _null_ _null_ ));
DESCR("float8 difference of two int4 values");
DATA(insert OID = 3923 (  int8range_subdiff		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "20 20" _null_ _null_ _null_ _null_ _null_ int8range_subdiff _null_ _null_ _null_ ));
DESCR("float8 difference of two int8 values");
DATA(insert OID = 3924 (  numrange_subdiff		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "1700 1700" _null_ _null_ _null_ _null_ _null_ numrange_subdiff _null_ _null_ _null_ ));
DESCR("float8 difference of two numeric values");
DATA(insert OID = 3925 (  daterange_subdiff		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "1082 1082" _null_ _null_ _null_ _null_ _null_ daterange_subdiff _null_ _null_ _null_ ));
DESCR("float8 difference of two date values");
DATA(insert OID = 3929 (  tsrange_subdiff		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "1114 1114" _null_ _null_ _null_ _null_ _null_ tsrange_subdiff _null_ _null_ _null_ ));
DESCR("float8 difference of two timestamp values");
DATA(insert OID = 3930 (  tstzrange_subdiff		   PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 701 "1184 1184" _null_ _null_ _null_ _null_ _null_ tstzrange_subdiff _null_ _null_ _null_ ));
DESCR("float8 difference of two timestamp with time zone values");

DATA(insert OID = 3840 (  int4range PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 3904 "23 23" _null_ _null_ _null_ _null_ _null_ range_constructor2 _null_ _null_ _null_ ));
DESCR("int4range constructor");
DATA(insert OID = 3841 (  int4range PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 3904 "23 23 25" _null_ _null_ _null_ _null_ _null_ range_constructor3 _null_ _null_ _null_ ));
DESCR("int4range constructor");
DATA(insert OID = 3844 (  numrange	PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 3906 "1700 1700" _null_ _null_ _null_ _null_ _null_ range_constructor2 _null_ _null_ _null_ ));
DESCR("numrange constructor");
DATA(insert OID = 3845 (  numrange	PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 3906 "1700 1700 25" _null_ _null_ _null_ _null_ _null_ range_constructor3 _null_ _null_ _null_ ));
DESCR("numrange constructor");
DATA(insert OID = 3933 (  tsrange	PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 3908 "1114 1114" _null_ _null_ _null_ _null_ _null_ range_constructor2 _null_ _null_ _null_ ));
DESCR("tsrange constructor");
DATA(insert OID = 3934 (  tsrange	PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 3908 "1114 1114 25" _null_ _null_ _null_ _null_ _null_ range_constructor3 _null_ _null_ _null_ ));
DESCR("tsrange constructor");
DATA(insert OID = 3937 (  tstzrange PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 3910 "1184 1184" _null_ _null_ _null_ _null_ _null_ range_constructor2 _null_ _null_ _null_ ));
DESCR("tstzrange constructor");
DATA(insert OID = 3938 (  tstzrange PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 3910 "1184 1184 25" _null_ _null_ _null_ _null_ _null_ range_constructor3 _null_ _null_ _null_ ));
DESCR("tstzrange constructor");
DATA(insert OID = 3941 (  daterange PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 3912 "1082 1082" _null_ _null_ _null_ _null_ _null_ range_constructor2 _null_ _null_ _null_ ));
DESCR("daterange constructor");
DATA(insert OID = 3942 (  daterange PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 3912 "1082 1082 25" _null_ _null_ _null_ _null_ _null_ range_constructor3 _null_ _null_ _null_ ));
DESCR("daterange constructor");
DATA(insert OID = 3945 (  int8range PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 3926 "20 20" _null_ _null_ _null_ _null_ _null_ range_constructor2 _null_ _null_ _null_ ));
DESCR("int8range constructor");
DATA(insert OID = 3946 (  int8range PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 3926 "20 20 25" _null_ _null_ _null_ _null_ _null_ range_constructor3 _null_ _null_ _null_ ));
DESCR("int8range constructor");

/* date, time, timestamp constructors */
DATA(insert OID = 3846 ( make_date	PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1082 "23 23 23" _null_ _null_ "{year,month,day}" _null_ _null_ make_date _null_ _null_ _null_ ));
DESCR("construct date");
DATA(insert OID = 3847 ( make_time	PGNSP PGUID 12 1 0 0 0 f f f f t f i 3 0 1083 "23 23 701" _null_ _null_ "{hour,min,sec}" _null_ _null_ make_time _null_ _null_ _null_ ));
DESCR("construct time");
DATA(insert OID = 3461 ( make_timestamp PGNSP PGUID 12 1 0 0 0 f f f f t f i 6 0 1114 "23 23 23 23 23 701" _null_ _null_ "{year,month,mday,hour,min,sec}" _null_ _null_ make_timestamp _null_ _null_ _null_ ));
DESCR("construct timestamp");
DATA(insert OID = 3462 ( make_timestamptz	PGNSP PGUID 12 1 0 0 0 f f f f t f s 6 0 1184 "23 23 23 23 23 701" _null_ _null_ "{year,month,mday,hour,min,sec}" _null_ _null_ make_timestamptz _null_ _null_ _null_ ));
DESCR("construct timestamp with time zone");
DATA(insert OID = 3463 ( make_timestamptz	PGNSP PGUID 12 1 0 0 0 f f f f t f s 7 0 1184 "23 23 23 23 23 701 25" _null_ _null_ "{year,month,mday,hour,min,sec,timezone}" _null_ _null_ make_timestamptz_at_timezone _null_ _null_ _null_ ));
DESCR("construct timestamp with time zone");
DATA(insert OID = 3464 ( make_interval	PGNSP PGUID 12 1 0 0 0 f f f f t f i 7 0 1186 "23 23 23 23 23 23 701" _null_ _null_ "{years,months,weeks,days,hours,mins,secs}" _null_ _null_ make_interval _null_ _null_ _null_ ));
DESCR("construct interval");

/* spgist support functions */
DATA(insert OID = 4001 (  spggettuple	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 16 "2281 2281" _null_ _null_ _null_ _null_ _null_	spggettuple _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4002 (  spggetbitmap	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 20 "2281 2281" _null_ _null_ _null_ _null_ _null_	spggetbitmap _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4003 (  spginsert		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 6 0 16 "2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_	spginsert _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4004 (  spgbeginscan	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_	spgbeginscan _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4005 (  spgrescan		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 5 0 2278 "2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ spgrescan _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4006 (  spgendscan	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ spgendscan _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4007 (  spgmarkpos	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ spgmarkpos _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4008 (  spgrestrpos	   PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ spgrestrpos _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4009 (  spgbuild		   PGNSP PGUID 12 1 0 0 0 f f f f t f v 3 0 2281 "2281 2281 2281" _null_ _null_ _null_ _null_ _null_ spgbuild _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4010 (  spgbuildempty    PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "2281" _null_ _null_ _null_ _null_ _null_ spgbuildempty _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4011 (  spgbulkdelete    PGNSP PGUID 12 1 0 0 0 f f f f t f v 4 0 2281 "2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ spgbulkdelete _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4012 (  spgvacuumcleanup	 PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2281 "2281 2281" _null_ _null_ _null_ _null_ _null_ spgvacuumcleanup _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4032 (  spgcanreturn	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 16 "2281 23" _null_ _null_ _null_ _null_ _null_ spgcanreturn _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4013 (  spgcostestimate  PGNSP PGUID 12 1 0 0 0 f f f f t f v 7 0 2278 "2281 2281 2281 2281 2281 2281 2281" _null_ _null_ _null_ _null_ _null_ spgcostestimate _null_ _null_ _null_ ));
DESCR("spgist(internal)");
DATA(insert OID = 4014 (  spgoptions	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 2 0 17 "1009 16" _null_ _null_ _null_ _null_  _null_ spgoptions _null_ _null_ _null_ ));
DESCR("spgist(internal)");

/* spgist opclasses */
DATA(insert OID = 4018 (  spg_quad_config	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_quad_config _null_ _null_ _null_ ));
DESCR("SP-GiST support for quad tree over point");
DATA(insert OID = 4019 (  spg_quad_choose	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_quad_choose _null_ _null_ _null_ ));
DESCR("SP-GiST support for quad tree over point");
DATA(insert OID = 4020 (  spg_quad_picksplit	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_quad_picksplit _null_ _null_ _null_ ));
DESCR("SP-GiST support for quad tree over point");
DATA(insert OID = 4021 (  spg_quad_inner_consistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_quad_inner_consistent _null_ _null_ _null_ ));
DESCR("SP-GiST support for quad tree over point");
DATA(insert OID = 4022 (  spg_quad_leaf_consistent	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_quad_leaf_consistent _null_ _null_ _null_ ));
DESCR("SP-GiST support for quad tree and k-d tree over point");

DATA(insert OID = 4023 (  spg_kd_config PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_kd_config _null_ _null_ _null_ ));
DESCR("SP-GiST support for k-d tree over point");
DATA(insert OID = 4024 (  spg_kd_choose PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_kd_choose _null_ _null_ _null_ ));
DESCR("SP-GiST support for k-d tree over point");
DATA(insert OID = 4025 (  spg_kd_picksplit	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_kd_picksplit _null_ _null_ _null_ ));
DESCR("SP-GiST support for k-d tree over point");
DATA(insert OID = 4026 (  spg_kd_inner_consistent	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_kd_inner_consistent _null_ _null_ _null_ ));
DESCR("SP-GiST support for k-d tree over point");

DATA(insert OID = 4027 (  spg_text_config	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_text_config _null_ _null_ _null_ ));
DESCR("SP-GiST support for radix tree over text");
DATA(insert OID = 4028 (  spg_text_choose	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_text_choose _null_ _null_ _null_ ));
DESCR("SP-GiST support for radix tree over text");
DATA(insert OID = 4029 (  spg_text_picksplit	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_text_picksplit _null_ _null_ _null_ ));
DESCR("SP-GiST support for radix tree over text");
DATA(insert OID = 4030 (  spg_text_inner_consistent PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_text_inner_consistent _null_ _null_ _null_ ));
DESCR("SP-GiST support for radix tree over text");
DATA(insert OID = 4031 (  spg_text_leaf_consistent	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_text_leaf_consistent _null_ _null_ _null_ ));
DESCR("SP-GiST support for radix tree over text");

DATA(insert OID = 3469 (  spg_range_quad_config PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_range_quad_config _null_ _null_ _null_ ));
DESCR("SP-GiST support for quad tree over range");
DATA(insert OID = 3470 (  spg_range_quad_choose PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_range_quad_choose _null_ _null_ _null_ ));
DESCR("SP-GiST support for quad tree over range");
DATA(insert OID = 3471 (  spg_range_quad_picksplit	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_range_quad_picksplit _null_ _null_ _null_ ));
DESCR("SP-GiST support for quad tree over range");
DATA(insert OID = 3472 (  spg_range_quad_inner_consistent	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 2278 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_range_quad_inner_consistent _null_ _null_ _null_ ));
DESCR("SP-GiST support for quad tree over range");
DATA(insert OID = 3473 (  spg_range_quad_leaf_consistent	PGNSP PGUID 12 1 0 0 0 f f f f t f i 2 0 16 "2281 2281" _null_ _null_ _null_ _null_  _null_ spg_range_quad_leaf_consistent _null_ _null_ _null_ ));
DESCR("SP-GiST support for quad tree over range");

/* replication slots */
DATA(insert OID = 3779 (  pg_create_physical_replication_slot PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2249 "19" "{19,19,3220}" "{i,o,o}" "{slot_name,slot_name,xlog_position}" _null_ _null_ pg_create_physical_replication_slot _null_ _null_ _null_ ));
DESCR("create a physical replication slot");
DATA(insert OID = 3780 (  pg_drop_replication_slot PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "19" _null_ _null_ _null_ _null_ _null_ pg_drop_replication_slot _null_ _null_ _null_ ));
DESCR("drop a replication slot");
DATA(insert OID = 3781 (  pg_get_replication_slots	PGNSP PGUID 12 1 10 0 0 f f f f f t s 0 0 2249 "" "{19,19,25,26,16,23,28,28,3220}" "{o,o,o,o,o,o,o,o,o}" "{slot_name,plugin,slot_type,datoid,active,active_pid,xmin,catalog_xmin,restart_lsn}" _null_ _null_ pg_get_replication_slots _null_ _null_ _null_ ));
DESCR("information about replication slots currently in use");
DATA(insert OID = 3786 (  pg_create_logical_replication_slot PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2249 "19 19" "{19,19,25,3220}" "{i,i,o,o}" "{slot_name,plugin,slot_name,xlog_position}" _null_ _null_ pg_create_logical_replication_slot _null_ _null_ _null_ ));
DESCR("set up a logical replication slot");
DATA(insert OID = 3782 (  pg_logical_slot_get_changes PGNSP PGUID 12 1000 1000 25 0 f f f f f t v 4 0 2249 "19 3220 23 1009" "{19,3220,23,1009,3220,28,25}" "{i,i,i,v,o,o,o}" "{slot_name,upto_lsn,upto_nchanges,options,location,xid,data}" _null_ _null_ pg_logical_slot_get_changes _null_ _null_ _null_ ));
DESCR("get changes from replication slot");
DATA(insert OID = 3783 (  pg_logical_slot_get_binary_changes PGNSP PGUID 12 1000 1000 25 0 f f f f f t v 4 0 2249 "19 3220 23 1009" "{19,3220,23,1009,3220,28,17}" "{i,i,i,v,o,o,o}" "{slot_name,upto_lsn,upto_nchanges,options,location,xid,data}" _null_ _null_ pg_logical_slot_get_binary_changes _null_ _null_ _null_ ));
DESCR("get binary changes from replication slot");
DATA(insert OID = 3784 (  pg_logical_slot_peek_changes PGNSP PGUID 12 1000 1000 25 0 f f f f f t v 4 0 2249 "19 3220 23 1009" "{19,3220,23,1009,3220,28,25}" "{i,i,i,v,o,o,o}" "{slot_name,upto_lsn,upto_nchanges,options,location,xid,data}" _null_ _null_ pg_logical_slot_peek_changes _null_ _null_ _null_ ));
DESCR("peek at changes from replication slot");
DATA(insert OID = 3785 (  pg_logical_slot_peek_binary_changes PGNSP PGUID 12 1000 1000 25 0 f f f f f t v 4 0 2249 "19 3220 23 1009" "{19,3220,23,1009,3220,28,17}" "{i,i,i,v,o,o,o}" "{slot_name,upto_lsn,upto_nchanges,options,location,xid,data}" _null_ _null_ pg_logical_slot_peek_binary_changes _null_ _null_ _null_ ));
DESCR("peek at binary changes from replication slot");

/* event triggers */
DATA(insert OID = 3566 (  pg_event_trigger_dropped_objects		PGNSP PGUID 12 10 100 0 0 f f f f t t s 0 0 2249 "" "{26,26,23,16,16,16,25,25,25,25,1009,1009}" "{o,o,o,o,o,o,o,o,o,o,o,o}" "{classid, objid, objsubid, original, normal, is_temporary, object_type, schema_name, object_name, object_identity, address_names, address_args}" _null_ _null_ pg_event_trigger_dropped_objects _null_ _null_ _null_ ));
DESCR("list objects dropped by the current command");
DATA(insert OID = 4566 (  pg_event_trigger_table_rewrite_oid	PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 26 "" "{26}" "{o}" "{oid}" _null_ _null_ pg_event_trigger_table_rewrite_oid _null_ _null_ _null_ ));
DESCR("return Oid of the table getting rewritten");
DATA(insert OID = 4567 (  pg_event_trigger_table_rewrite_reason PGNSP PGUID 12 1 0 0 0 f f f f t f s 0 0 23 "" _null_ _null_ _null_ _null_ _null_ pg_event_trigger_table_rewrite_reason _null_ _null_ _null_ ));
DESCR("return reason code for table getting rewritten");
DATA(insert OID = 4568 (  pg_event_trigger_ddl_commands			PGNSP PGUID 12 10 100 0 0 f f f f t t s 0 0 2249 "" "{26,26,23,25,25,25,25,16,32}" "{o,o,o,o,o,o,o,o,o}" "{classid, objid, objsubid, command_tag, object_type, schema_name, object_identity, in_extension, command}" _null_ _null_ pg_event_trigger_ddl_commands _null_ _null_ _null_ ));
DESCR("list DDL actions being executed by the current command");

/* generic transition functions for ordered-set aggregates */
DATA(insert OID = 3970 ( ordered_set_transition			PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2281 "2281 2276" _null_ _null_ _null_ _null_ _null_ ordered_set_transition _null_ _null_ _null_ ));
DESCR("aggregate transition function");
DATA(insert OID = 3971 ( ordered_set_transition_multi	PGNSP PGUID 12 1 0 2276 0 f f f f f f i 2 0 2281 "2281 2276" "{2281,2276}" "{i,v}" _null_ _null_ _null_ ordered_set_transition_multi _null_ _null_ _null_ ));
DESCR("aggregate transition function");

/* inverse distribution aggregates (and their support functions) */
DATA(insert OID = 3972 ( percentile_disc		PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 2283 "701 2283" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("discrete percentile");
DATA(insert OID = 3973 ( percentile_disc_final	PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 2283 "2281 701 2283" _null_ _null_ _null_ _null_ _null_ percentile_disc_final _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3974 ( percentile_cont		PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 701 "701 701" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("continuous distribution percentile");
DATA(insert OID = 3975 ( percentile_cont_float8_final	PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 701 "2281 701" _null_ _null_ _null_ _null_ _null_ percentile_cont_float8_final _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3976 ( percentile_cont		PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 1186 "701 1186" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("continuous distribution percentile");
DATA(insert OID = 3977 ( percentile_cont_interval_final PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 1186 "2281 701" _null_ _null_ _null_ _null_ _null_ percentile_cont_interval_final _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3978 ( percentile_disc		PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 2277 "1022 2283" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("multiple discrete percentiles");
DATA(insert OID = 3979 ( percentile_disc_multi_final	PGNSP PGUID 12 1 0 0 0 f f f f f f i 3 0 2277 "2281 1022 2283" _null_ _null_ _null_ _null_ _null_ percentile_disc_multi_final _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3980 ( percentile_cont		PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 1022 "1022 701" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("multiple continuous percentiles");
DATA(insert OID = 3981 ( percentile_cont_float8_multi_final PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 1022 "2281 1022" _null_ _null_ _null_ _null_ _null_ percentile_cont_float8_multi_final _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3982 ( percentile_cont		PGNSP PGUID 12 1 0 0 0 t f f f f f i 2 0 1187 "1022 1186" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("multiple continuous percentiles");
DATA(insert OID = 3983 ( percentile_cont_interval_multi_final	PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 1187 "2281 1022" _null_ _null_ _null_ _null_ _null_ percentile_cont_interval_multi_final _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3984 ( mode					PGNSP PGUID 12 1 0 0 0 t f f f f f i 1 0 2283 "2283" _null_ _null_ _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("most common value");
DATA(insert OID = 3985 ( mode_final				PGNSP PGUID 12 1 0 0 0 f f f f f f i 2 0 2283 "2281 2283" _null_ _null_ _null_ _null_ _null_	mode_final _null_ _null_ _null_ ));
DESCR("aggregate final function");

/* hypothetical-set aggregates (and their support functions) */
DATA(insert OID = 3986 ( rank				PGNSP PGUID 12 1 0 2276 0 t f f f f f i 1 0 20 "2276" "{2276}" "{v}" _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("rank of hypothetical row");
DATA(insert OID = 3987 ( rank_final			PGNSP PGUID 12 1 0 2276 0 f f f f f f i 2 0 20 "2281 2276" "{2281,2276}" "{i,v}" _null_ _null_ _null_	hypothetical_rank_final _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3988 ( percent_rank		PGNSP PGUID 12 1 0 2276 0 t f f f f f i 1 0 701 "2276" "{2276}" "{v}" _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("fractional rank of hypothetical row");
DATA(insert OID = 3989 ( percent_rank_final PGNSP PGUID 12 1 0 2276 0 f f f f f f i 2 0 701 "2281 2276" "{2281,2276}" "{i,v}" _null_ _null_ _null_ hypothetical_percent_rank_final _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3990 ( cume_dist			PGNSP PGUID 12 1 0 2276 0 t f f f f f i 1 0 701 "2276" "{2276}" "{v}" _null_ _null_ _null_ aggregate_dummy _null_ _null_ _null_ ));
DESCR("cumulative distribution of hypothetical row");
DATA(insert OID = 3991 ( cume_dist_final	PGNSP PGUID 12 1 0 2276 0 f f f f f f i 2 0 701 "2281 2276" "{2281,2276}" "{i,v}" _null_ _null_ _null_ hypothetical_cume_dist_final _null_ _null_ _null_ ));
DESCR("aggregate final function");
DATA(insert OID = 3992 ( dense_rank			PGNSP PGUID 12 1 0 2276 0 t f f f f f i 1 0 20 "2276" "{2276}" "{v}" _null_ _null_ _null_	aggregate_dummy _null_ _null_ _null_ ));
DESCR("rank of hypothetical row without gaps");
DATA(insert OID = 3993 ( dense_rank_final	PGNSP PGUID 12 1 0 2276 0 f f f f f f i 2 0 20 "2281 2276" "{2281,2276}" "{i,v}" _null_ _null_ _null_	hypothetical_dense_rank_final _null_ _null_ _null_ ));
DESCR("aggregate final function");

/* pg_upgrade support */
DATA(insert OID = 3582 ( binary_upgrade_set_next_pg_type_oid PGNSP PGUID  12 1 0 0 0 f f f f t f v 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ binary_upgrade_set_next_pg_type_oid _null_ _null_ _null_ ));
DESCR("for use by pg_upgrade");
DATA(insert OID = 3584 ( binary_upgrade_set_next_array_pg_type_oid PGNSP PGUID	12 1 0 0 0 f f f f t f v 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ binary_upgrade_set_next_array_pg_type_oid _null_ _null_ _null_ ));
DESCR("for use by pg_upgrade");
DATA(insert OID = 3585 ( binary_upgrade_set_next_toast_pg_type_oid PGNSP PGUID	12 1 0 0 0 f f f f t f v 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ binary_upgrade_set_next_toast_pg_type_oid _null_ _null_ _null_ ));
DESCR("for use by pg_upgrade");
DATA(insert OID = 3586 ( binary_upgrade_set_next_heap_pg_class_oid PGNSP PGUID	12 1 0 0 0 f f f f t f v 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ binary_upgrade_set_next_heap_pg_class_oid _null_ _null_ _null_ ));
DESCR("for use by pg_upgrade");
DATA(insert OID = 3587 ( binary_upgrade_set_next_index_pg_class_oid PGNSP PGUID  12 1 0 0 0 f f f f t f v 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ binary_upgrade_set_next_index_pg_class_oid _null_ _null_ _null_ ));
DESCR("for use by pg_upgrade");
DATA(insert OID = 3588 ( binary_upgrade_set_next_toast_pg_class_oid PGNSP PGUID  12 1 0 0 0 f f f f t f v 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ binary_upgrade_set_next_toast_pg_class_oid _null_ _null_ _null_ ));
DESCR("for use by pg_upgrade");
DATA(insert OID = 3589 ( binary_upgrade_set_next_pg_enum_oid PGNSP PGUID  12 1 0 0 0 f f f f t f v 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ binary_upgrade_set_next_pg_enum_oid _null_ _null_ _null_ ));
DESCR("for use by pg_upgrade");
DATA(insert OID = 3590 ( binary_upgrade_set_next_pg_authid_oid PGNSP PGUID	12 1 0 0 0 f f f f t f v 1 0 2278 "26" _null_ _null_ _null_ _null_ _null_ binary_upgrade_set_next_pg_authid_oid _null_ _null_ _null_ ));
DESCR("for use by pg_upgrade");
DATA(insert OID = 3591 ( binary_upgrade_create_empty_extension PGNSP PGUID	12 1 0 0 0 f f f f f f v 7 0 2278 "25 25 16 25 1028 1009 1009" _null_ _null_ _null_ _null_ _null_ binary_upgrade_create_empty_extension _null_ _null_ _null_ ));
DESCR("for use by pg_upgrade");

/* replication/origin.h */
DATA(insert OID = 6003 ( pg_replication_origin_create PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 26 "25" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_create _null_ _null_ _null_ ));
DESCR("create a replication origin");

DATA(insert OID = 6004 ( pg_replication_origin_drop PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "25" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_drop _null_ _null_ _null_ ));
DESCR("drop replication origin identified by its name");

DATA(insert OID = 6005 ( pg_replication_origin_oid PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 26 "25" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_oid _null_ _null_ _null_ ));
DESCR("translate the replication origin's name to its id");

DATA(insert OID = 6006 ( pg_replication_origin_session_setup PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 2278 "25" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_session_setup _null_ _null_ _null_ ));
DESCR("configure session to maintain replication progress tracking for the passed in origin");

DATA(insert OID = 6007 ( pg_replication_origin_session_reset PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 2278 "" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_session_reset _null_ _null_ _null_ ));
DESCR("teardown configured replication progress tracking");

DATA(insert OID = 6008 ( pg_replication_origin_session_is_setup PGNSP PGUID 12 1 0 0 0 f f f f t f v 0 0 16 "" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_session_is_setup _null_ _null_ _null_ ));
DESCR("is a replication origin configured in this session");

DATA(insert OID = 6009 ( pg_replication_origin_session_progress PGNSP PGUID 12 1 0 0 0 f f f f t f v 1 0 3220 "16" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_session_progress _null_ _null_ _null_ ));
DESCR("get the replication progress of the current session");

DATA(insert OID = 6010 ( pg_replication_origin_xact_setup PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2278 "3220 1184" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_xact_setup _null_ _null_ _null_ ));
DESCR("setup the transaction's origin lsn and timestamp");

DATA(insert OID = 6011 ( pg_replication_origin_xact_reset PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2278 "3220 1184" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_xact_reset _null_ _null_ _null_ ));
DESCR("reset the transaction's origin lsn and timestamp");

DATA(insert OID = 6012 ( pg_replication_origin_advance PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 2278 "25 3220" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_advance _null_ _null_ _null_ ));
DESCR("advance replication itentifier to specific location");

DATA(insert OID = 6013 ( pg_replication_origin_progress PGNSP PGUID 12 1 0 0 0 f f f f t f v 2 0 3220 "25 16" _null_ _null_ _null_ _null_ _null_ pg_replication_origin_progress _null_ _null_ _null_ ));
DESCR("get an individual replication origin's replication progress");

DATA(insert OID = 6014 ( pg_show_replication_origin_status PGNSP PGUID 12 1 100 0 0 f f f f f t v 0 0 2249 "" "{26,25,3220,3220}" "{o,o,o,o}" "{local_id, external_id, remote_lsn, local_lsn}" _null_ _null_ pg_show_replication_origin_status _null_ _null_ _null_ ));
DESCR("get progress for all replication origins");

/* rls */
DATA(insert OID = 3298 (  row_security_active	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 16 "26" _null_ _null_ _null_ _null_ _null_	row_security_active _null_ _null_ _null_ ));
DESCR("row security for current context active on table by table oid");
DATA(insert OID = 3299 (  row_security_active	   PGNSP PGUID 12 1 0 0 0 f f f f t f s 1 0 16 "25" _null_ _null_ _null_ _null_ _null_	row_security_active_name _null_ _null_ _null_ ));
DESCR("row security for current context active on table by table name");

/*
 * Symbolic values for provolatile column: these indicate whether the result
 * of a function is dependent *only* on the values of its explicit arguments,
 * or can change due to outside factors (such as parameter variables or
 * table contents).  NOTE: functions having side-effects, such as setval(),
 * must be labeled volatile to ensure they will not get optimized away,
 * even if the actual return value is not changeable.
 */
#define PROVOLATILE_IMMUTABLE	'i'		/* never changes for given input */
#define PROVOLATILE_STABLE		's'		/* does not change within a scan */
#define PROVOLATILE_VOLATILE	'v'		/* can change even within a scan */

/*
 * Symbolic values for proargmodes column.  Note that these must agree with
 * the FunctionParameterMode enum in parsenodes.h; we declare them here to
 * be accessible from either header.
 */
#define PROARGMODE_IN		'i'
#define PROARGMODE_OUT		'o'
#define PROARGMODE_INOUT	'b'
#define PROARGMODE_VARIADIC 'v'
#define PROARGMODE_TABLE	't'

#endif   /* PG_PROC_H */
