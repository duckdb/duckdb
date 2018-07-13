/*-------------------------------------------------------------------------
 *
 * pg_class.h
 *	  definition of the system "relation" relation (pg_class)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_class.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLASS_H
#define PG_CLASS_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_class definition.  cpp turns this into
 *		typedef struct FormData_pg_class
 * ----------------
 */
#define RelationRelationId	1259
#define RelationRelation_Rowtype_Id  83

CATALOG(pg_class,1259) BKI_BOOTSTRAP BKI_ROWTYPE_OID(83) BKI_SCHEMA_MACRO
{
	NameData	relname;		/* class name */
	Oid			relnamespace;	/* OID of namespace containing this class */
	Oid			reltype;		/* OID of entry in pg_type for table's
								 * implicit row type */
	Oid			reloftype;		/* OID of entry in pg_type for underlying
								 * composite type */
	Oid			relowner;		/* class owner */
	Oid			relam;			/* index access method; 0 if not an index */
	Oid			relfilenode;	/* identifier of physical storage file */

	/* relfilenode == 0 means it is a "mapped" relation, see relmapper.c */
	Oid			reltablespace;	/* identifier of table space for relation */
	int32		relpages;		/* # of blocks (not always up-to-date) */
	float4		reltuples;		/* # of tuples (not always up-to-date) */
	int32		relallvisible;	/* # of all-visible blocks (not always
								 * up-to-date) */
	Oid			reltoastrelid;	/* OID of toast table; 0 if none */
	bool		relhasindex;	/* T if has (or has had) any indexes */
	bool		relisshared;	/* T if shared across databases */
	char		relpersistence; /* see RELPERSISTENCE_xxx constants below */
	char		relkind;		/* see RELKIND_xxx constants below */
	int16		relnatts;		/* number of user attributes */

	/*
	 * Class pg_attribute must contain exactly "relnatts" user attributes
	 * (with attnums ranging from 1 to relnatts) for this class.  It may also
	 * contain entries with negative attnums for system attributes.
	 */
	int16		relchecks;		/* # of CHECK constraints for class */
	bool		relhasoids;		/* T if we generate OIDs for rows of rel */
	bool		relhaspkey;		/* has (or has had) PRIMARY KEY index */
	bool		relhasrules;	/* has (or has had) any rules */
	bool		relhastriggers; /* has (or has had) any TRIGGERs */
	bool		relhassubclass; /* has (or has had) derived classes */
	bool		relrowsecurity; /* row security is enabled or not */
	bool		relforcerowsecurity; /* row security forced for owners or not */
	bool		relispopulated; /* matview currently holds query results */
	char		relreplident;	/* see REPLICA_IDENTITY_xxx constants  */
	TransactionId relfrozenxid; /* all Xids < this are frozen in this rel */
	TransactionId relminmxid;	/* all multixacts in this rel are >= this.
								 * this is really a MultiXactId */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	/* NOTE: These fields are not present in a relcache entry's rd_rel field. */
	aclitem		relacl[1];		/* access permissions */
	text		reloptions[1];	/* access-method-specific options */
#endif
} FormData_pg_class;

/* Size of fixed part of pg_class tuples, not counting var-length fields */
#define CLASS_TUPLE_SIZE \
	 (offsetof(FormData_pg_class,relminmxid) + sizeof(TransactionId))

/* ----------------
 *		Form_pg_class corresponds to a pointer to a tuple with
 *		the format of pg_class relation.
 * ----------------
 */
typedef FormData_pg_class *Form_pg_class;

/* ----------------
 *		compiler constants for pg_class
 * ----------------
 */

#define Natts_pg_class						31
#define Anum_pg_class_relname				1
#define Anum_pg_class_relnamespace			2
#define Anum_pg_class_reltype				3
#define Anum_pg_class_reloftype				4
#define Anum_pg_class_relowner				5
#define Anum_pg_class_relam					6
#define Anum_pg_class_relfilenode			7
#define Anum_pg_class_reltablespace			8
#define Anum_pg_class_relpages				9
#define Anum_pg_class_reltuples				10
#define Anum_pg_class_relallvisible			11
#define Anum_pg_class_reltoastrelid			12
#define Anum_pg_class_relhasindex			13
#define Anum_pg_class_relisshared			14
#define Anum_pg_class_relpersistence		15
#define Anum_pg_class_relkind				16
#define Anum_pg_class_relnatts				17
#define Anum_pg_class_relchecks				18
#define Anum_pg_class_relhasoids			19
#define Anum_pg_class_relhaspkey			20
#define Anum_pg_class_relhasrules			21
#define Anum_pg_class_relhastriggers		22
#define Anum_pg_class_relhassubclass		23
#define Anum_pg_class_relrowsecurity		24
#define Anum_pg_class_relforcerowsecurity	25
#define Anum_pg_class_relispopulated		26
#define Anum_pg_class_relreplident			27
#define Anum_pg_class_relfrozenxid			28
#define Anum_pg_class_relminmxid			29
#define Anum_pg_class_relacl				30
#define Anum_pg_class_reloptions			31

/* ----------------
 *		initial contents of pg_class
 *
 * NOTE: only "bootstrapped" relations need to be declared here.  Be sure that
 * the OIDs listed here match those given in their CATALOG macros, and that
 * the relnatts values are correct.
 * ----------------
 */

/*
 * Note: "3" in the relfrozenxid column stands for FirstNormalTransactionId;
 * similarly, "1" in relminmxid stands for FirstMultiXactId
 */
DATA(insert OID = 1247 (  pg_type		PGNSP 71 0 PGUID 0 0 0 0 0 0 0 f f p r 30 0 t f f f f f f t n 3 1 _null_ _null_ ));
DESCR("");
DATA(insert OID = 1249 (  pg_attribute	PGNSP 75 0 PGUID 0 0 0 0 0 0 0 f f p r 21 0 f f f f f f f t n 3 1 _null_ _null_ ));
DESCR("");
DATA(insert OID = 1255 (  pg_proc		PGNSP 81 0 PGUID 0 0 0 0 0 0 0 f f p r 28 0 t f f f f f f t n 3 1 _null_ _null_ ));
DESCR("");
DATA(insert OID = 1259 (  pg_class		PGNSP 83 0 PGUID 0 0 0 0 0 0 0 f f p r 31 0 t f f f f f f t n 3 1 _null_ _null_ ));
DESCR("");


#define		  RELKIND_RELATION		  'r'		/* ordinary table */
#define		  RELKIND_INDEX			  'i'		/* secondary index */
#define		  RELKIND_SEQUENCE		  'S'		/* sequence object */
#define		  RELKIND_TOASTVALUE	  't'		/* for out-of-line values */
#define		  RELKIND_VIEW			  'v'		/* view */
#define		  RELKIND_COMPOSITE_TYPE  'c'		/* composite type */
#define		  RELKIND_FOREIGN_TABLE   'f'		/* foreign table */
#define		  RELKIND_MATVIEW		  'm'		/* materialized view */

#define		  RELPERSISTENCE_PERMANENT	'p'		/* regular table */
#define		  RELPERSISTENCE_UNLOGGED	'u'		/* unlogged permanent table */
#define		  RELPERSISTENCE_TEMP		't'		/* temporary table */

/* default selection for replica identity (primary key or nothing) */
#define		  REPLICA_IDENTITY_DEFAULT	'd'
/* no replica identity is logged for this relation */
#define		  REPLICA_IDENTITY_NOTHING	'n'
/* all columns are logged as replica identity */
#define		  REPLICA_IDENTITY_FULL		'f'
/*
 * an explicitly chosen candidate key's columns are used as identity;
 * will still be set if the index has been dropped, in that case it
 * has the same meaning as 'd'
 */
#define		  REPLICA_IDENTITY_INDEX	'i'
#endif   /* PG_CLASS_H */
