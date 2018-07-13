/*-------------------------------------------------------------------------
 *
 * pg_authid.h
 *	  definition of the system "authorization identifier" relation (pg_authid)
 *	  along with the relation's initial contents.
 *
 *	  pg_shadow and pg_group are now publicly accessible views on pg_authid.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_authid.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AUTHID_H
#define PG_AUTHID_H

#include "catalog/genbki.h"

/*
 * The CATALOG definition has to refer to the type of rolvaliduntil as
 * "timestamptz" (lower case) so that bootstrap mode recognizes it.  But
 * the C header files define this type as TimestampTz.  Since the field is
 * potentially-null and therefore can't be accessed directly from C code,
 * there is no particular need for the C struct definition to show the
 * field type as TimestampTz --- instead we just make it int.
 */
#define timestamptz int


/* ----------------
 *		pg_authid definition.  cpp turns this into
 *		typedef struct FormData_pg_authid
 * ----------------
 */
#define AuthIdRelationId	1260
#define AuthIdRelation_Rowtype_Id	2842

CATALOG(pg_authid,1260) BKI_SHARED_RELATION BKI_ROWTYPE_OID(2842) BKI_SCHEMA_MACRO
{
	NameData	rolname;		/* name of role */
	bool		rolsuper;		/* read this field via superuser() only! */
	bool		rolinherit;		/* inherit privileges from other roles? */
	bool		rolcreaterole;	/* allowed to create more roles? */
	bool		rolcreatedb;	/* allowed to create databases? */
	bool		rolcanlogin;	/* allowed to log in as session user? */
	bool		rolreplication; /* role used for streaming replication */
	bool		rolbypassrls;	/* bypasses row level security? */
	int32		rolconnlimit;	/* max connections allowed (-1=no limit) */

	/* remaining fields may be null; use heap_getattr to read them! */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		rolpassword;	/* password, if any */
	timestamptz rolvaliduntil;	/* password expiration time, if any */
#endif
} FormData_pg_authid;

#undef timestamptz


/* ----------------
 *		Form_pg_authid corresponds to a pointer to a tuple with
 *		the format of pg_authid relation.
 * ----------------
 */
typedef FormData_pg_authid *Form_pg_authid;

/* ----------------
 *		compiler constants for pg_authid
 * ----------------
 */
#define Natts_pg_authid					11
#define Anum_pg_authid_rolname			1
#define Anum_pg_authid_rolsuper			2
#define Anum_pg_authid_rolinherit		3
#define Anum_pg_authid_rolcreaterole	4
#define Anum_pg_authid_rolcreatedb		5
#define Anum_pg_authid_rolcanlogin		6
#define Anum_pg_authid_rolreplication	7
#define Anum_pg_authid_rolbypassrls		8
#define Anum_pg_authid_rolconnlimit		9
#define Anum_pg_authid_rolpassword		10
#define Anum_pg_authid_rolvaliduntil	11

/* ----------------
 *		initial contents of pg_authid
 *
 * The uppercase quantities will be replaced at initdb time with
 * user choices.
 * ----------------
 */
DATA(insert OID = 10 ( "POSTGRES" t t t t t t t -1 _null_ _null_));

#define BOOTSTRAP_SUPERUSERID 10

#endif   /* PG_AUTHID_H */
