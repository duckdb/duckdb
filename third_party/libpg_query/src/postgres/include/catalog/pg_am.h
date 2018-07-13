/*-------------------------------------------------------------------------
 *
 * pg_am.h
 *	  definition of the system "access method" relation (pg_am)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_am.h
 *
 * NOTES
 *		the genbki.pl script reads this file and generates .bki
 *		information from the DATA() statements.
 *
 *		XXX do NOT break up DATA() statements into multiple lines!
 *			the scripts are not as smart as you might think...
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AM_H
#define PG_AM_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_am definition.  cpp turns this into
 *		typedef struct FormData_pg_am
 * ----------------
 */
#define AccessMethodRelationId	2601

CATALOG(pg_am,2601)
{
	NameData	amname;			/* access method name */
	int16		amstrategies;	/* total number of strategies (operators) by
								 * which we can traverse/search this AM. Zero
								 * if AM does not have a fixed set of strategy
								 * assignments. */
	int16		amsupport;		/* total number of support functions that this
								 * AM uses */
	bool		amcanorder;		/* does AM support order by column value? */
	bool		amcanorderbyop; /* does AM support order by operator result? */
	bool		amcanbackward;	/* does AM support backward scan? */
	bool		amcanunique;	/* does AM support UNIQUE indexes? */
	bool		amcanmulticol;	/* does AM support multi-column indexes? */
	bool		amoptionalkey;	/* can query omit key for the first column? */
	bool		amsearcharray;	/* can AM handle ScalarArrayOpExpr quals? */
	bool		amsearchnulls;	/* can AM search for NULL/NOT NULL entries? */
	bool		amstorage;		/* can storage type differ from column type? */
	bool		amclusterable;	/* does AM support cluster command? */
	bool		ampredlocks;	/* does AM handle predicate locks? */
	Oid			amkeytype;		/* type of data in index, or InvalidOid */
	regproc		aminsert;		/* "insert this tuple" function */
	regproc		ambeginscan;	/* "prepare for index scan" function */
	regproc		amgettuple;		/* "next valid tuple" function, or 0 */
	regproc		amgetbitmap;	/* "fetch all valid tuples" function, or 0 */
	regproc		amrescan;		/* "(re)start index scan" function */
	regproc		amendscan;		/* "end index scan" function */
	regproc		ammarkpos;		/* "mark current scan position" function */
	regproc		amrestrpos;		/* "restore marked scan position" function */
	regproc		ambuild;		/* "build new index" function */
	regproc		ambuildempty;	/* "build empty index" function */
	regproc		ambulkdelete;	/* bulk-delete function */
	regproc		amvacuumcleanup;	/* post-VACUUM cleanup function */
	regproc		amcanreturn;	/* can indexscan return IndexTuples? */
	regproc		amcostestimate; /* estimate cost of an indexscan */
	regproc		amoptions;		/* parse AM-specific parameters */
} FormData_pg_am;

/* ----------------
 *		Form_pg_am corresponds to a pointer to a tuple with
 *		the format of pg_am relation.
 * ----------------
 */
typedef FormData_pg_am *Form_pg_am;

/* ----------------
 *		compiler constants for pg_am
 * ----------------
 */
#define Natts_pg_am						30
#define Anum_pg_am_amname				1
#define Anum_pg_am_amstrategies			2
#define Anum_pg_am_amsupport			3
#define Anum_pg_am_amcanorder			4
#define Anum_pg_am_amcanorderbyop		5
#define Anum_pg_am_amcanbackward		6
#define Anum_pg_am_amcanunique			7
#define Anum_pg_am_amcanmulticol		8
#define Anum_pg_am_amoptionalkey		9
#define Anum_pg_am_amsearcharray		10
#define Anum_pg_am_amsearchnulls		11
#define Anum_pg_am_amstorage			12
#define Anum_pg_am_amclusterable		13
#define Anum_pg_am_ampredlocks			14
#define Anum_pg_am_amkeytype			15
#define Anum_pg_am_aminsert				16
#define Anum_pg_am_ambeginscan			17
#define Anum_pg_am_amgettuple			18
#define Anum_pg_am_amgetbitmap			19
#define Anum_pg_am_amrescan				20
#define Anum_pg_am_amendscan			21
#define Anum_pg_am_ammarkpos			22
#define Anum_pg_am_amrestrpos			23
#define Anum_pg_am_ambuild				24
#define Anum_pg_am_ambuildempty			25
#define Anum_pg_am_ambulkdelete			26
#define Anum_pg_am_amvacuumcleanup		27
#define Anum_pg_am_amcanreturn			28
#define Anum_pg_am_amcostestimate		29
#define Anum_pg_am_amoptions			30

/* ----------------
 *		initial contents of pg_am
 * ----------------
 */

DATA(insert OID = 403 (  btree		5 2 t f t t t t t t f t t 0 btinsert btbeginscan btgettuple btgetbitmap btrescan btendscan btmarkpos btrestrpos btbuild btbuildempty btbulkdelete btvacuumcleanup btcanreturn btcostestimate btoptions ));
DESCR("b-tree index access method");
#define BTREE_AM_OID 403
DATA(insert OID = 405 (  hash		1 1 f f t f f f f f f f f 23 hashinsert hashbeginscan hashgettuple hashgetbitmap hashrescan hashendscan hashmarkpos hashrestrpos hashbuild hashbuildempty hashbulkdelete hashvacuumcleanup - hashcostestimate hashoptions ));
DESCR("hash index access method");
#define HASH_AM_OID 405
DATA(insert OID = 783 (  gist		0 9 f t f f t t f t t t f 0 gistinsert gistbeginscan gistgettuple gistgetbitmap gistrescan gistendscan gistmarkpos gistrestrpos gistbuild gistbuildempty gistbulkdelete gistvacuumcleanup gistcanreturn gistcostestimate gistoptions ));
DESCR("GiST index access method");
#define GIST_AM_OID 783
DATA(insert OID = 2742 (  gin		0 6 f f f f t t f f t f f 0 gininsert ginbeginscan - gingetbitmap ginrescan ginendscan ginmarkpos ginrestrpos ginbuild ginbuildempty ginbulkdelete ginvacuumcleanup - gincostestimate ginoptions ));
DESCR("GIN index access method");
#define GIN_AM_OID 2742
DATA(insert OID = 4000 (  spgist	0 5 f f f f f t f t f f f 0 spginsert spgbeginscan spggettuple spggetbitmap spgrescan spgendscan spgmarkpos spgrestrpos spgbuild spgbuildempty spgbulkdelete spgvacuumcleanup spgcanreturn spgcostestimate spgoptions ));
DESCR("SP-GiST index access method");
#define SPGIST_AM_OID 4000
DATA(insert OID = 3580 (  brin	   0 15 f f f f t t f t t f f 0 brininsert brinbeginscan - bringetbitmap brinrescan brinendscan brinmarkpos brinrestrpos brinbuild brinbuildempty brinbulkdelete brinvacuumcleanup - brincostestimate brinoptions ));
DESCR("block range index (BRIN) access method");
#define BRIN_AM_OID 3580

#endif   /* PG_AM_H */
