/*-------------------------------------------------------------------------
 *
 * genbki.h
 *	  Required include file for all POSTGRES catalog header files
 *
 * genbki.h defines CATALOG(), DATA(), BKI_BOOTSTRAP and related macros
 * so that the catalog header files can be read by the C compiler.
 * (These same words are recognized by genbki.pl to build the BKI
 * bootstrap file from these header files.)
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/genbki.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GENBKI_H
#define GENBKI_H

/* Introduces a catalog's structure definition */
#define CATALOG(name,oid)	typedef struct CppConcat(FormData_,name)

/* Options that may appear after CATALOG (on the same line) */
#define BKI_BOOTSTRAP
#define BKI_SHARED_RELATION
#define BKI_WITHOUT_OIDS
#define BKI_ROWTYPE_OID(oid)
#define BKI_SCHEMA_MACRO
#define BKI_FORCE_NULL
#define BKI_FORCE_NOT_NULL

/*
 * This is never defined; it's here only for documentation.
 *
 * Variable-length catalog fields (except possibly the first not nullable one)
 * should not be visible in C structures, so they are made invisible by #ifdefs
 * of an undefined symbol.  See also MARKNOTNULL in bootstrap.c for how this is
 * handled.
 */
#undef CATALOG_VARLEN

/* Declarations that provide the initial content of a catalog */
/* In C, these need to expand into some harmless, repeatable declaration */
#define DATA(x)   extern int no_such_variable
#define DESCR(x)  extern int no_such_variable
#define SHDESCR(x) extern int no_such_variable

#endif   /* GENBKI_H */
