/*-------------------------------------------------------------------------
 *
 * bootstrap.h
 *	  include file for the bootstrapping code
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/bootstrap/bootstrap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BOOTSTRAP_H
#define BOOTSTRAP_H

#include "nodes/execnodes.h"


/*
 * MAXATTR is the maximum number of attributes in a relation supported
 * at bootstrap time (i.e., the max possible in a system table).
 */
#define MAXATTR 40

#define BOOTCOL_NULL_AUTO			1
#define BOOTCOL_NULL_FORCE_NULL		2
#define BOOTCOL_NULL_FORCE_NOT_NULL 3

extern Relation boot_reldesc;
extern Form_pg_attribute attrtypes[MAXATTR];
extern int	numattr;


extern void AuxiliaryProcessMain(int argc, char *argv[]) pg_attribute_noreturn();

extern void err_out(void);

extern void closerel(char *name);
extern void boot_openrel(char *name);

extern void DefineAttr(char *name, char *type, int attnum, int nullness);
extern void InsertOneTuple(Oid objectid);
extern void InsertOneValue(char *value, int i);
extern void InsertOneNull(int i);

extern char *MapArrayTypeName(const char *s);

extern void index_register(Oid heap, Oid ind, IndexInfo *indexInfo);
extern void build_indices(void);

extern void boot_get_type_io_data(Oid typid,
					  int16 *typlen,
					  bool *typbyval,
					  char *typalign,
					  char *typdelim,
					  Oid *typioparam,
					  Oid *typinput,
					  Oid *typoutput);

extern int	boot_yyparse(void);

extern int	boot_yylex(void);
extern void boot_yyerror(const char *str) pg_attribute_noreturn();

#endif   /* BOOTSTRAP_H */
