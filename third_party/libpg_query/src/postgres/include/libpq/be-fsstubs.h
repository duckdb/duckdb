/*-------------------------------------------------------------------------
 *
 * be-fsstubs.h
 *
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/be-fsstubs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BE_FSSTUBS_H
#define BE_FSSTUBS_H

#include "fmgr.h"

/*
 * LO functions available via pg_proc entries
 */
extern Datum lo_import(PG_FUNCTION_ARGS);
extern Datum lo_import_with_oid(PG_FUNCTION_ARGS);
extern Datum lo_export(PG_FUNCTION_ARGS);

extern Datum lo_creat(PG_FUNCTION_ARGS);
extern Datum lo_create(PG_FUNCTION_ARGS);
extern Datum lo_from_bytea(PG_FUNCTION_ARGS);

extern Datum lo_open(PG_FUNCTION_ARGS);
extern Datum lo_close(PG_FUNCTION_ARGS);

extern Datum loread(PG_FUNCTION_ARGS);
extern Datum lowrite(PG_FUNCTION_ARGS);

extern Datum lo_get(PG_FUNCTION_ARGS);
extern Datum lo_get_fragment(PG_FUNCTION_ARGS);
extern Datum lo_put(PG_FUNCTION_ARGS);

extern Datum lo_lseek(PG_FUNCTION_ARGS);
extern Datum lo_tell(PG_FUNCTION_ARGS);
extern Datum lo_lseek64(PG_FUNCTION_ARGS);
extern Datum lo_tell64(PG_FUNCTION_ARGS);
extern Datum lo_unlink(PG_FUNCTION_ARGS);
extern Datum lo_truncate(PG_FUNCTION_ARGS);
extern Datum lo_truncate64(PG_FUNCTION_ARGS);

/*
 * compatibility option for access control
 */
extern bool lo_compat_privileges;

/*
 * These are not fmgr-callable, but are available to C code.
 * Probably these should have had the underscore-free names,
 * but too late now...
 */
extern int	lo_read(int fd, char *buf, int len);
extern int	lo_write(int fd, const char *buf, int len);

/*
 * Cleanup LOs at xact commit/abort
 */
extern void AtEOXact_LargeObject(bool isCommit);
extern void AtEOSubXact_LargeObject(bool isCommit, SubTransactionId mySubid,
						SubTransactionId parentSubid);

#endif   /* BE_FSSTUBS_H */
