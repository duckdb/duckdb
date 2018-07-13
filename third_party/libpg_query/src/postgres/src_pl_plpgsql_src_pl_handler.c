/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - plpgsql_variable_conflict
 * - plpgsql_print_strict_params
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * pl_handler.c		- Handler for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_handler.c
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql.h"

#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static bool plpgsql_extra_checks_check_hook(char **newvalue, void **extra, GucSource source);
static void plpgsql_extra_warnings_assign_hook(const char *newvalue, void *extra);
static void plpgsql_extra_errors_assign_hook(const char *newvalue, void *extra);

;

/* Custom GUC variable */


__thread int			plpgsql_variable_conflict = PLPGSQL_RESOLVE_ERROR;


__thread bool		plpgsql_print_strict_params = false;









/* Hook for plugins */










/*
 * _PG_init()			- library load-time initialization
 *
 * DO NOT make this static nor change its name!
 */


/* ----------
 * plpgsql_call_handler
 *
 * The PostgreSQL function manager and trigger manager
 * call this function for execution of PL/pgSQL procedures.
 * ----------
 */
;



/* ----------
 * plpgsql_inline_handler
 *
 * Called by PostgreSQL to execute an anonymous code block
 * ----------
 */
;



/* ----------
 * plpgsql_validator
 *
 * This function attempts to validate a PL/pgSQL function at
 * CREATE FUNCTION time.
 * ----------
 */
;


