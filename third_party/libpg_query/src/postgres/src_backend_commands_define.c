/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - defWithOids
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * define.c
 *	  Support routines for various kinds of object creation.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/define.c
 *
 * DESCRIPTION
 *	  The "DefineFoo" routines take the parse tree and pick out the
 *	  appropriate arguments/flags, passing the results to the
 *	  corresponding "FooDefine" routines (in src/catalog) that do
 *	  the actual catalog-munging.  These routines also verify permission
 *	  of the user to execute the command.
 *
 * NOTES
 *	  These things must be defined and committed in the following order:
 *		"create function":
 *				input/output, recv/send procedures
 *		"create type":
 *				type
 *		"create operator":
 *				operators
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <math.h>

#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "parser/scansup.h"
#include "utils/int8.h"

/*
 * Extract a string value (otherwise uninterpreted) from a DefElem.
 */


/*
 * Extract a numeric value (actually double) from a DefElem.
 */


/*
 * Extract a boolean value from a DefElem.
 */


/*
 * Extract an int32 value from a DefElem.
 */


/*
 * Extract an int64 value from a DefElem.
 */


/*
 * Extract a possibly-qualified name (as a List of Strings) from a DefElem.
 */


/*
 * Extract a TypeName from a DefElem.
 *
 * Note: we do not accept a List arg here, because the parser will only
 * return a bare List when the name looks like an operator name.
 */


/*
 * Extract a type length indicator (either absolute bytes, or
 * -1 for "variable") from a DefElem.
 */


/*
 * Create a DefElem setting "oids" to the specified value.
 */
DefElem *
defWithOids(bool value)
{
	return makeDefElem("oids", (Node *) makeInteger(value));
}
