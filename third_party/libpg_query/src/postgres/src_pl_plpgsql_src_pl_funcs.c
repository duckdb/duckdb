/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - plpgsql_ns_init
 * - ns_top
 * - plpgsql_ns_push
 * - plpgsql_ns_additem
 * - plpgsql_ns_pop
 * - plpgsql_ns_lookup
 * - plpgsql_ns_top
 * - plpgsql_getdiag_kindname
 * - plpgsql_ns_lookup_label
 * - plpgsql_free_function_memory
 * - free_expr
 * - free_block
 * - free_stmts
 * - free_stmt
 * - free_assign
 * - free_if
 * - free_case
 * - free_loop
 * - free_while
 * - free_fori
 * - free_fors
 * - free_forc
 * - free_foreach_a
 * - free_exit
 * - free_return
 * - free_return_next
 * - free_return_query
 * - free_raise
 * - free_assert
 * - free_execsql
 * - free_dynexecute
 * - free_dynfors
 * - free_getdiag
 * - free_open
 * - free_fetch
 * - free_close
 * - free_perform
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * pl_funcs.c		- Misc functions for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_funcs.c
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql.h"

#include "utils/memutils.h"


/* ----------
 * Local variables for namespace handling
 *
 * The namespace structure actually forms a tree, of which only one linear
 * list or "chain" (from the youngest item to the root) is accessible from
 * any one plpgsql statement.  During initial parsing of a function, ns_top
 * points to the youngest item accessible from the block currently being
 * parsed.  We store the entire tree, however, since at runtime we will need
 * to access the chain that's relevant to any one statement.
 *
 * Block boundaries in the namespace chain are marked by PLPGSQL_NSTYPE_LABEL
 * items.
 * ----------
 */
static PLpgSQL_nsitem *ns_top = NULL;


/* ----------
 * plpgsql_ns_init			Initialize namespace processing for a new function
 * ----------
 */
void
plpgsql_ns_init(void)
{
	ns_top = NULL;
}


/* ----------
 * plpgsql_ns_push			Create a new namespace level
 * ----------
 */
void
plpgsql_ns_push(const char *label)
{
	if (label == NULL)
		label = "";
	plpgsql_ns_additem(PLPGSQL_NSTYPE_LABEL, 0, label);
}


/* ----------
 * plpgsql_ns_pop			Pop entries back to (and including) the last label
 * ----------
 */
void
plpgsql_ns_pop(void)
{
	Assert(ns_top != NULL);
	while (ns_top->itemtype != PLPGSQL_NSTYPE_LABEL)
		ns_top = ns_top->prev;
	ns_top = ns_top->prev;
}


/* ----------
 * plpgsql_ns_top			Fetch the current namespace chain end
 * ----------
 */
PLpgSQL_nsitem *
plpgsql_ns_top(void)
{
	return ns_top;
}


/* ----------
 * plpgsql_ns_additem		Add an item to the current namespace chain
 * ----------
 */
void
plpgsql_ns_additem(int itemtype, int itemno, const char *name)
{
	PLpgSQL_nsitem *nse;

	Assert(name != NULL);
	/* first item added must be a label */
	Assert(ns_top != NULL || itemtype == PLPGSQL_NSTYPE_LABEL);

	nse = palloc(offsetof(PLpgSQL_nsitem, name) +strlen(name) + 1);
	nse->itemtype = itemtype;
	nse->itemno = itemno;
	nse->prev = ns_top;
	strcpy(nse->name, name);
	ns_top = nse;
}


/* ----------
 * plpgsql_ns_lookup		Lookup an identifier in the given namespace chain
 *
 * Note that this only searches for variables, not labels.
 *
 * If localmode is TRUE, only the topmost block level is searched.
 *
 * name1 must be non-NULL.  Pass NULL for name2 and/or name3 if parsing a name
 * with fewer than three components.
 *
 * If names_used isn't NULL, *names_used receives the number of names
 * matched: 0 if no match, 1 if name1 matched an unqualified variable name,
 * 2 if name1 and name2 matched a block label + variable name.
 *
 * Note that name3 is never directly matched to anything.  However, if it
 * isn't NULL, we will disregard qualified matches to scalar variables.
 * Similarly, if name2 isn't NULL, we disregard unqualified matches to
 * scalar variables.
 * ----------
 */
PLpgSQL_nsitem *
plpgsql_ns_lookup(PLpgSQL_nsitem *ns_cur, bool localmode,
				  const char *name1, const char *name2, const char *name3,
				  int *names_used)
{
	/* Outer loop iterates once per block level in the namespace chain */
	while (ns_cur != NULL)
	{
		PLpgSQL_nsitem *nsitem;

		/* Check this level for unqualified match to variable name */
		for (nsitem = ns_cur;
			 nsitem->itemtype != PLPGSQL_NSTYPE_LABEL;
			 nsitem = nsitem->prev)
		{
			if (strcmp(nsitem->name, name1) == 0)
			{
				if (name2 == NULL ||
					nsitem->itemtype != PLPGSQL_NSTYPE_VAR)
				{
					if (names_used)
						*names_used = 1;
					return nsitem;
				}
			}
		}

		/* Check this level for qualified match to variable name */
		if (name2 != NULL &&
			strcmp(nsitem->name, name1) == 0)
		{
			for (nsitem = ns_cur;
				 nsitem->itemtype != PLPGSQL_NSTYPE_LABEL;
				 nsitem = nsitem->prev)
			{
				if (strcmp(nsitem->name, name2) == 0)
				{
					if (name3 == NULL ||
						nsitem->itemtype != PLPGSQL_NSTYPE_VAR)
					{
						if (names_used)
							*names_used = 2;
						return nsitem;
					}
				}
			}
		}

		if (localmode)
			break;				/* do not look into upper levels */

		ns_cur = nsitem->prev;
	}

	/* This is just to suppress possibly-uninitialized-variable warnings */
	if (names_used)
		*names_used = 0;
	return NULL;				/* No match found */
}


/* ----------
 * plpgsql_ns_lookup_label		Lookup a label in the given namespace chain
 * ----------
 */
PLpgSQL_nsitem *
plpgsql_ns_lookup_label(PLpgSQL_nsitem *ns_cur, const char *name)
{
	while (ns_cur != NULL)
	{
		if (ns_cur->itemtype == PLPGSQL_NSTYPE_LABEL &&
			strcmp(ns_cur->name, name) == 0)
			return ns_cur;
		ns_cur = ns_cur->prev;
	}

	return NULL;				/* label not found */
}


/*
 * Statement type as a string, for use in error messages etc.
 */


/*
 * GET DIAGNOSTICS item name as a string, for use in error messages etc.
 */
const char *
plpgsql_getdiag_kindname(int kind)
{
	switch (kind)
	{
		case PLPGSQL_GETDIAG_ROW_COUNT:
			return "ROW_COUNT";
		case PLPGSQL_GETDIAG_RESULT_OID:
			return "RESULT_OID";
		case PLPGSQL_GETDIAG_CONTEXT:
			return "PG_CONTEXT";
		case PLPGSQL_GETDIAG_ERROR_CONTEXT:
			return "PG_EXCEPTION_CONTEXT";
		case PLPGSQL_GETDIAG_ERROR_DETAIL:
			return "PG_EXCEPTION_DETAIL";
		case PLPGSQL_GETDIAG_ERROR_HINT:
			return "PG_EXCEPTION_HINT";
		case PLPGSQL_GETDIAG_RETURNED_SQLSTATE:
			return "RETURNED_SQLSTATE";
		case PLPGSQL_GETDIAG_COLUMN_NAME:
			return "COLUMN_NAME";
		case PLPGSQL_GETDIAG_CONSTRAINT_NAME:
			return "CONSTRAINT_NAME";
		case PLPGSQL_GETDIAG_DATATYPE_NAME:
			return "PG_DATATYPE_NAME";
		case PLPGSQL_GETDIAG_MESSAGE_TEXT:
			return "MESSAGE_TEXT";
		case PLPGSQL_GETDIAG_TABLE_NAME:
			return "TABLE_NAME";
		case PLPGSQL_GETDIAG_SCHEMA_NAME:
			return "SCHEMA_NAME";
	}

	return "unknown";
}


/**********************************************************************
 * Release memory when a PL/pgSQL function is no longer needed
 *
 * The code for recursing through the function tree is really only
 * needed to locate PLpgSQL_expr nodes, which may contain references
 * to saved SPI Plans that must be freed.  The function tree itself,
 * along with subsidiary data, is freed in one swoop by freeing the
 * function's permanent memory context.
 **********************************************************************/
static void free_stmt(PLpgSQL_stmt *stmt);
static void free_block(PLpgSQL_stmt_block *block);
static void free_assign(PLpgSQL_stmt_assign *stmt);
static void free_if(PLpgSQL_stmt_if *stmt);
static void free_case(PLpgSQL_stmt_case *stmt);
static void free_loop(PLpgSQL_stmt_loop *stmt);
static void free_while(PLpgSQL_stmt_while *stmt);
static void free_fori(PLpgSQL_stmt_fori *stmt);
static void free_fors(PLpgSQL_stmt_fors *stmt);
static void free_forc(PLpgSQL_stmt_forc *stmt);
static void free_foreach_a(PLpgSQL_stmt_foreach_a *stmt);
static void free_exit(PLpgSQL_stmt_exit *stmt);
static void free_return(PLpgSQL_stmt_return *stmt);
static void free_return_next(PLpgSQL_stmt_return_next *stmt);
static void free_return_query(PLpgSQL_stmt_return_query *stmt);
static void free_raise(PLpgSQL_stmt_raise *stmt);
static void free_assert(PLpgSQL_stmt_assert *stmt);
static void free_execsql(PLpgSQL_stmt_execsql *stmt);
static void free_dynexecute(PLpgSQL_stmt_dynexecute *stmt);
static void free_dynfors(PLpgSQL_stmt_dynfors *stmt);
static void free_getdiag(PLpgSQL_stmt_getdiag *stmt);
static void free_open(PLpgSQL_stmt_open *stmt);
static void free_fetch(PLpgSQL_stmt_fetch *stmt);
static void free_close(PLpgSQL_stmt_close *stmt);
static void free_perform(PLpgSQL_stmt_perform *stmt);
static void free_expr(PLpgSQL_expr *expr);


static void
free_stmt(PLpgSQL_stmt *stmt)
{
	switch ((enum PLpgSQL_stmt_types) stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			free_block((PLpgSQL_stmt_block *) stmt);
			break;
		case PLPGSQL_STMT_ASSIGN:
			free_assign((PLpgSQL_stmt_assign *) stmt);
			break;
		case PLPGSQL_STMT_IF:
			free_if((PLpgSQL_stmt_if *) stmt);
			break;
		case PLPGSQL_STMT_CASE:
			free_case((PLpgSQL_stmt_case *) stmt);
			break;
		case PLPGSQL_STMT_LOOP:
			free_loop((PLpgSQL_stmt_loop *) stmt);
			break;
		case PLPGSQL_STMT_WHILE:
			free_while((PLpgSQL_stmt_while *) stmt);
			break;
		case PLPGSQL_STMT_FORI:
			free_fori((PLpgSQL_stmt_fori *) stmt);
			break;
		case PLPGSQL_STMT_FORS:
			free_fors((PLpgSQL_stmt_fors *) stmt);
			break;
		case PLPGSQL_STMT_FORC:
			free_forc((PLpgSQL_stmt_forc *) stmt);
			break;
		case PLPGSQL_STMT_FOREACH_A:
			free_foreach_a((PLpgSQL_stmt_foreach_a *) stmt);
			break;
		case PLPGSQL_STMT_EXIT:
			free_exit((PLpgSQL_stmt_exit *) stmt);
			break;
		case PLPGSQL_STMT_RETURN:
			free_return((PLpgSQL_stmt_return *) stmt);
			break;
		case PLPGSQL_STMT_RETURN_NEXT:
			free_return_next((PLpgSQL_stmt_return_next *) stmt);
			break;
		case PLPGSQL_STMT_RETURN_QUERY:
			free_return_query((PLpgSQL_stmt_return_query *) stmt);
			break;
		case PLPGSQL_STMT_RAISE:
			free_raise((PLpgSQL_stmt_raise *) stmt);
			break;
		case PLPGSQL_STMT_ASSERT:
			free_assert((PLpgSQL_stmt_assert *) stmt);
			break;
		case PLPGSQL_STMT_EXECSQL:
			free_execsql((PLpgSQL_stmt_execsql *) stmt);
			break;
		case PLPGSQL_STMT_DYNEXECUTE:
			free_dynexecute((PLpgSQL_stmt_dynexecute *) stmt);
			break;
		case PLPGSQL_STMT_DYNFORS:
			free_dynfors((PLpgSQL_stmt_dynfors *) stmt);
			break;
		case PLPGSQL_STMT_GETDIAG:
			free_getdiag((PLpgSQL_stmt_getdiag *) stmt);
			break;
		case PLPGSQL_STMT_OPEN:
			free_open((PLpgSQL_stmt_open *) stmt);
			break;
		case PLPGSQL_STMT_FETCH:
			free_fetch((PLpgSQL_stmt_fetch *) stmt);
			break;
		case PLPGSQL_STMT_CLOSE:
			free_close((PLpgSQL_stmt_close *) stmt);
			break;
		case PLPGSQL_STMT_PERFORM:
			free_perform((PLpgSQL_stmt_perform *) stmt);
			break;
		default:
			elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
			break;
	}
}

static void
free_stmts(List *stmts)
{
	ListCell   *s;

	foreach(s, stmts)
	{
		free_stmt((PLpgSQL_stmt *) lfirst(s));
	}
}

static void
free_block(PLpgSQL_stmt_block *block)
{
	free_stmts(block->body);
	if (block->exceptions)
	{
		ListCell   *e;

		foreach(e, block->exceptions->exc_list)
		{
			PLpgSQL_exception *exc = (PLpgSQL_exception *) lfirst(e);

			free_stmts(exc->action);
		}
	}
}

static void
free_assign(PLpgSQL_stmt_assign *stmt)
{
	free_expr(stmt->expr);
}

static void
free_if(PLpgSQL_stmt_if *stmt)
{
	ListCell   *l;

	free_expr(stmt->cond);
	free_stmts(stmt->then_body);
	foreach(l, stmt->elsif_list)
	{
		PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(l);

		free_expr(elif->cond);
		free_stmts(elif->stmts);
	}
	free_stmts(stmt->else_body);
}

static void
free_case(PLpgSQL_stmt_case *stmt)
{
	ListCell   *l;

	free_expr(stmt->t_expr);
	foreach(l, stmt->case_when_list)
	{
		PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);

		free_expr(cwt->expr);
		free_stmts(cwt->stmts);
	}
	free_stmts(stmt->else_stmts);
}

static void
free_loop(PLpgSQL_stmt_loop *stmt)
{
	free_stmts(stmt->body);
}

static void
free_while(PLpgSQL_stmt_while *stmt)
{
	free_expr(stmt->cond);
	free_stmts(stmt->body);
}

static void
free_fori(PLpgSQL_stmt_fori *stmt)
{
	free_expr(stmt->lower);
	free_expr(stmt->upper);
	free_expr(stmt->step);
	free_stmts(stmt->body);
}

static void
free_fors(PLpgSQL_stmt_fors *stmt)
{
	free_stmts(stmt->body);
	free_expr(stmt->query);
}

static void
free_forc(PLpgSQL_stmt_forc *stmt)
{
	free_stmts(stmt->body);
	free_expr(stmt->argquery);
}

static void
free_foreach_a(PLpgSQL_stmt_foreach_a *stmt)
{
	free_expr(stmt->expr);
	free_stmts(stmt->body);
}

static void
free_open(PLpgSQL_stmt_open *stmt)
{
	ListCell   *lc;

	free_expr(stmt->argquery);
	free_expr(stmt->query);
	free_expr(stmt->dynquery);
	foreach(lc, stmt->params)
	{
		free_expr((PLpgSQL_expr *) lfirst(lc));
	}
}

static void
free_fetch(PLpgSQL_stmt_fetch *stmt)
{
	free_expr(stmt->expr);
}

static void
free_close(PLpgSQL_stmt_close *stmt)
{
}

static void
free_perform(PLpgSQL_stmt_perform *stmt)
{
	free_expr(stmt->expr);
}

static void
free_exit(PLpgSQL_stmt_exit *stmt)
{
	free_expr(stmt->cond);
}

static void
free_return(PLpgSQL_stmt_return *stmt)
{
	free_expr(stmt->expr);
}

static void
free_return_next(PLpgSQL_stmt_return_next *stmt)
{
	free_expr(stmt->expr);
}

static void
free_return_query(PLpgSQL_stmt_return_query *stmt)
{
	ListCell   *lc;

	free_expr(stmt->query);
	free_expr(stmt->dynquery);
	foreach(lc, stmt->params)
	{
		free_expr((PLpgSQL_expr *) lfirst(lc));
	}
}

static void
free_raise(PLpgSQL_stmt_raise *stmt)
{
	ListCell   *lc;

	foreach(lc, stmt->params)
	{
		free_expr((PLpgSQL_expr *) lfirst(lc));
	}
	foreach(lc, stmt->options)
	{
		PLpgSQL_raise_option *opt = (PLpgSQL_raise_option *) lfirst(lc);

		free_expr(opt->expr);
	}
}

static void
free_assert(PLpgSQL_stmt_assert *stmt)
{
	free_expr(stmt->cond);
	free_expr(stmt->message);
}

static void
free_execsql(PLpgSQL_stmt_execsql *stmt)
{
	free_expr(stmt->sqlstmt);
}

static void
free_dynexecute(PLpgSQL_stmt_dynexecute *stmt)
{
	ListCell   *lc;

	free_expr(stmt->query);
	foreach(lc, stmt->params)
	{
		free_expr((PLpgSQL_expr *) lfirst(lc));
	}
}

static void
free_dynfors(PLpgSQL_stmt_dynfors *stmt)
{
	ListCell   *lc;

	free_stmts(stmt->body);
	free_expr(stmt->query);
	foreach(lc, stmt->params)
	{
		free_expr((PLpgSQL_expr *) lfirst(lc));
	}
}

static void
free_getdiag(PLpgSQL_stmt_getdiag *stmt)
{
}

static void free_expr(PLpgSQL_expr *expr) {}


void
plpgsql_free_function_memory(PLpgSQL_function *func)
{
	int			i;

	/* Better not call this on an in-use function */
	Assert(func->use_count == 0);

	/* Release plans associated with variable declarations */
	for (i = 0; i < func->ndatums; i++)
	{
		PLpgSQL_datum *d = func->datums[i];

		switch (d->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
				{
					PLpgSQL_var *var = (PLpgSQL_var *) d;

					free_expr(var->default_val);
					free_expr(var->cursor_explicit_expr);
				}
				break;
			case PLPGSQL_DTYPE_ROW:
				break;
			case PLPGSQL_DTYPE_REC:
				break;
			case PLPGSQL_DTYPE_RECFIELD:
				break;
			case PLPGSQL_DTYPE_ARRAYELEM:
				free_expr(((PLpgSQL_arrayelem *) d)->subscript);
				break;
			default:
				elog(ERROR, "unrecognized data type: %d", d->dtype);
		}
	}
	func->ndatums = 0;

	/* Release plans in statement tree */
	if (func->action)
		free_block(func->action);
	func->action = NULL;

	/*
	 * And finally, release all memory except the PLpgSQL_function struct
	 * itself (which has to be kept around because there may be multiple
	 * fn_extra pointers to it).
	 */
	if (func->fn_cxt)
		MemoryContextDelete(func->fn_cxt);
	func->fn_cxt = NULL;
}


/**********************************************************************
 * Debug functions for analyzing the compiled code
 **********************************************************************/


static void dump_ind(void);
static void dump_stmt(PLpgSQL_stmt *stmt);
static void dump_block(PLpgSQL_stmt_block *block);
static void dump_assign(PLpgSQL_stmt_assign *stmt);
static void dump_if(PLpgSQL_stmt_if *stmt);
static void dump_case(PLpgSQL_stmt_case *stmt);
static void dump_loop(PLpgSQL_stmt_loop *stmt);
static void dump_while(PLpgSQL_stmt_while *stmt);
static void dump_fori(PLpgSQL_stmt_fori *stmt);
static void dump_fors(PLpgSQL_stmt_fors *stmt);
static void dump_forc(PLpgSQL_stmt_forc *stmt);
static void dump_foreach_a(PLpgSQL_stmt_foreach_a *stmt);
static void dump_exit(PLpgSQL_stmt_exit *stmt);
static void dump_return(PLpgSQL_stmt_return *stmt);
static void dump_return_next(PLpgSQL_stmt_return_next *stmt);
static void dump_return_query(PLpgSQL_stmt_return_query *stmt);
static void dump_raise(PLpgSQL_stmt_raise *stmt);
static void dump_assert(PLpgSQL_stmt_assert *stmt);
static void dump_execsql(PLpgSQL_stmt_execsql *stmt);
static void dump_dynexecute(PLpgSQL_stmt_dynexecute *stmt);
static void dump_dynfors(PLpgSQL_stmt_dynfors *stmt);
static void dump_getdiag(PLpgSQL_stmt_getdiag *stmt);
static void dump_open(PLpgSQL_stmt_open *stmt);
static void dump_fetch(PLpgSQL_stmt_fetch *stmt);
static void dump_cursor_direction(PLpgSQL_stmt_fetch *stmt);
static void dump_close(PLpgSQL_stmt_close *stmt);
static void dump_perform(PLpgSQL_stmt_perform *stmt);
static void dump_expr(PLpgSQL_expr *expr);





























































