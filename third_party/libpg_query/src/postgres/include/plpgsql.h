/*-------------------------------------------------------------------------
 *
 * plpgsql.h		- Definitions for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/plpgsql.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLPGSQL_H
#define PLPGSQL_H

#include "postgres.h"

#include "access/xact.h"
#include "commands/event_trigger.h"
#include "commands/trigger.h"
#include "executor/spi.h"

/**********************************************************************
 * Definitions
 **********************************************************************/

/* define our text domain for translations */
#undef TEXTDOMAIN
#define TEXTDOMAIN PG_TEXTDOMAIN("plpgsql")

#undef _
#define _(x) dgettext(TEXTDOMAIN, x)

/* ----------
 * Compiler's namespace item types
 * ----------
 */
enum
{
	PLPGSQL_NSTYPE_LABEL,
	PLPGSQL_NSTYPE_VAR,
	PLPGSQL_NSTYPE_ROW,
	PLPGSQL_NSTYPE_REC
};

/* ----------
 * Datum array node types
 * ----------
 */
enum
{
	PLPGSQL_DTYPE_VAR,
	PLPGSQL_DTYPE_ROW,
	PLPGSQL_DTYPE_REC,
	PLPGSQL_DTYPE_RECFIELD,
	PLPGSQL_DTYPE_ARRAYELEM,
	PLPGSQL_DTYPE_EXPR
};

/* ----------
 * Variants distinguished in PLpgSQL_type structs
 * ----------
 */
enum
{
	PLPGSQL_TTYPE_SCALAR,		/* scalar types and domains */
	PLPGSQL_TTYPE_ROW,			/* composite types */
	PLPGSQL_TTYPE_REC,			/* RECORD pseudotype */
	PLPGSQL_TTYPE_PSEUDO		/* other pseudotypes */
};

/* ----------
 * Execution tree node types
 * ----------
 */
enum PLpgSQL_stmt_types
{
	PLPGSQL_STMT_BLOCK,
	PLPGSQL_STMT_ASSIGN,
	PLPGSQL_STMT_IF,
	PLPGSQL_STMT_CASE,
	PLPGSQL_STMT_LOOP,
	PLPGSQL_STMT_WHILE,
	PLPGSQL_STMT_FORI,
	PLPGSQL_STMT_FORS,
	PLPGSQL_STMT_FORC,
	PLPGSQL_STMT_FOREACH_A,
	PLPGSQL_STMT_EXIT,
	PLPGSQL_STMT_RETURN,
	PLPGSQL_STMT_RETURN_NEXT,
	PLPGSQL_STMT_RETURN_QUERY,
	PLPGSQL_STMT_RAISE,
	PLPGSQL_STMT_ASSERT,
	PLPGSQL_STMT_EXECSQL,
	PLPGSQL_STMT_DYNEXECUTE,
	PLPGSQL_STMT_DYNFORS,
	PLPGSQL_STMT_GETDIAG,
	PLPGSQL_STMT_OPEN,
	PLPGSQL_STMT_FETCH,
	PLPGSQL_STMT_CLOSE,
	PLPGSQL_STMT_PERFORM
};


/* ----------
 * Execution node return codes
 * ----------
 */
enum
{
	PLPGSQL_RC_OK,
	PLPGSQL_RC_EXIT,
	PLPGSQL_RC_RETURN,
	PLPGSQL_RC_CONTINUE
};

/* ----------
 * GET DIAGNOSTICS information items
 * ----------
 */
enum
{
	PLPGSQL_GETDIAG_ROW_COUNT,
	PLPGSQL_GETDIAG_RESULT_OID,
	PLPGSQL_GETDIAG_CONTEXT,
	PLPGSQL_GETDIAG_ERROR_CONTEXT,
	PLPGSQL_GETDIAG_ERROR_DETAIL,
	PLPGSQL_GETDIAG_ERROR_HINT,
	PLPGSQL_GETDIAG_RETURNED_SQLSTATE,
	PLPGSQL_GETDIAG_COLUMN_NAME,
	PLPGSQL_GETDIAG_CONSTRAINT_NAME,
	PLPGSQL_GETDIAG_DATATYPE_NAME,
	PLPGSQL_GETDIAG_MESSAGE_TEXT,
	PLPGSQL_GETDIAG_TABLE_NAME,
	PLPGSQL_GETDIAG_SCHEMA_NAME
};

/* --------
 * RAISE statement options
 * --------
 */
enum
{
	PLPGSQL_RAISEOPTION_ERRCODE,
	PLPGSQL_RAISEOPTION_MESSAGE,
	PLPGSQL_RAISEOPTION_DETAIL,
	PLPGSQL_RAISEOPTION_HINT,
	PLPGSQL_RAISEOPTION_COLUMN,
	PLPGSQL_RAISEOPTION_CONSTRAINT,
	PLPGSQL_RAISEOPTION_DATATYPE,
	PLPGSQL_RAISEOPTION_TABLE,
	PLPGSQL_RAISEOPTION_SCHEMA
};

/* --------
 * Behavioral modes for plpgsql variable resolution
 * --------
 */
typedef enum
{
	PLPGSQL_RESOLVE_ERROR,		/* throw error if ambiguous */
	PLPGSQL_RESOLVE_VARIABLE,	/* prefer plpgsql var to table column */
	PLPGSQL_RESOLVE_COLUMN		/* prefer table column to plpgsql var */
} PLpgSQL_resolve_option;


/**********************************************************************
 * Node and structure definitions
 **********************************************************************/


typedef struct
{								/* Postgres data type */
	char	   *typname;		/* (simple) name of the type */
	Oid			typoid;			/* OID of the data type */
	int			ttype;			/* PLPGSQL_TTYPE_ code */
	int16		typlen;			/* stuff copied from its pg_type entry */
	bool		typbyval;
	char		typtype;
	Oid			typrelid;
	Oid			collation;		/* from pg_type, but can be overridden */
	bool		typisarray;		/* is "true" array, or domain over one */
	int32		atttypmod;		/* typmod (taken from someplace else) */
} PLpgSQL_type;


/*
 * PLpgSQL_datum is the common supertype for PLpgSQL_expr, PLpgSQL_var,
 * PLpgSQL_row, PLpgSQL_rec, PLpgSQL_recfield, and PLpgSQL_arrayelem
 */
typedef struct
{								/* Generic datum array item		*/
	int			dtype;
	int			dno;
} PLpgSQL_datum;

/*
 * The variants PLpgSQL_var, PLpgSQL_row, and PLpgSQL_rec share these
 * fields
 */
typedef struct
{								/* Scalar or composite variable */
	int			dtype;
	int			dno;
	char	   *refname;
	int			lineno;
} PLpgSQL_variable;

typedef struct PLpgSQL_expr
{								/* SQL Query to plan and execute	*/
	int			dtype;
	int			dno;
	char	   *query;
	SPIPlanPtr	plan;
	Bitmapset  *paramnos;		/* all dnos referenced by this query */
	int			rwparam;		/* dno of read/write param, or -1 if none */

	/* function containing this expr (not set until we first parse query) */
	struct PLpgSQL_function *func;

	/* namespace chain visible to this expr */
	struct PLpgSQL_nsitem *ns;

	/* fields for "simple expression" fast-path execution: */
	Expr	   *expr_simple_expr;		/* NULL means not a simple expr */
	int			expr_simple_generation; /* plancache generation we checked */
	Oid			expr_simple_type;		/* result type Oid, if simple */
	int32		expr_simple_typmod;		/* result typmod, if simple */

	/*
	 * if expr is simple AND prepared in current transaction,
	 * expr_simple_state and expr_simple_in_use are valid. Test validity by
	 * seeing if expr_simple_lxid matches current LXID.  (If not,
	 * expr_simple_state probably points at garbage!)
	 */
	ExprState  *expr_simple_state;		/* eval tree for expr_simple_expr */
	bool		expr_simple_in_use;		/* true if eval tree is active */
	LocalTransactionId expr_simple_lxid;
} PLpgSQL_expr;


typedef struct
{								/* Scalar variable */
	int			dtype;
	int			dno;
	char	   *refname;
	int			lineno;

	PLpgSQL_type *datatype;
	int			isconst;
	int			notnull;
	PLpgSQL_expr *default_val;
	PLpgSQL_expr *cursor_explicit_expr;
	int			cursor_explicit_argrow;
	int			cursor_options;

	Datum		value;
	bool		isnull;
	bool		freeval;
} PLpgSQL_var;


typedef struct
{								/* Row variable */
	int			dtype;
	int			dno;
	char	   *refname;
	int			lineno;

	TupleDesc	rowtupdesc;

	/*
	 * Note: TupleDesc is only set up for named rowtypes, else it is NULL.
	 *
	 * Note: if the underlying rowtype contains a dropped column, the
	 * corresponding fieldnames[] entry will be NULL, and there is no
	 * corresponding var (varnos[] will be -1).
	 */
	int			nfields;
	char	  **fieldnames;
	int		   *varnos;
} PLpgSQL_row;


typedef struct
{								/* Record variable (non-fixed structure) */
	int			dtype;
	int			dno;
	char	   *refname;
	int			lineno;

	HeapTuple	tup;
	TupleDesc	tupdesc;
	bool		freetup;
	bool		freetupdesc;
} PLpgSQL_rec;


typedef struct
{								/* Field in record */
	int			dtype;
	int			dno;
	char	   *fieldname;
	int			recparentno;	/* dno of parent record */
} PLpgSQL_recfield;


typedef struct
{								/* Element of array variable */
	int			dtype;
	int			dno;
	PLpgSQL_expr *subscript;
	int			arrayparentno;	/* dno of parent array variable */
	/* Remaining fields are cached info about the array variable's type */
	Oid			parenttypoid;	/* type of array variable; 0 if not yet set */
	int32		parenttypmod;	/* typmod of array variable */
	Oid			arraytypoid;	/* OID of actual array type */
	int32		arraytypmod;	/* typmod of array (and its elements too) */
	int16		arraytyplen;	/* typlen of array type */
	Oid			elemtypoid;		/* OID of array element type */
	int16		elemtyplen;		/* typlen of element type */
	bool		elemtypbyval;	/* element type is pass-by-value? */
	char		elemtypalign;	/* typalign of element type */
} PLpgSQL_arrayelem;


typedef struct PLpgSQL_nsitem
{								/* Item in the compilers namespace tree */
	int			itemtype;
	int			itemno;
	struct PLpgSQL_nsitem *prev;
	char		name[FLEXIBLE_ARRAY_MEMBER];	/* nul-terminated string */
} PLpgSQL_nsitem;


typedef struct
{								/* Generic execution node		*/
	int			cmd_type;
	int			lineno;
} PLpgSQL_stmt;


typedef struct PLpgSQL_condition
{								/* One EXCEPTION condition name */
	int			sqlerrstate;	/* SQLSTATE code */
	char	   *condname;		/* condition name (for debugging) */
	struct PLpgSQL_condition *next;
} PLpgSQL_condition;

typedef struct
{
	int			sqlstate_varno;
	int			sqlerrm_varno;
	List	   *exc_list;		/* List of WHEN clauses */
} PLpgSQL_exception_block;

typedef struct
{								/* One EXCEPTION ... WHEN clause */
	int			lineno;
	PLpgSQL_condition *conditions;
	List	   *action;			/* List of statements */
} PLpgSQL_exception;


typedef struct
{								/* Block of statements			*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	List	   *body;			/* List of statements */
	int			n_initvars;
	int		   *initvarnos;
	PLpgSQL_exception_block *exceptions;
} PLpgSQL_stmt_block;


typedef struct
{								/* Assign statement			*/
	int			cmd_type;
	int			lineno;
	int			varno;
	PLpgSQL_expr *expr;
} PLpgSQL_stmt_assign;

typedef struct
{								/* PERFORM statement		*/
	int			cmd_type;
	int			lineno;
	PLpgSQL_expr *expr;
} PLpgSQL_stmt_perform;

typedef struct
{								/* Get Diagnostics item		*/
	int			kind;			/* id for diagnostic value desired */
	int			target;			/* where to assign it */
} PLpgSQL_diag_item;

typedef struct
{								/* Get Diagnostics statement		*/
	int			cmd_type;
	int			lineno;
	bool		is_stacked;		/* STACKED or CURRENT diagnostics area? */
	List	   *diag_items;		/* List of PLpgSQL_diag_item */
} PLpgSQL_stmt_getdiag;


typedef struct
{								/* IF statement				*/
	int			cmd_type;
	int			lineno;
	PLpgSQL_expr *cond;			/* boolean expression for THEN */
	List	   *then_body;		/* List of statements */
	List	   *elsif_list;		/* List of PLpgSQL_if_elsif structs */
	List	   *else_body;		/* List of statements */
} PLpgSQL_stmt_if;

typedef struct					/* one ELSIF arm of IF statement */
{
	int			lineno;
	PLpgSQL_expr *cond;			/* boolean expression for this case */
	List	   *stmts;			/* List of statements */
} PLpgSQL_if_elsif;


typedef struct					/* CASE statement */
{
	int			cmd_type;
	int			lineno;
	PLpgSQL_expr *t_expr;		/* test expression, or NULL if none */
	int			t_varno;		/* var to store test expression value into */
	List	   *case_when_list; /* List of PLpgSQL_case_when structs */
	bool		have_else;		/* flag needed because list could be empty */
	List	   *else_stmts;		/* List of statements */
} PLpgSQL_stmt_case;

typedef struct					/* one arm of CASE statement */
{
	int			lineno;
	PLpgSQL_expr *expr;			/* boolean expression for this case */
	List	   *stmts;			/* List of statements */
} PLpgSQL_case_when;


typedef struct
{								/* Unconditional LOOP statement		*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	List	   *body;			/* List of statements */
} PLpgSQL_stmt_loop;


typedef struct
{								/* WHILE cond LOOP statement		*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLpgSQL_expr *cond;
	List	   *body;			/* List of statements */
} PLpgSQL_stmt_while;


typedef struct
{								/* FOR statement with integer loopvar	*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLpgSQL_var *var;
	PLpgSQL_expr *lower;
	PLpgSQL_expr *upper;
	PLpgSQL_expr *step;			/* NULL means default (ie, BY 1) */
	int			reverse;
	List	   *body;			/* List of statements */
} PLpgSQL_stmt_fori;


/*
 * PLpgSQL_stmt_forq represents a FOR statement running over a SQL query.
 * It is the common supertype of PLpgSQL_stmt_fors, PLpgSQL_stmt_forc
 * and PLpgSQL_dynfors.
 */
typedef struct
{
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLpgSQL_rec *rec;
	PLpgSQL_row *row;
	List	   *body;			/* List of statements */
} PLpgSQL_stmt_forq;

typedef struct
{								/* FOR statement running over SELECT	*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLpgSQL_rec *rec;
	PLpgSQL_row *row;
	List	   *body;			/* List of statements */
	/* end of fields that must match PLpgSQL_stmt_forq */
	PLpgSQL_expr *query;
} PLpgSQL_stmt_fors;

typedef struct
{								/* FOR statement running over cursor	*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLpgSQL_rec *rec;
	PLpgSQL_row *row;
	List	   *body;			/* List of statements */
	/* end of fields that must match PLpgSQL_stmt_forq */
	int			curvar;
	PLpgSQL_expr *argquery;		/* cursor arguments if any */
} PLpgSQL_stmt_forc;

typedef struct
{								/* FOR statement running over EXECUTE	*/
	int			cmd_type;
	int			lineno;
	char	   *label;
	PLpgSQL_rec *rec;
	PLpgSQL_row *row;
	List	   *body;			/* List of statements */
	/* end of fields that must match PLpgSQL_stmt_forq */
	PLpgSQL_expr *query;
	List	   *params;			/* USING expressions */
} PLpgSQL_stmt_dynfors;


typedef struct
{								/* FOREACH item in array loop */
	int			cmd_type;
	int			lineno;
	char	   *label;
	int			varno;			/* loop target variable */
	int			slice;			/* slice dimension, or 0 */
	PLpgSQL_expr *expr;			/* array expression */
	List	   *body;			/* List of statements */
} PLpgSQL_stmt_foreach_a;


typedef struct
{								/* OPEN a curvar					*/
	int			cmd_type;
	int			lineno;
	int			curvar;
	int			cursor_options;
	PLpgSQL_row *returntype;
	PLpgSQL_expr *argquery;
	PLpgSQL_expr *query;
	PLpgSQL_expr *dynquery;
	List	   *params;			/* USING expressions */
} PLpgSQL_stmt_open;


typedef struct
{								/* FETCH or MOVE statement */
	int			cmd_type;
	int			lineno;
	PLpgSQL_rec *rec;			/* target, as record or row */
	PLpgSQL_row *row;
	int			curvar;			/* cursor variable to fetch from */
	FetchDirection direction;	/* fetch direction */
	long		how_many;		/* count, if constant (expr is NULL) */
	PLpgSQL_expr *expr;			/* count, if expression */
	bool		is_move;		/* is this a fetch or move? */
	bool		returns_multiple_rows;	/* can return more than one row? */
} PLpgSQL_stmt_fetch;


typedef struct
{								/* CLOSE curvar						*/
	int			cmd_type;
	int			lineno;
	int			curvar;
} PLpgSQL_stmt_close;


typedef struct
{								/* EXIT or CONTINUE statement			*/
	int			cmd_type;
	int			lineno;
	bool		is_exit;		/* Is this an exit or a continue? */
	char	   *label;			/* NULL if it's an unlabelled EXIT/CONTINUE */
	PLpgSQL_expr *cond;
} PLpgSQL_stmt_exit;


typedef struct
{								/* RETURN statement			*/
	int			cmd_type;
	int			lineno;
	PLpgSQL_expr *expr;
	int			retvarno;
} PLpgSQL_stmt_return;

typedef struct
{								/* RETURN NEXT statement */
	int			cmd_type;
	int			lineno;
	PLpgSQL_expr *expr;
	int			retvarno;
} PLpgSQL_stmt_return_next;

typedef struct
{								/* RETURN QUERY statement */
	int			cmd_type;
	int			lineno;
	PLpgSQL_expr *query;		/* if static query */
	PLpgSQL_expr *dynquery;		/* if dynamic query (RETURN QUERY EXECUTE) */
	List	   *params;			/* USING arguments for dynamic query */
} PLpgSQL_stmt_return_query;

typedef struct
{								/* RAISE statement			*/
	int			cmd_type;
	int			lineno;
	int			elog_level;
	char	   *condname;		/* condition name, SQLSTATE, or NULL */
	char	   *message;		/* old-style message format literal, or NULL */
	List	   *params;			/* list of expressions for old-style message */
	List	   *options;		/* list of PLpgSQL_raise_option */
} PLpgSQL_stmt_raise;

typedef struct
{								/* RAISE statement option */
	int			opt_type;
	PLpgSQL_expr *expr;
} PLpgSQL_raise_option;

typedef struct
{								/* ASSERT statement */
	int			cmd_type;
	int			lineno;
	PLpgSQL_expr *cond;
	PLpgSQL_expr *message;
} PLpgSQL_stmt_assert;

typedef struct
{								/* Generic SQL statement to execute */
	int			cmd_type;
	int			lineno;
	PLpgSQL_expr *sqlstmt;
	bool		mod_stmt;		/* is the stmt INSERT/UPDATE/DELETE? */
	/* note: mod_stmt is set when we plan the query */
	bool		into;			/* INTO supplied? */
	bool		strict;			/* INTO STRICT flag */
	PLpgSQL_rec *rec;			/* INTO target, if record */
	PLpgSQL_row *row;			/* INTO target, if row */
} PLpgSQL_stmt_execsql;


typedef struct
{								/* Dynamic SQL string to execute */
	int			cmd_type;
	int			lineno;
	PLpgSQL_expr *query;		/* string expression */
	bool		into;			/* INTO supplied? */
	bool		strict;			/* INTO STRICT flag */
	PLpgSQL_rec *rec;			/* INTO target, if record */
	PLpgSQL_row *row;			/* INTO target, if row */
	List	   *params;			/* USING expressions */
} PLpgSQL_stmt_dynexecute;


typedef struct PLpgSQL_func_hashkey
{								/* Hash lookup key for functions */
	Oid			funcOid;

	bool		isTrigger;		/* true if called as a trigger */

	/* be careful that pad bytes in this struct get zeroed! */

	/*
	 * For a trigger function, the OID of the relation triggered on is part of
	 * the hash key --- we want to compile the trigger separately for each
	 * relation it is used with, in case the rowtype is different.  Zero if
	 * not called as a trigger.
	 */
	Oid			trigrelOid;

	/*
	 * We must include the input collation as part of the hash key too,
	 * because we have to generate different plans (with different Param
	 * collations) for different collation settings.
	 */
	Oid			inputCollation;

	/*
	 * We include actual argument types in the hash key to support polymorphic
	 * PLpgSQL functions.  Be careful that extra positions are zeroed!
	 */
	Oid			argtypes[FUNC_MAX_ARGS];
} PLpgSQL_func_hashkey;

typedef enum PLpgSQL_trigtype
{
	PLPGSQL_DML_TRIGGER,
	PLPGSQL_EVENT_TRIGGER,
	PLPGSQL_NOT_TRIGGER
} PLpgSQL_trigtype;

typedef struct PLpgSQL_function
{								/* Complete compiled function	  */
	char	   *fn_signature;
	Oid			fn_oid;
	TransactionId fn_xmin;
	ItemPointerData fn_tid;
	PLpgSQL_trigtype fn_is_trigger;
	Oid			fn_input_collation;
	PLpgSQL_func_hashkey *fn_hashkey;	/* back-link to hashtable key */
	MemoryContext fn_cxt;

	Oid			fn_rettype;
	int			fn_rettyplen;
	bool		fn_retbyval;
	bool		fn_retistuple;
	bool		fn_retset;
	bool		fn_readonly;

	int			fn_nargs;
	int			fn_argvarnos[FUNC_MAX_ARGS];
	int			out_param_varno;
	int			found_varno;
	int			new_varno;
	int			old_varno;
	int			tg_name_varno;
	int			tg_when_varno;
	int			tg_level_varno;
	int			tg_op_varno;
	int			tg_relid_varno;
	int			tg_relname_varno;
	int			tg_table_name_varno;
	int			tg_table_schema_varno;
	int			tg_nargs_varno;
	int			tg_argv_varno;

	/* for event triggers */
	int			tg_event_varno;
	int			tg_tag_varno;

	PLpgSQL_resolve_option resolve_option;

	bool		print_strict_params;

	/* extra checks */
	int			extra_warnings;
	int			extra_errors;

	int			ndatums;
	PLpgSQL_datum **datums;
	PLpgSQL_stmt_block *action;

	/* these fields change when the function is used */
	struct PLpgSQL_execstate *cur_estate;
	unsigned long use_count;
} PLpgSQL_function;


typedef struct PLpgSQL_execstate
{								/* Runtime execution data	*/
	PLpgSQL_function *func;		/* function being executed */

	Datum		retval;
	bool		retisnull;
	Oid			rettype;		/* type of current retval */

	Oid			fn_rettype;		/* info about declared function rettype */
	bool		retistuple;
	bool		retisset;

	bool		readonly_func;

	TupleDesc	rettupdesc;
	char	   *exitlabel;		/* the "target" label of the current EXIT or
								 * CONTINUE stmt, if any */
	ErrorData  *cur_error;		/* current exception handler's error */

	Tuplestorestate *tuple_store;		/* SRFs accumulate results here */
	MemoryContext tuple_store_cxt;
	ResourceOwner tuple_store_owner;
	ReturnSetInfo *rsi;

	/* the datums representing the function's local variables */
	int			found_varno;
	int			ndatums;
	PLpgSQL_datum **datums;

	/* we pass datums[i] to the executor, when needed, in paramLI->params[i] */
	ParamListInfo paramLI;

	/* EState to use for "simple" expression evaluation */
	EState	   *simple_eval_estate;

	/* Lookup table to use for executing type casts */
	HTAB	   *cast_hash;
	MemoryContext cast_hash_context;

	/* temporary state for results from evaluation of query or expr */
	SPITupleTable *eval_tuptable;
	uint32		eval_processed;
	Oid			eval_lastoid;
	ExprContext *eval_econtext; /* for executing simple expressions */

	/* status information for error context reporting */
	PLpgSQL_stmt *err_stmt;		/* current stmt */
	const char *err_text;		/* additional state info */

	void	   *plugin_info;	/* reserved for use by optional plugin */
} PLpgSQL_execstate;


/*
 * A PLpgSQL_plugin structure represents an instrumentation plugin.
 * To instrument PL/pgSQL, a plugin library must access the rendezvous
 * variable "PLpgSQL_plugin" and set it to point to a PLpgSQL_plugin struct.
 * Typically the struct could just be static data in the plugin library.
 * We expect that a plugin would do this at library load time (_PG_init()).
 * It must also be careful to set the rendezvous variable back to NULL
 * if it is unloaded (_PG_fini()).
 *
 * This structure is basically a collection of function pointers --- at
 * various interesting points in pl_exec.c, we call these functions
 * (if the pointers are non-NULL) to give the plugin a chance to watch
 * what we are doing.
 *
 *	func_setup is called when we start a function, before we've initialized
 *	the local variables defined by the function.
 *
 *	func_beg is called when we start a function, after we've initialized
 *	the local variables.
 *
 *	func_end is called at the end of a function.
 *
 *	stmt_beg and stmt_end are called before and after (respectively) each
 *	statement.
 *
 * Also, immediately before any call to func_setup, PL/pgSQL fills in the
 * error_callback and assign_expr fields with pointers to its own
 * plpgsql_exec_error_callback and exec_assign_expr functions.  This is
 * a somewhat ad-hoc expedient to simplify life for debugger plugins.
 */

typedef struct
{
	/* Function pointers set up by the plugin */
	void		(*func_setup) (PLpgSQL_execstate *estate, PLpgSQL_function *func);
	void		(*func_beg) (PLpgSQL_execstate *estate, PLpgSQL_function *func);
	void		(*func_end) (PLpgSQL_execstate *estate, PLpgSQL_function *func);
	void		(*stmt_beg) (PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);
	void		(*stmt_end) (PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

	/* Function pointers set by PL/pgSQL itself */
	void		(*error_callback) (void *arg);
	void		(*assign_expr) (PLpgSQL_execstate *estate, PLpgSQL_datum *target,
											PLpgSQL_expr *expr);
} PLpgSQL_plugin;


/* Struct types used during parsing */

typedef struct
{
	char	   *ident;			/* palloc'd converted identifier */
	bool		quoted;			/* Was it double-quoted? */
} PLword;

typedef struct
{
	List	   *idents;			/* composite identifiers (list of String) */
} PLcword;

typedef struct
{
	PLpgSQL_datum *datum;		/* referenced variable */
	char	   *ident;			/* valid if simple name */
	bool		quoted;
	List	   *idents;			/* valid if composite name */
} PLwdatum;

/**********************************************************************
 * Global variable declarations
 **********************************************************************/

typedef enum
{
	IDENTIFIER_LOOKUP_NORMAL,	/* normal processing of var names */
	IDENTIFIER_LOOKUP_DECLARE,	/* In DECLARE --- don't look up names */
	IDENTIFIER_LOOKUP_EXPR		/* In SQL expression --- special case */
} IdentifierLookup;

extern __thread  IdentifierLookup plpgsql_IdentifierLookup;

extern __thread  int plpgsql_variable_conflict;

extern __thread  bool plpgsql_print_strict_params;

extern bool plpgsql_check_asserts;

/* extra compile-time checks */
#define PLPGSQL_XCHECK_NONE			0
#define PLPGSQL_XCHECK_SHADOWVAR	1
#define PLPGSQL_XCHECK_ALL			((int) ~0)

extern int	plpgsql_extra_warnings;
extern int	plpgsql_extra_errors;

extern __thread  bool plpgsql_check_syntax;
extern __thread  bool plpgsql_DumpExecTree;

extern __thread  PLpgSQL_stmt_block *plpgsql_parse_result;

extern __thread  int plpgsql_nDatums;
extern __thread  PLpgSQL_datum **plpgsql_Datums;

extern __thread  char *plpgsql_error_funcname;

extern __thread  PLpgSQL_function *plpgsql_curr_compile;
extern __thread  MemoryContext compile_tmp_cxt;

extern PLpgSQL_plugin **plugin_ptr;

/**********************************************************************
 * Function declarations
 **********************************************************************/

/* ----------
 * Functions in pl_comp.c
 * ----------
 */
extern PLpgSQL_function *plpgsql_compile(FunctionCallInfo fcinfo,
				bool forValidator);
extern PLpgSQL_function *plpgsql_compile_inline(char *proc_source);
extern void plpgsql_parser_setup(struct ParseState *pstate,
					 PLpgSQL_expr *expr);
extern bool plpgsql_parse_word(char *word1, const char *yytxt,
				   PLwdatum *wdatum, PLword *word);
extern bool plpgsql_parse_dblword(char *word1, char *word2,
					  PLwdatum *wdatum, PLcword *cword);
extern bool plpgsql_parse_tripword(char *word1, char *word2, char *word3,
					   PLwdatum *wdatum, PLcword *cword);
extern PLpgSQL_type *plpgsql_parse_wordtype(char *ident);
extern PLpgSQL_type *plpgsql_parse_cwordtype(List *idents);
extern PLpgSQL_type *plpgsql_parse_wordrowtype(char *ident);
extern PLpgSQL_type *plpgsql_parse_cwordrowtype(List *idents);
extern PLpgSQL_type *plpgsql_build_datatype(Oid typeOid, int32 typmod,
					   Oid collation);
extern PLpgSQL_variable *plpgsql_build_variable(const char *refname, int lineno,
					   PLpgSQL_type *dtype,
					   bool add2namespace);
extern PLpgSQL_rec *plpgsql_build_record(const char *refname, int lineno,
					 bool add2namespace);
extern int plpgsql_recognize_err_condition(const char *condname,
								bool allow_sqlstate);
extern PLpgSQL_condition *plpgsql_parse_err_condition(char *condname);
extern void plpgsql_adddatum(PLpgSQL_datum *new);
extern int	plpgsql_add_initdatums(int **varnos);
extern void plpgsql_HashTableInit(void);

/* ----------
 * Functions in pl_handler.c
 * ----------
 */
extern void _PG_init(void);

/* ----------
 * Functions in pl_exec.c
 * ----------
 */
extern Datum plpgsql_exec_function(PLpgSQL_function *func,
					  FunctionCallInfo fcinfo,
					  EState *simple_eval_estate);
extern HeapTuple plpgsql_exec_trigger(PLpgSQL_function *func,
					 TriggerData *trigdata);
extern void plpgsql_exec_event_trigger(PLpgSQL_function *func,
						   EventTriggerData *trigdata);
extern void plpgsql_xact_cb(XactEvent event, void *arg);
extern void plpgsql_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
				   SubTransactionId parentSubid, void *arg);
extern Oid exec_get_datum_type(PLpgSQL_execstate *estate,
					PLpgSQL_datum *datum);
extern void exec_get_datum_type_info(PLpgSQL_execstate *estate,
						 PLpgSQL_datum *datum,
						 Oid *typeid, int32 *typmod, Oid *collation);

/* ----------
 * Functions for namespace handling in pl_funcs.c
 * ----------
 */
extern void plpgsql_ns_init(void);
extern void plpgsql_ns_push(const char *label);
extern void plpgsql_ns_pop(void);
extern PLpgSQL_nsitem *plpgsql_ns_top(void);
extern void plpgsql_ns_additem(int itemtype, int itemno, const char *name);
extern PLpgSQL_nsitem *plpgsql_ns_lookup(PLpgSQL_nsitem *ns_cur, bool localmode,
				  const char *name1, const char *name2,
				  const char *name3, int *names_used);
extern PLpgSQL_nsitem *plpgsql_ns_lookup_label(PLpgSQL_nsitem *ns_cur,
						const char *name);

/* ----------
 * Other functions in pl_funcs.c
 * ----------
 */
extern const char *plpgsql_stmt_typename(PLpgSQL_stmt *stmt);
extern const char *plpgsql_getdiag_kindname(int kind);
extern void plpgsql_free_function_memory(PLpgSQL_function *func);
extern void plpgsql_dumptree(PLpgSQL_function *func);

/* ----------
 * Scanner functions in pl_scanner.c
 * ----------
 */
extern int	plpgsql_base_yylex(void);
extern int	plpgsql_yylex(void);
extern void plpgsql_push_back_token(int token);
extern bool plpgsql_token_is_unreserved_keyword(int token);
extern void plpgsql_append_source_text(StringInfo buf,
						   int startlocation, int endlocation);
extern int	plpgsql_peek(void);
extern void plpgsql_peek2(int *tok1_p, int *tok2_p, int *tok1_loc,
			  int *tok2_loc);
extern int	plpgsql_scanner_errposition(int location);
extern void plpgsql_yyerror(const char *message) pg_attribute_noreturn();
extern int	plpgsql_location_to_lineno(int location);
extern int	plpgsql_latest_lineno(void);
extern void plpgsql_scanner_init(const char *str);
extern void plpgsql_scanner_finish(void);

/* ----------
 * Externs in gram.y
 * ----------
 */
extern int	plpgsql_yyparse(void);

#endif   /* PLPGSQL_H */
