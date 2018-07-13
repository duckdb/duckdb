/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - quote_identifier
 * - quote_all_identifiers
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * ruleutils.c
 *	  Functions to convert stored expressions/querytrees back to
 *	  source text
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/ruleutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_language.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "parser/keywords.h"
#include "parser/parse_node.h"
#include "parser/parse_agg.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteSupport.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/typcache.h"
#include "utils/xml.h"


/* ----------
 * Pretty formatting constants
 * ----------
 */

/* Indent counts */
#define PRETTYINDENT_STD		8
#define PRETTYINDENT_JOIN		4
#define PRETTYINDENT_VAR		4

#define PRETTYINDENT_LIMIT		40		/* wrap limit */

/* Pretty flags */
#define PRETTYFLAG_PAREN		1
#define PRETTYFLAG_INDENT		2

/* Default line length for pretty-print wrapping: 0 means wrap always */
#define WRAP_COLUMN_DEFAULT		0

/* macro to test if pretty action needed */
#define PRETTY_PAREN(context)	((context)->prettyFlags & PRETTYFLAG_PAREN)
#define PRETTY_INDENT(context)	((context)->prettyFlags & PRETTYFLAG_INDENT)


/* ----------
 * Local data types
 * ----------
 */

/* Context info needed for invoking a recursive querytree display routine */
typedef struct
{
	StringInfo	buf;			/* output buffer to append to */
	List	   *namespaces;		/* List of deparse_namespace nodes */
	List	   *windowClause;	/* Current query level's WINDOW clause */
	List	   *windowTList;	/* targetlist for resolving WINDOW clause */
	int			prettyFlags;	/* enabling of pretty-print functions */
	int			wrapColumn;		/* max line length, or -1 for no limit */
	int			indentLevel;	/* current indent level for prettyprint */
	bool		varprefix;		/* TRUE to print prefixes on Vars */
	ParseExprKind special_exprkind;		/* set only for exprkinds needing
										 * special handling */
} deparse_context;

/*
 * Each level of query context around a subtree needs a level of Var namespace.
 * A Var having varlevelsup=N refers to the N'th item (counting from 0) in
 * the current context's namespaces list.
 *
 * The rangetable is the list of actual RTEs from the query tree, and the
 * cte list is the list of actual CTEs.
 *
 * rtable_names holds the alias name to be used for each RTE (either a C
 * string, or NULL for nameless RTEs such as unnamed joins).
 * rtable_columns holds the column alias names to be used for each RTE.
 *
 * In some cases we need to make names of merged JOIN USING columns unique
 * across the whole query, not only per-RTE.  If so, unique_using is TRUE
 * and using_names is a list of C strings representing names already assigned
 * to USING columns.
 *
 * When deparsing plan trees, there is always just a single item in the
 * deparse_namespace list (since a plan tree never contains Vars with
 * varlevelsup > 0).  We store the PlanState node that is the immediate
 * parent of the expression to be deparsed, as well as a list of that
 * PlanState's ancestors.  In addition, we store its outer and inner subplan
 * state nodes, as well as their plan nodes' targetlists, and the index tlist
 * if the current plan node might contain INDEX_VAR Vars.  (These fields could
 * be derived on-the-fly from the current PlanState, but it seems notationally
 * clearer to set them up as separate fields.)
 */
typedef struct
{
	List	   *rtable;			/* List of RangeTblEntry nodes */
	List	   *rtable_names;	/* Parallel list of names for RTEs */
	List	   *rtable_columns; /* Parallel list of deparse_columns structs */
	List	   *ctes;			/* List of CommonTableExpr nodes */
	/* Workspace for column alias assignment: */
	bool		unique_using;	/* Are we making USING names globally unique */
	List	   *using_names;	/* List of assigned names for USING columns */
	/* Remaining fields are used only when deparsing a Plan tree: */
	PlanState  *planstate;		/* immediate parent of current expression */
	List	   *ancestors;		/* ancestors of planstate */
	PlanState  *outer_planstate;	/* outer subplan state, or NULL if none */
	PlanState  *inner_planstate;	/* inner subplan state, or NULL if none */
	List	   *outer_tlist;	/* referent for OUTER_VAR Vars */
	List	   *inner_tlist;	/* referent for INNER_VAR Vars */
	List	   *index_tlist;	/* referent for INDEX_VAR Vars */
} deparse_namespace;

/*
 * Per-relation data about column alias names.
 *
 * Selecting aliases is unreasonably complicated because of the need to dump
 * rules/views whose underlying tables may have had columns added, deleted, or
 * renamed since the query was parsed.  We must nonetheless print the rule/view
 * in a form that can be reloaded and will produce the same results as before.
 *
 * For each RTE used in the query, we must assign column aliases that are
 * unique within that RTE.  SQL does not require this of the original query,
 * but due to factors such as *-expansion we need to be able to uniquely
 * reference every column in a decompiled query.  As long as we qualify all
 * column references, per-RTE uniqueness is sufficient for that.
 *
 * However, we can't ensure per-column name uniqueness for unnamed join RTEs,
 * since they just inherit column names from their input RTEs, and we can't
 * rename the columns at the join level.  Most of the time this isn't an issue
 * because we don't need to reference the join's output columns as such; we
 * can reference the input columns instead.  That approach can fail for merged
 * JOIN USING columns, however, so when we have one of those in an unnamed
 * join, we have to make that column's alias globally unique across the whole
 * query to ensure it can be referenced unambiguously.
 *
 * Another problem is that a JOIN USING clause requires the columns to be
 * merged to have the same aliases in both input RTEs, and that no other
 * columns in those RTEs or their children conflict with the USING names.
 * To handle that, we do USING-column alias assignment in a recursive
 * traversal of the query's jointree.  When descending through a JOIN with
 * USING, we preassign the USING column names to the child columns, overriding
 * other rules for column alias assignment.  We also mark each RTE with a list
 * of all USING column names selected for joins containing that RTE, so that
 * when we assign other columns' aliases later, we can avoid conflicts.
 *
 * Another problem is that if a JOIN's input tables have had columns added or
 * deleted since the query was parsed, we must generate a column alias list
 * for the join that matches the current set of input columns --- otherwise, a
 * change in the number of columns in the left input would throw off matching
 * of aliases to columns of the right input.  Thus, positions in the printable
 * column alias list are not necessarily one-for-one with varattnos of the
 * JOIN, so we need a separate new_colnames[] array for printing purposes.
 */
typedef struct
{
	/*
	 * colnames is an array containing column aliases to use for columns that
	 * existed when the query was parsed.  Dropped columns have NULL entries.
	 * This array can be directly indexed by varattno to get a Var's name.
	 *
	 * Non-NULL entries are guaranteed unique within the RTE, *except* when
	 * this is for an unnamed JOIN RTE.  In that case we merely copy up names
	 * from the two input RTEs.
	 *
	 * During the recursive descent in set_using_names(), forcible assignment
	 * of a child RTE's column name is represented by pre-setting that element
	 * of the child's colnames array.  So at that stage, NULL entries in this
	 * array just mean that no name has been preassigned, not necessarily that
	 * the column is dropped.
	 */
	int			num_cols;		/* length of colnames[] array */
	char	  **colnames;		/* array of C strings and NULLs */

	/*
	 * new_colnames is an array containing column aliases to use for columns
	 * that would exist if the query was re-parsed against the current
	 * definitions of its base tables.  This is what to print as the column
	 * alias list for the RTE.  This array does not include dropped columns,
	 * but it will include columns added since original parsing.  Indexes in
	 * it therefore have little to do with current varattno values.  As above,
	 * entries are unique unless this is for an unnamed JOIN RTE.  (In such an
	 * RTE, we never actually print this array, but we must compute it anyway
	 * for possible use in computing column names of upper joins.) The
	 * parallel array is_new_col marks which of these columns are new since
	 * original parsing.  Entries with is_new_col false must match the
	 * non-NULL colnames entries one-for-one.
	 */
	int			num_new_cols;	/* length of new_colnames[] array */
	char	  **new_colnames;	/* array of C strings */
	bool	   *is_new_col;		/* array of bool flags */

	/* This flag tells whether we should actually print a column alias list */
	bool		printaliases;

	/* This list has all names used as USING names in joins above this RTE */
	List	   *parentUsing;	/* names assigned to parent merged columns */

	/*
	 * If this struct is for a JOIN RTE, we fill these fields during the
	 * set_using_names() pass to describe its relationship to its child RTEs.
	 *
	 * leftattnos and rightattnos are arrays with one entry per existing
	 * output column of the join (hence, indexable by join varattno).  For a
	 * simple reference to a column of the left child, leftattnos[i] is the
	 * child RTE's attno and rightattnos[i] is zero; and conversely for a
	 * column of the right child.  But for merged columns produced by JOIN
	 * USING/NATURAL JOIN, both leftattnos[i] and rightattnos[i] are nonzero.
	 * Also, if the column has been dropped, both are zero.
	 *
	 * If it's a JOIN USING, usingNames holds the alias names selected for the
	 * merged columns (these might be different from the original USING list,
	 * if we had to modify names to achieve uniqueness).
	 */
	int			leftrti;		/* rangetable index of left child */
	int			rightrti;		/* rangetable index of right child */
	int		   *leftattnos;		/* left-child varattnos of join cols, or 0 */
	int		   *rightattnos;	/* right-child varattnos of join cols, or 0 */
	List	   *usingNames;		/* names assigned to merged columns */
} deparse_columns;

/* This macro is analogous to rt_fetch(), but for deparse_columns structs */
#define deparse_columns_fetch(rangetable_index, dpns) \
	((deparse_columns *) list_nth((dpns)->rtable_columns, (rangetable_index)-1))

/*
 * Entry in set_rtable_names' hash table
 */
typedef struct
{
	char		name[NAMEDATALEN];		/* Hash key --- must be first */
	int			counter;		/* Largest addition used so far for name */
} NameHashEntry;


/* ----------
 * Global data
 * ----------
 */





/* GUC parameters */
__thread bool		quote_all_identifiers = false;



/* ----------
 * Local functions
 *
 * Most of these functions used to use fixed-size buffers to build their
 * results.  Now, they take an (already initialized) StringInfo object
 * as a parameter, and append their text output to its contents.
 * ----------
 */
static char *deparse_expression_pretty(Node *expr, List *dpcontext,
						  bool forceprefix, bool showimplicit,
						  int prettyFlags, int startIndent);
static char *pg_get_viewdef_worker(Oid viewoid,
					  int prettyFlags, int wrapColumn);
static char *pg_get_triggerdef_worker(Oid trigid, bool pretty);
static void decompile_column_index_array(Datum column_index_array, Oid relId,
							 StringInfo buf);
static char *pg_get_ruledef_worker(Oid ruleoid, int prettyFlags);
static char *pg_get_indexdef_worker(Oid indexrelid, int colno,
					   const Oid *excludeOps,
					   bool attrsOnly, bool showTblSpc,
					   int prettyFlags);
static char *pg_get_constraintdef_worker(Oid constraintId, bool fullCommand,
							int prettyFlags);
static text *pg_get_expr_worker(text *expr, Oid relid, const char *relname,
				   int prettyFlags);
static int print_function_arguments(StringInfo buf, HeapTuple proctup,
						 bool print_table_args, bool print_defaults);
static void print_function_rettype(StringInfo buf, HeapTuple proctup);
static void print_function_trftypes(StringInfo buf, HeapTuple proctup);
static void set_rtable_names(deparse_namespace *dpns, List *parent_namespaces,
				 Bitmapset *rels_used);
static void set_deparse_for_query(deparse_namespace *dpns, Query *query,
					  List *parent_namespaces);
static void set_simple_column_names(deparse_namespace *dpns);
static bool has_dangerous_join_using(deparse_namespace *dpns, Node *jtnode);
static void set_using_names(deparse_namespace *dpns, Node *jtnode,
				List *parentUsing);
static void set_relation_column_names(deparse_namespace *dpns,
						  RangeTblEntry *rte,
						  deparse_columns *colinfo);
static void set_join_column_names(deparse_namespace *dpns, RangeTblEntry *rte,
					  deparse_columns *colinfo);
static bool colname_is_unique(char *colname, deparse_namespace *dpns,
				  deparse_columns *colinfo);
static char *make_colname_unique(char *colname, deparse_namespace *dpns,
					deparse_columns *colinfo);
static void expand_colnames_array_to(deparse_columns *colinfo, int n);
static void identify_join_columns(JoinExpr *j, RangeTblEntry *jrte,
					  deparse_columns *colinfo);
static void flatten_join_using_qual(Node *qual,
						List **leftvars, List **rightvars);
static char *get_rtable_name(int rtindex, deparse_context *context);
static void set_deparse_planstate(deparse_namespace *dpns, PlanState *ps);
static void push_child_plan(deparse_namespace *dpns, PlanState *ps,
				deparse_namespace *save_dpns);
static void pop_child_plan(deparse_namespace *dpns,
			   deparse_namespace *save_dpns);
static void push_ancestor_plan(deparse_namespace *dpns, ListCell *ancestor_cell,
				   deparse_namespace *save_dpns);
static void pop_ancestor_plan(deparse_namespace *dpns,
				  deparse_namespace *save_dpns);
static void make_ruledef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc,
			 int prettyFlags);
static void make_viewdef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc,
			 int prettyFlags, int wrapColumn);
static void get_query_def(Query *query, StringInfo buf, List *parentnamespace,
			  TupleDesc resultDesc,
			  int prettyFlags, int wrapColumn, int startIndent);
static void get_values_def(List *values_lists, deparse_context *context);
static void get_with_clause(Query *query, deparse_context *context);
static void get_select_query_def(Query *query, deparse_context *context,
					 TupleDesc resultDesc);
static void get_insert_query_def(Query *query, deparse_context *context);
static void get_update_query_def(Query *query, deparse_context *context);
static void get_update_query_targetlist_def(Query *query, List *targetList,
								deparse_context *context,
								RangeTblEntry *rte);
static void get_delete_query_def(Query *query, deparse_context *context);
static void get_utility_query_def(Query *query, deparse_context *context);
static void get_basic_select_query(Query *query, deparse_context *context,
					   TupleDesc resultDesc);
static void get_target_list(List *targetList, deparse_context *context,
				TupleDesc resultDesc);
static void get_setop_query(Node *setOp, Query *query,
				deparse_context *context,
				TupleDesc resultDesc);
static Node *get_rule_sortgroupclause(Index ref, List *tlist,
						 bool force_colno,
						 deparse_context *context);
static void get_rule_groupingset(GroupingSet *gset, List *targetlist,
					 bool omit_parens, deparse_context *context);
static void get_rule_orderby(List *orderList, List *targetList,
				 bool force_colno, deparse_context *context);
static void get_rule_windowclause(Query *query, deparse_context *context);
static void get_rule_windowspec(WindowClause *wc, List *targetList,
					deparse_context *context);
static char *get_variable(Var *var, int levelsup, bool istoplevel,
			 deparse_context *context);
static Node *find_param_referent(Param *param, deparse_context *context,
					deparse_namespace **dpns_p, ListCell **ancestor_cell_p);
static void get_parameter(Param *param, deparse_context *context);
static const char *get_simple_binary_op_name(OpExpr *expr);
static bool isSimpleNode(Node *node, Node *parentNode, int prettyFlags);
static void appendContextKeyword(deparse_context *context, const char *str,
					 int indentBefore, int indentAfter, int indentPlus);
static void removeStringInfoSpaces(StringInfo str);
static void get_rule_expr(Node *node, deparse_context *context,
			  bool showimplicit);
static void get_rule_expr_toplevel(Node *node, deparse_context *context,
					   bool showimplicit);
static void get_oper_expr(OpExpr *expr, deparse_context *context);
static void get_func_expr(FuncExpr *expr, deparse_context *context,
			  bool showimplicit);
static void get_agg_expr(Aggref *aggref, deparse_context *context);
static void get_windowfunc_expr(WindowFunc *wfunc, deparse_context *context);
static void get_coercion_expr(Node *arg, deparse_context *context,
				  Oid resulttype, int32 resulttypmod,
				  Node *parentNode);
static void get_const_expr(Const *constval, deparse_context *context,
			   int showtype);
static void get_const_collation(Const *constval, deparse_context *context);
static void simple_quote_literal(StringInfo buf, const char *val);
static void get_sublink_expr(SubLink *sublink, deparse_context *context);
static void get_from_clause(Query *query, const char *prefix,
				deparse_context *context);
static void get_from_clause_item(Node *jtnode, Query *query,
					 deparse_context *context);
static void get_column_alias_list(deparse_columns *colinfo,
					  deparse_context *context);
static void get_from_clause_coldeflist(RangeTblFunction *rtfunc,
						   deparse_columns *colinfo,
						   deparse_context *context);
static void get_tablesample_def(TableSampleClause *tablesample,
					deparse_context *context);
static void get_opclass_name(Oid opclass, Oid actual_datatype,
				 StringInfo buf);
static Node *processIndirection(Node *node, deparse_context *context,
				   bool printit);
static void printSubscripts(ArrayRef *aref, deparse_context *context);
static char *get_relation_name(Oid relid);
static char *generate_relation_name(Oid relid, List *namespaces);
static char *generate_qualified_relation_name(Oid relid);
static char *generate_function_name(Oid funcid, int nargs,
					   List *argnames, Oid *argtypes,
					   bool has_variadic, bool *use_variadic_p,
					   ParseExprKind special_exprkind);
static char *generate_operator_name(Oid operid, Oid arg1, Oid arg2);
static text *string_to_text(char *str);
static char *flatten_reloptions(Oid relid);

#define only_marker(rte)  ((rte)->inh ? "" : "ONLY ")


/* ----------
 * get_ruledef			- Do it all and return a text
 *				  that could be used as a statement
 *				  to recreate the rule
 * ----------
 */









/* ----------
 * get_viewdef			- Mainly the same thing, but we
 *				  only return the SELECT part of a view
 * ----------
 */












/*
 * Common code for by-OID and by-name variants of pg_get_viewdef
 */


/* ----------
 * get_triggerdef			- Get the definition of a trigger
 * ----------
 */






/* ----------
 * get_indexdef			- Get the definition of an index
 *
 * In the extended version, there is a colno argument as well as pretty bool.
 *	if colno == 0, we want a complete index definition.
 *	if colno > 0, we only want the Nth index key's variable or expression.
 *
 * Note that the SQL-function versions of this omit any info about the
 * index tablespace; this is intentional because pg_dump wants it that way.
 * However pg_get_indexdef_string() includes index tablespace if not default.
 * ----------
 */




/* Internal version that returns a palloc'd C string; no pretty-printing */


/* Internal version that just reports the column definitions */


/*
 * Internal workhorse to decompile an index definition.
 *
 * This is now used for exclusion constraints as well: if excludeOps is not
 * NULL then it points to an array of exclusion operator OIDs.
 */



/*
 * pg_get_constraintdef
 *
 * Returns the definition for the constraint, ie, everything that needs to
 * appear after "ALTER TABLE ... ADD CONSTRAINT <constraintname>".
 */




/*
 * Internal version that returns a full ALTER TABLE ... ADD CONSTRAINT command
 */


/*
 * As of 9.4, we now use an MVCC snapshot for this.
 */



/*
 * Convert an int16[] Datum into a comma-separated list of column names
 * for the indicated relation; append the list to buf.
 */



/* ----------
 * get_expr			- Decompile an expression tree
 *
 * Input: an expression tree in nodeToString form, and a relation OID
 *
 * Output: reverse-listed expression
 *
 * Currently, the expression can only refer to a single relation, namely
 * the one specified by the second parameter.  This is sufficient for
 * partial indexes, column default expressions, etc.  We also support
 * Var-free expressions, for which the OID can be InvalidOid.
 * ----------
 */







/* ----------
 * get_userbyid			- Get a user name by roleid and
 *				  fallback to 'unknown (OID=n)'
 * ----------
 */



/*
 * pg_get_serial_sequence
 *		Get the name of the sequence used by a serial column,
 *		formatted suitably for passing to setval, nextval or currval.
 *		First parameter is not treated as double-quoted, second parameter
 *		is --- see documentation for reason.
 */



/*
 * pg_get_functiondef
 *		Returns the complete "CREATE OR REPLACE FUNCTION ..." statement for
 *		the specified function.
 *
 * Note: if you change the output format of this function, be careful not
 * to break psql's rules (in \ef and \sf) for identifying the start of the
 * function body.  To wit: the function body starts on a line that begins
 * with "AS ", and no preceding line will look like that.
 */


/*
 * pg_get_function_arguments
 *		Get a nicely-formatted list of arguments for a function.
 *		This is everything that would go between the parentheses in
 *		CREATE FUNCTION.
 */


/*
 * pg_get_function_identity_arguments
 *		Get a formatted list of arguments for a function.
 *		This is everything that would go between the parentheses in
 *		ALTER FUNCTION, etc.  In particular, don't print defaults.
 */


/*
 * pg_get_function_result
 *		Get a nicely-formatted version of the result type of a function.
 *		This is what would appear after RETURNS in CREATE FUNCTION.
 */


/*
 * Guts of pg_get_function_result: append the function's return type
 * to the specified buffer.
 */


/*
 * Common code for pg_get_function_arguments and pg_get_function_result:
 * append the desired subset of arguments to buf.  We print only TABLE
 * arguments when print_table_args is true, and all the others when it's false.
 * We print argument defaults only if print_defaults is true.
 * Function return value is the number of arguments printed.
 */




/*
 * Append used transformated types to specified buffer
 */


/*
 * Get textual representation of a function argument's default value.  The
 * second argument of this function is the argument number among all arguments
 * (i.e. proallargtypes, *not* proargtypes), starting with 1, because that's
 * how information_schema.sql uses it.
 */



/*
 * deparse_expression			- General utility for deparsing expressions
 *
 * calls deparse_expression_pretty with all prettyPrinting disabled
 */


/* ----------
 * deparse_expression_pretty	- General utility for deparsing expressions
 *
 * expr is the node tree to be deparsed.  It must be a transformed expression
 * tree (ie, not the raw output of gram.y).
 *
 * dpcontext is a list of deparse_namespace nodes representing the context
 * for interpreting Vars in the node tree.  It can be NIL if no Vars are
 * expected.
 *
 * forceprefix is TRUE to force all Vars to be prefixed with their table names.
 *
 * showimplicit is TRUE to force all implicit casts to be shown explicitly.
 *
 * Tries to pretty up the output according to prettyFlags and startIndent.
 *
 * The result is a palloc'd string.
 * ----------
 */


/* ----------
 * deparse_context_for			- Build deparse context for a single relation
 *
 * Given the reference name (alias) and OID of a relation, build deparsing
 * context for an expression referencing only that relation (as varno 1,
 * varlevelsup 0).  This is sufficient for many uses of deparse_expression.
 * ----------
 */


/*
 * deparse_context_for_plan_rtable - Build deparse context for a plan's rtable
 *
 * When deparsing an expression in a Plan tree, we use the plan's rangetable
 * to resolve names of simple Vars.  The initialization of column names for
 * this is rather expensive if the rangetable is large, and it'll be the same
 * for every expression in the Plan tree; so we do it just once and re-use
 * the result of this function for each expression.  (Note that the result
 * is not usable until set_deparse_context_planstate() is applied to it.)
 *
 * In addition to the plan's rangetable list, pass the per-RTE alias names
 * assigned by a previous call to select_rtable_names_for_explain.
 */


/*
 * set_deparse_context_planstate	- Specify Plan node containing expression
 *
 * When deparsing an expression in a Plan tree, we might have to resolve
 * OUTER_VAR, INNER_VAR, or INDEX_VAR references.  To do this, the caller must
 * provide the parent PlanState node.  Then OUTER_VAR and INNER_VAR references
 * can be resolved by drilling down into the left and right child plans.
 * Similarly, INDEX_VAR references can be resolved by reference to the
 * indextlist given in a parent IndexOnlyScan node, or to the scan tlist in
 * ForeignScan and CustomScan nodes.  (Note that we don't currently support
 * deparsing of indexquals in regular IndexScan or BitmapIndexScan nodes;
 * for those, we can only deparse the indexqualorig fields, which won't
 * contain INDEX_VAR Vars.)
 *
 * Note: planstate really ought to be declared as "PlanState *", but we use
 * "Node *" to avoid having to include execnodes.h in ruleutils.h.
 *
 * The ancestors list is a list of the PlanState's parent PlanStates, the
 * most-closely-nested first.  This is needed to resolve PARAM_EXEC Params.
 * Note we assume that all the PlanStates share the same rtable.
 *
 * Once this function has been called, deparse_expression() can be called on
 * subsidiary expression(s) of the specified PlanState node.  To deparse
 * expressions of a different Plan node in the same Plan tree, re-call this
 * function to identify the new parent Plan node.
 *
 * The result is the same List passed in; this is a notational convenience.
 */


/*
 * select_rtable_names_for_explain	- Select RTE aliases for EXPLAIN
 *
 * Determine the relation aliases we'll use during an EXPLAIN operation.
 * This is just a frontend to set_rtable_names.  We have to expose the aliases
 * to EXPLAIN because EXPLAIN needs to know the right alias names to print.
 */


/*
 * set_rtable_names: select RTE aliases to be used in printing a query
 *
 * We fill in dpns->rtable_names with a list of names that is one-for-one with
 * the already-filled dpns->rtable list.  Each RTE name is unique among those
 * in the new namespace plus any ancestor namespaces listed in
 * parent_namespaces.
 *
 * If rels_used isn't NULL, only RTE indexes listed in it are given aliases.
 *
 * Note that this function is only concerned with relation names, not column
 * names.
 */


/*
 * set_deparse_for_query: set up deparse_namespace for deparsing a Query tree
 *
 * For convenience, this is defined to initialize the deparse_namespace struct
 * from scratch.
 */


/*
 * set_simple_column_names: fill in column aliases for non-query situations
 *
 * This handles EXPLAIN and cases where we only have relation RTEs.  Without
 * a join tree, we can't do anything smart about join RTEs, but we don't
 * need to (note that EXPLAIN should never see join alias Vars anyway).
 * If we do hit a join RTE we'll just process it like a non-table base RTE.
 */


/*
 * has_dangerous_join_using: search jointree for unnamed JOIN USING
 *
 * Merged columns of a JOIN USING may act differently from either of the input
 * columns, either because they are merged with COALESCE (in a FULL JOIN) or
 * because an implicit coercion of the underlying input column is required.
 * In such a case the column must be referenced as a column of the JOIN not as
 * a column of either input.  And this is problematic if the join is unnamed
 * (alias-less): we cannot qualify the column's name with an RTE name, since
 * there is none.  (Forcibly assigning an alias to the join is not a solution,
 * since that will prevent legal references to tables below the join.)
 * To ensure that every column in the query is unambiguously referenceable,
 * we must assign such merged columns names that are globally unique across
 * the whole query, aliasing other columns out of the way as necessary.
 *
 * Because the ensuing re-aliasing is fairly damaging to the readability of
 * the query, we don't do this unless we have to.  So, we must pre-scan
 * the join tree to see if we have to, before starting set_using_names().
 */


/*
 * set_using_names: select column aliases to be used for merged USING columns
 *
 * We do this during a recursive descent of the query jointree.
 * dpns->unique_using must already be set to determine the global strategy.
 *
 * Column alias info is saved in the dpns->rtable_columns list, which is
 * assumed to be filled with pre-zeroed deparse_columns structs.
 *
 * parentUsing is a list of all USING aliases assigned in parent joins of
 * the current jointree node.  (The passed-in list must not be modified.)
 */


/*
 * set_relation_column_names: select column aliases for a non-join RTE
 *
 * Column alias info is saved in *colinfo, which is assumed to be pre-zeroed.
 * If any colnames entries are already filled in, those override local
 * choices.
 */


/*
 * set_join_column_names: select column aliases for a join RTE
 *
 * Column alias info is saved in *colinfo, which is assumed to be pre-zeroed.
 * If any colnames entries are already filled in, those override local
 * choices.  Also, names for USING columns were already chosen by
 * set_using_names().  We further expect that column alias selection has been
 * completed for both input RTEs.
 */
#ifdef USE_ASSERT_CHECKING
#endif

/*
 * colname_is_unique: is colname distinct from already-chosen column names?
 *
 * dpns is query-wide info, colinfo is for the column's RTE
 */


/*
 * make_colname_unique: modify colname if necessary to make it unique
 *
 * dpns is query-wide info, colinfo is for the column's RTE
 */


/*
 * expand_colnames_array_to: make colinfo->colnames at least n items long
 *
 * Any added array entries are initialized to zero.
 */


/*
 * identify_join_columns: figure out where columns of a join come from
 *
 * Fills the join-specific fields of the colinfo struct, except for
 * usingNames which is filled later.
 */


/*
 * flatten_join_using_qual: extract Vars being joined from a JOIN/USING qual
 *
 * We assume that transformJoinUsingClause won't have produced anything except
 * AND nodes, equality operator nodes, and possibly implicit coercions, and
 * that the AND node inputs match left-to-right with the original USING list.
 *
 * Caller must initialize the result lists to NIL.
 */


/*
 * get_rtable_name: convenience function to get a previously assigned RTE alias
 *
 * The RTE must belong to the topmost namespace level in "context".
 */


/*
 * set_deparse_planstate: set up deparse_namespace to parse subexpressions
 * of a given PlanState node
 *
 * This sets the planstate, outer_planstate, inner_planstate, outer_tlist,
 * inner_tlist, and index_tlist fields.  Caller is responsible for adjusting
 * the ancestors list if necessary.  Note that the rtable and ctes fields do
 * not need to change when shifting attention to different plan nodes in a
 * single plan tree.
 */


/*
 * push_child_plan: temporarily transfer deparsing attention to a child plan
 *
 * When expanding an OUTER_VAR or INNER_VAR reference, we must adjust the
 * deparse context in case the referenced expression itself uses
 * OUTER_VAR/INNER_VAR.  We modify the top stack entry in-place to avoid
 * affecting levelsup issues (although in a Plan tree there really shouldn't
 * be any).
 *
 * Caller must provide a local deparse_namespace variable to save the
 * previous state for pop_child_plan.
 */


/*
 * pop_child_plan: undo the effects of push_child_plan
 */


/*
 * push_ancestor_plan: temporarily transfer deparsing attention to an
 * ancestor plan
 *
 * When expanding a Param reference, we must adjust the deparse context
 * to match the plan node that contains the expression being printed;
 * otherwise we'd fail if that expression itself contains a Param or
 * OUTER_VAR/INNER_VAR/INDEX_VAR variable.
 *
 * The target ancestor is conveniently identified by the ListCell holding it
 * in dpns->ancestors.
 *
 * Caller must provide a local deparse_namespace variable to save the
 * previous state for pop_ancestor_plan.
 */


/*
 * pop_ancestor_plan: undo the effects of push_ancestor_plan
 */



/* ----------
 * make_ruledef			- reconstruct the CREATE RULE command
 *				  for a given pg_rewrite tuple
 * ----------
 */



/* ----------
 * make_viewdef			- reconstruct the SELECT part of a
 *				  view rewrite rule
 * ----------
 */



/* ----------
 * get_query_def			- Parse back one query parsetree
 *
 * If resultDesc is not NULL, then it is the output tuple descriptor for
 * the view represented by a SELECT query.
 * ----------
 */


/* ----------
 * get_values_def			- Parse back a VALUES list
 * ----------
 */


/* ----------
 * get_with_clause			- Parse back a WITH clause
 * ----------
 */


/* ----------
 * get_select_query_def			- Parse back a SELECT parsetree
 * ----------
 */


/*
 * Detect whether query looks like SELECT ... FROM VALUES();
 * if so, return the VALUES RTE.  Otherwise return NULL.
 */




/* ----------
 * get_target_list			- Parse back a SELECT target list
 *
 * This is also used for RETURNING lists in INSERT/UPDATE/DELETE.
 * ----------
 */




/*
 * Display a sort/group clause.
 *
 * Also returns the expression tree, so caller need not find it again.
 */


/*
 * Display a GroupingSet
 */


/*
 * Display an ORDER BY list.
 */


/*
 * Display a WINDOW clause.
 *
 * Note that the windowClause list might contain only anonymous window
 * specifications, in which case we should print nothing here.
 */


/*
 * Display a window definition
 */


/* ----------
 * get_insert_query_def			- Parse back an INSERT parsetree
 * ----------
 */



/* ----------
 * get_update_query_def			- Parse back an UPDATE parsetree
 * ----------
 */



/* ----------
 * get_update_query_targetlist_def			- Parse back an UPDATE targetlist
 * ----------
 */



/* ----------
 * get_delete_query_def			- Parse back a DELETE parsetree
 * ----------
 */



/* ----------
 * get_utility_query_def			- Parse back a UTILITY parsetree
 * ----------
 */



/*
 * Display a Var appropriately.
 *
 * In some cases (currently only when recursing into an unnamed join)
 * the Var's varlevelsup has to be interpreted with respect to a context
 * above the current one; levelsup indicates the offset.
 *
 * If istoplevel is TRUE, the Var is at the top level of a SELECT's
 * targetlist, which means we need special treatment of whole-row Vars.
 * Instead of the normal "tab.*", we'll print "tab.*::typename", which is a
 * dirty hack to prevent "tab.*" from being expanded into multiple columns.
 * (The parser will strip the useless coercion, so no inefficiency is added in
 * dump and reload.)  We used to print just "tab" in such cases, but that is
 * ambiguous and will yield the wrong result if "tab" is also a plain column
 * name in the query.
 *
 * Returns the attname of the Var, or NULL if the Var has no attname (because
 * it is a whole-row Var or a subplan output reference).
 */



/*
 * Get the name of a field of an expression of composite type.  The
 * expression is usually a Var, but we handle other cases too.
 *
 * levelsup is an extra offset to interpret the Var's varlevelsup correctly.
 *
 * This is fairly straightforward when the expression has a named composite
 * type; we need only look up the type in the catalogs.  However, the type
 * could also be RECORD.  Since no actual table or view column is allowed to
 * have type RECORD, a Var of type RECORD must refer to a JOIN or FUNCTION RTE
 * or to a subquery output.  We drill down to find the ultimate defining
 * expression and attempt to infer the field name from it.  We ereport if we
 * can't determine the name.
 *
 * Similarly, a PARAM of type RECORD has to refer to some expression of
 * a determinable composite type.
 */


/*
 * Try to find the referenced expression for a PARAM_EXEC Param that might
 * reference a parameter supplied by an upper NestLoop or SubPlan plan node.
 *
 * If successful, return the expression and set *dpns_p and *ancestor_cell_p
 * appropriately for calling push_ancestor_plan().  If no referent can be
 * found, return NULL.
 */


/*
 * Display a Param appropriately.
 */


/*
 * get_simple_binary_op_name
 *
 * helper function for isSimpleNode
 * will return single char binary operator name, or NULL if it's not
 */



/*
 * isSimpleNode - check if given node is simple (doesn't need parenthesizing)
 *
 *	true   : simple in the context of parent node's type
 *	false  : not simple
 */



/*
 * appendContextKeyword - append a keyword to buffer
 *
 * If prettyPrint is enabled, perform a line break, and adjust indentation.
 * Otherwise, just append the keyword.
 */


/*
 * removeStringInfoSpaces - delete trailing spaces from a buffer.
 *
 * Possibly this should move to stringinfo.c at some point.
 */



/*
 * get_rule_expr_paren	- deparse expr using get_rule_expr,
 * embracing the string with parentheses if necessary for prettyPrint.
 *
 * Never embrace if prettyFlags=0, because it's done in the calling node.
 *
 * Any node that does *not* embrace its argument node by sql syntax (with
 * parentheses, non-operator keywords like CASE/WHEN/ON, or comma etc) should
 * use get_rule_expr_paren instead of get_rule_expr so parentheses can be
 * added.
 */



/* ----------
 * get_rule_expr			- Parse back an expression
 *
 * Note: showimplicit determines whether we display any implicit cast that
 * is present at the top of the expression tree.  It is a passed argument,
 * not a field of the context struct, because we change the value as we
 * recurse down into the expression.  In general we suppress implicit casts
 * when the result type is known with certainty (eg, the arguments of an
 * OR must be boolean).  We display implicit casts for arguments of functions
 * and operators, since this is needed to be certain that the same function
 * or operator will be chosen when the expression is re-parsed.
 * ----------
 */


/*
 * get_rule_expr_toplevel		- Parse back a toplevel expression
 *
 * Same as get_rule_expr(), except that if the expr is just a Var, we pass
 * istoplevel = true not false to get_variable().  This causes whole-row Vars
 * to get printed with decoration that will prevent expansion of "*".
 * We need to use this in contexts such as ROW() and VALUES(), where the
 * parser would expand "foo.*" appearing at top level.  (In principle we'd
 * use this in get_target_list() too, but that has additional worries about
 * whether to print AS, so it needs to invoke get_variable() directly anyway.)
 */



/*
 * get_oper_expr			- Parse back an OpExpr node
 */


/*
 * get_func_expr			- Parse back a FuncExpr node
 */


/*
 * get_agg_expr			- Parse back an Aggref node
 */


/*
 * get_windowfunc_expr	- Parse back a WindowFunc node
 */


/* ----------
 * get_coercion_expr
 *
 *	Make a string representation of a value coerced to a specific type
 * ----------
 */


/* ----------
 * get_const_expr
 *
 *	Make a string representation of a Const
 *
 * showtype can be -1 to never show "::typename" decoration, or +1 to always
 * show it, or 0 to show it only if the constant wouldn't be assumed to be
 * the right type by default.
 *
 * If the Const's collation isn't default for its type, show that too.
 * We mustn't do this when showtype is -1 (since that means the caller will
 * print "::typename", and we can't put a COLLATE clause in between).  It's
 * caller's responsibility that collation isn't missed in such cases.
 * ----------
 */


/*
 * helper for get_const_expr: append COLLATE if needed
 */


/*
 * simple_quote_literal - Format a string as a SQL literal, append to buf
 */



/* ----------
 * get_sublink_expr			- Parse back a sublink
 * ----------
 */



/* ----------
 * get_from_clause			- Parse back a FROM clause
 *
 * "prefix" is the keyword that denotes the start of the list of FROM
 * elements. It is FROM when used to parse back SELECT and UPDATE, but
 * is USING when parsing back DELETE.
 * ----------
 */




/*
 * get_column_alias_list - print column alias list for an RTE
 *
 * Caller must already have printed the relation's alias name.
 */


/*
 * get_from_clause_coldeflist - reproduce FROM clause coldeflist
 *
 * When printing a top-level coldeflist (which is syntactically also the
 * relation's column alias list), use column names from colinfo.  But when
 * printing a coldeflist embedded inside ROWS FROM(), we prefer to use the
 * original coldeflist's names, which are available in rtfunc->funccolnames.
 * Pass NULL for colinfo to select the latter behavior.
 *
 * The coldeflist is appended immediately (no space) to buf.  Caller is
 * responsible for ensuring that an alias or AS is present before it.
 */


/*
 * get_tablesample_def			- print a TableSampleClause
 */


/*
 * get_opclass_name			- fetch name of an index operator class
 *
 * The opclass name is appended (after a space) to buf.
 *
 * Output is suppressed if the opclass is the default for the given
 * actual_datatype.  (If you don't want this behavior, just pass
 * InvalidOid for actual_datatype.)
 */


/*
 * processIndirection - take care of array and subfield assignment
 *
 * We strip any top-level FieldStore or assignment ArrayRef nodes that
 * appear in the input, and return the subexpression that's to be assigned.
 * If printit is true, we also print out the appropriate decoration for the
 * base column name (that the caller just printed).
 */




/*
 * quote_identifier			- Quote an identifier only if needed
 *
 * When quotes are needed, we palloc the required space; slightly
 * space-wasteful but well worth it for notational simplicity.
 */
const char *
quote_identifier(const char *ident)
{
	/*
	 * Can avoid quoting if ident starts with a lowercase letter or underscore
	 * and contains only lowercase letters, digits, and underscores, *and* is
	 * not any SQL keyword.  Otherwise, supply quotes.
	 */
	int			nquotes = 0;
	bool		safe;
	const char *ptr;
	char	   *result;
	char	   *optr;

	/*
	 * would like to use <ctype.h> macros here, but they might yield unwanted
	 * locale-specific results...
	 */
	safe = ((ident[0] >= 'a' && ident[0] <= 'z') || ident[0] == '_');

	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if ((ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') ||
			(ch == '_'))
		{
			/* okay */
		}
		else
		{
			safe = false;
			if (ch == '"')
				nquotes++;
		}
	}

	if (quote_all_identifiers)
		safe = false;

	if (safe)
	{
		/*
		 * Check for keyword.  We quote keywords except for unreserved ones.
		 * (In some cases we could avoid quoting a col_name or type_func_name
		 * keyword, but it seems much harder than it's worth to tell that.)
		 *
		 * Note: ScanKeywordLookup() does case-insensitive comparison, but
		 * that's fine, since we already know we have all-lower-case.
		 */
		const ScanKeyword *keyword = ScanKeywordLookup(ident,
													   ScanKeywords,
													   NumScanKeywords);

		if (keyword != NULL && keyword->category != UNRESERVED_KEYWORD)
			safe = false;
	}

	if (safe)
		return ident;			/* no change needed */

	result = (char *) palloc(strlen(ident) + nquotes + 2 + 1);

	optr = result;
	*optr++ = '"';
	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if (ch == '"')
			*optr++ = '"';
		*optr++ = ch;
	}
	*optr++ = '"';
	*optr = '\0';

	return result;
}

/*
 * quote_qualified_identifier	- Quote a possibly-qualified identifier
 *
 * Return a name of the form qualifier.ident, or just ident if qualifier
 * is NULL, quoting each component if necessary.  The result is palloc'd.
 */


/*
 * get_relation_name
 *		Get the unqualified name of a relation specified by OID
 *
 * This differs from the underlying get_rel_name() function in that it will
 * throw error instead of silently returning NULL if the OID is bad.
 */


/*
 * generate_relation_name
 *		Compute the name to display for a relation specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 *
 * If namespaces isn't NIL, it must be a list of deparse_namespace nodes.
 * We will forcibly qualify the relation name if it equals any CTE name
 * visible in the namespace list.
 */


/*
 * generate_qualified_relation_name
 *		Compute the name to display for a relation specified by OID
 *
 * As above, but unconditionally schema-qualify the name.
 */


/*
 * generate_function_name
 *		Compute the name to display for a function specified by OID,
 *		given that it is being called with the specified actual arg names and
 *		types.  (Those matter because of ambiguous-function resolution rules.)
 *
 * If we're dealing with a potentially variadic function (in practice, this
 * means a FuncExpr or Aggref, not some other way of calling a function), then
 * has_variadic must specify whether variadic arguments have been merged,
 * and *use_variadic_p will be set to indicate whether to print VARIADIC in
 * the output.  For non-FuncExpr cases, has_variadic should be FALSE and
 * use_variadic_p can be NULL.
 *
 * The result includes all necessary quoting and schema-prefixing.
 */


/*
 * generate_operator_name
 *		Compute the name to display for an operator specified by OID,
 *		given that it is being called with the specified actual arg types.
 *		(Arg types matter because of ambiguous-operator resolution rules.
 *		Pass InvalidOid for unused arg of a unary operator.)
 *
 * The result includes all necessary quoting and schema-prefixing,
 * plus the OPERATOR() decoration needed to use a qualified operator name
 * in an expression.
 */


/*
 * generate_collation_name
 *		Compute the name to display for a collation specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 */


/*
 * Given a C string, produce a TEXT datum.
 *
 * We assume that the input was palloc'd and may be freed.
 */


/*
 * Generate a C string representing a relation's reloptions, or NULL if none.
 */

