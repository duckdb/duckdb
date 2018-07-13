#include "pg_query.h"
#include "pg_query_json_plpgsql.h"

#include "pg_query_json_helper.c"

#define WRITE_OBJ_FIELD(fldname, outfunc) \
	if (node->fldname != NULL) { \
		 appendStringInfo(str, "\"" CppAsString(fldname) "\": {"); \
		 outfunc(str, node->fldname); \
		 removeTrailingDelimiter(str); \
		 appendStringInfo(str, "}}, "); \
	}

#define WRITE_LIST_FIELD(fldname, fldtype, outfunc) \
	if (node->fldname != NULL) { \
		ListCell *lc; \
		appendStringInfo(str, "\"" CppAsString(fldname) "\": ["); \
		foreach(lc, node->fldname) { \
			appendStringInfoString(str, "{"); \
			outfunc(str, (fldtype *) lfirst(lc)); \
			removeTrailingDelimiter(str); \
			appendStringInfoString(str, "}},"); \
		} \
		removeTrailingDelimiter(str); \
		appendStringInfoString(str, "], "); \
  }

  #define WRITE_STATEMENTS_FIELD(fldname) \
  	if (node->fldname != NULL) { \
  		ListCell *lc; \
  		appendStringInfo(str, "\"" CppAsString(fldname) "\": ["); \
  		foreach(lc, node->fldname) { \
  			dump_stmt(str, (PLpgSQL_stmt *) lfirst(lc)); \
  		} \
  		removeTrailingDelimiter(str); \
  		appendStringInfoString(str, "],"); \
    }

#define WRITE_EXPR_FIELD(fldname)   WRITE_OBJ_FIELD(fldname, dump_expr)
#define WRITE_BLOCK_FIELD(fldname)  WRITE_OBJ_FIELD(fldname, dump_block)
#define WRITE_RECORD_FIELD(fldname) WRITE_OBJ_FIELD(fldname, dump_record)
#define WRITE_ROW_FIELD(fldname)    WRITE_OBJ_FIELD(fldname, dump_row)
#define WRITE_VAR_FIELD(fldname)    WRITE_OBJ_FIELD(fldname, dump_var)

static void dump_record(StringInfo str, PLpgSQL_rec *stmt);
static void dump_row(StringInfo str, PLpgSQL_row *stmt);
static void dump_var(StringInfo str, PLpgSQL_var *stmt);
static void dump_record_field(StringInfo str, PLpgSQL_recfield *node);
static void dump_array_elem(StringInfo str, PLpgSQL_arrayelem *node);
static void dump_stmt(StringInfo str, PLpgSQL_stmt *stmt);
static void dump_block(StringInfo str, PLpgSQL_stmt_block *block);
static void dump_exception_block(StringInfo str, PLpgSQL_exception_block *node);
static void dump_assign(StringInfo str, PLpgSQL_stmt_assign *stmt);
static void dump_if(StringInfo str, PLpgSQL_stmt_if *stmt);
static void dump_if_elsif(StringInfo str, PLpgSQL_if_elsif *node);
static void dump_case(StringInfo str, PLpgSQL_stmt_case *stmt);
static void dump_case_when(StringInfo str, PLpgSQL_case_when *node);
static void dump_loop(StringInfo str, PLpgSQL_stmt_loop *stmt);
static void dump_while(StringInfo str, PLpgSQL_stmt_while *stmt);
static void dump_fori(StringInfo str, PLpgSQL_stmt_fori *stmt);
static void dump_fors(StringInfo str, PLpgSQL_stmt_fors *stmt);
static void dump_forc(StringInfo str, PLpgSQL_stmt_forc *stmt);
static void dump_foreach_a(StringInfo str, PLpgSQL_stmt_foreach_a *stmt);
static void dump_exit(StringInfo str, PLpgSQL_stmt_exit *stmt);
static void dump_return(StringInfo str, PLpgSQL_stmt_return *stmt);
static void dump_return_next(StringInfo str, PLpgSQL_stmt_return_next *stmt);
static void dump_return_query(StringInfo str, PLpgSQL_stmt_return_query *stmt);
static void dump_raise(StringInfo str, PLpgSQL_stmt_raise *stmt);
static void dump_raise_option(StringInfo str, PLpgSQL_raise_option *node);
static void dump_execsql(StringInfo str, PLpgSQL_stmt_execsql *stmt);
static void dump_dynexecute(StringInfo str, PLpgSQL_stmt_dynexecute *stmt);
static void dump_dynfors(StringInfo str, PLpgSQL_stmt_dynfors *stmt);
static void dump_getdiag(StringInfo str, PLpgSQL_stmt_getdiag *stmt);
static void dump_getdiag_item(StringInfo str, PLpgSQL_diag_item *node);
static void dump_open(StringInfo str, PLpgSQL_stmt_open *stmt);
static void dump_fetch(StringInfo str, PLpgSQL_stmt_fetch *stmt);
static void dump_close(StringInfo str, PLpgSQL_stmt_close *stmt);
static void dump_perform(StringInfo str, PLpgSQL_stmt_perform *stmt);
static void dump_expr(StringInfo str, PLpgSQL_expr *expr);
static void dump_function(StringInfo str, PLpgSQL_function *func);
static void dump_exception(StringInfo str, PLpgSQL_exception *node);
static void dump_condition(StringInfo str, PLpgSQL_condition *node);
static void dump_type(StringInfo str, PLpgSQL_type *node);

static void
dump_stmt(StringInfo str, PLpgSQL_stmt *node)
{
	appendStringInfoChar(str, '{');
	switch ((enum PLpgSQL_stmt_types) node->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			dump_block(str, (PLpgSQL_stmt_block *) node);
			break;
		case PLPGSQL_STMT_ASSIGN:
			dump_assign(str, (PLpgSQL_stmt_assign *) node);
			break;
		case PLPGSQL_STMT_IF:
			dump_if(str, (PLpgSQL_stmt_if *) node);
			break;
		case PLPGSQL_STMT_CASE:
			dump_case(str, (PLpgSQL_stmt_case *) node);
			break;
		case PLPGSQL_STMT_LOOP:
			dump_loop(str, (PLpgSQL_stmt_loop *) node);
			break;
		case PLPGSQL_STMT_WHILE:
			dump_while(str, (PLpgSQL_stmt_while *) node);
			break;
		case PLPGSQL_STMT_FORI:
			dump_fori(str, (PLpgSQL_stmt_fori *) node);
			break;
		case PLPGSQL_STMT_FORS:
			dump_fors(str, (PLpgSQL_stmt_fors *) node);
			break;
		case PLPGSQL_STMT_FORC:
			dump_forc(str, (PLpgSQL_stmt_forc *) node);
			break;
		case PLPGSQL_STMT_FOREACH_A:
			dump_foreach_a(str, (PLpgSQL_stmt_foreach_a *) node);
			break;
		case PLPGSQL_STMT_EXIT:
			dump_exit(str, (PLpgSQL_stmt_exit *) node);
			break;
		case PLPGSQL_STMT_RETURN:
			dump_return(str, (PLpgSQL_stmt_return *) node);
			break;
		case PLPGSQL_STMT_RETURN_NEXT:
			dump_return_next(str, (PLpgSQL_stmt_return_next *) node);
			break;
		case PLPGSQL_STMT_RETURN_QUERY:
			dump_return_query(str, (PLpgSQL_stmt_return_query *) node);
			break;
		case PLPGSQL_STMT_RAISE:
			dump_raise(str, (PLpgSQL_stmt_raise *) node);
			break;
		case PLPGSQL_STMT_EXECSQL:
			dump_execsql(str, (PLpgSQL_stmt_execsql *) node);
			break;
		case PLPGSQL_STMT_DYNEXECUTE:
			dump_dynexecute(str, (PLpgSQL_stmt_dynexecute *) node);
			break;
		case PLPGSQL_STMT_DYNFORS:
			dump_dynfors(str, (PLpgSQL_stmt_dynfors *) node);
			break;
		case PLPGSQL_STMT_GETDIAG:
			dump_getdiag(str, (PLpgSQL_stmt_getdiag *) node);
			break;
		case PLPGSQL_STMT_OPEN:
			dump_open(str, (PLpgSQL_stmt_open *) node);
			break;
		case PLPGSQL_STMT_FETCH:
			dump_fetch(str, (PLpgSQL_stmt_fetch *) node);
			break;
		case PLPGSQL_STMT_CLOSE:
			dump_close(str, (PLpgSQL_stmt_close *) node);
			break;
		case PLPGSQL_STMT_PERFORM:
			dump_perform(str, (PLpgSQL_stmt_perform *) node);
			break;
		default:
			elog(ERROR, "unrecognized cmd_type: %d", node->cmd_type);
			break;
	}
	removeTrailingDelimiter(str);
	appendStringInfoString(str, "}}, ");
}

static void
dump_block(StringInfo str, PLpgSQL_stmt_block *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_block");

	WRITE_INT_FIELD(lineno);
  	WRITE_STRING_FIELD(label);
	WRITE_STATEMENTS_FIELD(body);
	WRITE_OBJ_FIELD(exceptions, dump_exception_block);

	removeTrailingDelimiter(str);
}

static void
dump_exception_block(StringInfo str, PLpgSQL_exception_block *node)
{
	WRITE_NODE_TYPE("PLpgSQL_exception_block");

	WRITE_LIST_FIELD(exc_list, PLpgSQL_exception, dump_exception);
}

static void
dump_exception(StringInfo str, PLpgSQL_exception *node)
{
	PLpgSQL_condition *cond;

	WRITE_NODE_TYPE("PLpgSQL_exception");

	appendStringInfo(str, "\"conditions\": [");
	for (cond = node->conditions; cond; cond = cond->next)
	{
		appendStringInfoString(str, "{");
		dump_condition(str, cond);
		removeTrailingDelimiter(str);
		appendStringInfoString(str, "}},");
	}
	removeTrailingDelimiter(str);
	appendStringInfoString(str, "], ");

	WRITE_STATEMENTS_FIELD(action);
}

static void
dump_condition(StringInfo str, PLpgSQL_condition *node)
{
	WRITE_NODE_TYPE("PLpgSQL_condition");

	WRITE_STRING_FIELD(condname);
}

static void
dump_assign(StringInfo str, PLpgSQL_stmt_assign *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_assign");

	WRITE_INT_FIELD(lineno);
	WRITE_INT_FIELD(varno);
	WRITE_EXPR_FIELD(expr);
}

static void
dump_if(StringInfo str, PLpgSQL_stmt_if *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_if");

	WRITE_INT_FIELD(lineno);
	WRITE_EXPR_FIELD(cond);
	WRITE_STATEMENTS_FIELD(then_body);
	WRITE_LIST_FIELD(elsif_list, PLpgSQL_if_elsif, dump_if_elsif);
	WRITE_STATEMENTS_FIELD(else_body);
}

static void
dump_if_elsif(StringInfo str, PLpgSQL_if_elsif *node)
{
	WRITE_NODE_TYPE("PLpgSQL_if_elsif");

	WRITE_INT_FIELD(lineno);
	WRITE_EXPR_FIELD(cond);
	WRITE_STATEMENTS_FIELD(stmts);
}

static void
dump_case(StringInfo str, PLpgSQL_stmt_case *node)
{
	ListCell   *l;

	WRITE_NODE_TYPE("PLpgSQL_stmt_case");

	WRITE_INT_FIELD(lineno);
	WRITE_EXPR_FIELD(t_expr);
	WRITE_INT_FIELD(t_varno);
	WRITE_LIST_FIELD(case_when_list, PLpgSQL_case_when, dump_case_when);
	WRITE_BOOL_FIELD(have_else);
	WRITE_STATEMENTS_FIELD(else_stmts);
}

static void
dump_case_when(StringInfo str, PLpgSQL_case_when *node)
{
	WRITE_NODE_TYPE("PLpgSQL_case_when");

	WRITE_INT_FIELD(lineno);
	WRITE_EXPR_FIELD(expr);
	WRITE_STATEMENTS_FIELD(stmts);
}

static void
dump_loop(StringInfo str, PLpgSQL_stmt_loop *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_loop");

	WRITE_INT_FIELD(lineno);
	WRITE_STRING_FIELD(label);
	WRITE_STATEMENTS_FIELD(body);
}

static void
dump_while(StringInfo str, PLpgSQL_stmt_while *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_while");

	WRITE_INT_FIELD(lineno);
	WRITE_STRING_FIELD(label);
	WRITE_EXPR_FIELD(cond);
	WRITE_STATEMENTS_FIELD(body);
}

/* FOR statement with integer loopvar	*/
static void
dump_fori(StringInfo str, PLpgSQL_stmt_fori *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_fori");

	WRITE_INT_FIELD(lineno);
	WRITE_STRING_FIELD(label);
	WRITE_VAR_FIELD(var);
	WRITE_EXPR_FIELD(lower);
	WRITE_EXPR_FIELD(upper);
	WRITE_EXPR_FIELD(step);
	WRITE_BOOL_FIELD(reverse);
	WRITE_STATEMENTS_FIELD(body);
}

static void
dump_fors(StringInfo str, PLpgSQL_stmt_fors *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_fors");

	WRITE_INT_FIELD(lineno);
	WRITE_STRING_FIELD(label);
	WRITE_RECORD_FIELD(rec);
	WRITE_ROW_FIELD(row);
	WRITE_STATEMENTS_FIELD(body);
	WRITE_EXPR_FIELD(query);
}

static void
dump_forc(StringInfo str, PLpgSQL_stmt_forc *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_forc");

	WRITE_INT_FIELD(lineno);
	WRITE_STRING_FIELD(label);
	WRITE_RECORD_FIELD(rec);
	WRITE_ROW_FIELD(row);
	WRITE_STATEMENTS_FIELD(body);
	WRITE_INT_FIELD(curvar);
	WRITE_EXPR_FIELD(argquery);
}

static void
dump_foreach_a(StringInfo str, PLpgSQL_stmt_foreach_a *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_foreach_a");

	WRITE_INT_FIELD(lineno);
	WRITE_STRING_FIELD(label);
	WRITE_INT_FIELD(varno);
	WRITE_INT_FIELD(slice);
	WRITE_EXPR_FIELD(expr);
	WRITE_STATEMENTS_FIELD(body);
}

static void
dump_open(StringInfo str, PLpgSQL_stmt_open *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_open");

	WRITE_INT_FIELD(lineno);
	WRITE_INT_FIELD(curvar);
	WRITE_INT_FIELD(cursor_options);
	WRITE_ROW_FIELD(returntype);
	WRITE_EXPR_FIELD(argquery);
	WRITE_EXPR_FIELD(query);
	WRITE_EXPR_FIELD(dynquery);
	WRITE_LIST_FIELD(params, PLpgSQL_expr, dump_expr);
}

static void
dump_fetch(StringInfo str, PLpgSQL_stmt_fetch *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_fetch");

	WRITE_INT_FIELD(lineno);
	WRITE_RECORD_FIELD(rec);
	WRITE_ROW_FIELD(row);
	WRITE_INT_FIELD(curvar);
	WRITE_ENUM_FIELD(direction);
	WRITE_LONG_FIELD(how_many);
	WRITE_EXPR_FIELD(expr);
	WRITE_BOOL_FIELD(is_move);
	WRITE_BOOL_FIELD(returns_multiple_rows);
}

static void
dump_close(StringInfo str, PLpgSQL_stmt_close *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_close");

	WRITE_INT_FIELD(lineno);
	WRITE_INT_FIELD(curvar);
}

static void
dump_perform(StringInfo str, PLpgSQL_stmt_perform *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_perform");

	WRITE_INT_FIELD(lineno);
	WRITE_EXPR_FIELD(expr);
}

static void
dump_exit(StringInfo str, PLpgSQL_stmt_exit *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_exit");

	WRITE_INT_FIELD(lineno);
	WRITE_BOOL_FIELD(is_exit);
	WRITE_STRING_FIELD(label);
	WRITE_EXPR_FIELD(cond);
}

static void
dump_return(StringInfo str, PLpgSQL_stmt_return *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_return");

	WRITE_INT_FIELD(lineno);
	WRITE_EXPR_FIELD(expr);
	//WRITE_INT_FIELD(retvarno);
}

static void
dump_return_next(StringInfo str, PLpgSQL_stmt_return_next *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_return_next");

	WRITE_INT_FIELD(lineno);
	WRITE_EXPR_FIELD(expr);
	//WRITE_INT_FIELD(retvarno);
}

static void
dump_return_query(StringInfo str, PLpgSQL_stmt_return_query *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_return_query");

	WRITE_INT_FIELD(lineno);
	WRITE_EXPR_FIELD(query);
	WRITE_EXPR_FIELD(dynquery);
	WRITE_LIST_FIELD(params, PLpgSQL_expr, dump_expr);
}

static void
dump_raise(StringInfo str, PLpgSQL_stmt_raise *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_raise");

	WRITE_INT_FIELD(lineno);
	WRITE_INT_FIELD(elog_level);
	WRITE_STRING_FIELD(condname);
	WRITE_STRING_FIELD(message);
	WRITE_LIST_FIELD(params, PLpgSQL_expr, dump_expr);
	WRITE_LIST_FIELD(options, PLpgSQL_raise_option, dump_raise_option);
}

static void
dump_raise_option(StringInfo str, PLpgSQL_raise_option *node)
{
	WRITE_NODE_TYPE("PLpgSQL_raise_option");

	WRITE_ENUM_FIELD(opt_type);
	WRITE_EXPR_FIELD(expr);
}

static void
dump_execsql(StringInfo str, PLpgSQL_stmt_execsql *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_execsql");

	WRITE_INT_FIELD(lineno);
	WRITE_EXPR_FIELD(sqlstmt);
	//WRITE_BOOL_FIELD(mod_stmt); // This is only populated when executing the function
	WRITE_BOOL_FIELD(into);
	WRITE_BOOL_FIELD(strict);
	WRITE_RECORD_FIELD(rec);
	WRITE_ROW_FIELD(row);
}

static void
dump_dynexecute(StringInfo str, PLpgSQL_stmt_dynexecute *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_dynexecute");

	WRITE_INT_FIELD(lineno);
	WRITE_EXPR_FIELD(query);
	WRITE_BOOL_FIELD(into);
	WRITE_BOOL_FIELD(strict);
	WRITE_RECORD_FIELD(rec);
	WRITE_ROW_FIELD(row);
	WRITE_LIST_FIELD(params, PLpgSQL_expr, dump_expr);
}

static void
dump_dynfors(StringInfo str, PLpgSQL_stmt_dynfors *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_dynfors");

	WRITE_INT_FIELD(lineno);
	WRITE_STRING_FIELD(label);
	WRITE_RECORD_FIELD(rec);
	WRITE_ROW_FIELD(row);
	WRITE_STATEMENTS_FIELD(body);
	WRITE_EXPR_FIELD(query);
	WRITE_LIST_FIELD(params, PLpgSQL_expr, dump_expr);
}

static void
dump_getdiag(StringInfo str, PLpgSQL_stmt_getdiag *node)
{
	WRITE_NODE_TYPE("PLpgSQL_stmt_getdiag");

	WRITE_INT_FIELD(lineno);
	WRITE_BOOL_FIELD(is_stacked);
	WRITE_LIST_FIELD(diag_items, PLpgSQL_diag_item, dump_getdiag_item);
}

static void
dump_getdiag_item(StringInfo str, PLpgSQL_diag_item *node)
{
	WRITE_NODE_TYPE("PLpgSQL_diag_item");

	WRITE_STRING_VALUE(kind, plpgsql_getdiag_kindname(node->kind));
	WRITE_INT_FIELD(target);
}

static void
dump_expr(StringInfo str, PLpgSQL_expr *node)
{
	WRITE_NODE_TYPE("PLpgSQL_expr");

	WRITE_STRING_FIELD(query);
}

static void
dump_function(StringInfo str, PLpgSQL_function *node)
{
	int			i;
	PLpgSQL_datum *d;

	WRITE_NODE_TYPE("PLpgSQL_function");

	appendStringInfoString(str, "\"datums\": ");
	appendStringInfoChar(str, '[');
	for (i = 0; i < node->ndatums; i++)
	{
	  appendStringInfoChar(str, '{');
		d = node->datums[i];

		switch (d->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
				dump_var(str, (PLpgSQL_var *) d);
				break;
			case PLPGSQL_DTYPE_ROW:
				dump_row(str, (PLpgSQL_row *) d);
				break;
			case PLPGSQL_DTYPE_REC:
				dump_record(str, (PLpgSQL_rec *) d);
				break;
			case PLPGSQL_DTYPE_RECFIELD:
				dump_record_field(str, (PLpgSQL_recfield *) d);
				break;
			case PLPGSQL_DTYPE_ARRAYELEM:
				dump_array_elem(str, (PLpgSQL_arrayelem *) d);
				break;
			default:
				elog(WARNING, "could not dump unrecognized dtype: %d",
					 (int) d->dtype);
		}
		removeTrailingDelimiter(str);
		appendStringInfoString(str, "}}, ");
	}
	removeTrailingDelimiter(str);
	appendStringInfoString(str, "], ");

	WRITE_BLOCK_FIELD(action);
}

static void
dump_var(StringInfo str, PLpgSQL_var *node)
{
	WRITE_NODE_TYPE("PLpgSQL_var");

	WRITE_STRING_FIELD(refname);
	WRITE_INT_FIELD(lineno);
	WRITE_OBJ_FIELD(datatype, dump_type);
	WRITE_BOOL_FIELD(isconst);
	WRITE_BOOL_FIELD(notnull);
	WRITE_EXPR_FIELD(default_val);
	WRITE_EXPR_FIELD(cursor_explicit_expr);
	WRITE_INT_FIELD(cursor_explicit_argrow);
	WRITE_INT_FIELD(cursor_options);
}

static void
dump_type(StringInfo str, PLpgSQL_type *node)
{
	WRITE_NODE_TYPE("PLpgSQL_type");

	WRITE_STRING_FIELD(typname);
}

static void
dump_row(StringInfo str, PLpgSQL_row *node)
{
	int i = 0;

	WRITE_NODE_TYPE("PLpgSQL_row");

	WRITE_STRING_FIELD(refname);
	WRITE_INT_FIELD(lineno);

	appendStringInfoString(str, "\"fields\": ");
	appendStringInfoChar(str, '[');

	for (i = 0; i < node->nfields; i++)
	{
		if (node->fieldnames[i]) {
		  appendStringInfoChar(str, '{');
			WRITE_STRING_VALUE(name, node->fieldnames[i]);
			WRITE_INT_VALUE(varno, node->varnos[i]);
			removeTrailingDelimiter(str);
			appendStringInfoString(str, "}, ");
		} else {
			appendStringInfoString(str, "null, ");
		}
	}
	removeTrailingDelimiter(str);

	appendStringInfoString(str, "], ");
}

static void
dump_record(StringInfo str, PLpgSQL_rec *node) {
	WRITE_NODE_TYPE("PLpgSQL_rec");

	WRITE_STRING_FIELD(refname);
	WRITE_INT_FIELD(lineno);
}

static void
dump_record_field(StringInfo str, PLpgSQL_recfield *node) {
	WRITE_NODE_TYPE("PLpgSQL_recfield");

	WRITE_STRING_FIELD(fieldname);
	WRITE_INT_FIELD(recparentno);
}

static void
dump_array_elem(StringInfo str, PLpgSQL_arrayelem *node) {
	WRITE_NODE_TYPE("PLpgSQL_arrayelem");

	WRITE_EXPR_FIELD(subscript);
	WRITE_INT_FIELD(arrayparentno);
}

char *
plpgsqlToJSON(PLpgSQL_function *func)
{
	StringInfoData str;

	initStringInfo(&str);

	appendStringInfoChar(&str, '{');

  dump_function(&str, func);

	removeTrailingDelimiter(&str);
  appendStringInfoString(&str, "}}");

  return str.data;
}
