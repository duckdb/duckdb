#include "pg_query.h"
#include "pg_query_internal.h"
#include "pg_query_json_plpgsql.h"

#include <assert.h>

#include <catalog/pg_type.h>
#include <catalog/pg_proc_fn.h>
#include <nodes/parsenodes.h>
#include <nodes/nodeFuncs.h>

#define _GNU_SOURCE // Necessary to get asprintf (which is a GNU extension)
#include <stdio.h>

typedef struct {
  PLpgSQL_function *func;
  PgQueryError* error;
} PgQueryInternalPlpgsqlFuncAndError;

static PgQueryInternalPlpgsqlFuncAndError pg_query_raw_parse_plpgsql(CreateFunctionStmt* stmt);

static int	datums_alloc;
extern __thread int			plpgsql_nDatums;
extern __thread PLpgSQL_datum **plpgsql_Datums;
static int	datums_last = 0;

static void add_dummy_return(PLpgSQL_function *function)
{
	/*
	 * If the outer block has an EXCEPTION clause, we need to make a new outer
	 * block, since the added RETURN shouldn't act like it is inside the
	 * EXCEPTION clause.
	 */
	if (function->action->exceptions != NULL)
	{
		PLpgSQL_stmt_block *new;

		new = palloc0(sizeof(PLpgSQL_stmt_block));
		new->cmd_type = PLPGSQL_STMT_BLOCK;
		new->body = list_make1(function->action);

		function->action = new;
	}
	if (function->action->body == NIL ||
		((PLpgSQL_stmt *) llast(function->action->body))->cmd_type != PLPGSQL_STMT_RETURN)
	{
		PLpgSQL_stmt_return *new;

		new = palloc0(sizeof(PLpgSQL_stmt_return));
		new->cmd_type = PLPGSQL_STMT_RETURN;
		new->expr = NULL;
		new->retvarno = function->out_param_varno;

		function->action->body = lappend(function->action->body, new);
	}
}

static void plpgsql_compile_error_callback(void *arg)
{
	if (arg)
	{
		/*
		 * Try to convert syntax error position to reference text of original
		 * CREATE FUNCTION or DO command.
		 */
		if (function_parse_error_transpose((const char *) arg))
			return;

		/*
		 * Done if a syntax error position was reported; otherwise we have to
		 * fall back to a "near line N" report.
		 */
	}

	if (plpgsql_error_funcname)
		errcontext("compilation of PL/pgSQL function \"%s\" near line %d",
				   plpgsql_error_funcname, plpgsql_latest_lineno());
}

static PLpgSQL_function *compile_create_function_stmt(CreateFunctionStmt* stmt)
{
	char *func_name;
    char *proc_source;
	PLpgSQL_function *function;
	ErrorContextCallback plerrcontext;
	PLpgSQL_variable *var;
	int			parse_rc;
	MemoryContext func_cxt;
	int			i;
	PLpgSQL_rec *rec;
    const ListCell *lc, *lc2, *lc3;
    bool is_trigger = false;
    bool is_setof = false;

    assert(IsA(stmt, CreateFunctionStmt));

    func_name = strVal(linitial(stmt->funcname));

    foreach(lc, stmt->options)
    {
      DefElem* elem = (DefElem*) lfirst(lc);

      if (strcmp(elem->defname, "as") == 0) {
          const ListCell *lc2;

          assert(IsA(elem->arg, List));

          foreach(lc2, (List*) elem->arg)
          {
              proc_source = strVal(lfirst(lc2));
          }
      }
    }

    if (stmt->returnType != NULL) {
        foreach(lc3, stmt->returnType->names)
        {
            char* val = strVal(lfirst(lc3));

            if (strcmp(val, "trigger") == 0) {
                is_trigger = true;
            }
        }

        if (stmt->returnType->setof) {
            is_setof = true;
        }
    }

	/*
	 * Setup the scanner input and error info.  We assume that this function
	 * cannot be invoked recursively, so there's no need to save and restore
	 * the static variables used here.
	 */
	plpgsql_scanner_init(proc_source);

	plpgsql_error_funcname = func_name;

	/*
	 * Setup error traceback support for ereport()
	 */
	plerrcontext.callback = plpgsql_compile_error_callback;
	plerrcontext.arg = proc_source;
	plerrcontext.previous = error_context_stack;
	error_context_stack = &plerrcontext;

	/* Do extra syntax checking if check_function_bodies is on */
	plpgsql_check_syntax = true;

	/* Function struct does not live past current statement */
	function = (PLpgSQL_function *) palloc0(sizeof(PLpgSQL_function));

	plpgsql_curr_compile = function;

	/*
	 * All the rest of the compile-time storage (e.g. parse tree) is kept in
	 * its own memory context, so it can be reclaimed easily.
	 */
	func_cxt = AllocSetContextCreate(CurrentMemoryContext,
									 "PL/pgSQL function context",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
	compile_tmp_cxt = MemoryContextSwitchTo(func_cxt);

	function->fn_signature = pstrdup(func_name);
	function->fn_is_trigger = PLPGSQL_NOT_TRIGGER;
	function->fn_input_collation = InvalidOid;
	function->fn_cxt = func_cxt;
	function->out_param_varno = -1;		/* set up for no OUT param */
	function->resolve_option = plpgsql_variable_conflict;
	function->print_strict_params = plpgsql_print_strict_params;

	/*
	 * don't do extra validation for inline code as we don't want to add spam
	 * at runtime
	 */
	function->extra_warnings = 0;
	function->extra_errors = 0;

	plpgsql_ns_init();
	plpgsql_ns_push(func_name);
	plpgsql_DumpExecTree = false;

	datums_alloc = 128;
	plpgsql_nDatums = 0;
	plpgsql_Datums = palloc(sizeof(PLpgSQL_datum *) * datums_alloc);
	datums_last = 0;

	/* Set up as though in a function returning VOID */
	function->fn_rettype = VOIDOID;
	function->fn_retset = is_setof;
	function->fn_retistuple = false;
	/* a bit of hardwired knowledge about type VOID here */
	function->fn_retbyval = true;
	function->fn_rettyplen = sizeof(int32);

	/*
	 * Remember if function is STABLE/IMMUTABLE.  XXX would it be better to
	 * set this TRUE inside a read-only transaction?  Not clear.
	 */
	function->fn_readonly = false;

	/*
	 * Create the magic FOUND variable.
	 */
	var = plpgsql_build_variable("found", 0,
								 plpgsql_build_datatype(BOOLOID,
														-1,
														InvalidOid),
								 true);
	function->found_varno = var->dno;

    if (is_trigger) {
    	/* Add the record for referencing NEW */
    	rec = plpgsql_build_record("new", 0, true);
    	function->new_varno = rec->dno;

    	/* Add the record for referencing OLD */
    	rec = plpgsql_build_record("old", 0, true);
    	function->old_varno = rec->dno;
    }

	/*
	 * Now parse the function's text
	 */
	parse_rc = plpgsql_yyparse();
	if (parse_rc != 0)
		elog(ERROR, "plpgsql parser returned %d", parse_rc);
	function->action = plpgsql_parse_result;

	plpgsql_scanner_finish();

	/*
	 * If it returns VOID (always true at the moment), we allow control to
	 * fall off the end without an explicit RETURN statement.
	 */
	if (function->fn_rettype == VOIDOID)
		add_dummy_return(function);

	/*
	 * Complete the function's info
	 */
	function->fn_nargs = 0;
	function->ndatums = plpgsql_nDatums;
	function->datums = palloc(sizeof(PLpgSQL_datum *) * plpgsql_nDatums);
	for (i = 0; i < plpgsql_nDatums; i++)
		function->datums[i] = plpgsql_Datums[i];

	/*
	 * Pop the error context stack
	 */
	error_context_stack = plerrcontext.previous;
	plpgsql_error_funcname = NULL;

	plpgsql_check_syntax = false;

	MemoryContextSwitchTo(compile_tmp_cxt);
	compile_tmp_cxt = NULL;
	return function;
}

PgQueryInternalPlpgsqlFuncAndError pg_query_raw_parse_plpgsql(CreateFunctionStmt* stmt)
{
	PgQueryInternalPlpgsqlFuncAndError result = {0};
	MemoryContext parse_context = CurrentMemoryContext;

	char stderr_buffer[STDERR_BUFFER_LEN + 1] = {0};
#ifndef DEBUG
	int stderr_global;
	int stderr_pipe[2];
#endif

#ifndef DEBUG
	// Setup pipe for stderr redirection
	if (pipe(stderr_pipe) != 0) {
		PgQueryError* error = malloc(sizeof(PgQueryError));

		error->message = strdup("Failed to open pipe, too many open file descriptors")

		result.error = error;

		return result;
	}

	fcntl(stderr_pipe[0], F_SETFL, fcntl(stderr_pipe[0], F_GETFL) | O_NONBLOCK);

	// Redirect stderr to the pipe
	stderr_global = dup(STDERR_FILENO);
	dup2(stderr_pipe[1], STDERR_FILENO);
	close(stderr_pipe[1]);
#endif

	PG_TRY();
	{
    result.func = compile_create_function_stmt(stmt);

#ifndef DEBUG
		// Save stderr for result
		read(stderr_pipe[0], stderr_buffer, STDERR_BUFFER_LEN);
#endif

        if (strlen(stderr_buffer) > 0) {
            PgQueryError* error = malloc(sizeof(PgQueryError));
    		error->message = strdup(stderr_buffer);
            error->filename = "";
    		error->funcname = "";
    		error->context  = "";
            result.error = error;
        }
	}
	PG_CATCH();
	{
		ErrorData* error_data;
		PgQueryError* error;

		MemoryContextSwitchTo(parse_context);
		error_data = CopyErrorData();

		// Note: This is intentionally malloc so exiting the memory context doesn't free this
		error = malloc(sizeof(PgQueryError));
		error->message   = strdup(error_data->message);
		error->filename  = strdup(error_data->filename);
		error->funcname  = strdup(error_data->funcname);
		error->context   = strdup(error_data->context);
		error->lineno    = error_data->lineno;
		error->cursorpos = error_data->cursorpos;

		result.error = error;
		FlushErrorState();
	}
	PG_END_TRY();

#ifndef DEBUG
	// Restore stderr, close pipe
	dup2(stderr_global, STDERR_FILENO);
	close(stderr_pipe[0]);
	close(stderr_global);
#endif

	return result;
}

typedef struct createFunctionStmts
{
	CreateFunctionStmt **stmts;
    int stmts_buf_size;
    int stmts_count;
} createFunctionStmts;

static bool create_function_stmts_walker(Node *node, createFunctionStmts *state)
{
	bool result;

	if (node == NULL) return false;

	if (IsA(node, CreateFunctionStmt))
	{
		if (state->stmts_count >= state->stmts_buf_size)
		{
			state->stmts_buf_size *= 2;
			state->stmts = (CreateFunctionStmt**) repalloc(state->stmts, state->stmts_buf_size * sizeof(CreateFunctionStmt*));
		}
		state->stmts[state->stmts_count] = (CreateFunctionStmt *) node;
		state->stmts_count++;
	}

	PG_TRY();
	{
		result = raw_expression_tree_walker(node, create_function_stmts_walker, (void*) state);
	}
	PG_CATCH();
	{
		FlushErrorState();
		result = false;
	}
	PG_END_TRY();

	return result;
}

PgQueryPlpgsqlParseResult pg_query_parse_plpgsql(const char* input)
{
	MemoryContext ctx = NULL;
	PgQueryPlpgsqlParseResult result = {0};
    PgQueryInternalParsetreeAndError parse_result;
    createFunctionStmts statements;
    size_t i;

	ctx = pg_query_enter_memory_context("pg_query_parse_plpgsql");

    parse_result = pg_query_raw_parse(input);
    result.error = parse_result.error;
    if (result.error != NULL) {
        pg_query_exit_memory_context(ctx);
        return result;
    }

    statements.stmts_buf_size = 100;
    statements.stmts = (CreateFunctionStmt**) palloc(statements.stmts_buf_size * sizeof(CreateFunctionStmt*));
    statements.stmts_count = 0;

    create_function_stmts_walker((Node*) parse_result.tree, &statements);

    result.plpgsql_funcs = strdup("[\n");

    for (i = 0; i < statements.stmts_count; i++) {
	    PgQueryInternalPlpgsqlFuncAndError func_and_error;

    	func_and_error = pg_query_raw_parse_plpgsql(statements.stmts[i]);

        // These are all malloc-ed and will survive exiting the memory context, the caller is responsible to free them now
        result.error = func_and_error.error;

        if (result.error != NULL) {
            pg_query_exit_memory_context(ctx);
        	return result;
        }

    	if (func_and_error.func != NULL) {
    		char *func_json;
            char *new_out;

    		func_json = plpgsqlToJSON(func_and_error.func);
            plpgsql_free_function_memory(func_and_error.func);

            asprintf(&new_out, "%s%s,\n", result.plpgsql_funcs, func_json);
            free(result.plpgsql_funcs);
            result.plpgsql_funcs = new_out;

    		pfree(func_json);
    	}
    }

    result.plpgsql_funcs[strlen(result.plpgsql_funcs) - 2] = '\n';
    result.plpgsql_funcs[strlen(result.plpgsql_funcs) - 1] = ']';

	pg_query_exit_memory_context(ctx);

	return result;
}

void pg_query_free_plpgsql_parse_result(PgQueryPlpgsqlParseResult result)
{
  if (result.error) {
		pg_query_free_error(result.error);
  }

  free(result.plpgsql_funcs);
}
