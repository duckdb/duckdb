#include "pg_query.h"
#include "pg_query_internal.h"
#include "pg_query_json.h"

#include "parser/parser.h"
#include "parser/scanner.h"
#include "parser/scansup.h"

#include <unistd.h>
#include <fcntl.h>

PgQueryInternalParsetreeAndError pg_query_raw_parse(const char* input)
{
	PgQueryInternalParsetreeAndError result = {0};
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
		result.tree = raw_parser(input);

#ifndef DEBUG
		// Save stderr for result
		read(stderr_pipe[0], stderr_buffer, STDERR_BUFFER_LEN);
#endif

		result.stderr_buffer = strdup(stderr_buffer);
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
		error->context   = NULL;
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

// This should be called before pg_query_parse
void* pg_query_parse_init() {
  return pg_query_enter_memory_context("pg_query_parse");
}

PgQueryInternalParsetreeAndError pg_query_parse(const char* input)
{
	return pg_query_raw_parse(input);
}

// DEBUG: print the json representation of parse tree
void print_pg_parse_tree(List* tree) {
  char* tree_json = pg_query_nodes_to_json(tree);
  printf("%s\n", tree_json);
  pfree(tree_json);
}

// This should be called after pg_query_parse
void pg_query_parse_finish(void* ctx) {
  pg_query_exit_memory_context((MemoryContext)ctx);
}

void pg_query_free_parse_result(PgQueryInternalParsetreeAndError result)
{
  if (result.error) {
		pg_query_free_error(result.error);
  }
  free(result.stderr_buffer);
}
