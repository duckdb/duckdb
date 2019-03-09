#include "pg_query.h"
#include "pg_query_internal.h"
#include <mb/pg_wchar.h>
#include <signal.h>

const char* progname = "pg_query";

MemoryContext pg_query_enter_memory_context(const char* ctx_name)
{
	MemoryContext ctx = NULL;

	// call MemoryContextInit to allocate the TopMemoryContext and ErrorContext
	MemoryContextInit();
	SetDatabaseEncoding(PG_UTF8);

	ctx = AllocSetContextCreate(TopMemoryContext,
								ctx_name,
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(ctx);

	return ctx;
}

void pg_query_exit_memory_context(MemoryContext ctx)
{
	// delete all memory contexts, including the TopMemoryContext and ErrorContext
	MemoryContextDelete(ctx);
	MemoryContextDelete(ErrorContext);
	MemoryContextDelete(TopMemoryContext);

	// reset all flags
	TopMemoryContext = NULL;
	ErrorContext = NULL;

	MemoryContextSwitchTo(NULL);
}

void pg_query_free_error(PgQueryError *error)
{
	free(error->message);
	free(error->funcname);
	free(error->filename);

	if (error->context) {
		free(error->context);
	}

	free(error);
}
