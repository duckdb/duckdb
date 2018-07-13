#include "pg_query.h"
#include "pg_query_internal.h"
#include <mb/pg_wchar.h>
#include <signal.h>

const char* progname = "pg_query";

__thread sig_atomic_t pg_query_initialized = 0;

void pg_query_init(void)
{
	if (pg_query_initialized != 0) return;
	pg_query_initialized = 1;

	MemoryContextInit();
	SetDatabaseEncoding(PG_UTF8);
}

MemoryContext pg_query_enter_memory_context(const char* ctx_name)
{
	MemoryContext ctx = NULL;

	pg_query_init();

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
	// Return to previous PostgreSQL memory context
	MemoryContextSwitchTo(TopMemoryContext);

	MemoryContextDelete(ctx);

    MemoryContext error_ctx = ErrorContext;
    MemoryContext top_ctx = TopMemoryContext;

//    TopMemoryContext = NULL;
//    ErrorContext = NULL;
//
//	if (error_ctx != NULL)
//    	MemoryContextDelete(error_ctx);
//    if (top_ctx != NULL)
//		MemoryContextDelete(top_ctx);


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
