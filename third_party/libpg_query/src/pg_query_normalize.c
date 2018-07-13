#include "pg_query.h"
#include "pg_query_internal.h"

#include "parser/parser.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "mb/pg_wchar.h"
#include "nodes/nodeFuncs.h"

/*
 * Struct for tracking locations/lengths of constants during normalization
 */
typedef struct pgssLocationLen
{
	int			location;		/* start offset in query text */
	int			length;			/* length in bytes, or -1 to ignore */
} pgssLocationLen;

/*
 * Working state for constant tree walker
 */
typedef struct pgssConstLocations
{
	/* Array of locations of constants that should be removed */
	pgssLocationLen *clocations;

	/* Allocated length of clocations array */
	int			clocations_buf_size;

	/* Current number of valid entries in clocations array */
	int			clocations_count;
} pgssConstLocations;

/*
 * comp_location: comparator for qsorting pgssLocationLen structs by location
 */
static int
comp_location(const void *a, const void *b)
{
	int			l = ((const pgssLocationLen *) a)->location;
	int			r = ((const pgssLocationLen *) b)->location;

	if (l < r)
		return -1;
	else if (l > r)
		return +1;
	else
		return 0;
}

/*
 * Given a valid SQL string and an array of constant-location records,
 * fill in the textual lengths of those constants.
 *
 * The constants may use any allowed constant syntax, such as float literals,
 * bit-strings, single-quoted strings and dollar-quoted strings.  This is
 * accomplished by using the public API for the core scanner.
 *
 * It is the caller's job to ensure that the string is a valid SQL statement
 * with constants at the indicated locations.  Since in practice the string
 * has already been parsed, and the locations that the caller provides will
 * have originated from within the authoritative parser, this should not be
 * a problem.
 *
 * Duplicate constant pointers are possible, and will have their lengths
 * marked as '-1', so that they are later ignored.  (Actually, we assume the
 * lengths were initialized as -1 to start with, and don't change them here.)
 *
 * N.B. There is an assumption that a '-' character at a Const location begins
 * a negative numeric constant.  This precludes there ever being another
 * reason for a constant to start with a '-'.
 */
static void
fill_in_constant_lengths(pgssConstLocations *jstate, const char *query)
{
	pgssLocationLen *locs;
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE		yylloc;
	int			last_loc = -1;
	int			i;

	/*
	 * Sort the records by location so that we can process them in order while
	 * scanning the query text.
	 */
	if (jstate->clocations_count > 1)
		qsort(jstate->clocations, jstate->clocations_count,
			  sizeof(pgssLocationLen), comp_location);
	locs = jstate->clocations;

	/* initialize the flex scanner --- should match raw_parser() */
	yyscanner = scanner_init(query,
							 &yyextra,
							 ScanKeywords,
							 NumScanKeywords);

	/* Search for each constant, in sequence */
	for (i = 0; i < jstate->clocations_count; i++)
	{
		int			loc = locs[i].location;
		int			tok;

		Assert(loc >= 0);

		if (loc <= last_loc)
			continue;			/* Duplicate constant, ignore */

		/* Lex tokens until we find the desired constant */
		for (;;)
		{
			tok = core_yylex(&yylval, &yylloc, yyscanner);

			/* We should not hit end-of-string, but if we do, behave sanely */
			if (tok == 0)
				break;			/* out of inner for-loop */

			/*
			 * We should find the token position exactly, but if we somehow
			 * run past it, work with that.
			 */
			if (yylloc >= loc)
			{
				if (query[loc] == '-')
				{
					/*
					 * It's a negative value - this is the one and only case
					 * where we replace more than a single token.
					 *
					 * Do not compensate for the core system's special-case
					 * adjustment of location to that of the leading '-'
					 * operator in the event of a negative constant.  It is
					 * also useful for our purposes to start from the minus
					 * symbol.  In this way, queries like "select * from foo
					 * where bar = 1" and "select * from foo where bar = -2"
					 * will have identical normalized query strings.
					 */
					tok = core_yylex(&yylval, &yylloc, yyscanner);
					if (tok == 0)
						break;	/* out of inner for-loop */
				}

				/*
				 * We now rely on the assumption that flex has placed a zero
				 * byte after the text of the current token in scanbuf.
				 */
				locs[i].length = (int) strlen(yyextra.scanbuf + loc);

				/* Quoted string with Unicode escapes
				 *
				 * The lexer consumes trailing whitespace in order to find UESCAPE, but if there
				 * is no UESCAPE it has still consumed it - don't include it in constant length.
				 */
				if (locs[i].length > 4 && /* U&'' */
					(yyextra.scanbuf[loc] == 'u' || yyextra.scanbuf[loc] == 'U') &&
					 yyextra.scanbuf[loc + 1] == '&' && yyextra.scanbuf[loc + 2] == '\'')
				{
					int j = locs[i].length - 1; /* Skip the \0 */
					for (; j >= 0 && scanner_isspace(yyextra.scanbuf[loc + j]); j--) {}
					locs[i].length = j + 1; /* Count the \0 */
				}

				break;			/* out of inner for-loop */
			}
		}

		/* If we hit end-of-string, give up, leaving remaining lengths -1 */
		if (tok == 0)
			break;

		last_loc = loc;
	}

	scanner_finish(yyscanner);
}

/*
 * Generate a normalized version of the query string that will be used to
 * represent all similar queries.
 *
 * Note that the normalized representation may well vary depending on
 * just which "equivalent" query is used to create the hashtable entry.
 * We assume this is OK.
 *
 * *query_len_p contains the input string length, and is updated with
 * the result string length (which cannot be longer) on exit.
 *
 * Returns a palloc'd string.
 */
static char *
generate_normalized_query(pgssConstLocations *jstate, const char *query,
						  int *query_len_p, int encoding)
{
	char	   *norm_query;
	int			query_len = *query_len_p;
	int			i,
				len_to_wrt,		/* Length (in bytes) to write */
				quer_loc = 0,	/* Source query byte location */
				n_quer_loc = 0, /* Normalized query byte location */
				last_off = 0,	/* Offset from start for previous tok */
				last_tok_len = 0;		/* Length (in bytes) of that tok */

	/*
	 * Get constants' lengths (core system only gives us locations).  Note
	 * this also ensures the items are sorted by location.
	 */
	fill_in_constant_lengths(jstate, query);

	/* Allocate result buffer */
	norm_query = palloc(query_len + 1);

	for (i = 0; i < jstate->clocations_count; i++)
	{
		int			off,		/* Offset from start for cur tok */
					tok_len;	/* Length (in bytes) of that tok */

		off = jstate->clocations[i].location;
		tok_len = jstate->clocations[i].length;

		if (tok_len < 0)
			continue;			/* ignore any duplicates */

		/* Copy next chunk (what precedes the next constant) */
		len_to_wrt = off - last_off;
		len_to_wrt -= last_tok_len;

		Assert(len_to_wrt >= 0);
		memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
		n_quer_loc += len_to_wrt;

		/* And insert a '?' in place of the constant token */
		norm_query[n_quer_loc++] = '?';

		quer_loc = off + tok_len;
		last_off = off;
		last_tok_len = tok_len;
	}

	/*
	 * We've copied up until the last ignorable constant.  Copy over the
	 * remaining bytes of the original query string.
	 */
	len_to_wrt = query_len - quer_loc;

	Assert(len_to_wrt >= 0);
	memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
	n_quer_loc += len_to_wrt;

	Assert(n_quer_loc <= query_len);
	norm_query[n_quer_loc] = '\0';

	*query_len_p = n_quer_loc;
	return norm_query;
}

static bool const_record_walker(Node *node, pgssConstLocations *jstate)
{
	bool result;

	if (node == NULL) return false;

	if ((IsA(node, A_Const) && ((A_Const *) node)->location >= 0) || (IsA(node, DefElem) && ((DefElem *) node)->location >= 0))
	{
		/* enlarge array if needed */
		if (jstate->clocations_count >= jstate->clocations_buf_size)
		{
			jstate->clocations_buf_size *= 2;
			jstate->clocations = (pgssLocationLen *)
				repalloc(jstate->clocations,
						 jstate->clocations_buf_size *
						 sizeof(pgssLocationLen));
		}
		jstate->clocations[jstate->clocations_count].location = IsA(node, DefElem) ? ((DefElem *) node)->location : ((A_Const *) node)->location;
		/* initialize lengths to -1 to simplify fill_in_constant_lengths */
		jstate->clocations[jstate->clocations_count].length = -1;
		jstate->clocations_count++;
	}
	else if (IsA(node, VariableSetStmt))
	{
		return const_record_walker((Node *) ((VariableSetStmt *) node)->args, jstate);
	}
	else if (IsA(node, CopyStmt))
	{
		return const_record_walker((Node *) ((CopyStmt *) node)->query, jstate);
	}
	else if (IsA(node, ExplainStmt))
	{
		return const_record_walker((Node *) ((ExplainStmt *) node)->query, jstate);
	}
	else if (IsA(node, AlterRoleStmt))
	{
		return const_record_walker((Node *) ((AlterRoleStmt *) node)->options, jstate);
	}

	PG_TRY();
	{
		result = raw_expression_tree_walker(node, const_record_walker, (void*) jstate);
	}
	PG_CATCH();
	{
		FlushErrorState();
		result = false;
	}
	PG_END_TRY();

	return result;
}

PgQueryNormalizeResult pg_query_normalize(const char* input)
{
	MemoryContext ctx = NULL;
	PgQueryNormalizeResult result = {0};

	ctx = pg_query_enter_memory_context("pg_query_normalize");

	PG_TRY();
	{
		List *tree;
		pgssConstLocations jstate;
		int query_len;

		/* Parse query */
		tree = raw_parser(input);

		/* Set up workspace for constant recording */
		jstate.clocations_buf_size = 32;
		jstate.clocations = (pgssLocationLen *)
			palloc(jstate.clocations_buf_size * sizeof(pgssLocationLen));
		jstate.clocations_count = 0;

		/* Walk tree and record const locations */
		const_record_walker((Node *) tree, &jstate);

		/* Normalize query */
		query_len = (int) strlen(input);
		result.normalized_query = strdup(generate_normalized_query(&jstate, input, &query_len, PG_UTF8));
	}
	PG_CATCH();
	{
		ErrorData* error_data;
		PgQueryError* error;

		MemoryContextSwitchTo(ctx);
		error_data = CopyErrorData();

		error = malloc(sizeof(PgQueryError));
		error->message   = strdup(error_data->message);
		error->filename  = strdup(error_data->filename);
		error->lineno    = error_data->lineno;
		error->cursorpos = error_data->cursorpos;

		result.error = error;
		FlushErrorState();
	}
	PG_END_TRY();

	pg_query_exit_memory_context(ctx);

	return result;
}

void pg_query_free_normalize_result(PgQueryNormalizeResult result)
{
  if (result.error) {
    free(result.error->message);
    free(result.error->filename);
    free(result.error);
  }

  free(result.normalized_query);
}
