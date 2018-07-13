/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - plpgsql_scanner_init
 * - plpgsql_IdentifierLookup
 * - yyscanner
 * - core_yy
 * - reserved_keywords
 * - num_reserved_keywords
 * - scanorig
 * - plpgsql_yytoken
 * - num_pushbacks
 * - location_lineno_init
 * - cur_line_start
 * - cur_line_num
 * - cur_line_end
 * - plpgsql_yylex
 * - internal_yylex
 * - pushback_token
 * - pushback_auxdata
 * - push_back_token
 * - unreserved_keywords
 * - num_unreserved_keywords
 * - plpgsql_yyleng
 * - plpgsql_location_to_lineno
 * - plpgsql_scanner_errposition
 * - plpgsql_yyerror
 * - plpgsql_push_back_token
 * - plpgsql_token_is_unreserved_keyword
 * - plpgsql_append_source_text
 * - plpgsql_peek2
 * - plpgsql_peek
 * - plpgsql_scanner_finish
 * - plpgsql_latest_lineno
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * pl_scanner.c
 *	  lexical scanning for PL/pgSQL
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_scanner.c
 *
 *-------------------------------------------------------------------------
 */
#include "plpgsql.h"

#include "mb/pg_wchar.h"
#include "parser/scanner.h"

#include "pl_gram.h"			/* must be after parser/scanner.h */

#define PG_KEYWORD(a,b,c) {a,b,c},


/* Klugy flag to tell scanner how to look up identifiers */
__thread IdentifierLookup plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;


/*
 * A word about keywords:
 *
 * We keep reserved and unreserved keywords in separate arrays.  The
 * reserved keywords are passed to the core scanner, so they will be
 * recognized before (and instead of) any variable name.  Unreserved words
 * are checked for separately, usually after determining that the identifier
 * isn't a known variable name.  If plpgsql_IdentifierLookup is DECLARE then
 * no variable names will be recognized, so the unreserved words always work.
 * (Note in particular that this helps us avoid reserving keywords that are
 * only needed in DECLARE sections.)
 *
 * In certain contexts it is desirable to prefer recognizing an unreserved
 * keyword over recognizing a variable name.  In particular, at the start
 * of a statement we should prefer unreserved keywords unless the statement
 * looks like an assignment (i.e., first token is followed by ':=' or '[').
 * This rule allows most statement-introducing keywords to be kept unreserved.
 * (We still have to reserve initial keywords that might follow a block
 * label, unfortunately, since the method used to determine if we are at
 * start of statement doesn't recognize such cases.  We'd also have to
 * reserve any keyword that could legitimately be followed by ':=' or '['.)
 * Some additional cases are handled in pl_gram.y using tok_is_keyword().
 *
 * We try to avoid reserving more keywords than we have to; but there's
 * little point in not reserving a word if it's reserved in the core grammar.
 * Currently, the following words are reserved here but not in the core:
 * BEGIN BY DECLARE EXECUTE FOREACH IF LOOP STRICT WHILE
 */

/*
 * Lists of keyword (name, token-value, category) entries.
 *
 * !!WARNING!!: These lists must be sorted by ASCII name, because binary
 *		 search is used to locate entries.
 *
 * Be careful not to put the same word in both lists.  Also be sure that
 * pl_gram.y's unreserved_keyword production agrees with the second list.
 */

static const ScanKeyword reserved_keywords[] = {
	PG_KEYWORD("all", K_ALL, RESERVED_KEYWORD)
	PG_KEYWORD("begin", K_BEGIN, RESERVED_KEYWORD)
	PG_KEYWORD("by", K_BY, RESERVED_KEYWORD)
	PG_KEYWORD("case", K_CASE, RESERVED_KEYWORD)
	PG_KEYWORD("declare", K_DECLARE, RESERVED_KEYWORD)
	PG_KEYWORD("else", K_ELSE, RESERVED_KEYWORD)
	PG_KEYWORD("end", K_END, RESERVED_KEYWORD)
	PG_KEYWORD("execute", K_EXECUTE, RESERVED_KEYWORD)
	PG_KEYWORD("for", K_FOR, RESERVED_KEYWORD)
	PG_KEYWORD("foreach", K_FOREACH, RESERVED_KEYWORD)
	PG_KEYWORD("from", K_FROM, RESERVED_KEYWORD)
	PG_KEYWORD("if", K_IF, RESERVED_KEYWORD)
	PG_KEYWORD("in", K_IN, RESERVED_KEYWORD)
	PG_KEYWORD("into", K_INTO, RESERVED_KEYWORD)
	PG_KEYWORD("loop", K_LOOP, RESERVED_KEYWORD)
	PG_KEYWORD("not", K_NOT, RESERVED_KEYWORD)
	PG_KEYWORD("null", K_NULL, RESERVED_KEYWORD)
	PG_KEYWORD("or", K_OR, RESERVED_KEYWORD)
	PG_KEYWORD("strict", K_STRICT, RESERVED_KEYWORD)
	PG_KEYWORD("then", K_THEN, RESERVED_KEYWORD)
	PG_KEYWORD("to", K_TO, RESERVED_KEYWORD)
	PG_KEYWORD("using", K_USING, RESERVED_KEYWORD)
	PG_KEYWORD("when", K_WHEN, RESERVED_KEYWORD)
	PG_KEYWORD("while", K_WHILE, RESERVED_KEYWORD)
};

static const int num_reserved_keywords = lengthof(reserved_keywords);

static const ScanKeyword unreserved_keywords[] = {
	PG_KEYWORD("absolute", K_ABSOLUTE, UNRESERVED_KEYWORD)
	PG_KEYWORD("alias", K_ALIAS, UNRESERVED_KEYWORD)
	PG_KEYWORD("array", K_ARRAY, UNRESERVED_KEYWORD)
	PG_KEYWORD("assert", K_ASSERT, UNRESERVED_KEYWORD)
	PG_KEYWORD("backward", K_BACKWARD, UNRESERVED_KEYWORD)
	PG_KEYWORD("close", K_CLOSE, UNRESERVED_KEYWORD)
	PG_KEYWORD("collate", K_COLLATE, UNRESERVED_KEYWORD)
	PG_KEYWORD("column", K_COLUMN, UNRESERVED_KEYWORD)
	PG_KEYWORD("column_name", K_COLUMN_NAME, UNRESERVED_KEYWORD)
	PG_KEYWORD("constant", K_CONSTANT, UNRESERVED_KEYWORD)
	PG_KEYWORD("constraint", K_CONSTRAINT, UNRESERVED_KEYWORD)
	PG_KEYWORD("constraint_name", K_CONSTRAINT_NAME, UNRESERVED_KEYWORD)
	PG_KEYWORD("continue", K_CONTINUE, UNRESERVED_KEYWORD)
	PG_KEYWORD("current", K_CURRENT, UNRESERVED_KEYWORD)
	PG_KEYWORD("cursor", K_CURSOR, UNRESERVED_KEYWORD)
	PG_KEYWORD("datatype", K_DATATYPE, UNRESERVED_KEYWORD)
	PG_KEYWORD("debug", K_DEBUG, UNRESERVED_KEYWORD)
	PG_KEYWORD("default", K_DEFAULT, UNRESERVED_KEYWORD)
	PG_KEYWORD("detail", K_DETAIL, UNRESERVED_KEYWORD)
	PG_KEYWORD("diagnostics", K_DIAGNOSTICS, UNRESERVED_KEYWORD)
	PG_KEYWORD("dump", K_DUMP, UNRESERVED_KEYWORD)
	PG_KEYWORD("elseif", K_ELSIF, UNRESERVED_KEYWORD)
	PG_KEYWORD("elsif", K_ELSIF, UNRESERVED_KEYWORD)
	PG_KEYWORD("errcode", K_ERRCODE, UNRESERVED_KEYWORD)
	PG_KEYWORD("error", K_ERROR, UNRESERVED_KEYWORD)
	PG_KEYWORD("exception", K_EXCEPTION, UNRESERVED_KEYWORD)
	PG_KEYWORD("exit", K_EXIT, UNRESERVED_KEYWORD)
	PG_KEYWORD("fetch", K_FETCH, UNRESERVED_KEYWORD)
	PG_KEYWORD("first", K_FIRST, UNRESERVED_KEYWORD)
	PG_KEYWORD("forward", K_FORWARD, UNRESERVED_KEYWORD)
	PG_KEYWORD("get", K_GET, UNRESERVED_KEYWORD)
	PG_KEYWORD("hint", K_HINT, UNRESERVED_KEYWORD)
	PG_KEYWORD("info", K_INFO, UNRESERVED_KEYWORD)
	PG_KEYWORD("insert", K_INSERT, UNRESERVED_KEYWORD)
	PG_KEYWORD("is", K_IS, UNRESERVED_KEYWORD)
	PG_KEYWORD("last", K_LAST, UNRESERVED_KEYWORD)
	PG_KEYWORD("log", K_LOG, UNRESERVED_KEYWORD)
	PG_KEYWORD("message", K_MESSAGE, UNRESERVED_KEYWORD)
	PG_KEYWORD("message_text", K_MESSAGE_TEXT, UNRESERVED_KEYWORD)
	PG_KEYWORD("move", K_MOVE, UNRESERVED_KEYWORD)
	PG_KEYWORD("next", K_NEXT, UNRESERVED_KEYWORD)
	PG_KEYWORD("no", K_NO, UNRESERVED_KEYWORD)
	PG_KEYWORD("notice", K_NOTICE, UNRESERVED_KEYWORD)
	PG_KEYWORD("open", K_OPEN, UNRESERVED_KEYWORD)
	PG_KEYWORD("option", K_OPTION, UNRESERVED_KEYWORD)
	PG_KEYWORD("perform", K_PERFORM, UNRESERVED_KEYWORD)
	PG_KEYWORD("pg_context", K_PG_CONTEXT, UNRESERVED_KEYWORD)
	PG_KEYWORD("pg_datatype_name", K_PG_DATATYPE_NAME, UNRESERVED_KEYWORD)
	PG_KEYWORD("pg_exception_context", K_PG_EXCEPTION_CONTEXT, UNRESERVED_KEYWORD)
	PG_KEYWORD("pg_exception_detail", K_PG_EXCEPTION_DETAIL, UNRESERVED_KEYWORD)
	PG_KEYWORD("pg_exception_hint", K_PG_EXCEPTION_HINT, UNRESERVED_KEYWORD)
	PG_KEYWORD("print_strict_params", K_PRINT_STRICT_PARAMS, UNRESERVED_KEYWORD)
	PG_KEYWORD("prior", K_PRIOR, UNRESERVED_KEYWORD)
	PG_KEYWORD("query", K_QUERY, UNRESERVED_KEYWORD)
	PG_KEYWORD("raise", K_RAISE, UNRESERVED_KEYWORD)
	PG_KEYWORD("relative", K_RELATIVE, UNRESERVED_KEYWORD)
	PG_KEYWORD("result_oid", K_RESULT_OID, UNRESERVED_KEYWORD)
	PG_KEYWORD("return", K_RETURN, UNRESERVED_KEYWORD)
	PG_KEYWORD("returned_sqlstate", K_RETURNED_SQLSTATE, UNRESERVED_KEYWORD)
	PG_KEYWORD("reverse", K_REVERSE, UNRESERVED_KEYWORD)
	PG_KEYWORD("row_count", K_ROW_COUNT, UNRESERVED_KEYWORD)
	PG_KEYWORD("rowtype", K_ROWTYPE, UNRESERVED_KEYWORD)
	PG_KEYWORD("schema", K_SCHEMA, UNRESERVED_KEYWORD)
	PG_KEYWORD("schema_name", K_SCHEMA_NAME, UNRESERVED_KEYWORD)
	PG_KEYWORD("scroll", K_SCROLL, UNRESERVED_KEYWORD)
	PG_KEYWORD("slice", K_SLICE, UNRESERVED_KEYWORD)
	PG_KEYWORD("sqlstate", K_SQLSTATE, UNRESERVED_KEYWORD)
	PG_KEYWORD("stacked", K_STACKED, UNRESERVED_KEYWORD)
	PG_KEYWORD("table", K_TABLE, UNRESERVED_KEYWORD)
	PG_KEYWORD("table_name", K_TABLE_NAME, UNRESERVED_KEYWORD)
	PG_KEYWORD("type", K_TYPE, UNRESERVED_KEYWORD)
	PG_KEYWORD("use_column", K_USE_COLUMN, UNRESERVED_KEYWORD)
	PG_KEYWORD("use_variable", K_USE_VARIABLE, UNRESERVED_KEYWORD)
	PG_KEYWORD("variable_conflict", K_VARIABLE_CONFLICT, UNRESERVED_KEYWORD)
	PG_KEYWORD("warning", K_WARNING, UNRESERVED_KEYWORD)
};

static const int num_unreserved_keywords = lengthof(unreserved_keywords);

/*
 * This macro must recognize all tokens that can immediately precede a
 * PL/pgSQL executable statement (that is, proc_sect or proc_stmt in the
 * grammar).  Fortunately, there are not very many, so hard-coding in this
 * fashion seems sufficient.
 */
#define AT_STMT_START(prev_token) \
	((prev_token) == ';' || \
	 (prev_token) == K_BEGIN || \
	 (prev_token) == K_THEN || \
	 (prev_token) == K_ELSE || \
	 (prev_token) == K_LOOP)


/* Auxiliary data about a token (other than the token type) */
typedef struct
{
	YYSTYPE		lval;			/* semantic information */
	YYLTYPE		lloc;			/* offset in scanbuf */
	int			leng;			/* length in bytes */
} TokenAuxData;

/*
 * Scanner working state.  At some point we might wish to fold all this
 * into a YY_EXTRA struct.  For the moment, there is no need for plpgsql's
 * lexer to be re-entrant, and the notational burden of passing a yyscanner
 * pointer around is great enough to not want to do it without need.
 */

/* The stuff the core lexer needs */
static core_yyscan_t yyscanner = NULL;
static core_yy_extra_type core_yy;

/* The original input string */
static const char *scanorig;

/* Current token's length (corresponds to plpgsql_yylval and plpgsql_yylloc) */
static int	plpgsql_yyleng;

/* Current token's code (corresponds to plpgsql_yylval and plpgsql_yylloc) */
static int	plpgsql_yytoken;

/* Token pushback stack */
#define MAX_PUSHBACKS 4

static int	num_pushbacks;
static int	pushback_token[MAX_PUSHBACKS];
static TokenAuxData pushback_auxdata[MAX_PUSHBACKS];

/* State for plpgsql_location_to_lineno() */
static const char *cur_line_start;
static const char *cur_line_end;
static int	cur_line_num;

/* Internal functions */
static int	internal_yylex(TokenAuxData *auxdata);
static void push_back_token(int token, TokenAuxData *auxdata);
static void location_lineno_init(void);


/*
 * This is the yylex routine called from the PL/pgSQL grammar.
 * It is a wrapper around the core lexer, with the ability to recognize
 * PL/pgSQL variables and return them as special T_DATUM tokens.  If a
 * word or compound word does not match any variable name, or if matching
 * is turned off by plpgsql_IdentifierLookup, it is returned as
 * T_WORD or T_CWORD respectively, or as an unreserved keyword if it
 * matches one of those.
 */
int
plpgsql_yylex(void)
{
	int			tok1;
	TokenAuxData aux1;
	const ScanKeyword *kw;

	tok1 = internal_yylex(&aux1);
	if (tok1 == IDENT || tok1 == PARAM)
	{
		int			tok2;
		TokenAuxData aux2;

		tok2 = internal_yylex(&aux2);
		if (tok2 == '.')
		{
			int			tok3;
			TokenAuxData aux3;

			tok3 = internal_yylex(&aux3);
			if (tok3 == IDENT)
			{
				int			tok4;
				TokenAuxData aux4;

				tok4 = internal_yylex(&aux4);
				if (tok4 == '.')
				{
					int			tok5;
					TokenAuxData aux5;

					tok5 = internal_yylex(&aux5);
					if (tok5 == IDENT)
					{
						if (plpgsql_parse_tripword(aux1.lval.str,
												   aux3.lval.str,
												   aux5.lval.str,
												   &aux1.lval.wdatum,
												   &aux1.lval.cword))
							tok1 = T_DATUM;
						else
							tok1 = T_CWORD;
					}
					else
					{
						/* not A.B.C, so just process A.B */
						push_back_token(tok5, &aux5);
						push_back_token(tok4, &aux4);
						if (plpgsql_parse_dblword(aux1.lval.str,
												  aux3.lval.str,
												  &aux1.lval.wdatum,
												  &aux1.lval.cword))
							tok1 = T_DATUM;
						else
							tok1 = T_CWORD;
					}
				}
				else
				{
					/* not A.B.C, so just process A.B */
					push_back_token(tok4, &aux4);
					if (plpgsql_parse_dblword(aux1.lval.str,
											  aux3.lval.str,
											  &aux1.lval.wdatum,
											  &aux1.lval.cword))
						tok1 = T_DATUM;
					else
						tok1 = T_CWORD;
				}
			}
			else
			{
				/* not A.B, so just process A */
				push_back_token(tok3, &aux3);
				push_back_token(tok2, &aux2);
				if (plpgsql_parse_word(aux1.lval.str,
									   core_yy.scanbuf + aux1.lloc,
									   &aux1.lval.wdatum,
									   &aux1.lval.word))
					tok1 = T_DATUM;
				else if (!aux1.lval.word.quoted &&
						 (kw = ScanKeywordLookup(aux1.lval.word.ident,
												 unreserved_keywords,
												 num_unreserved_keywords)))
				{
					aux1.lval.keyword = kw->name;
					tok1 = kw->value;
				}
				else
					tok1 = T_WORD;
			}
		}
		else
		{
			/* not A.B, so just process A */
			push_back_token(tok2, &aux2);

			/*
			 * If we are at start of statement, prefer unreserved keywords
			 * over variable names, unless the next token is assignment or
			 * '[', in which case prefer variable names.  (Note we need not
			 * consider '.' as the next token; that case was handled above,
			 * and we always prefer variable names in that case.)  If we are
			 * not at start of statement, always prefer variable names over
			 * unreserved keywords.
			 */
			if (AT_STMT_START(plpgsql_yytoken) &&
				!(tok2 == '=' || tok2 == COLON_EQUALS || tok2 == '['))
			{
				/* try for unreserved keyword, then for variable name */
				if (core_yy.scanbuf[aux1.lloc] != '"' &&
					(kw = ScanKeywordLookup(aux1.lval.str,
											unreserved_keywords,
											num_unreserved_keywords)))
				{
					aux1.lval.keyword = kw->name;
					tok1 = kw->value;
				}
				else if (plpgsql_parse_word(aux1.lval.str,
											core_yy.scanbuf + aux1.lloc,
											&aux1.lval.wdatum,
											&aux1.lval.word))
					tok1 = T_DATUM;
				else
					tok1 = T_WORD;
			}
			else
			{
				/* try for variable name, then for unreserved keyword */
				if (plpgsql_parse_word(aux1.lval.str,
									   core_yy.scanbuf + aux1.lloc,
									   &aux1.lval.wdatum,
									   &aux1.lval.word))
					tok1 = T_DATUM;
				else if (!aux1.lval.word.quoted &&
						 (kw = ScanKeywordLookup(aux1.lval.word.ident,
												 unreserved_keywords,
												 num_unreserved_keywords)))
				{
					aux1.lval.keyword = kw->name;
					tok1 = kw->value;
				}
				else
					tok1 = T_WORD;
			}
		}
	}
	else
	{
		/*
		 * Not a potential plpgsql variable name, just return the data.
		 *
		 * Note that we also come through here if the grammar pushed back a
		 * T_DATUM, T_CWORD, T_WORD, or unreserved-keyword token returned by a
		 * previous lookup cycle; thus, pushbacks do not incur extra lookup
		 * work, since we'll never do the above code twice for the same token.
		 * This property also makes it safe to rely on the old value of
		 * plpgsql_yytoken in the is-this-start-of-statement test above.
		 */
	}

	plpgsql_yylval = aux1.lval;
	plpgsql_yylloc = aux1.lloc;
	plpgsql_yyleng = aux1.leng;
	plpgsql_yytoken = tok1;
	return tok1;
}

/*
 * Internal yylex function.  This wraps the core lexer and adds one feature:
 * a token pushback stack.  We also make a couple of trivial single-token
 * translations from what the core lexer does to what we want, in particular
 * interfacing from the core_YYSTYPE to YYSTYPE union.
 */
static int
internal_yylex(TokenAuxData *auxdata)
{
	int			token;
	const char *yytext;

	if (num_pushbacks > 0)
	{
		num_pushbacks--;
		token = pushback_token[num_pushbacks];
		*auxdata = pushback_auxdata[num_pushbacks];
	}
	else
	{
		token = core_yylex(&auxdata->lval.core_yystype,
						   &auxdata->lloc,
						   yyscanner);

		/* remember the length of yytext before it gets changed */
		yytext = core_yy.scanbuf + auxdata->lloc;
		auxdata->leng = strlen(yytext);

		/* Check for << >> and #, which the core considers operators */
		if (token == Op)
		{
			if (strcmp(auxdata->lval.str, "<<") == 0)
				token = LESS_LESS;
			else if (strcmp(auxdata->lval.str, ">>") == 0)
				token = GREATER_GREATER;
			else if (strcmp(auxdata->lval.str, "#") == 0)
				token = '#';
		}

		/* The core returns PARAM as ival, but we treat it like IDENT */
		else if (token == PARAM)
		{
			auxdata->lval.str = pstrdup(yytext);
		}
	}

	return token;
}

/*
 * Push back a token to be re-read by next internal_yylex() call.
 */
static void
push_back_token(int token, TokenAuxData *auxdata)
{
	if (num_pushbacks >= MAX_PUSHBACKS)
		elog(ERROR, "too many tokens pushed back");
	pushback_token[num_pushbacks] = token;
	pushback_auxdata[num_pushbacks] = *auxdata;
	num_pushbacks++;
}

/*
 * Push back a single token to be re-read by next plpgsql_yylex() call.
 *
 * NOTE: this does not cause yylval or yylloc to "back up".  Also, it
 * is not a good idea to push back a token code other than what you read.
 */
void
plpgsql_push_back_token(int token)
{
	TokenAuxData auxdata;

	auxdata.lval = plpgsql_yylval;
	auxdata.lloc = plpgsql_yylloc;
	auxdata.leng = plpgsql_yyleng;
	push_back_token(token, &auxdata);
}

/*
 * Tell whether a token is an unreserved keyword.
 *
 * (If it is, its lowercased form was returned as the token value, so we
 * do not need to offer that data here.)
 */
bool
plpgsql_token_is_unreserved_keyword(int token)
{
	int			i;

	for (i = 0; i < num_unreserved_keywords; i++)
	{
		if (unreserved_keywords[i].value == token)
			return true;
	}
	return false;
}

/*
 * Append the function text starting at startlocation and extending to
 * (not including) endlocation onto the existing contents of "buf".
 */
void
plpgsql_append_source_text(StringInfo buf,
						   int startlocation, int endlocation)
{
	Assert(startlocation <= endlocation);
	appendBinaryStringInfo(buf, scanorig + startlocation,
						   endlocation - startlocation);
}

/*
 * Peek one token ahead in the input stream.  Only the token code is
 * made available, not any of the auxiliary info such as location.
 *
 * NB: no variable or unreserved keyword lookup is performed here, they will
 * be returned as IDENT. Reserved keywords are resolved as usual.
 */
int
plpgsql_peek(void)
{
	int			tok1;
	TokenAuxData aux1;

	tok1 = internal_yylex(&aux1);
	push_back_token(tok1, &aux1);
	return tok1;
}

/*
 * Peek two tokens ahead in the input stream. The first token and its
 * location in the query are returned in *tok1_p and *tok1_loc, second token
 * and its location in *tok2_p and *tok2_loc.
 *
 * NB: no variable or unreserved keyword lookup is performed here, they will
 * be returned as IDENT. Reserved keywords are resolved as usual.
 */
void
plpgsql_peek2(int *tok1_p, int *tok2_p, int *tok1_loc, int *tok2_loc)
{
	int			tok1,
				tok2;
	TokenAuxData aux1,
				aux2;

	tok1 = internal_yylex(&aux1);
	tok2 = internal_yylex(&aux2);

	*tok1_p = tok1;
	if (tok1_loc)
		*tok1_loc = aux1.lloc;
	*tok2_p = tok2;
	if (tok2_loc)
		*tok2_loc = aux2.lloc;

	push_back_token(tok2, &aux2);
	push_back_token(tok1, &aux1);
}

/*
 * plpgsql_scanner_errposition
 *		Report an error cursor position, if possible.
 *
 * This is expected to be used within an ereport() call.  The return value
 * is a dummy (always 0, in fact).
 *
 * Note that this can only be used for messages emitted during initial
 * parsing of a plpgsql function, since it requires the scanorig string
 * to still be available.
 */
int
plpgsql_scanner_errposition(int location)
{
	int			pos;

	if (location < 0 || scanorig == NULL)
		return 0;				/* no-op if location is unknown */

	/* Convert byte offset to character number */
	pos = pg_mbstrlen_with_len(scanorig, location) + 1;
	/* And pass it to the ereport mechanism */
	(void) internalerrposition(pos);
	/* Also pass the function body string */
	return internalerrquery(scanorig);
}

/*
 * plpgsql_yyerror
 *		Report a lexer or grammar error.
 *
 * The message's cursor position refers to the current token (the one
 * last returned by plpgsql_yylex()).
 * This is OK for syntax error messages from the Bison parser, because Bison
 * parsers report error as soon as the first unparsable token is reached.
 * Beware of using yyerror for other purposes, as the cursor position might
 * be misleading!
 */
void
plpgsql_yyerror(const char *message)
{
	char	   *yytext = core_yy.scanbuf + plpgsql_yylloc;

	if (*yytext == '\0')
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
		/* translator: %s is typically the translation of "syntax error" */
				 errmsg("%s at end of input", _(message)),
				 plpgsql_scanner_errposition(plpgsql_yylloc)));
	}
	else
	{
		/*
		 * If we have done any lookahead then flex will have restored the
		 * character after the end-of-token.  Zap it again so that we report
		 * only the single token here.  This modifies scanbuf but we no longer
		 * care about that.
		 */
		yytext[plpgsql_yyleng] = '\0';

		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
		/* translator: first %s is typically the translation of "syntax error" */
				 errmsg("%s at or near \"%s\"", _(message), yytext),
				 plpgsql_scanner_errposition(plpgsql_yylloc)));
	}
}

/*
 * Given a location (a byte offset in the function source text),
 * return a line number.
 *
 * We expect that this is typically called for a sequence of increasing
 * location values, so optimize accordingly by tracking the endpoints
 * of the "current" line.
 */
int
plpgsql_location_to_lineno(int location)
{
	const char *loc;

	if (location < 0 || scanorig == NULL)
		return 0;				/* garbage in, garbage out */
	loc = scanorig + location;

	/* be correct, but not fast, if input location goes backwards */
	if (loc < cur_line_start)
		location_lineno_init();

	while (cur_line_end != NULL && loc > cur_line_end)
	{
		cur_line_start = cur_line_end + 1;
		cur_line_num++;
		cur_line_end = strchr(cur_line_start, '\n');
	}

	return cur_line_num;
}

/* initialize or reset the state for plpgsql_location_to_lineno */
static void
location_lineno_init(void)
{
	cur_line_start = scanorig;
	cur_line_num = 1;

	cur_line_end = strchr(cur_line_start, '\n');
}

/* return the most recently computed lineno */
int
plpgsql_latest_lineno(void)
{
	return cur_line_num;
}


/*
 * Called before any actual parsing is done
 *
 * Note: the passed "str" must remain valid until plpgsql_scanner_finish().
 * Although it is not fed directly to flex, we need the original string
 * to cite in error messages.
 */
void
plpgsql_scanner_init(const char *str)
{
	/* Start up the core scanner */
	yyscanner = scanner_init(str, &core_yy,
							 reserved_keywords, num_reserved_keywords);

	/*
	 * scanorig points to the original string, which unlike the scanner's
	 * scanbuf won't be modified on-the-fly by flex.  Notice that although
	 * yytext points into scanbuf, we rely on being able to apply locations
	 * (offsets from string start) to scanorig as well.
	 */
	scanorig = str;

	/* Other setup */
	plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
	plpgsql_yytoken = 0;

	num_pushbacks = 0;

	location_lineno_init();
}

/*
 * Called after parsing is done to clean up after plpgsql_scanner_init()
 */
void
plpgsql_scanner_finish(void)
{
	/* release storage */
	scanner_finish(yyscanner);
	/* avoid leaving any dangling pointers */
	yyscanner = NULL;
	scanorig = NULL;
}
