#include "postgres.h"
#include "pg_query.h"
#include "pg_query_internal.h"
#include "pg_query_fingerprint.h"

#include "sha1.h"
#include "lib/ilist.h"

#include "parser/parser.h"
#include "parser/scanner.h"
#include "parser/scansup.h"

#include "nodes/parsenodes.h"
#include "nodes/value.h"

#include <unistd.h>
#include <fcntl.h>

// Definitions

typedef struct FingerprintContext
{
	dlist_head tokens;
	SHA1_CTX *sha1; // If this is NULL we write tokens, otherwise we write the sha1sum directly
} FingerprintContext;

typedef struct FingerprintToken
{
	char *str;
	dlist_node list_node;
} FingerprintToken;

static void _fingerprintNode(FingerprintContext *ctx, const void *obj, const void *parent, char *parent_field_name, unsigned int depth);
static void _fingerprintInitForTokens(FingerprintContext *ctx);
static void _fingerprintCopyTokens(FingerprintContext *source, FingerprintContext *target, char *field_name);

#define PG_QUERY_FINGERPRINT_VERSION 1

// Implementations

static void
_fingerprintString(FingerprintContext *ctx, const char *str)
{
	if (ctx->sha1 != NULL) {
		SHA1Update(ctx->sha1, (uint8*) str, strlen(str));
	} else {
		FingerprintToken *token = palloc0(sizeof(FingerprintToken));
		token->str = pstrdup(str);
		dlist_push_tail(&ctx->tokens, &token->list_node);
	}
}

static void
_fingerprintInteger(FingerprintContext *ctx, const Value *node)
{
	if (node->val.ival != 0) {
		_fingerprintString(ctx, "Integer");
		_fingerprintString(ctx, "ival");
		char buffer[50];
		sprintf(buffer, "%ld", node->val.ival);
		_fingerprintString(ctx, buffer);
	}
}

static void
_fingerprintFloat(FingerprintContext *ctx, const Value *node)
{
	if (node->val.str != NULL) {
		_fingerprintString(ctx, "Float");
		_fingerprintString(ctx, "str");
		_fingerprintString(ctx, node->val.str);
	}
}

static void
_fingerprintBitString(FingerprintContext *ctx, const Value *node)
{
	if (node->val.str != NULL) {
		_fingerprintString(ctx, "BitString");
		_fingerprintString(ctx, "str");
		_fingerprintString(ctx, node->val.str);
	}
}

#define FINGERPRINT_CMP_STRBUF 1024

static int compareFingerprintContext(const void *a, const void *b)
{
	FingerprintContext *ca = *(FingerprintContext**) a;
	FingerprintContext *cb = *(FingerprintContext**) b;

	char strBufA[FINGERPRINT_CMP_STRBUF + 1] = {'\0'};
	char strBufB[FINGERPRINT_CMP_STRBUF + 1] = {'\0'};

	dlist_iter iterA;
	dlist_iter iterB;

	dlist_foreach(iterA, &ca->tokens)
	{
		FingerprintToken *token = dlist_container(FingerprintToken, list_node, iterA.cur);

		strncat(strBufA, token->str, FINGERPRINT_CMP_STRBUF - strlen(strBufA));
	}

	dlist_foreach(iterB, &cb->tokens)
	{
		FingerprintToken *token = dlist_container(FingerprintToken, list_node, iterB.cur);

		strncat(strBufB, token->str, FINGERPRINT_CMP_STRBUF - strlen(strBufB));
	}

	//printf("COMP %s <=> %s = %d\n", strBufA, strBufB, strcmp(strBufA, strBufB));

	return strcmp(strBufA, strBufB);
}

static void
_fingerprintList(FingerprintContext *ctx, const List *node, const void *parent, char *field_name, unsigned int depth)
{
	if (field_name != NULL && (strcmp(field_name, "fromClause") == 0 || strcmp(field_name, "targetList") == 0 ||
			strcmp(field_name, "cols") == 0 || strcmp(field_name, "rexpr") == 0)) {

		FingerprintContext** subCtxArr = palloc0(node->length * sizeof(FingerprintContext*));
		size_t subCtxCount = 0;
		size_t i;
		const ListCell *lc;

		foreach(lc, node)
		{
			FingerprintContext* subCtx = palloc0(sizeof(FingerprintContext));

			_fingerprintInitForTokens(subCtx);
			_fingerprintNode(subCtx, lfirst(lc), parent, field_name, depth + 1);

			bool exists = false;
			for (i = 0; i < subCtxCount; i++) {
				if (compareFingerprintContext(&subCtxArr[i], &subCtx) == 0) {
					exists = true;
					break;
				}
			}

			if (!exists) {
				subCtxArr[subCtxCount] = subCtx;
				subCtxCount += 1;
			}

			lnext(lc);
		}

		pg_qsort(subCtxArr, subCtxCount, sizeof(FingerprintContext*), compareFingerprintContext);

		for (i = 0; i < subCtxCount; i++) {
			_fingerprintCopyTokens(subCtxArr[i], ctx, NULL);
		}
	} else {
		const ListCell *lc;

		foreach(lc, node)
		{
			_fingerprintNode(ctx, lfirst(lc), parent, field_name, depth + 1);

			lnext(lc);
		}
	}
}

static void
_fingerprintInitForTokens(FingerprintContext *ctx) {
	ctx->sha1 = NULL;
	dlist_init(&ctx->tokens);
}

static void
_fingerprintCopyTokens(FingerprintContext *source, FingerprintContext *target, char *field_name) {
	dlist_iter iter;

	if (dlist_is_empty(&source->tokens)) return;

	if (field_name != NULL) {
		_fingerprintString(target, field_name);
	}

	dlist_foreach(iter, &source->tokens)
	{
		FingerprintToken *token = dlist_container(FingerprintToken, list_node, iter.cur);

		_fingerprintString(target, token->str);
	}
}

#include "pg_query_fingerprint_defs.c"

void
_fingerprintNode(FingerprintContext *ctx, const void *obj, const void *parent, char *field_name, unsigned int depth)
{
	// Some queries are overly complex in their parsetree - lets consistently cut them off at 100 nodes deep
	if (depth >= 100) {
		return;
	}

	if (obj == NULL)
	{
		return; // Ignore
	}

	if (IsA(obj, List))
	{
		_fingerprintList(ctx, obj, parent, field_name, depth);
	}
	else
	{
		switch (nodeTag(obj))
		{
			case T_Integer:
				_fingerprintInteger(ctx, obj);
				break;
			case T_Float:
				_fingerprintFloat(ctx, obj);
				break;
			case T_String:
				_fingerprintString(ctx, "String");
				_fingerprintString(ctx, "str");
				_fingerprintString(ctx, ((Value*) obj)->val.str);
				break;
			case T_BitString:
				_fingerprintBitString(ctx, obj);
				break;

			#include "pg_query_fingerprint_conds.c"

			default:
				elog(WARNING, "could not fingerprint unrecognized node type: %d",
					 (int) nodeTag(obj));

				return;
		}
	}
}

PgQueryFingerprintResult pg_query_fingerprint_with_opts(const char* input, bool printTokens)
{
	MemoryContext ctx = NULL;
	PgQueryInternalParsetreeAndError parsetree_and_error;
	PgQueryFingerprintResult result = {0};

	ctx = pg_query_enter_memory_context("pg_query_fingerprint");

	parsetree_and_error = pg_query_raw_parse(input);

	// These are all malloc-ed and will survive exiting the memory context, the caller is responsible to free them now
	result.stderr_buffer = parsetree_and_error.stderr_buffer;
	result.error = parsetree_and_error.error;

	if (parsetree_and_error.tree != NULL || result.error == NULL) {
		FingerprintContext ctx;
		int i;
		uint8 sha1result[SHA1_RESULTLEN];

		ctx.sha1 = palloc0(sizeof(SHA1_CTX));
		SHA1Init(ctx.sha1);

		if (parsetree_and_error.tree != NULL) {
			_fingerprintNode(&ctx, parsetree_and_error.tree, NULL, NULL, 0);
		}

		SHA1Final(sha1result, ctx.sha1);

		// This is intentionally malloc-ed and will survive exiting the memory context
		result.hexdigest = calloc((1 + SHA1_RESULTLEN) * 2 + 1, sizeof(char));

		sprintf(result.hexdigest, "%02x", PG_QUERY_FINGERPRINT_VERSION);

		for (i = 0; i < SHA1_RESULTLEN; i++) {
			sprintf(result.hexdigest + (1 + i) * 2, "%02x", sha1result[i]);
		}

		if (printTokens) {
			FingerprintContext debugCtx;
			dlist_iter iter;

			_fingerprintInitForTokens(&debugCtx);
			_fingerprintNode(&debugCtx, parsetree_and_error.tree, NULL, NULL, 0);

			printf("[");

			dlist_foreach(iter, &debugCtx.tokens)
			{
				FingerprintToken *token = dlist_container(FingerprintToken, list_node, iter.cur);

				printf("%s, ", token->str);
			}

			printf("]\n");
		}
	}

	pg_query_exit_memory_context(ctx);

	return result;
}

PgQueryFingerprintResult pg_query_fingerprint(const char* input)
{
	return pg_query_fingerprint_with_opts(input, false);
}

void pg_query_free_fingerprint_result(PgQueryFingerprintResult result)
{
	if (result.error) {
		free(result.error->message);
		free(result.error->filename);
		free(result.error);
	}

	free(result.hexdigest);
	free(result.stderr_buffer);
}
