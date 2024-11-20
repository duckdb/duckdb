#pragma once

#include <stdlib.h>
#include <string>

#define fprintf(...)

#include "pg_definitions.hpp"

#include "nodes/pg_list.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb_libpgquery {

typedef struct parse_result_str parse_result;
struct parse_result_str {
	bool success;
	PGList *parse_tree;
	std::string error_message;
	int error_location;
};

void pg_parser_init();
void pg_parser_parse(const char *query, parse_result *res);
void pg_parser_cleanup();

// error handling
int ereport(int code, ...);

void elog(int code, const char *fmt, ...);
int errcode(int sqlerrcode);
int errmsg(const char *fmt, ...);
int errhint(const char *msg);
int errmsg_internal(const char *fmt, ...);
int errdetail(const char *fmt, ...);
int errposition(int cursorpos);
char *psprintf(const char *fmt, ...);

// memory mgmt
char *pstrdup(const char *in);
void *palloc(size_t n);
void pfree(void *ptr);
void *palloc0fast(size_t n);
void *repalloc(void *ptr, size_t n);

char *NameListToString(PGList *names);
void *copyObject(const void *from);
bool equal(const void *a, const void *b);
int exprLocation(const PGNode *expr);

// string gunk
int pg_database_encoding_max_length(void);
bool pg_verifymbstr(const char *mbstr, int len, bool noError);
int pg_mbstrlen_with_len(const char *mbstr, int len);
int pg_mbcliplen(const char *mbstr, int len, int limit);
int pg_mblen(const char *mbstr);

PGDefElem *defWithOids(bool value);

typedef unsigned int pg_wchar;
unsigned char *unicode_to_utf8(pg_wchar c, unsigned char *utf8string);

}
