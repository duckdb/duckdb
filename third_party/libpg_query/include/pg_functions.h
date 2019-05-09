#ifndef PG_COMPAT_H
#define PG_COMPAT_H

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

#include "pg_definitions.h"

#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"

typedef struct parse_result_str parse_result;
struct parse_result_str {
	bool success;
	List* parse_tree;
	char* error_message;
	int error_location;
};

void pg_parser_init();
void pg_parser_parse(const char* query, parse_result *res);
void pg_parser_cleanup();

// error handling
int ereport(int code, ...);

void elog(int code, char* fmt,...);
int errcode(int sqlerrcode);
int errmsg(char* fmt, ...);
int errhint(char* msg);
int	errmsg_internal(const char *fmt,...);
int	errdetail(const char *fmt,...);
int	errposition(int cursorpos);
char *psprintf(const char *fmt,...);

// memory mgmt
char *pstrdup(const char *in);
void* palloc(size_t n);
void pfree(void* ptr);
void* palloc0fast(size_t n);
void* repalloc(void* ptr, size_t n);

char *NameListToString(List *names);
void * copyObject(const void *from);
bool equal(const void *a, const void *b);
int exprLocation(const Node *expr);

// string gunk
int GetDatabaseEncoding(void);
int pg_database_encoding_max_length(void);
bool pg_verifymbstr(const char *mbstr, int len, bool noError);
int	pg_mbstrlen_with_len(const char *mbstr, int len);
int	pg_get_client_encoding(void);
int pg_mbcliplen(const char *mbstr, int len, int limit);
int pg_mblen(const char *mbstr);

DefElem * defWithOids(bool value);

typedef unsigned int pg_wchar;
unsigned char *unicode_to_utf8(pg_wchar c, unsigned char *utf8string);


#ifdef __cplusplus
};
#endif

#endif
