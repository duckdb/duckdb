#ifndef PG_COMPAT_H
#define PG_COMPAT_H


#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "compat.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"

#ifdef __cplusplus
extern "C" {
#endif

// error handling
void elog(int code, char* fmt,...);
int errcode(int sqlerrcode);
void errmsg(char* fmt, ...);
void errhint(char* msg);
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
