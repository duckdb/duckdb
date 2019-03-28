#include "pg_compat.h"

#include <stdexcept>

bool operator_precedence_warning;

void elog(int code, char* fmt,...) {
    throw std::runtime_error("elog NOT IMPLEMENTED");
}
int errcode(int sqlerrcode) {
    throw std::runtime_error("errcode()");
}
void errmsg(char* fmt, ...) {
    throw std::runtime_error("errmsg NOT IMPLEMENTED");
}
void errhint(char* msg) {
    throw std::runtime_error("errhint NOT IMPLEMENTED");
}
int	errmsg_internal(const char *fmt,...) {
    throw std::runtime_error("errmsg_internal NOT IMPLEMENTED");
}
int	errdetail(const char *fmt,...) {
    throw std::runtime_error("errdetail NOT IMPLEMENTED");
}
int	errposition(int cursorpos) {
    throw std::runtime_error("errposition NOT IMPLEMENTED");
}
char *psprintf(const char *fmt,...) {
    throw std::runtime_error("psprintf NOT IMPLEMENTED");
}
char *pstrdup(const char *in) {
	return strdup(in);
    //throw std::runtime_error("pstrdup NOT IMPLEMENTED");
}
void* palloc(size_t n) {
	return malloc(n);
    //throw std::runtime_error("palloc NOT IMPLEMENTED");
}
void pfree(void* ptr) {
    throw std::runtime_error("pfree NOT IMPLEMENTED");
}
void* palloc0fast(size_t n) {
    return palloc(n);
	//throw std::runtime_error("palloc0fast NOT IMPLEMENTED");
}
void* repalloc(void* ptr, size_t n) {
    throw std::runtime_error("repalloc NOT IMPLEMENTED");
}
char *NameListToString(List *names) {
    throw std::runtime_error("NameListToString NOT IMPLEMENTED");
}
int GetDatabaseEncoding(void) {
    throw std::runtime_error("copyObject NOT IMPLEMENTED");
}
void * copyObject(const void *from) {
    throw std::runtime_error("copyObject NOT IMPLEMENTED");
}
bool equal(const void *a, const void *b) {
    throw std::runtime_error("equal NOT IMPLEMENTED");
}
int exprLocation(const Node *expr) {
    throw std::runtime_error("exprLocation NOT IMPLEMENTED");
}
int pg_database_encoding_max_length(void) {
    //throw std::runtime_error("pg_database_encoding_max_length NOT IMPLEMENTED");
	return 4; // UTF8
}
int	pg_get_client_encoding(void) {
    throw std::runtime_error("pg_get_client_encoding NOT IMPLEMENTED");
}
bool pg_verifymbstr(const char *mbstr, int len, bool noError) {
    throw std::runtime_error("pg_verifymbstr NOT IMPLEMENTED");
}
int	pg_mbstrlen_with_len(const char *mbstr, int len) {
    throw std::runtime_error("pg_mbstrlen_with_len NOT IMPLEMENTED");
}
int pg_mbcliplen(const char *mbstr, int len, int limit) {
    throw std::runtime_error("pg_mbcliplen NOT IMPLEMENTED");
}
int pg_mblen(const char *mbstr) {
    throw std::runtime_error("pg_mblen NOT IMPLEMENTED");
}
DefElem * defWithOids(bool value) {
    throw std::runtime_error("defWithOids NOT IMPLEMENTED");
}
unsigned char *unicode_to_utf8(pg_wchar c, unsigned char *utf8string) {
    throw std::runtime_error("unicode_to_utf8 NOT IMPLEMENTED");
}
extern "C" {
#include "parser/parser.h"
}
int main() {
	printf("hello world\n");
//	List* a = raw_parser(	"select sum(a+2) FROM b");
	List* a = raw_parser(	"select\n l_returnflag,\n l_linestatus,\n sum(l_quantity) as sum_qty,\n sum(l_extendedprice) as sum_base_price,\n sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n avg(l_quantity) as avg_qty,\n avg(l_extendedprice) as avg_price,\n avg(l_discount) as avg_disc,\n count(*) as count_order\nfrom\n lineitem\nwhere\n l_shipdate <= cast('1998-09-02' as date)\ngroup by\n l_returnflag,\n l_linestatus\norder by\n l_returnflag,\n l_linestatus;\n");

	printf("%d\n", list_length(a));
	Node* one = (Node*) a->head->data.ptr_value;
	printf("%d\n", one->type);
	SelectStmt* sel = (SelectStmt*) one;
}
