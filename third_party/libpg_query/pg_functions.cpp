#include <stdexcept>
#include <string>
#include <thread>
#include <mutex>
#include "pg_functions.h"
#include "parser/parser.h"


#ifdef _MSC_VER
# error No Windows support yet :/
#else

#include <pthread.h>

static pthread_key_t key;
static pthread_once_t key_once = PTHREAD_ONCE_INIT;

typedef struct parser_state_str parser_state;
struct parser_state_str {
	int pg_err_code;
	int pg_err_pos;
	char pg_err_msg[BUFSIZ];
};

static void create_key() {
    pthread_key_create(&key, NULL);

}

static void init_thread_storage() {
	pthread_once(&key_once, create_key);
	void* ptr;
    if ((ptr = pthread_getspecific(key)) == NULL) {
    	ptr = malloc(sizeof(parser_state));
        pthread_setspecific(key, ptr);
    }
}

static parser_state* get_parser_state() {
	return (parser_state* ) pthread_getspecific(key);
}

#endif


void pg_parser_init() {
	init_thread_storage();

	get_parser_state()->pg_err_code = UNDEFINED;
}
void pg_parser_parse(const char* query, parse_result *res) {
	res->parse_tree = raw_parser(query);
	auto state = get_parser_state();
	res->success = state->pg_err_code == UNDEFINED;
	res->error_location = state->pg_err_pos;
	res->error_message = state->pg_err_msg;
}
void pg_parser_cleanup() {
	// TODO
}

int ereport(int code, ...) {
	std::string err = "parser error : " + std::string(get_parser_state()->pg_err_msg);
    throw std::runtime_error(err);
}
void elog(int code, char* fmt,...) {
    throw std::runtime_error("elog NOT IMPLEMENTED");
}
int errcode(int sqlerrcode) {
	get_parser_state()->pg_err_code = sqlerrcode;
	return 1;
}
int errmsg(char* fmt, ...) {
	 va_list argptr;
	 va_start(argptr, fmt);
	 vsnprintf(get_parser_state()->pg_err_msg, BUFSIZ, fmt, argptr);
	 va_end(argptr);
	 return 1;
}
int errhint(char* msg) {
    throw std::runtime_error("errhint NOT IMPLEMENTED");
}
int	errmsg_internal(const char *fmt,...) {
    throw std::runtime_error("errmsg_internal NOT IMPLEMENTED");
}
int	errdetail(const char *fmt,...) {
    throw std::runtime_error("errdetail NOT IMPLEMENTED");
}
int	errposition(int cursorpos) {
	get_parser_state()->pg_err_pos = cursorpos;
	return 1;
}
char *psprintf(const char *fmt,...) {
    throw std::runtime_error("psprintf NOT IMPLEMENTED");
}
char *pstrdup(const char *in) {
	return strdup(in);
    //throw std::runtime_error("pstrdup NOT IMPLEMENTED");
}
void* palloc(size_t n) {
	void* ptr = malloc(n);
	memset(ptr, 0, n);
	return ptr;
    //throw std::runtime_error("palloc NOT IMPLEMENTED");
}
void pfree(void* ptr) {
    free(ptr);
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
int	pg_get_client_encoding(void) {
    throw std::runtime_error("pg_get_client_encoding NOT IMPLEMENTED");
}
bool pg_verifymbstr(const char *mbstr, int len, bool noError) {
    throw std::runtime_error("pg_verifymbstr NOT IMPLEMENTED");
}


int pg_database_encoding_max_length(void) {
    //throw std::runtime_error("pg_database_encoding_max_length NOT IMPLEMENTED");
	return 4; // UTF8
}

static int
pg_utf_mblen(const unsigned char *s)
{
	int			len;

	if ((*s & 0x80) == 0)
		len = 1;
	else if ((*s & 0xe0) == 0xc0)
		len = 2;
	else if ((*s & 0xf0) == 0xe0)
		len = 3;
	else if ((*s & 0xf8) == 0xf0)
		len = 4;
#ifdef NOT_USED
	else if ((*s & 0xfc) == 0xf8)
		len = 5;
	else if ((*s & 0xfe) == 0xfc)
		len = 6;
#endif
	else
		len = 1;
	return len;
}


int	pg_mbstrlen_with_len(const char *mbstr, int len) {
	return pg_utf_mblen((const unsigned char*) mbstr);
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

