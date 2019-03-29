#include <stdexcept>
#include <string>
#include <thread>
#include <mutex>
#include "pg_functions.h"
#include "parser/parser.h"
#include <stdarg.h>
#include <mutex>


#ifdef _MSC_VER
#include <windows.h>

static DWORD key = nullptr;
std::mutex key_once;

static void init_thread_storage() {
	if (!key) {
		std::lock_guard<std::mutex> lock(key_once);
		if (!key && (key = TlsAlloc()) == TLS_OUT_OF_INDEXES) {
			throw std::runtime_error("Failed to allocate per-thread storage");
		}
	}

	void* ptr = TlsGetValue(key);
	if (!ptr && GetLastError() != ERROR_SUCCESS) {
		throw std::runtime_error("Failed to access per-thread storage");
	}
	if (!ptr) {
		ptr = malloc(sizeof(parser_state));
		if (!ptr) {
			throw std::runtime_error("Memory allocation failure");
		}
		if (!TlsSetValue(key, ptr))
			throw std::runtime_error("Failed to set per-thread storage");
	}
}

static parser_state* get_parser_state() {
	parser_state* ptr =  (parser_state* ) TlsGetValue(key);
	if (!ptr) {
		throw std::runtime_error("Failed to access per-thread storage");
	}

	return ptr;
}


#else

#include <pthread.h>

static pthread_key_t key;
static pthread_once_t key_once = PTHREAD_ONCE_INIT;

// max parse tree size approx 100 MB, should be enough
#define PG_MALLOC_SIZE 102400
#define PG_MALLOC_LIMIT 1024

typedef struct parser_state_str parser_state;
struct parser_state_str {
	int pg_err_code;
	int pg_err_pos;
	char pg_err_msg[BUFSIZ];

	size_t malloc_pos;
	size_t malloc_ptr_idx;
	char* malloc_ptrs[PG_MALLOC_LIMIT];
};

static void create_key() {
    int res = pthread_key_create(&key, NULL);
    if (res != 0) {
	    throw std::runtime_error("Failed to allocate per-thread storage");
    }
}

static void init_thread_storage() {
	pthread_once(&key_once, create_key);
	void* ptr;
    if ((ptr = pthread_getspecific(key)) == NULL) {
    	ptr = malloc(sizeof(parser_state));
    	if (!ptr) {
    		throw std::runtime_error("Memory allocation failure");
    	}
        if (pthread_setspecific(key, ptr) != 0) {
    		throw std::runtime_error("Failed to set per-thread storage");
        }
    }
}

static parser_state* get_parser_state() {
	parser_state* ptr = (parser_state* ) pthread_getspecific(key);
	if (!ptr) {
		throw std::runtime_error("Failed to access per-thread storage");

	}
	return ptr;
}
#endif


static void allocate_new(parser_state* state) {
	if (state->malloc_ptr_idx + 1 >= PG_MALLOC_LIMIT) {
		throw std::runtime_error("Memory allocation failure");
	}
	char* base_ptr = (char*) malloc(PG_MALLOC_SIZE);
	if (!base_ptr) {
		throw std::runtime_error("Memory allocation failure");
	}
	state->malloc_ptrs[state->malloc_ptr_idx] = base_ptr;
	state->malloc_ptr_idx++;
	state->malloc_pos = 0;
}


void* palloc(size_t n) {
	parser_state* state = get_parser_state();
	if (n > PG_MALLOC_SIZE) {
		throw std::runtime_error("Memory allocation request too large");
	}
	if (state->malloc_pos + n > PG_MALLOC_SIZE) {
		allocate_new(state);
	}
	assert(state->malloc_pos + n <= PG_MALLOC_SIZE);


	void* ptr = state->malloc_ptrs[state->malloc_ptr_idx-1] + state->malloc_pos;
	memset(ptr, 0, n);
	state->malloc_pos += n;
	return ptr;
}



void pg_parser_init() {
	init_thread_storage();
	parser_state* state = get_parser_state();
	state->pg_err_code = UNDEFINED;
	state->pg_err_msg[0] = '\0';

	state->malloc_ptr_idx = 0;
	allocate_new(state);
}

void pg_parser_parse(const char* query, parse_result *res) {
	parser_state* state = get_parser_state();

	res->parse_tree = nullptr;
	try{
		res->parse_tree = raw_parser(query);
		res->success = state->pg_err_code == UNDEFINED;
	} catch (...) {
		res->success = false;

	}
	res->error_message = state->pg_err_msg;
	res->error_location = state->pg_err_pos;
}


void pg_parser_cleanup() {
	parser_state* state = get_parser_state();
	if (!state) {
		return;
	}
	for (size_t ptr_idx = 0; ptr_idx < state->malloc_ptr_idx; ptr_idx++) {
		char* ptr = state->malloc_ptrs[ptr_idx];
		if (ptr) {
			free(ptr);
			state->malloc_ptrs[ptr_idx] = nullptr;
		}
	}

	free(state);
	pthread_setspecific(key, NULL);
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
	char* new_str = (char*) palloc(strlen(in)+1);
	memcpy(new_str, in, strlen(in));
	return new_str;
}

void pfree(void* ptr) {
    // nop, we free up entire context on parser cleanup
}
void* palloc0fast(size_t n) { // very fast
    return palloc(n);
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

