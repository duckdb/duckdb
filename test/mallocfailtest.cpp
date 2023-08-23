#include "duckdb.hpp"
#include "tpch_extension.hpp"
#include <dlfcn.h>
#include <execinfo.h>
#include <cxxabi.h>
#include "re2/re2.h"

using namespace duckdb;

void handler() {
}

static int64_t mallocs_remaining = LLONG_MAX;

// TODO cache the symbols so we don't look up the names every time / and-or demangle

void check_stack() {
	duckdb_re2::RE2 regex("\\d+\\s+\\w+\\s+\\w+\\s+(\\w+)\\s+.*");
	string result;
	auto max_depth = 1000;
	auto callstack = unique_ptr<void *[]>(new void *[max_depth]);
	int frames = backtrace(callstack.get(), max_depth);
	char **strs = backtrace_symbols(callstack.get(), frames);
	bool malloc_from_destructor = false;
	for (int i = 0; i < frames; i++) {
		duckdb_re2::StringPiece input(strs[i]); // Wrap a StringPiece around it
		string symbol;
		if (RE2::FindAndConsume(&input, regex, &symbol)) {
			int status = -1;
			char *demangled_name = abi::__cxa_demangle(symbol.c_str(), NULL, NULL, &status);
			if (demangled_name) {
				if (StringUtil::Contains(demangled_name, "~")) { // Destructor!
					malloc_from_destructor = true;
				}
				free(demangled_name);
			}
		}
	}
	if (malloc_from_destructor) {
		printf("malloc() called from destructor\n");
		abort();
	}
	free(strs);
}

extern "C" {
void set_mallocs_remaining(size_t n) {
	mallocs_remaining = n;
}

void *calloc(size_t count, size_t size) {
	if (mallocs_remaining < 1) {
		mallocs_remaining = LLONG_MAX;
		return nullptr;
	}
	mallocs_remaining--;
	typedef void *(*callocfun)(size_t, size_t);
	auto real_calloc = (callocfun)dlsym(RTLD_NEXT, "calloc");

	return real_calloc(count, size);
}

// this is useful as a breakpoint
static void *malloc_hook_failure() {
	check_stack();
	return nullptr;
}

void *malloc(size_t size) {
	if (mallocs_remaining < 1) {
		mallocs_remaining = LLONG_MAX;
		return malloc_hook_failure();
	}
	mallocs_remaining--;
	typedef void *(*mallocfun)(size_t);
	auto real_malloc = (mallocfun)dlsym(RTLD_NEXT, "malloc");
	return real_malloc(size);
}

void *realloc(void *ptr, size_t size) {
	if (mallocs_remaining < 1) {
		mallocs_remaining = LLONG_MAX;
		return nullptr;
	}
	mallocs_remaining--;
	typedef void *(*reallocfun)(void *, size_t);
	auto real_realloc = (reallocfun)dlsym(RTLD_NEXT, "realloc");
	return real_realloc(ptr, size);
}

void *reallocf(void *ptr, size_t size) {
	printf("reallocf");
	abort();
}

void *valloc(size_t size) {
	printf("valloc");
	abort();
}

void *aligned_alloc(size_t alignment, size_t size) {
	printf("aligned_alloc");
	abort();
}
}

void *operator new(std::size_t size) throw(std::bad_alloc) {
	void *mem = malloc(size == 0 ? 1 : size);
	if (mem == 0) {
		throw std::bad_alloc();
	}
	return mem;
}

void *operator new(std::size_t size, const std::nothrow_t& tag) noexcept  {
	return malloc(size == 0 ? 1 : size);
}

int main(int argc, char *argv[]) {
	{
		DuckDB db("/tmp/mallocfail");
		db.LoadExtension<TpchExtension>();
		Connection con(db);
		con.Query("CALL dbgen(sf=0.01)");
	}
	std::set_new_handler(handler);

	for (idx_t mallocs = atoi(argv[1]); mallocs < atoi(argv[2]); mallocs++) {
		set_mallocs_remaining(mallocs);

		try {
			DuckDB db("/tmp/mallocfail");
			Connection con(db);
			auto result = con.Query(TpchExtension::GetQuery(1));
			if (result->HasError() && result->GetError() != "Invalid Error: std::bad_alloc" &&
			    result->GetError() != "Invalid Error: Memory allocation failure") {
				printf("%llu caught error %s\n", mallocs, result->GetError().c_str());
			}
			if (!result->HasError()) {
				printf("Success %lld\n", mallocs);
				exit(0);
			}

		} catch (std::bad_alloc &e) {
		}
	}
}
