//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../../third_party/catch/catch.hpp"
#include "duckdb/common/exception.hpp"
#include <type_traits>

#ifdef CATCH_CONFIG_DISABLE
#define DUCKDB_CATCH_ASSERTION(...) ((void)0)
#else
#define DUCKDB_CATCH_ASSERTION(...) __VA_ARGS__
#endif

#define DUCKDB_CATCH_EXCEPTION_ASSERTION(exception_type, ...)                                                          \
	do {                                                                                                               \
		static_assert(                                                                                                 \
		    !std::is_base_of<duckdb::Exception, exception_type>::value,                                                \
		    "Match std::exception with ExceptionMatches from test_exception_matcher.hpp for DuckDB exceptions");       \
		DUCKDB_CATCH_ASSERTION(__VA_ARGS__);                                                                           \
	} while (false)

#ifdef REQUIRE_THROWS_AS
#undef REQUIRE_THROWS_AS
#define REQUIRE_THROWS_AS(expr, exception_type)                                                                        \
	DUCKDB_CATCH_EXCEPTION_ASSERTION(exception_type, INTERNAL_CATCH_THROWS_AS("REQUIRE_THROWS_AS", exception_type,     \
	                                                                          Catch::ResultDisposition::Normal, expr))
#endif

#ifdef CHECK_THROWS_AS
#undef CHECK_THROWS_AS
#define CHECK_THROWS_AS(expr, exception_type)                                                                          \
	DUCKDB_CATCH_EXCEPTION_ASSERTION(exception_type,                                                                   \
	                                 INTERNAL_CATCH_THROWS_AS("CHECK_THROWS_AS", exception_type,                       \
	                                                          Catch::ResultDisposition::ContinueOnFailure, expr))
#endif

#ifdef CATCH_REQUIRE_THROWS_AS
#undef CATCH_REQUIRE_THROWS_AS
#define CATCH_REQUIRE_THROWS_AS(expr, exception_type)                                                                  \
	DUCKDB_CATCH_EXCEPTION_ASSERTION(                                                                                  \
	    exception_type,                                                                                                \
	    INTERNAL_CATCH_THROWS_AS("CATCH_REQUIRE_THROWS_AS", exception_type, Catch::ResultDisposition::Normal, expr))
#endif

#ifdef CATCH_CHECK_THROWS_AS
#undef CATCH_CHECK_THROWS_AS
#define CATCH_CHECK_THROWS_AS(expr, exception_type)                                                                    \
	DUCKDB_CATCH_EXCEPTION_ASSERTION(exception_type,                                                                   \
	                                 INTERNAL_CATCH_THROWS_AS("CATCH_CHECK_THROWS_AS", exception_type,                 \
	                                                          Catch::ResultDisposition::ContinueOnFailure, expr))
#endif

#ifdef REQUIRE_THROWS_MATCHES
#undef REQUIRE_THROWS_MATCHES
#define REQUIRE_THROWS_MATCHES(expr, exception_type, matcher)                                                          \
	DUCKDB_CATCH_EXCEPTION_ASSERTION(exception_type,                                                                   \
	                                 INTERNAL_CATCH_THROWS_MATCHES("REQUIRE_THROWS_MATCHES", exception_type,           \
	                                                               Catch::ResultDisposition::Normal, matcher, expr))
#endif

#ifdef CHECK_THROWS_MATCHES
#undef CHECK_THROWS_MATCHES
#define CHECK_THROWS_MATCHES(expr, exception_type, matcher)                                                            \
	DUCKDB_CATCH_EXCEPTION_ASSERTION(                                                                                  \
	    exception_type, INTERNAL_CATCH_THROWS_MATCHES("CHECK_THROWS_MATCHES", exception_type,                          \
	                                                  Catch::ResultDisposition::ContinueOnFailure, matcher, expr))
#endif

#ifdef CATCH_REQUIRE_THROWS_MATCHES
#undef CATCH_REQUIRE_THROWS_MATCHES
#define CATCH_REQUIRE_THROWS_MATCHES(expr, exception_type, matcher)                                                    \
	DUCKDB_CATCH_EXCEPTION_ASSERTION(exception_type,                                                                   \
	                                 INTERNAL_CATCH_THROWS_MATCHES("CATCH_REQUIRE_THROWS_MATCHES", exception_type,     \
	                                                               Catch::ResultDisposition::Normal, matcher, expr))
#endif

#ifdef CATCH_CHECK_THROWS_MATCHES
#undef CATCH_CHECK_THROWS_MATCHES
#define CATCH_CHECK_THROWS_MATCHES(expr, exception_type, matcher)                                                      \
	DUCKDB_CATCH_EXCEPTION_ASSERTION(                                                                                  \
	    exception_type, INTERNAL_CATCH_THROWS_MATCHES("CATCH_CHECK_THROWS_MATCHES", exception_type,                    \
	                                                  Catch::ResultDisposition::ContinueOnFailure, matcher, expr))
#endif
