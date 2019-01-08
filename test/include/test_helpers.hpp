#pragma once

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#ifdef _MSC_VER
// these break enum.hpp otherwise
#undef DELETE
#undef DEFAULT
#undef EXISTS
#undef IN
// this breaks file_system.cpp otherwise
#undef CreateDirectory
#endif

#include "common/string_util.hpp"
#include "compare_result.hpp"
#include "duckdb.hpp"

#define TESTING_DIRECTORY_NAME "duckdb_unittest_tempdir"

namespace duckdb {

#define REQUIRE_NO_FAIL(result) REQUIRE((result)->GetSuccess())
#define REQUIRE_FAIL(result) REQUIRE(!(result)->GetSuccess())

#define COMPARE_CSV(result, csv, header)                                                                               \
	{                                                                                                                  \
		auto res = compare_csv(*result, csv, header);                                                                  \
		if (!res.empty())                                                                                              \
			FAIL(res);                                                                                                 \
	}

} // namespace duckdb
