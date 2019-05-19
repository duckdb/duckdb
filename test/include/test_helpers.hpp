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
#undef RemoveDirectory
#endif

#include "common/string_util.hpp"
#include "compare_result.hpp"
#include "duckdb.hpp"

namespace duckdb {

void DeleteDatabase(string path);
void TestDeleteDirectory(string path);
void TestCreateDirectory(string path);
void TestDeleteFile(string path);
string TestCreatePath(string suffix);

bool ApproxEqual(float l, float r);
bool ApproxEqual(double l, double r);

#define REQUIRE_NO_FAIL(result) REQUIRE((result)->success)
#define REQUIRE_FAIL(result) REQUIRE(!(result)->success)

#define COMPARE_CSV(result, csv, header)                                                                               \
	{                                                                                                                  \
		auto res = compare_csv(*result, csv, header);                                                                  \
		if (!res.empty())                                                                                              \
			FAIL(res);                                                                                                 \
	}

} // namespace duckdb
