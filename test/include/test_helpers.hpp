#pragma once

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

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "compare_result.hpp"
#include "duckdb.hpp"

namespace duckdb {

void DeleteDatabase(string path);
void TestDeleteDirectory(string path);
void TestCreateDirectory(string path);
void TestDeleteFile(string path);
string TestCreatePath(string suffix);
unique_ptr<DBConfig> GetTestConfig();

bool NO_FAIL(QueryResult &result);
bool NO_FAIL(unique_ptr<QueryResult> result);

#define REQUIRE_NO_FAIL(result) REQUIRE(NO_FAIL((result)))
#define REQUIRE_FAIL(result) REQUIRE(!(result)->success)

#define COMPARE_CSV(result, csv, header)                                                                               \
	{                                                                                                                  \
		auto res = compare_csv(*result, csv, header);                                                                  \
		if (!res.empty())                                                                                              \
			FAIL(res);                                                                                                 \
	}

} // namespace duckdb
