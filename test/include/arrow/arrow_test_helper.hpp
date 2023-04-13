//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// arrow/arrow_test_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "test_helpers.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {
class ArrowTestFactory {
public:
	ArrowTestFactory(vector<LogicalType> types_p, vector<string> names_p, string tz_p,
	                 duckdb::unique_ptr<QueryResult> result_p, bool big_result)
	    : types(std::move(types_p)), names(std::move(names_p)), tz(std::move(tz_p)), result(std::move(result_p)),
	      big_result(big_result) {
	}

	vector<LogicalType> types;
	vector<string> names;
	string tz;
	duckdb::unique_ptr<QueryResult> result;
	bool big_result;
	struct ArrowArrayStreamData {
		explicit ArrowArrayStreamData(ArrowTestFactory &factory) : factory(factory) {
		}

		ArrowTestFactory &factory;
	};

	static int ArrowArrayStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out);

	static int ArrowArrayStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out);

	static const char *ArrowArrayStreamGetLastError(struct ArrowArrayStream *stream);

	static void ArrowArrayStreamRelease(struct ArrowArrayStream *stream);

	static duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> CreateStream(uintptr_t this_ptr,
	                                                                        ArrowStreamParameters &parameters);

	static void GetSchema(uintptr_t factory_ptr, duckdb::ArrowSchemaWrapper &schema);

	void ToArrowSchema(struct ArrowSchema *out);
};

class ArrowTestHelper {
public:
	static bool RunArrowComparison(Connection &con, const string &query, bool big_result = false);
};
} // namespace duckdb
