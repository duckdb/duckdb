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
#include "duckdb/common/arrow/arrow_query_result.hpp"

class ArrowStreamTestFactory {
public:
	static duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> CreateStream(uintptr_t this_ptr,
	                                                                        duckdb::ArrowStreamParameters &parameters);

	static void GetSchema(ArrowArrayStream *arrow_array_stream, ArrowSchema &schema);
};

namespace duckdb {
class ArrowTestFactory {
public:
	ArrowTestFactory(vector<LogicalType> types_p, vector<string> names_p, duckdb::unique_ptr<QueryResult> result_p,
	                 bool big_result, ClientProperties options)
	    : types(std::move(types_p)), names(std::move(names_p)), result(std::move(result_p)), big_result(big_result),
	      options(options) {
		if (result->type == QueryResultType::ARROW_RESULT) {
			auto &arrow_result = result->Cast<ArrowQueryResult>();
			prefetched_chunks = arrow_result.ConsumeArrays();
			chunk_iterator = prefetched_chunks.begin();
		}
	}

	vector<LogicalType> types;
	vector<string> names;
	duckdb::unique_ptr<QueryResult> result;
	vector<unique_ptr<ArrowArrayWrapper>> prefetched_chunks;
	vector<unique_ptr<ArrowArrayWrapper>>::iterator chunk_iterator;
	bool big_result;
	ClientProperties options;

	struct ArrowArrayStreamData {
		explicit ArrowArrayStreamData(ArrowTestFactory &factory, ClientProperties options)
		    : factory(factory), options(options) {
		}

		ArrowTestFactory &factory;
		ClientProperties options;
	};

	static int ArrowArrayStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out);

	static int ArrowArrayStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out);

	static const char *ArrowArrayStreamGetLastError(struct ArrowArrayStream *stream);

	static void ArrowArrayStreamRelease(struct ArrowArrayStream *stream);

	static duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> CreateStream(uintptr_t this_ptr,
	                                                                        ArrowStreamParameters &parameters);

	static void GetSchema(ArrowArrayStream *, ArrowSchema &schema);

	void ToArrowSchema(struct ArrowSchema *out);
};

class ArrowTestHelper {
public:
	//! Used in the Arrow Roundtrip Tests
	static bool RunArrowComparison(Connection &con, const string &query, bool big_result = false);
	//! Used in the ADBC Testing
	static bool RunArrowComparison(Connection &con, const string &query, ArrowArrayStream &arrow_stream);

private:
	static bool CompareResults(Connection &con, unique_ptr<QueryResult> arrow, unique_ptr<MaterializedQueryResult> duck,
	                           const string &query);

public:
	static unique_ptr<QueryResult> ScanArrowObject(Connection &con, vector<Value> &params);
	static vector<Value> ConstructArrowScan(ArrowTestFactory &factory);
	static vector<Value> ConstructArrowScan(ArrowArrayStream &stream);
};
} // namespace duckdb
