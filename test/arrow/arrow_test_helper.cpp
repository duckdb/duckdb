#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/arrow/physical_arrow_collector.hpp"
#include "duckdb/common/arrow/arrow_query_result.hpp"
#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/relation/materialized_relation.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/relation/query_relation.hpp"

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
ArrowStreamTestFactory::CreateStream(uintptr_t this_ptr, duckdb::ArrowStreamParameters &parameters) {
	auto stream_wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
	stream_wrapper->arrow_array_stream = *(ArrowArrayStream *)this_ptr;

	return stream_wrapper;
}

void ArrowStreamTestFactory::GetSchema(ArrowArrayStream *arrow_array_stream, ArrowSchema &schema) {
	arrow_array_stream->get_schema(arrow_array_stream, &schema);
}

namespace duckdb {

int ArrowTestFactory::ArrowArrayStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream->private_data) {
		throw InternalException("No private data!?");
	}
	auto &data = *((ArrowArrayStreamData *)stream->private_data);
	data.factory.ToArrowSchema(out);
	return 0;
}

static int NextFromMaterialized(MaterializedQueryResult &res, bool big, ClientProperties properties,
                                struct ArrowArray *out) {
	auto &types = res.types;
	unordered_map<idx_t, const duckdb::shared_ptr<ArrowTypeExtensionData>> extension_type_cast;
	if (big) {
		// Combine all chunks into a single ArrowArray
		ArrowAppender appender(types, STANDARD_VECTOR_SIZE, properties, extension_type_cast);
		idx_t count = 0;
		while (true) {
			auto chunk = res.Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			count += chunk->size();
			appender.Append(*chunk, 0, chunk->size(), chunk->size());
		}
		if (count > 0) {
			*out = appender.Finalize();
		}
	} else {
		auto chunk = res.Fetch();
		if (!chunk || chunk->size() == 0) {
			return 0;
		}
		ArrowConverter::ToArrowArray(*chunk, out, properties, extension_type_cast);
	}
	return 0;
}

static int NextFromArrow(ArrowTestFactory &factory, struct ArrowArray *out) {
	auto &it = factory.chunk_iterator;

	unique_ptr<ArrowArrayWrapper> next_array;
	if (it != factory.prefetched_chunks.end()) {
		next_array = std::move(*it);
		it++;
	}

	if (!next_array) {
		return 0;
	}
	*out = next_array->arrow_array;
	next_array->arrow_array.release = nullptr;
	return 0;
}

int ArrowTestFactory::ArrowArrayStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream->private_data) {
		throw InternalException("No private data!?");
	}
	auto &data = *((ArrowArrayStreamData *)stream->private_data);
	if (data.factory.result->type == QueryResultType::MATERIALIZED_RESULT) {
		auto &materialized_result = data.factory.result->Cast<MaterializedQueryResult>();
		return NextFromMaterialized(materialized_result, data.factory.big_result, data.options, out);
	} else {
		D_ASSERT(data.factory.result->type == QueryResultType::ARROW_RESULT);
		return NextFromArrow(data.factory, out);
	}
}

const char *ArrowTestFactory::ArrowArrayStreamGetLastError(struct ArrowArrayStream *stream) {
	throw InternalException("Error!?!!");
}

void ArrowTestFactory::ArrowArrayStreamRelease(struct ArrowArrayStream *stream) {
	if (!stream || !stream->private_data) {
		return;
	}
	auto data = (ArrowArrayStreamData *)stream->private_data;
	delete data;
	stream->private_data = nullptr;
	stream->release = nullptr;
}

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> ArrowTestFactory::CreateStream(uintptr_t this_ptr,
                                                                                   ArrowStreamParameters &parameters) {
	//! Create a new batch reader
	auto &factory = *reinterpret_cast<ArrowTestFactory *>(this_ptr); //! NOLINT
	if (!factory.result) {
		throw InternalException("Stream already consumed!");
	}

	auto stream_wrapper = make_uniq<ArrowArrayStreamWrapper>();
	auto private_data = make_uniq<ArrowArrayStreamData>(factory, factory.options);
	stream_wrapper->arrow_array_stream.get_schema = ArrowArrayStreamGetSchema;
	stream_wrapper->arrow_array_stream.get_next = ArrowArrayStreamGetNext;
	stream_wrapper->arrow_array_stream.get_last_error = ArrowArrayStreamGetLastError;
	stream_wrapper->arrow_array_stream.release = ArrowArrayStreamRelease;
	stream_wrapper->arrow_array_stream.private_data = private_data.release();

	return stream_wrapper;
}

void ArrowTestFactory::GetSchema(ArrowArrayStream *factory_ptr, ArrowSchema &schema) {
	//! Create a new batch reader
	auto &factory = *reinterpret_cast<ArrowTestFactory *>(factory_ptr); //! NOLINT
	factory.ToArrowSchema(&schema);
}

void ArrowTestFactory::ToArrowSchema(struct ArrowSchema *out) {
	ArrowConverter::ToArrowSchema(out, types, names, options);
}

unique_ptr<QueryResult> ArrowTestHelper::ScanArrowObject(Connection &con, vector<Value> &params) {
	auto arrow_result = con.TableFunction("arrow_scan", params)->Execute();
	if (arrow_result->type != QueryResultType::MATERIALIZED_RESULT) {
		printf("Arrow Result must materialized");
		return nullptr;
	}
	if (arrow_result->HasError()) {
		printf("-------------------------------------\n");
		printf("Arrow round-trip query error: %s\n", arrow_result->GetError().c_str());
		printf("-------------------------------------\n");
		printf("-------------------------------------\n");
		return nullptr;
	}
	return arrow_result;
}

bool ArrowTestHelper::CompareResults(Connection &con, shared_ptr<Relation> arrow_tbl, const string &query) {
	// run FROM arrow_scan(...) EXCEPT ALL <query> - this should be empty

	shared_ptr<Relation> regular_result;
	try {
		regular_result = con.RelationFromQuery(query, "regular_result");
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (StringUtil::Contains(error.Message(), "Expected a single SELECT")) {
			return true;
		}
		printf("%s", error.Message().c_str());
		return false;
	}

	auto result = arrow_tbl->Except(regular_result)->Execute();
	if (result->HasError()) {
		std::ostringstream error_msg;
		error_msg << "-------------------------------------\n";
		error_msg << "Arrow round-trip type comparison failed\n";
		error_msg << "-------------------------------------\n";
		error_msg << "Query: " << query.c_str() << "\n";
		error_msg << "-------------------------------------\n";
		error_msg << "Query failed to execute:\n";
		error_msg << result->GetError();
		error_msg << "-------------------------------------\n";
		printf("%s", error_msg.str().c_str());
		return false;
	}
	vector<string> rows;
	for (auto &row : *result) {
		string row_str;
		for (idx_t c = 0; c < result->ColumnCount(); ++c) {
			if (!row_str.empty()) {
				row_str += "\t";
			}
			row_str += row.GetValue<string>(c);
		}
		rows.push_back(row_str);
	}
	if (!rows.empty()) {
		std::ostringstream error_msg;
		error_msg << "-------------------------------------\n";
		error_msg << "Arrow round-trip type comparison failed\n";
		error_msg << "-------------------------------------\n";
		error_msg << "Query: " << query.c_str() << "\n";
		error_msg << "-------------------------------------\n";
		error_msg << "Rows existed in Arrow result set but not in regular result set:\n";
		error_msg << "-------------------------------------\n";
		for (auto &row : rows) {
			error_msg << row << "\n";
		}
		printf("%s", error_msg.str().c_str());
		return false;
	}
	return true;
}

vector<Value> ArrowTestHelper::ConstructArrowScan(ArrowTestFactory &factory) {
	vector<Value> params;
	auto arrow_object = (uintptr_t)(&factory);
	params.push_back(Value::POINTER(arrow_object));
	params.push_back(Value::POINTER((uintptr_t)&ArrowTestFactory::CreateStream));
	params.push_back(Value::POINTER((uintptr_t)&ArrowTestFactory::GetSchema));
	return params;
}

vector<Value> ArrowTestHelper::ConstructArrowScan(ArrowArrayStream &stream) {
	vector<Value> params;
	auto arrow_object = (uintptr_t)(&stream);
	params.push_back(Value::POINTER(arrow_object));
	params.push_back(Value::POINTER((uintptr_t)&ArrowStreamTestFactory::CreateStream));
	params.push_back(Value::POINTER((uintptr_t)&ArrowStreamTestFactory::GetSchema));
	return params;
}

bool ArrowTestHelper::RunArrowComparison(Connection &con, const string &query, bool big_result) {
	unique_ptr<QueryResult> initial_result;

	// Using the PhysicalArrowCollector, we create a ArrowQueryResult from the result
	{
		auto &config = ClientConfig::GetConfig(*con.context);
		// we can't have a too large number here because a multiple of this batch size is passed into an allocation
		idx_t batch_size = big_result ? 1000000 : 10000;

		// Set up the result collector to use
		ScopedConfigSetting setting(
		    config,
		    [&batch_size](ClientConfig &config) {
			    config.get_result_collector =
			        [&batch_size](ClientContext &context, PreparedStatementData &data) -> unique_ptr<PhysicalOperator> {
				    return PhysicalArrowCollector::Create(context, data, batch_size);
			    };
		    },
		    [](ClientConfig &config) { config.get_result_collector = nullptr; });

		// run the query
		initial_result = con.context->Query(query, false);
		if (initial_result->HasError()) {
			initial_result->Print();
			printf("Query: %s\n", query.c_str());
			return false;
		}
	}

	auto client_properties = con.context->GetClientProperties();
	auto types = initial_result->types;
	auto names = initial_result->names;
	// We create an "arrow object" that consists of the arrays from our ArrowQueryResult
	ArrowTestFactory factory(std::move(types), std::move(names), std::move(initial_result), big_result,
	                         client_properties, *con.context);
	// And construct a `arrow_scan` to read the created "arrow object"
	auto params = ConstructArrowScan(factory);

	auto arrow_scan = con.TableFunction("arrow_scan", params);
	return CompareResults(con, std::move(arrow_scan), query);
}

bool ArrowTestHelper::RunArrowComparison(Connection &con, const string &query, ArrowArrayStream &arrow_stream) {
	unique_ptr<QueryResult> arrow_result;
	shared_ptr<Relation> arrow_scan;
	if (!arrow_stream.private_data) {
		// no data, treat as empty result
		vector<vector<Value>> values {{42}};
		arrow_scan = con.Values(values, {"i"}, "empty_arrow_tbl")->Filter("i > 100");
	} else {
		// construct the arrow scan
		auto params = ConstructArrowScan(arrow_stream);
		arrow_scan = con.TableFunction("arrow_scan", params);
	}

	auto success = CompareResults(con, std::move(arrow_scan), query);
	arrow_stream.release = nullptr;
	return success;
}

} // namespace duckdb
