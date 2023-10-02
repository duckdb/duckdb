#include "arrow/arrow_test_helper.hpp"

namespace duckdb {

int ArrowTestFactory::ArrowArrayStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream->private_data) {
		throw InternalException("No private data!?");
	}
	auto &data = *((ArrowArrayStreamData *)stream->private_data);
	data.factory.ToArrowSchema(out);
	return 0;
}

int ArrowTestFactory::ArrowArrayStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream->private_data) {
		throw InternalException("No private data!?");
	}
	auto &data = *((ArrowArrayStreamData *)stream->private_data);
	if (!data.factory.big_result) {
		auto chunk = data.factory.result->Fetch();
		if (!chunk || chunk->size() == 0) {
			return 0;
		}
		ArrowConverter::ToArrowArray(*chunk, out, data.options);
	} else {
		ArrowAppender appender(data.factory.result->types, STANDARD_VECTOR_SIZE, data.options);
		idx_t count = 0;
		while (true) {
			auto chunk = data.factory.result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			count += chunk->size();
			appender.Append(*chunk, 0, chunk->size(), chunk->size());
		}
		if (count > 0) {
			*out = appender.Finalize();
		}
	}
	return 0;
}

const char *ArrowTestFactory::ArrowArrayStreamGetLastError(struct ArrowArrayStream *stream) {
	throw InternalException("Error!?!!");
}

void ArrowTestFactory::ArrowArrayStreamRelease(struct ArrowArrayStream *stream) {
	if (!stream->private_data) {
		return;
	}
	auto data = (ArrowArrayStreamData *)stream->private_data;
	delete data;
	stream->private_data = nullptr;
}

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> ArrowTestFactory::CreateStream(uintptr_t this_ptr,
                                                                                   ArrowStreamParameters &parameters) {
	//! Create a new batch reader
	auto &factory = *reinterpret_cast<ArrowTestFactory *>(this_ptr); //! NOLINT
	if (!factory.result) {
		throw InternalException("Stream already consumed!");
	}

	auto stream_wrapper = make_uniq<ArrowArrayStreamWrapper>();
	stream_wrapper->number_of_rows = -1;
	auto private_data = make_uniq<ArrowArrayStreamData>(factory, factory.options);
	stream_wrapper->arrow_array_stream.get_schema = ArrowArrayStreamGetSchema;
	stream_wrapper->arrow_array_stream.get_next = ArrowArrayStreamGetNext;
	stream_wrapper->arrow_array_stream.get_last_error = ArrowArrayStreamGetLastError;
	stream_wrapper->arrow_array_stream.release = ArrowArrayStreamRelease;
	stream_wrapper->arrow_array_stream.private_data = private_data.release();

	return stream_wrapper;
}

void ArrowTestFactory::GetSchema(uintptr_t factory_ptr, duckdb::ArrowSchemaWrapper &schema) {
	//! Create a new batch reader
	auto &factory = *reinterpret_cast<ArrowTestFactory *>(factory_ptr); //! NOLINT
	factory.ToArrowSchema(&schema.arrow_schema);
}

void ArrowTestFactory::ToArrowSchema(struct ArrowSchema *out) {
	ArrowConverter::ToArrowSchema(out, types, names, options);
}

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
ArrowStreamTestFactory::CreateStream(uintptr_t this_ptr, ArrowStreamParameters &parameters) {
	auto stream_wrapper = make_uniq<ArrowArrayStreamWrapper>();
	stream_wrapper->number_of_rows = -1;
	stream_wrapper->arrow_array_stream = *(ArrowArrayStream *)this_ptr;

	return stream_wrapper;
}

void ArrowStreamTestFactory::GetSchema(uintptr_t factory_ptr, duckdb::ArrowSchemaWrapper &schema) {
	auto &factory = *reinterpret_cast<ArrowArrayStreamWrapper *>(factory_ptr); //! NOLINT
	factory.arrow_array_stream.get_schema(&factory.arrow_array_stream, &schema.arrow_schema);
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

bool ArrowTestHelper::CompareResults(unique_ptr<QueryResult> arrow, unique_ptr<MaterializedQueryResult> duck,
                                     const string &query) {
	auto &materialized_arrow = (MaterializedQueryResult &)*arrow;
	// compare the results
	string error;
	if (!ColumnDataCollection::ResultEquals(duck->Collection(), materialized_arrow.Collection(), error)) {
		printf("-------------------------------------\n");
		printf("Arrow round-trip failed: %s\n", error.c_str());
		printf("-------------------------------------\n");
		printf("Query: %s\n", query.c_str());
		printf("-----------------DuckDB-------------------\n");
		duck->Print();
		printf("-----------------Arrow--------------------\n");
		materialized_arrow.Print();
		printf("-------------------------------------\n");
		return false;
	}
	return true;
}

vector<Value> ArrowTestHelper::ConstructArrowScan(uintptr_t arrow_object, bool from_duckdb_result) {
	vector<Value> params;
	params.push_back(Value::POINTER(arrow_object));
	if (from_duckdb_result) {
		params.push_back(Value::POINTER((uintptr_t)&ArrowTestFactory::CreateStream));
		params.push_back(Value::POINTER((uintptr_t)&ArrowTestFactory::GetSchema));
	} else {
		params.push_back(Value::POINTER((uintptr_t)&ArrowStreamTestFactory::CreateStream));
		params.push_back(Value::POINTER((uintptr_t)&ArrowStreamTestFactory::GetSchema));
	}

	return params;
}

bool ArrowTestHelper::RunArrowComparison(Connection &con, const string &query, bool big_result) {
	// run the query
	auto initial_result = con.Query(query);
	if (initial_result->HasError()) {
		initial_result->Print();
		printf("Query: %s\n", query.c_str());
		return false;
	}
	// create the roundtrip factory
	auto client_properties = con.context->GetClientProperties();
	auto types = initial_result->types;
	auto names = initial_result->names;
	ArrowTestFactory factory(std::move(types), std::move(names), std::move(initial_result), big_result,
	                         client_properties);

	// construct the arrow scan
	auto params = ConstructArrowScan((uintptr_t)&factory, true);

	// run the arrow scan over the result
	auto arrow_result = ScanArrowObject(con, params);
	if (!arrow_result) {
		printf("Query: %s\n", query.c_str());
		return false;
	}

	return CompareResults(std::move(arrow_result), con.Query(query), query);
}

bool ArrowTestHelper::RunArrowComparison(Connection &con, const string &query, ArrowArrayStream &arrow_stream) {
	// construct the arrow scan
	auto params = ConstructArrowScan((uintptr_t)&arrow_stream, false);

	// run the arrow scan over the result
	auto arrow_result = ScanArrowObject(con, params);
	arrow_stream.release = nullptr;

	if (!arrow_result) {
		printf("Query: %s\n", query.c_str());
		return false;
	}

	return CompareResults(std::move(arrow_result), con.Query(query), query);
}

} // namespace duckdb
