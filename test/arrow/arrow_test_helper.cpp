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
		ArrowConverter::ToArrowArray(*chunk, out);
	} else {
		ArrowAppender appender(data.factory.result->types, STANDARD_VECTOR_SIZE);
		idx_t count = 0;
		while (true) {
			auto chunk = data.factory.result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			count += chunk->size();
			appender.Append(*chunk);
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
	auto private_data = make_uniq<ArrowArrayStreamData>(factory);
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
	ArrowConverter::ToArrowSchema(out, types, names, tz);
}

bool ArrowTestHelper::RunArrowComparison(Connection &con, const string &query, bool big_result) {
	// run the query
	auto initial_result = con.Query(query);
	if (initial_result->HasError()) {
		initial_result->Print();
		return false;
	}
	// create the roundtrip factory
	auto tz = ClientConfig::GetConfig(*con.context).ExtractTimezone();
	auto types = initial_result->types;
	auto names = initial_result->names;
	ArrowTestFactory factory(std::move(types), std::move(names), tz, std::move(initial_result), big_result);

	// construct the arrow scan
	vector<Value> params;
	params.push_back(Value::POINTER((uintptr_t)&factory));
	params.push_back(Value::POINTER((uintptr_t)&ArrowTestFactory::CreateStream));
	params.push_back(Value::POINTER((uintptr_t)&ArrowTestFactory::GetSchema));

	// run the arrow scan over the result
	auto arrow_result = con.TableFunction("arrow_scan", params)->Execute();
	if (arrow_result->type != QueryResultType::MATERIALIZED_RESULT) {
		printf("Arrow Result must materialized");
		return false;
	}
	if (arrow_result->HasError()) {
		printf("-------------------------------------\n");
		printf("Arrow round-trip query error: %s\n", arrow_result->GetError().c_str());
		printf("-------------------------------------\n");
		printf("Query: %s\n", query.c_str());
		printf("-------------------------------------\n");
		return false;
	}
	auto &materialized_arrow = (MaterializedQueryResult &)*arrow_result;

	auto result = con.Query(query);

	// compare the results
	string error;
	if (!ColumnDataCollection::ResultEquals(result->Collection(), materialized_arrow.Collection(), error)) {
		printf("-------------------------------------\n");
		printf("Arrow round-trip failed: %s\n", error.c_str());
		printf("-------------------------------------\n");
		printf("Query: %s\n", query.c_str());
		printf("-----------------DuckDB-------------------\n");
		result->Print();
		printf("-----------------Arrow--------------------\n");
		materialized_arrow.Print();
		printf("-------------------------------------\n");
		return false;
	}
	return true;
}

} // namespace duckdb
