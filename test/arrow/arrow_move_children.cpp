#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"

using namespace duckdb;

template <class T>
void AssertExpectedResult(ArrowArrayWrapper &array, T expected_value, bool is_null = false) {
	array.arrow_array.release(&array.arrow_array);
}

vector<ArrowArrayWrapper> FetchChildrenFromArray(shared_ptr<ArrowArrayWrapper> parent) {
	D_ASSERT(parent->arrow_array.release);
	vector<ArrowArrayWrapper> children;
	children.resize(parent->arrow_array.n_children);
	for (int64_t i = 0; i < parent->arrow_array.n_children; i++) {
		auto child = parent->arrow_array.children[i];
		auto &wrapper = children[i];
		wrapper.arrow_array = *child;
		// Unset the 'release' method to null for the child inside the parent
		// to indicate that it has been moved
		child->release = nullptr;
	}
	// Release the parent, should not affect the children
	parent->arrow_array.release(&parent->arrow_array);
	return children;
}

// https://arrow.apache.org/docs/format/CDataInterface.html#moving-child-arrays
TEST_CASE("Test move children", "[arrow]") {
	auto query = "select 'a', 'this is a long string', 42, true, NULL from range(4096);";

	// Create the stream that will produce arrow arrays
	DuckDB db(nullptr);
	Connection conn(db);
	auto initial_result = conn.Query(query);
	auto client_properties = conn.context->GetClientProperties();
	auto types = initial_result->types;
	auto names = initial_result->names;

	// Scan every column
	ArrowStreamParameters parameters;
	for (idx_t idx = 0; idx < initial_result->names.size(); idx++) {
		auto col_idx = idx;
		auto &name = initial_result->names[idx];
		if (col_idx != COLUMN_IDENTIFIER_ROW_ID) {
			parameters.projected_columns.projection_map[idx] = name;
			parameters.projected_columns.columns.emplace_back(name);
		}
	}

	// Create a test factory and produce a stream from it
	auto factory =
	    ArrowTestFactory(std::move(types), std::move(names), std::move(initial_result), false, client_properties);
	auto stream = ArrowTestFactory::CreateStream((uintptr_t)&factory, parameters);

	// For every array, extract the children and scan them
	while (true) {
		auto chunk = stream->GetNextChunk();
		if (!chunk || !chunk->arrow_array.release) {
			break;
		}
		auto children = FetchChildrenFromArray(std::move(chunk));
		D_ASSERT(children.size() == 5);
		for (idx_t i = 0; i < children.size(); i++) {
			if (i == 0) {
				AssertExpectedResult<string>(children[i], "a");
			} else if (i == 1) {
				AssertExpectedResult<string>(children[i], "this is a long string");
			} else if (i == 2) {
				AssertExpectedResult<int32_t>(children[i], 42);
			} else if (i == 3) {
				AssertExpectedResult<bool>(children[i], true);
			} else if (i == 4) {
				AssertExpectedResult<int32_t>(children[i], 0, true);
			} else {
				// Not possible
				REQUIRE(false);
			}
		}
	}
}
