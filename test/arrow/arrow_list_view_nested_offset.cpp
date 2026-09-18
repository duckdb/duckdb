#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"

#include <string>
#include <utility>
#include <vector>

using namespace duckdb;

namespace {

void ListViewReleaseSchema(ArrowSchema *schema) {
	schema->release = nullptr;
}

void ListViewReleaseArray(ArrowArray *array) {
	array->release = nullptr;
}

template <class OFFSET_TYPE>
struct ListViewColumn {
	ListViewColumn(std::vector<OFFSET_TYPE> offsets_p, std::vector<OFFSET_TYPE> sizes_p, std::vector<int32_t> values_p)
	    : offsets(std::move(offsets_p)), sizes(std::move(sizes_p)), values(std::move(values_p)) {
		child_schema.format = "i";
		child_schema.name = "item";
		child_schema.flags = 2;
		child_schema.release = ListViewReleaseSchema;
		child_buffers[0] = nullptr;
		child_buffers[1] = values.data();
		child_array.length = int64_t(values.size());
		child_array.n_buffers = 2;
		child_array.buffers = child_buffers;
		child_array.release = ListViewReleaseArray;

		schema.format = sizeof(OFFSET_TYPE) == sizeof(int32_t) ? "+vl" : "+vL";
		schema.name = "a";
		schema.flags = 2;
		schema.n_children = 1;
		schema.children = schema_children;
		schema.release = ListViewReleaseSchema;
		schema_children[0] = &child_schema;

		buffers[0] = nullptr;
		buffers[1] = offsets.data();
		buffers[2] = sizes.data();
		array.length = int64_t(offsets.size());
		array.n_buffers = 3;
		array.buffers = buffers;
		array.n_children = 1;
		array.children = array_children;
		array.release = ListViewReleaseArray;
		array_children[0] = &child_array;
	}
	ListViewColumn(const ListViewColumn &) = delete;

	std::vector<OFFSET_TYPE> offsets;
	std::vector<OFFSET_TYPE> sizes;
	std::vector<int32_t> values;
	const void *child_buffers[2];
	const void *buffers[3];
	ArrowSchema *schema_children[1];
	ArrowArray *array_children[1];
	ArrowSchema child_schema {};
	ArrowSchema schema {};
	ArrowArray child_array {};
	ArrowArray array {};
};

bool ListViewScanMatches(ArrowSchema &column_schema, ArrowArray &column_array, const string &query) {
	ArrowSchema *schema_children[1] = {&column_schema};
	ArrowSchema record_schema {};
	record_schema.format = "+s";
	record_schema.n_children = 1;
	record_schema.children = schema_children;
	record_schema.release = ListViewReleaseSchema;

	ArrowArray *array_children[1] = {&column_array};
	const void *buffers[1] = {nullptr};
	ArrowArray record_array {};
	record_array.length = column_array.length;
	record_array.n_buffers = 1;
	record_array.buffers = buffers;
	record_array.n_children = 1;
	record_array.children = array_children;
	record_array.release = ListViewReleaseArray;

	ArrowArrayStream stream {};
	AdbcError error {};
	if (duckdb_adbc::BatchToArrayStream(&record_array, &record_schema, &stream, &error) != ADBC_STATUS_OK) {
		return false;
	}
	DuckDB db(nullptr);
	Connection con(db);
	return ArrowTestHelper::RunArrowComparison(con, query, stream);
}

} // namespace

TEST_CASE("Arrow scan of ListView with disjoint child ranges", "[arrow]") {
	ListViewColumn<int32_t> column({4, 0}, {3, 2}, {0, 1, 2, 3, 4, 5, 6});
	REQUIRE(
	    ListViewScanMatches(column.schema, column.array,
	                        "SELECT CASE WHEN r = 0 THEN [4, 5, 6]::INT[] ELSE [0, 1]::INT[] END FROM range(2) t(r)"));
}

TEST_CASE("Arrow scan of ListView with overlapping child ranges", "[arrow]") {
	ListViewColumn<int64_t> column({0, 0}, {3, 3}, {10, 20, 30});
	REQUIRE(ListViewScanMatches(column.schema, column.array, "SELECT [10, 20, 30]::INT[] FROM range(2)"));
}
