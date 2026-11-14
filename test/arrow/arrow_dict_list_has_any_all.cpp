// list_has_any / list_has_all over Arrow LIST columns whose child is dictionary-encoded.

#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"

#include <vector>

using namespace duckdb;

namespace {

void ReleaseListSchema(ArrowSchema *s) {
	s->release = nullptr;
}
void ReleaseListArray(ArrowArray *a) {
	a->release = nullptr;
}

// One-column batch "a" of type LIST(dict int32), every list holding a single child slot.
struct DictListBatch {
	DictListBatch(std::vector<int32_t> dict_values_p, std::vector<int32_t> indices_p)
	    : dict_values(std::move(dict_values_p)), indices(std::move(indices_p)), offsets(indices.size() + 1) {
		for (idx_t i = 0; i < offsets.size(); i++) {
			offsets[i] = int32_t(i);
		}
		dict_schema.format = "i";
		dict_schema.release = ReleaseListSchema;
		dict_buffers[0] = nullptr;
		dict_buffers[1] = dict_values.data();
		dict_array.length = int64_t(dict_values.size());
		dict_array.n_buffers = 2;
		dict_array.buffers = dict_buffers;
		dict_array.release = ReleaseListArray;

		child_schema.format = "i";
		child_schema.name = "item";
		child_schema.flags = 2; // ARROW_FLAG_NULLABLE
		child_schema.dictionary = &dict_schema;
		child_schema.release = ReleaseListSchema;
		child_buffers[0] = nullptr;
		child_buffers[1] = indices.data();
		child_array.length = int64_t(indices.size());
		child_array.n_buffers = 2;
		child_array.buffers = child_buffers;
		child_array.dictionary = &dict_array;
		child_array.release = ReleaseListArray;

		list_schema_children[0] = &child_schema;
		list_schema.format = "+l";
		list_schema.name = "a";
		list_schema.flags = 2;
		list_schema.n_children = 1;
		list_schema.children = list_schema_children;
		list_schema.release = ReleaseListSchema;
		list_buffers[0] = nullptr;
		list_buffers[1] = offsets.data();
		list_array_children[0] = &child_array;
		list_array.length = int64_t(indices.size());
		list_array.n_buffers = 2;
		list_array.buffers = list_buffers;
		list_array.n_children = 1;
		list_array.children = list_array_children;
		list_array.release = ReleaseListArray;

		record_schema_children[0] = &list_schema;
		record_schema.format = "+s";
		record_schema.n_children = 1;
		record_schema.children = record_schema_children;
		record_schema.release = ReleaseListSchema;
		record_array_children[0] = &list_array;
		record_array.length = list_array.length;
		record_array.n_buffers = 1;
		record_array.buffers = record_buffers;
		record_array.n_children = 1;
		record_array.children = record_array_children;
		record_array.release = ReleaseListArray;
	}
	DictListBatch(const DictListBatch &) = delete;

	std::vector<int32_t> dict_values;
	std::vector<int32_t> indices;
	std::vector<int32_t> offsets;
	const void *dict_buffers[2];
	const void *child_buffers[2];
	const void *list_buffers[2];
	const void *record_buffers[1] = {nullptr};
	ArrowSchema *list_schema_children[1];
	ArrowArray *list_array_children[1];
	ArrowSchema *record_schema_children[1];
	ArrowArray *record_array_children[1];
	ArrowSchema dict_schema {};
	ArrowSchema child_schema {};
	ArrowSchema list_schema {};
	ArrowSchema record_schema {};
	ArrowArray dict_array {};
	ArrowArray child_array {};
	ArrowArray list_array {};
	ArrowArray record_array {};
};

// Projects "expressions" over arrow_scan of the batch and returns one row per list as strings.
vector<string> ScanProject(DictListBatch &batch, const string &expressions) {
	ArrowArrayStream stream {};
	AdbcError err {};
	REQUIRE(duckdb_adbc::BatchToArrayStream(&batch.record_array, &batch.record_schema, &stream, &err) ==
	        ADBC_STATUS_OK);
	DuckDB db(nullptr);
	Connection con(db);
	auto params = ArrowTestHelper::ConstructArrowScan(stream);
	auto result = con.TableFunction("arrow_scan", {}, {}, params)->Project(expressions)->Execute();
	stream.release = nullptr;
	REQUIRE(!result->HasError());
	vector<string> rows;
	while (auto chunk = result->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			string row;
			for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
				row += (c ? "," : "") + chunk->GetValue(c, r).ToString();
			}
			rows.push_back(row);
		}
	}
	return rows;
}

} // namespace

TEST_CASE("list_has_any/list_has_all on Arrow LIST(dict int32) with reordered dictionary indices", "[arrow]") {
	// lists are [30], [20], [10]
	DictListBatch batch({10, 20, 30}, {2, 1, 0});
	auto rows = ScanProject(batch, "list_has_any(a, [10]), list_has_all(a, [30]), a && [20], [10] <@ a");
	REQUIRE(rows == vector<string>({"false,true,false,false", "false,false,true,false", "true,false,false,true"}));
}

TEST_CASE("list_has_any/list_has_all on Arrow LIST(dict int32) with a dictionary larger than the list child",
          "[arrow]") {
	// the only list is [4095], its child slot points at the last of 4096 dictionary entries
	std::vector<int32_t> dict_values(4096);
	for (idx_t i = 0; i < dict_values.size(); i++) {
		dict_values[i] = int32_t(i);
	}
	DictListBatch batch(std::move(dict_values), {4095});
	auto rows = ScanProject(batch, "list_has_any(a, [4095]), list_has_all(a, [4095])");
	REQUIRE(rows == vector<string>({"true,true"}));
}
