#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"

#include <cstring>

using namespace duckdb;

//------------------------------------------------------------------------------
// Self-contained Arrow stream that produces a FixedShapeTensor column.
// All children are stored as members — no heap allocation for Arrow structs.
//------------------------------------------------------------------------------
struct FixedShapeTensorStream {
	idx_t rows;
	vector<float> flat_data;
	bool done = false;

	// Metadata storage (must outlive schema)
	string format_str;
	unsafe_unique_array<char> metadata_buf;

	// Schema tree: root(struct) → child(FixedSizeList) → elem(float)
	ArrowSchema root_schema, child_schema, elem_schema;
	ArrowSchema *root_children[1];
	ArrowSchema *child_children[1];

	// Array tree: root(struct) → child(FixedSizeList) → elem(values)
	ArrowArray root_array, child_array, elem_array;
	ArrowArray *root_children_arr[1];
	ArrowArray *child_children_arr[1];
	const void *root_buffers[1];
	const void *child_buffers[1];
	const void *elem_buffers[2];

	FixedShapeTensorStream(idx_t rows_p, vector<idx_t> shape, vector<idx_t> permutation = {}) : rows(rows_p) {
		idx_t flat_size = 1;
		for (auto d : shape) {
			flat_size *= d;
		}

		// Generate sequential float data
		flat_data.resize(rows * flat_size);
		for (idx_t i = 0; i < flat_data.size(); i++) {
			flat_data[i] = static_cast<float>(i);
		}

		// Build FixedSizeList format string "+w:N"
		format_str = "+w:" + to_string(flat_size);

		// Build arrow.fixed_shape_tensor extension metadata
		ArrowSchemaMetadata metadata;
		metadata.AddOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME, "arrow.fixed_shape_tensor");
		string meta_json = "{\"shape\": [";
		for (idx_t i = 0; i < shape.size(); i++) {
			if (i > 0) {
				meta_json += ", ";
			}
			meta_json += to_string(shape[i]);
		}
		meta_json += "]";
		if (!permutation.empty()) {
			meta_json += ", \"permutation\": [";
			for (idx_t i = 0; i < permutation.size(); i++) {
				if (i > 0) {
					meta_json += ", ";
				}
				meta_json += to_string(permutation[i]);
			}
			meta_json += "]";
		}
		meta_json += "}";
		metadata.AddOption(ArrowSchemaMetadata::ARROW_METADATA_KEY, meta_json);
		metadata_buf = metadata.SerializeMetadata();

		// Zero-init all Arrow structs
		memset(&root_schema, 0, sizeof(root_schema));
		memset(&child_schema, 0, sizeof(child_schema));
		memset(&elem_schema, 0, sizeof(elem_schema));
		memset(&root_array, 0, sizeof(root_array));
		memset(&child_array, 0, sizeof(child_array));
		memset(&elem_array, 0, sizeof(elem_array));
	}

	ArrowArrayStream CreateStream() {
		ArrowArrayStream stream;
		memset(&stream, 0, sizeof(stream));
		stream.get_schema = GetSchema;
		stream.get_next = GetNext;
		stream.get_last_error = [](ArrowArrayStream *) -> const char * {
			return nullptr;
		};
		stream.release = [](ArrowArrayStream *s) {
			s->release = nullptr;
		};
		stream.private_data = this;
		return stream;
	}

private:
	static void ReleaseSchema(ArrowSchema *s) {
		for (int64_t i = 0; i < s->n_children; i++) {
			if (s->children[i] && s->children[i]->release) {
				s->children[i]->release(s->children[i]);
			}
		}
		s->release = nullptr;
	}
	static void ReleaseArray(ArrowArray *a) {
		for (int64_t i = 0; i < a->n_children; i++) {
			if (a->children[i] && a->children[i]->release) {
				a->children[i]->release(a->children[i]);
			}
		}
		a->release = nullptr;
	}

	static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
		auto &self = *static_cast<FixedShapeTensorStream *>(stream->private_data);

		// Leaf: float32
		self.elem_schema.format = "f";
		self.elem_schema.name = "item";
		self.elem_schema.release = ReleaseSchema;

		// Child: FixedSizeList with tensor extension metadata
		self.child_children[0] = &self.elem_schema;
		self.child_schema.format = self.format_str.c_str();
		self.child_schema.name = "tensor";
		self.child_schema.metadata = self.metadata_buf.get();
		self.child_schema.n_children = 1;
		self.child_schema.children = self.child_children;
		self.child_schema.release = ReleaseSchema;

		// Root: struct
		self.root_children[0] = &self.child_schema;
		self.root_schema.format = "+s";
		self.root_schema.name = "";
		self.root_schema.n_children = 1;
		self.root_schema.children = self.root_children;
		self.root_schema.release = ReleaseSchema;

		*out = self.root_schema;
		return 0;
	}

	static int GetNext(ArrowArrayStream *stream, ArrowArray *out) {
		auto &self = *static_cast<FixedShapeTensorStream *>(stream->private_data);
		if (self.done) {
			out->release = nullptr;
			out->length = 0;
			return 0;
		}
		self.done = true;

		auto n = NumericCast<int64_t>(self.rows);
		auto total = NumericCast<int64_t>(self.flat_data.size());

		// Leaf: flat float values
		self.elem_buffers[0] = nullptr;
		self.elem_buffers[1] = self.flat_data.data();
		self.elem_array.length = total;
		self.elem_array.n_buffers = 2;
		self.elem_array.buffers = self.elem_buffers;
		self.elem_array.release = ReleaseArray;

		// Child: FixedSizeList
		self.child_buffers[0] = nullptr;
		self.child_children_arr[0] = &self.elem_array;
		self.child_array.length = n;
		self.child_array.n_buffers = 1;
		self.child_array.buffers = self.child_buffers;
		self.child_array.n_children = 1;
		self.child_array.children = self.child_children_arr;
		self.child_array.release = ReleaseArray;

		// Root: struct
		self.root_buffers[0] = nullptr;
		self.root_children_arr[0] = &self.child_array;
		self.root_array.length = n;
		self.root_array.n_buffers = 1;
		self.root_array.buffers = self.root_buffers;
		self.root_array.n_children = 1;
		self.root_array.children = self.root_children_arr;
		self.root_array.release = ReleaseArray;

		*out = self.root_array;
		return 0;
	}
};

TEST_CASE("Test FixedShapeTensor Arrow import", "[arrow]") {
	// Simulate an external Arrow source producing 5 rows of shape [2,3] float tensors
	FixedShapeTensorStream tensor_stream(5, {2, 3});
	auto stream = tensor_stream.CreateStream();

	DuckDB db;
	Connection con(db);

	auto params = ArrowTestHelper::ConstructArrowScan(stream);
	auto result = ArrowTestHelper::ScanArrowObject(con, params);
	REQUIRE(result);
	REQUIRE(!result->HasError());

	// Verify the result type is a nested 2D ARRAY
	REQUIRE(result->types.size() == 1);
	auto &result_type = result->types[0];
	REQUIRE(result_type.id() == LogicalTypeId::ARRAY);
	auto &inner_type = ArrayType::GetChildType(result_type);
	REQUIRE(inner_type.id() == LogicalTypeId::ARRAY);
	REQUIRE(ArrayType::GetSize(result_type) == 2);
	REQUIRE(ArrayType::GetSize(inner_type) == 3);

	// Verify data values — collect across chunks to tolerate small STANDARD_VECTOR_SIZE
	vector<string> rows;
	while (auto chunk = result->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			rows.push_back(chunk->GetValue(0, i).ToString());
		}
	}
	REQUIRE(rows.size() == 5);
	REQUIRE(rows[0] == "[[0.0, 1.0, 2.0], [3.0, 4.0, 5.0]]");
	REQUIRE(rows[4] == "[[24.0, 25.0, 26.0], [27.0, 28.0, 29.0]]");
}

TEST_CASE("Test FixedShapeTensor Arrow import with column-major permutation", "[arrow]") {
	// shape [2, 3] with permutation [1, 0] means column-major storage
	// Flat data is sequential: 0,1,2,3,4,5
	// With permutation [1,0], physical dim 0 = logical dim 1 (cols=3), physical dim 1 = logical dim 0 (rows=2)
	// So flat layout is column-major: [0][0]=0, [1][0]=1, [0][1]=2, [1][1]=3, [0][2]=4, [1][2]=5
	// After reordering to row-major: [0][0]=0, [0][1]=2, [0][2]=4, [1][0]=1, [1][1]=3, [1][2]=5
	FixedShapeTensorStream tensor_stream(2, {2, 3}, {1, 0});
	auto stream = tensor_stream.CreateStream();

	DuckDB db;
	Connection con(db);

	auto params = ArrowTestHelper::ConstructArrowScan(stream);
	auto result = ArrowTestHelper::ScanArrowObject(con, params);
	REQUIRE(result);
	REQUIRE(!result->HasError());

	// Type should still be [2][3] (logical shape, not physical)
	REQUIRE(result->types.size() == 1);
	auto &result_type = result->types[0];
	REQUIRE(result_type.id() == LogicalTypeId::ARRAY);
	REQUIRE(ArrayType::GetSize(result_type) == 2);
	REQUIRE(ArrayType::GetSize(ArrayType::GetChildType(result_type)) == 3);

	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 2);

	// Row 0: flat [0,1,2,3,4,5] column-major → row-major [[0,2,4],[1,3,5]]
	REQUIRE(chunk->GetValue(0, 0).ToString() == "[[0.0, 2.0, 4.0], [1.0, 3.0, 5.0]]");
	// Row 1: flat [6,7,8,9,10,11] column-major → row-major [[6,8,10],[7,9,11]]
	REQUIRE(chunk->GetValue(0, 1).ToString() == "[[6.0, 8.0, 10.0], [7.0, 9.0, 11.0]]");
}

TEST_CASE("Test FixedShapeTensor Arrow import with identity permutation", "[arrow]") {
	// Identity permutation [0, 1] should behave the same as no permutation
	FixedShapeTensorStream tensor_stream(2, {2, 3}, {0, 1});
	auto stream = tensor_stream.CreateStream();

	DuckDB db;
	Connection con(db);

	auto params = ArrowTestHelper::ConstructArrowScan(stream);
	auto result = ArrowTestHelper::ScanArrowObject(con, params);
	REQUIRE(result);
	REQUIRE(!result->HasError());

	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 2);
	// Identity permutation: data stays in original order
	REQUIRE(chunk->GetValue(0, 0).ToString() == "[[0.0, 1.0, 2.0], [3.0, 4.0, 5.0]]");
	REQUIRE(chunk->GetValue(0, 1).ToString() == "[[6.0, 7.0, 8.0], [9.0, 10.0, 11.0]]");
}

TEST_CASE("Test nested array Arrow round-trip (DuckDB -> Arrow -> DuckDB)", "[arrow]") {
	DuckDB db;
	Connection con(db);

	// 1D array baseline
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT array_value(i, i+1, i+2)::INT[3] FROM range(10) tbl(i)",
	                                            true));

	// 2D square array
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT [[i, i+1],[i+2, i+3]]::FLOAT[2,2] FROM range(10) tbl(i)",
	                                            true));

	// 2D non-square array
	REQUIRE(ArrowTestHelper::RunArrowComparison(
	    con, "SELECT [[i, i+1, i+2],[i+3, i+4, i+5]]::DOUBLE[3,2] FROM range(10) tbl(i)", true));

	// Large batch (> STANDARD_VECTOR_SIZE)
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT [[i, i+1],[i+2, i+3]]::FLOAT[2,2] FROM range(3000) tbl(i)",
	                                            true));

	// Small batch variant
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT [[i, i+1],[i+2, i+3]]::FLOAT[2,2] FROM range(10) tbl(i)",
	                                            false));
}

TEST_CASE("Test nested array exports as FixedShapeTensor with arrow_output_fixed_shape_tensor", "[arrow]") {
	DuckDB db;
	Connection con(db);
	con.Query("SET arrow_output_fixed_shape_tensor = true");

	// 2D square array round-trip through FixedShapeTensor
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT [[i, i+1],[i+2, i+3]]::FLOAT[2,2] FROM range(10) tbl(i)",
	                                            true));

	// 2D non-square array
	REQUIRE(ArrowTestHelper::RunArrowComparison(
	    con, "SELECT [[i, i+1, i+2],[i+3, i+4, i+5]]::DOUBLE[3,2] FROM range(10) tbl(i)", true));

	// Large batch (> STANDARD_VECTOR_SIZE)
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT [[i, i+1],[i+2, i+3]]::FLOAT[2,2] FROM range(3000) tbl(i)",
	                                            true));

	// 1D arrays should NOT be affected (still regular FixedSizeList)
	REQUIRE(ArrowTestHelper::RunArrowComparison(con, "SELECT array_value(i, i+1, i+2)::INT[3] FROM range(10) tbl(i)",
	                                            true));
}

TEST_CASE("Test tensor functions end-to-end", "[arrow]") {
	DuckDB db;
	Connection con(db);

	// transpose(transpose(x)) == x
	auto result =
	    con.Query("SELECT transpose(transpose([[1,2,3],[4,5,6]]::FLOAT[3,2])) = [[1,2,3],[4,5,6]]::FLOAT[3,2]");
	REQUIRE(!result->HasError());
	REQUIRE(result->GetValue(0, 0).GetValue<bool>() == true);

	// flatten(reshape(x)) == x
	result = con.Query("SELECT array_flatten(reshape(array_value(1.0,2.0,3.0,4.0,5.0,6.0)::FLOAT[6], 2, 3)) = "
	                   "array_value(1.0,2.0,3.0,4.0,5.0,6.0)::FLOAT[6]");
	REQUIRE(!result->HasError());
	REQUIRE(result->GetValue(0, 0).GetValue<bool>() == true);

	// array_matmul with identity
	result = con.Query(
	    "SELECT array_matmul([[1,2],[3,4]]::FLOAT[2,2], [[1,0],[0,1]]::FLOAT[2,2]) = [[1,2],[3,4]]::FLOAT[2,2]");
	REQUIRE(!result->HasError());
	REQUIRE(result->GetValue(0, 0).GetValue<bool>() == true);
}
