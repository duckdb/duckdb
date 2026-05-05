#include "catch.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <string>
#include <vector>

using namespace duckdb; // NOLINT

static void FillVarcharVector(Vector &target, const duckdb::vector<string> &values) {
	auto writer = FlatVector::Writer<string_t>(target, values.size());
	for (const auto &value : values) {
		writer.WriteValue(StringVector::AddString(target, value));
	}
	FlatVector::SetSize(target, count_t(values.size()));
}

static duckdb::vector<hash_t> HashVector(Vector &input, idx_t count) {
	Vector hashes(LogicalType::HASH, count);
	VectorOperations::Hash(input, hashes, count);
	auto data = FlatVector::GetData<hash_t>(hashes);
	return duckdb::vector<hash_t>(data, data + count);
}

TEST_CASE("Vector hash matches dictionary slice values", "[vector][dictionary][hash]") {
	constexpr idx_t dictionary_size = 4;
	constexpr idx_t selection_size = 2;
	const duckdb::vector<string> initial_values = {"a", "b", "c", "d"};
	const duckdb::vector<string> updated_values = {"bb", "dd"};

	Vector base_vector(LogicalType::VARCHAR, dictionary_size);
	FillVarcharVector(base_vector, initial_values);

	SelectionVector selection(selection_size);
	selection.set_index(0, 1);
	selection.set_index(1, 3);

	Vector slice_vector = Vector::Ref(base_vector);
	slice_vector.Slice(selection, selection_size);
	REQUIRE(slice_vector.GetVectorType() == VectorType::DICTIONARY_VECTOR);

	Vector expected_initial(LogicalType::VARCHAR, selection_size);
	FillVarcharVector(expected_initial, {initial_values[1], initial_values[3]});

	auto slice_hash_initial = HashVector(slice_vector, selection_size);
	auto flat_hash_initial = HashVector(expected_initial, selection_size);
	REQUIRE(slice_hash_initial == flat_hash_initial);

	auto base_data = FlatVector::GetDataMutable<string_t>(base_vector);
	base_data[1] = StringVector::AddString(base_vector, updated_values[0]);
	base_data[3] = StringVector::AddString(base_vector, updated_values[1]);

	Vector expected_updated(LogicalType::VARCHAR, selection_size);
	FillVarcharVector(expected_updated, updated_values);

	auto slice_hash_updated = HashVector(slice_vector, selection_size);
	auto flat_hash_updated = HashVector(expected_updated, selection_size);
	REQUIRE(slice_hash_updated == flat_hash_updated);
	REQUIRE(slice_hash_updated != slice_hash_initial);
}
