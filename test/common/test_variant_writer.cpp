#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/types/variant_writer.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

using namespace duckdb;

namespace {

//! Primitives, objects and arrays, with strings and keys that do not fit in an inlined string_t, and a SQL NULL
struct VariantSource {
	explicit VariantSource(Connection &con) {
		result = con.Query(R"(
			SELECT v FROM (VALUES
				(42::VARIANT),
				('a string that does not fit in an inlined string_t'::VARIANT),
				({'name': 'duck', 'tags': ['a', 'b'], 'nested': {'x': 1.5, 'y': NULL}}::VARIANT),
				([[1, 2], [3, 4]]::VARIANT),
				(NULL::VARIANT),
				({'a key that does not fit in an inlined string_t': [{'k': 1}, {'k': 2}]}::VARIANT),
				(DATE '2024-01-01'::VARIANT),
				(1.25::DECIMAL(5, 2)::VARIANT)
			) t(v)
		)");
		REQUIRE_NO_FAIL(*result);
		// the rows arrive in several chunks when STANDARD_VECTOR_SIZE is smaller than 8
		const idx_t row_count = 8;
		chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::DefaultAllocator(), result->GetTypes(), row_count);
		while (auto next = result->Fetch()) {
			if (next->size() == 0) {
				break;
			}
			chunk->Append(*next);
		}
		REQUIRE(chunk->size() == row_count);
	}

	Vector &Variants() {
		return chunk->data[0];
	}
	idx_t Count() const {
		return chunk->size();
	}

	duckdb::unique_ptr<QueryResult> result;
	duckdb::unique_ptr<DataChunk> chunk;
};

//! The source rows of a list written to a LIST(VARIANT)
struct SourceList {
	bool is_null;
	duckdb::vector<idx_t> rows;
};

duckdb::vector<SourceList> SourceLists() {
	return {{false, {0, 1, 2}}, {true, {}}, {false, {}}, {false, {3, 4, 5}}, {false, {6, 7, 2, 5}}};
}

void WriteRow(const VectorIterator<VectorVariantType> &variants, idx_t row, VectorWriter<VectorVariantType> &writer) {
	if (!variants.RowIsValid(row)) {
		writer.WriteNull();
		return;
	}
	writer.WriteValue(variants[row]);
}

void RequireSameValue(const Value &expected, const Value &actual) {
	REQUIRE(expected.IsNull() == actual.IsNull());
	if (!expected.IsNull()) {
		REQUIRE(expected.ToString() == actual.ToString());
	}
}

void RequireList(const Vector &input, const Value &actual, const SourceList &list) {
	REQUIRE(actual.IsNull() == list.is_null);
	if (list.is_null) {
		return;
	}
	auto &elements = ListValue::GetChildren(actual);
	REQUIRE(elements.size() == list.rows.size());
	for (idx_t i = 0; i < elements.size(); i++) {
		RequireSameValue(input.GetValue(list.rows[i]), elements[i]);
	}
}

} // namespace

TEST_CASE("VectorWriter<VectorVariantType> copies VARIANT values", "[variant][variant_writer]") {
	DuckDB db(nullptr);
	Connection con(db);
	VariantSource source(con);
	auto &input = source.Variants();
	const auto count = source.Count();
	const VectorIterator<VectorVariantType> variants(input);

	Vector result(LogicalType::VARIANT(), count);
	SECTION("WriteValue(node)") {
		auto writer = FlatVector::Writer<VectorVariantType>(result, count);
		for (idx_t row = 0; row < count; row++) {
			WriteRow(variants, row, writer);
		}
	}
	SECTION("WriteValue(node, type_id)") {
		auto writer = FlatVector::Writer<VectorVariantType>(result, count);
		for (idx_t row = 0; row < count; row++) {
			if (!variants.RowIsValid(row)) {
				writer.WriteNull();
				continue;
			}
			const auto node = variants[row];
			writer.WriteValue(node, node.GetTypeId());
		}
	}
	result.Verify();
	for (idx_t row = 0; row < count; row++) {
		RequireSameValue(input.GetValue(row), result.GetValue(row));
	}
}

TEST_CASE("VectorWriter<VectorVariantType> copies nested VARIANT values", "[variant][variant_writer]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto query = con.Query("SELECT v, variant_extract(v, 'name'), variant_extract(v, 'tags'), "
	                       "variant_extract(v, 'nested') "
	                       "FROM (SELECT {'name': 'duck', 'tags': ['a', 'b'], 'nested': {'x': 1.5}}::VARIANT v)");
	REQUIRE_NO_FAIL(*query);
	auto chunk = query->Fetch();
	REQUIRE(chunk);
	const VectorIterator<VectorVariantType> variants(chunk->data[0]);
	const auto root = variants[0];

	const duckdb::vector<string> keys {"name", "tags", "nested"};
	duckdb::vector<VariantNode> nodes;
	for (auto &key : keys) {
		for (const auto &entry : root.GetObjectChildren()) {
			if (entry.key == string_t(key)) {
				nodes.push_back(entry.value);
			}
		}
	}
	REQUIRE(nodes.size() == keys.size());

	// a NULL node is written as a VARIANT null value, not as a SQL NULL
	Vector result(LogicalType::VARIANT(), nodes.size() + 1);
	{
		auto writer = FlatVector::Writer<VectorVariantType>(result, nodes.size() + 1);
		for (auto &node : nodes) {
			writer.WriteValue(node);
		}
		writer.WriteValue(VariantNode());
	}
	result.Verify();
	for (idx_t i = 0; i < nodes.size(); i++) {
		RequireSameValue(chunk->data[i + 1].GetValue(0), result.GetValue(i));
	}
	const VectorIterator<VectorVariantType> written(result);
	REQUIRE(written.RowIsValid(nodes.size()));
	REQUIRE(written[nodes.size()].GetTypeId() == LogicalTypeId::SQLNULL);
}

TEST_CASE("VectorWriter<VectorVariantType> appends at an offset", "[variant][variant_writer]") {
	DuckDB db(nullptr);
	Connection con(db);
	VariantSource source(con);
	auto &input = source.Variants();
	const auto count = source.Count();
	const VectorIterator<VectorVariantType> variants(input);
	const idx_t split = 3;

	Vector result(LogicalType::VARIANT(), count);
	SECTION("after another writer") {
		auto writer = FlatVector::Writer<VectorVariantType>(result, split);
		for (idx_t row = 0; row < split; row++) {
			WriteRow(variants, row, writer);
		}
	}
	SECTION("after BuildVariant") {
		// BuildVariant leaves the keys as a dictionary vector
		VariantIterator iterator(input);
		VariantUtils::ToVariant(iterator, split, result);
	}
	{
		auto writer = FlatVector::Writer<VectorVariantType>(result, count - split, split);
		for (idx_t row = split; row < count; row++) {
			WriteRow(variants, row, writer);
		}
	}
	result.Verify();
	for (idx_t row = 0; row < count; row++) {
		RequireSameValue(input.GetValue(row), result.GetValue(row));
	}
}

TEST_CASE("VectorWriter<VectorListType<VectorVariantType>> writes LIST(VARIANT)", "[variant][variant_writer]") {
	DuckDB db(nullptr);
	Connection con(db);
	VariantSource source(con);
	auto &input = source.Variants();
	const VectorIterator<VectorVariantType> variants(input);
	const auto lists = SourceLists();

	enum class ListMode { FIXED, DYNAMIC, MIXED };
	auto write_lists = [&](Vector &result, ListMode mode) {
		auto writer = FlatVector::Writer<VectorListType<VectorVariantType>>(result, lists.size());
		for (idx_t list_idx = 0; list_idx < lists.size(); list_idx++) {
			auto &list = lists[list_idx];
			if (list.is_null) {
				writer.WriteNull();
				continue;
			}
			const auto dynamic = mode == ListMode::DYNAMIC || (mode == ListMode::MIXED && list_idx % 2 == 1);
			if (dynamic) {
				auto list_writer = writer.WriteDynamicList();
				for (auto row : list.rows) {
					WriteRow(variants, row, list_writer.WriteElement());
				}
				continue;
			}
			idx_t element_idx = 0;
			for (auto &element_writer : writer.WriteList(list.rows.size())) {
				WriteRow(variants, list.rows[element_idx++], element_writer);
			}
		}
	};

	Vector result(LogicalType::LIST(LogicalType::VARIANT()), lists.size());
	SECTION("WriteList") {
		write_lists(result, ListMode::FIXED);
	}
	SECTION("WriteDynamicList") {
		write_lists(result, ListMode::DYNAMIC);
	}
	SECTION("WriteList and WriteDynamicList") {
		write_lists(result, ListMode::MIXED);
	}
	result.Verify();
	for (idx_t i = 0; i < lists.size(); i++) {
		RequireList(input, result.GetValue(i), lists[i]);
	}
}

TEST_CASE("VectorWriter<VectorListType<VectorVariantType>> grows the element vector", "[variant][variant_writer]") {
	DuckDB db(nullptr);
	Connection con(db);
	VariantSource source(con);
	auto &input = source.Variants();
	const auto count = source.Count();
	const VectorIterator<VectorVariantType> variants(input);

	SECTION("many lists") {
		const idx_t list_count = 3000;
		const idx_t list_length = 3;
		Vector result(LogicalType::LIST(LogicalType::VARIANT()), list_count);
		{
			auto writer = FlatVector::Writer<VectorListType<VectorVariantType>>(result, list_count);
			for (idx_t list_idx = 0; list_idx < list_count; list_idx++) {
				idx_t element_idx = 0;
				for (auto &element_writer : writer.WriteList(list_length)) {
					WriteRow(variants, (list_idx + element_idx++) % count, element_writer);
				}
			}
		}
		result.Verify();
		for (idx_t list_idx = 0; list_idx < list_count; list_idx++) {
			SourceList list {false, {}};
			for (idx_t element_idx = 0; element_idx < list_length; element_idx++) {
				list.rows.push_back((list_idx + element_idx) % count);
			}
			RequireList(input, result.GetValue(list_idx), list);
		}
	}
	SECTION("one long dynamic list") {
		const idx_t list_length = 5000;
		Vector result(LogicalType::LIST(LogicalType::VARIANT()), 1);
		{
			auto writer = FlatVector::Writer<VectorListType<VectorVariantType>>(result, 1);
			auto list_writer = writer.WriteDynamicList();
			for (idx_t element_idx = 0; element_idx < list_length; element_idx++) {
				WriteRow(variants, element_idx % count, list_writer.WriteElement());
			}
		}
		result.Verify();
		SourceList list {false, {}};
		for (idx_t element_idx = 0; element_idx < list_length; element_idx++) {
			list.rows.push_back(element_idx % count);
		}
		RequireList(input, result.GetValue(0), list);
	}
}

TEST_CASE("VectorWriter<VectorVariantType> writes nested VARIANT children", "[variant][variant_writer]") {
	DuckDB db(nullptr);
	Connection con(db);
	VariantSource source(con);
	auto &input = source.Variants();
	const auto count = source.Count();
	const VectorIterator<VectorVariantType> variants(input);
	const auto lists = SourceLists();

	SECTION("STRUCT(VARIANT, BIGINT)") {
		const idx_t null_row = 1;
		Vector result(LogicalType::STRUCT({{"v", LogicalType::VARIANT()}, {"i", LogicalType::BIGINT}}), count);
		{
			auto writer = FlatVector::Writer<VectorStructType<VectorVariantType, int64_t>>(result, count);
			for (idx_t row = 0; row < count; row++) {
				if (row == null_row) {
					writer.WriteNull();
					continue;
				}
				writer.WriteValue(
				    [&](VectorWriter<VectorVariantType> &variant_writer, VectorWriter<int64_t> &int_writer) {
					    WriteRow(variants, row, variant_writer);
					    int_writer.WriteValue(NumericCast<int64_t>(row));
				    });
			}
		}
		result.Verify();
		for (idx_t row = 0; row < count; row++) {
			auto value = result.GetValue(row);
			REQUIRE(value.IsNull() == (row == null_row));
			if (row == null_row) {
				continue;
			}
			auto &fields = StructValue::GetChildren(value);
			RequireSameValue(input.GetValue(row), fields[0]);
			REQUIRE(fields[1].GetValue<int64_t>() == NumericCast<int64_t>(row));
		}
	}
	SECTION("LIST(STRUCT(VARIANT))") {
		// the generic list writer creates a VARIANT writer for every list, each appending to the previous ones
		Vector result(LogicalType::LIST(LogicalType::STRUCT({{"v", LogicalType::VARIANT()}})), lists.size());
		{
			auto writer = FlatVector::Writer<VectorListType<VectorStructType<VectorVariantType>>>(result, lists.size());
			for (auto &list : lists) {
				if (list.is_null) {
					writer.WriteNull();
					continue;
				}
				idx_t element_idx = 0;
				for (auto &struct_writer : writer.WriteList(list.rows.size())) {
					const auto row = list.rows[element_idx++];
					struct_writer.WriteValue([&](VectorWriter<VectorVariantType> &variant_writer) {
						WriteRow(variants, row, variant_writer);
					});
				}
			}
		}
		result.Verify();
		for (idx_t i = 0; i < lists.size(); i++) {
			auto value = result.GetValue(i);
			REQUIRE(value.IsNull() == lists[i].is_null);
			if (lists[i].is_null) {
				continue;
			}
			auto &elements = ListValue::GetChildren(value);
			REQUIRE(elements.size() == lists[i].rows.size());
			for (idx_t j = 0; j < elements.size(); j++) {
				RequireSameValue(input.GetValue(lists[i].rows[j]), StructValue::GetChildren(elements[j])[0]);
			}
		}
	}
	SECTION("LIST(LIST(VARIANT))") {
		// every outer list creates a LIST(VARIANT) writer, which appends to the elements of the previous ones
		const idx_t outer_count = 3;
		Vector result(LogicalType::LIST(LogicalType::LIST(LogicalType::VARIANT())), outer_count);
		{
			auto writer = FlatVector::Writer<VectorListType<VectorListType<VectorVariantType>>>(result, outer_count);
			for (idx_t outer_idx = 0; outer_idx < outer_count; outer_idx++) {
				idx_t list_idx = 0;
				for (auto &list_writer : writer.WriteList(lists.size())) {
					auto &list = lists[list_idx++];
					if (list.is_null) {
						list_writer.WriteNull();
						continue;
					}
					idx_t element_idx = 0;
					for (auto &element_writer : list_writer.WriteList(list.rows.size())) {
						WriteRow(variants, list.rows[element_idx++], element_writer);
					}
				}
			}
		}
		result.Verify();
		for (idx_t outer_idx = 0; outer_idx < outer_count; outer_idx++) {
			auto value = result.GetValue(outer_idx);
			auto &inner = ListValue::GetChildren(value);
			REQUIRE(inner.size() == lists.size());
			for (idx_t i = 0; i < lists.size(); i++) {
				RequireList(input, inner[i], lists[i]);
			}
		}
	}
}
