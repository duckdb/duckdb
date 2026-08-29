#include "duckdb/execution/row_id_deduplicator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

RowIdDeduplicator::RowIdDeduplicator(ClientContext &context, vector<LogicalType> row_id_types_p)
    : row_id_types(std::move(row_id_types_p)), addresses(LogicalType::POINTER), new_groups(STANDARD_VECTOR_SIZE) {
	if (row_id_types.empty()) {
		throw InternalException("Cannot deduplicate an empty row ID");
	}
	row_id_chunk.InitializeEmpty(row_id_types);
	hash_table = make_uniq<GroupedAggregateHashTable>(context, Allocator::Get(context), row_id_types);
}

RowIdDeduplicator::~RowIdDeduplicator() {
}

idx_t RowIdDeduplicator::Register(DataChunk &input, idx_t row_id_start, optional_ptr<SelectionVector> sel) {
	if (row_id_start >= input.ColumnCount() || input.ColumnCount() - row_id_start != row_id_types.size()) {
		throw InternalException("Row ID column range does not match the deduplicator types");
	}
	for (idx_t i = 0; i < row_id_types.size(); i++) {
		auto &input_vector = input.data[row_id_start + i];
		if (input_vector.GetType() != row_id_types[i]) {
			throw InternalException("Row ID type mismatch in deduplicator");
		}
		row_id_chunk.data[i].Reference(input_vector);
	}
	row_id_chunk.SetCardinalityUnsafe(input.size());
	return Register(row_id_chunk, sel);
}

idx_t RowIdDeduplicator::Register(const Vector &row_ids, idx_t count, optional_ptr<SelectionVector> sel) {
	if (row_id_types.size() != 1 || row_ids.GetType() != row_id_types[0]) {
		throw InternalException("Single row ID vector does not match the deduplicator types");
	}
	if (count > row_ids.size()) {
		throw InternalException("Row ID count exceeds vector size");
	}
	row_id_chunk.data[0].Reference(row_ids);
	row_id_chunk.SetCardinalityUnsafe(count);
	return Register(row_id_chunk, sel);
}

idx_t RowIdDeduplicator::Register(DataChunk &row_ids, optional_ptr<SelectionVector> sel) {
	auto count = row_ids.size();
	if (count == 0) {
		return 0;
	}
	addresses.Reserve(count);
	auto &result_sel = sel ? *sel : new_groups;
	if (result_sel.Capacity() < count) {
		result_sel.Initialize(count);
	}
	auto distinct_count = hash_table->FindOrCreateGroups(row_ids, addresses, result_sel);
	if (sel) {
		// The hash table may discover new groups in probe order rather than input order.
		result_sel.Sort(distinct_count);
	}
	return distinct_count;
}

} // namespace duckdb
