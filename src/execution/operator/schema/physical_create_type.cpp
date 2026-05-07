#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/execution/operator/schema/physical_create_type.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

PhysicalCreateType::PhysicalCreateType(PhysicalPlan &physical_plan, unique_ptr<CreateTypeInfo> info_p,
                                       idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_TYPE, {LogicalType::BIGINT}, estimated_cardinality),
      info(std::move(info_p)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateTypeGlobalState : public GlobalSinkState {
public:
	explicit CreateTypeGlobalState(ClientContext &context) : result(LogicalType::VARCHAR) {
	}
	Vector result;
	idx_t size = 0;
	idx_t capacity = STANDARD_VECTOR_SIZE;
	string_set_t found_strings;
};

unique_ptr<GlobalSinkState> PhysicalCreateType::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CreateTypeGlobalState>(context);
}

SinkResultType PhysicalCreateType::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CreateTypeGlobalState>();
	idx_t total_row_count = gstate.size + chunk.size();
	if (total_row_count > NumericLimits<uint32_t>::Maximum()) {
		throw InvalidInputException("Attempted to create ENUM of size %llu, which exceeds the maximum size of %llu",
		                            total_row_count, NumericLimits<uint32_t>::Maximum());
	}
	if (total_row_count > gstate.capacity) {
		// We must resize our result vector
		gstate.result.Resize(gstate.capacity, gstate.capacity * 2);
		gstate.capacity *= 2;
	}

	auto entries = chunk.data[0].Values<string_t>(chunk.size());
	auto result_data = FlatVector::ScatterWriter<string_t>(gstate.result);
	// Input vector has NULL value, we just throw an exception
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto vec_entry = entries[i];
		if (!vec_entry.IsValid()) {
			continue;
		}
		auto str = vec_entry.GetValue();
		auto found = gstate.found_strings.find(str);
		if (found != gstate.found_strings.end()) {
			// entry was already found - skip
			continue;
		}
		result_data[gstate.size] = str;
		gstate.found_strings.insert(result_data[gstate.size]);
		gstate.size++;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCreateType::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	if (IsSink()) {
		D_ASSERT(info->type == LogicalType::INVALID);
		auto &g_sink_state = sink_state->Cast<CreateTypeGlobalState>();
		info->type = LogicalType::ENUM(g_sink_state.result, g_sink_state.size);
	}

	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	catalog.CreateType(context.client, *info);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
