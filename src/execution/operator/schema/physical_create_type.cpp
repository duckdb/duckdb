#include "duckdb/execution/operator/schema/physical_create_type.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

PhysicalCreateType::PhysicalCreateType(unique_ptr<CreateTypeInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TYPE, {LogicalType::BIGINT}, estimated_cardinality),
      info(move(info)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateTypeGlobalState : public GlobalSinkState {
public:
	explicit CreateTypeGlobalState(ClientContext &context) : collection(context, {LogicalType::VARCHAR}) {
	}

	ColumnDataCollection collection;
};

unique_ptr<GlobalSinkState> PhysicalCreateType::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<CreateTypeGlobalState>(context);
}

SinkResultType PhysicalCreateType::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                        DataChunk &input) const {
	auto &gstate = (CreateTypeGlobalState &)gstate_p;
	idx_t total_row_count = gstate.collection.Count() + input.size();
	if (total_row_count > NumericLimits<uint32_t>::Maximum()) {
		throw InvalidInputException("Attempted to create ENUM of size %llu, which exceeds the maximum size of %llu",
		                            total_row_count, NumericLimits<uint32_t>::Maximum());
	}
	UnifiedVectorFormat sdata;
	input.data[0].ToUnifiedFormat(input.size(), sdata);

	// Input vector has NULL value, we just throw an exception
	for (idx_t i = 0; i < input.size(); i++) {
		idx_t idx = sdata.sel->get_index(i);
		if (!sdata.validity.RowIsValid(idx)) {
			throw InvalidInputException("Attempted to create ENUM type with NULL value!");
		}
	}

	gstate.collection.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateTypeSourceState : public GlobalSourceState {
public:
	CreateTypeSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateType::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateTypeSourceState>();
}

void PhysicalCreateType::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                 LocalSourceState &lstate) const {
	auto &state = (CreateTypeSourceState &)gstate;
	if (state.finished) {
		return;
	}

	if (IsSink()) {
		D_ASSERT(info->type == LogicalType::INVALID);

		auto &g_sink_state = (CreateTypeGlobalState &)*sink_state;
		auto &collection = g_sink_state.collection;

		idx_t total_row_count = collection.Count();

		ColumnDataScanState scan_state;
		collection.InitializeScan(scan_state);

		DataChunk scan_chunk;
		collection.InitializeScanChunk(scan_chunk);

		Vector result(LogicalType::VARCHAR, total_row_count);
		auto result_ptr = FlatVector::GetData<string_t>(result);

		idx_t offset = 0;
		while (collection.Scan(scan_state, scan_chunk)) {
			idx_t src_row_count = scan_chunk.size();
			auto &src_vec = scan_chunk.data[0];
			D_ASSERT(src_vec.GetVectorType() == VectorType::FLAT_VECTOR);
			D_ASSERT(src_vec.GetType().id() == LogicalType::VARCHAR);

			auto src_ptr = FlatVector::GetData<string_t>(src_vec);

			for (idx_t i = 0; i < src_row_count; i++) {
				idx_t target_index = offset + i;
				result_ptr[target_index] =
				    StringVector::AddStringOrBlob(result, src_ptr[i].GetDataUnsafe(), src_ptr[i].GetSize());
			}

			offset += src_row_count;
		}

		info->type = LogicalType::ENUM(info->name, result, total_row_count);
	}

	auto &catalog = Catalog::GetCatalog(context.client);
	catalog.CreateType(context.client, info.get());
	state.finished = true;
}

} // namespace duckdb
