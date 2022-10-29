#include "duckdb/execution/operator/schema/physical_create_type.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/common/types/null_value.hpp"

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

	mutex glock;
	ColumnDataCollection collection;
};

unique_ptr<GlobalSinkState> PhysicalCreateType::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<CreateTypeGlobalState>(context);
}

SinkResultType PhysicalCreateType::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                        DataChunk &input) const {
	auto &gstate = (CreateTypeGlobalState &)gstate_p;
	lock_guard<mutex> lock(gstate.glock);
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
		ColumnDataScanState scan_state;
		collection.InitializeScan(scan_state);

		DataChunk scan_chunk;
		collection.InitializeScanChunk(scan_chunk);

		idx_t total_row_count = collection.Count();
		Vector result(LogicalType::VARCHAR, total_row_count);
		auto result_ptr = FlatVector::GetData<string_t>(result);
		auto &res_validity = FlatVector::Validity(result);

		idx_t offset = 0;
		while (collection.Scan(scan_state, scan_chunk)) {
			idx_t src_row_count = scan_chunk.size();
			auto &src_vec = scan_chunk.data[0];
			D_ASSERT(src_vec.GetVectorType() == VectorType::FLAT_VECTOR);
			D_ASSERT(src_vec.GetType().id() == LogicalType::VARCHAR);

			auto src_ptr = FlatVector::GetData<string_t>(src_vec);
			auto &src_validity = FlatVector::Validity(src_vec);

			if (!src_validity.AllValid()) {
				// lazy initialize
				if (res_validity.AllValid()) {
					res_validity.Initialize(total_row_count);
				}

				if (offset % ValidityMask::BITS_PER_VALUE == 0) {
					auto dst = res_validity.GetData() + res_validity.EntryCount(offset);
					auto src = src_validity.GetData();

					for (auto entry_count = src_validity.EntryCount(src_row_count); entry_count-- > 0;) {
						*dst++ = *src++;
					}
				} else {
					for (idx_t i = 0; i < src_row_count; ++i) {
						res_validity.Set(offset + i, src_validity.RowIsValid(i));
					}
				}
			}

			for (idx_t i = 0; i < src_row_count; i++) {
				idx_t target_index = offset + i;
				if (res_validity.RowIsValid(target_index)) {
					result_ptr[target_index] =
					    StringVector::AddStringOrBlob(result, src_ptr[i].GetDataUnsafe(), src_ptr[i].GetSize());
				} else {
					result_ptr[target_index] = NullValue<string_t>();
				}
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
