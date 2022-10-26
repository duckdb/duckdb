#include "duckdb/execution/operator/schema/physical_create_type.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column_data_collection.hpp"

namespace duckdb {

PhysicalCreateType::PhysicalCreateType(unique_ptr<CreateTypeInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TYPE, {LogicalType::BIGINT}, estimated_cardinality),
      info(move(info)) {
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
    auto &g_sink_state = (CreateTypeGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}

    if (IsSink()) {
        D_ASSERT(info->type == LogicalType::INVALID);
        auto &collection = g_sink_state.collection;
        ColumnDataScanState scan_state;
	    collection.InitializeScan(scan_state);

	    DataChunk scan_chunk;
	    collection.InitializeScanChunk(scan_chunk);

        idx_t total_row = collection.Count();
        Vector result(LogicalType::VARCHAR, total_row);
        auto result_ptr = FlatVector::GetData<string_t>(result);
        auto &res_validity = FlatVector::Validity(result);
        
        idx_t offset = 0;
        while (collection.Scan(scan_state, scan_chunk)) {
            idx_t src_total_row = scan_chunk.size();
            auto &src_vec = scan_chunk.data[0];
            D_ASSERT(src_vec.GetVectorType() == VectorType::FLAT_VECTOR);
            auto src_ptr = FlatVector::GetData<string_t>(src_vec);
            auto &src_validity = FlatVector::Validity(src_vec);
            // memcpy(result_ptr, src_ptr, src_total_row);
            
            for (idx_t i = 0; i < src_total_row; i++) {
                idx_t index = offset + i;
                result_ptr[index] = StringVector::AddStringOrBlob(result, src_ptr[index].GetDataUnsafe(), src_ptr[index].GetSize());
            }
            result_ptr += (src_total_row * sizeof(string_t));

            if (!src_validity.AllValid()) {
                if (res_validity.AllValid()) {
                    res_validity.Initialize(total_row);
                }
                for (idx_t i = 0; i < src_total_row; ++i) {
				    res_validity.Set(offset + i, src_validity.RowIsValid(i));
			    }
            }
            offset += src_total_row;
        }

        info->type = LogicalType::ENUM(info->name, result, total_row);
    }

	auto &catalog = Catalog::GetCatalog(context.client);
	catalog.CreateType(context.client, info.get());
	state.finished = true;
}

class CreateTypeGlobalState : public GlobalSinkState {
public:
    explicit CreateTypeGlobalState(ClientContext &context, vector<LogicalType> types)
	    : collection(context, types) {
	}

	mutex glock;
	ColumnDataCollection collection;
};

class CreateTypeLocalState : public LocalSinkState {

};

unique_ptr<GlobalSinkState> PhysicalCreateType::GetGlobalSinkState(ClientContext &context) const {
    return make_unique<CreateTypeGlobalState>(context, types);
}

unique_ptr<LocalSinkState> PhysicalCreateType::GetLocalSinkState(ExecutionContext &context) const {
    return make_unique<CreateTypeLocalState>();
}

SinkResultType PhysicalCreateType::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
	                    DataChunk &input) const {
    auto &gstate = (CreateTypeGlobalState &)gstate_p;
    lock_guard<mutex> lock(gstate.glock);
	gstate.collection.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
