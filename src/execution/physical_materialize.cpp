#include <iostream>
#include "duckdb/execution/physical_materialize.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {

    using collection_map_t = std::map<idx_t, unique_ptr<ChunkCollection>>;

PhysicalMaterialize::PhysicalMaterialize(vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::MATERIALIZE, move(types), estimated_cardinality) {
}

class MaterializeGlobalSinkState : public GlobalSinkState {
public:
    explicit MaterializeGlobalSinkState() : finished(false) {
    }

    mutex lock;
    ChunkCollection result_set;
    collection_map_t collection_map;
    bool finished;
};

unique_ptr<GlobalSinkState> PhysicalMaterialize::GetGlobalSinkState(ClientContext & context) const {
    return make_unique<MaterializeGlobalSinkState>();
}

class MaterializeLocalSinkState : public LocalSinkState {
public:
    explicit MaterializeLocalSinkState() {}
    vector<idx_t> data_indexes;
    vector<unique_ptr<ChunkCollection>> collections;
};

unique_ptr<LocalSinkState> PhysicalMaterialize::GetLocalSinkState(ExecutionContext &context) const {
    return make_unique<MaterializeLocalSinkState>();
}

SinkResultType PhysicalMaterialize::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
        DataChunk &input) const {
    D_ASSERT(lstate.is_ordered);
    D_ASSERT(lstate.source_index != DConstants::INVALID_INDEX);

    auto &local_state = (MaterializeLocalSinkState &)lstate;
    auto index = lstate.source_index;

    if (local_state.data_indexes.empty() || local_state.data_indexes.back() != index) {
        local_state.data_indexes.push_back(index);
        local_state.collections.emplace_back(make_unique<ChunkCollection>());
    }
    local_state.collections.back()->Append(input);

    return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalMaterialize::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const {
    auto &gstate = (MaterializeGlobalSinkState &)state;
    auto &source = (MaterializeLocalSinkState &)lstate;
    D_ASSERT(!gstate.finished);

    lock_guard<mutex> glock(gstate.lock);
    for (idx_t i = 0; i < source.data_indexes.size(); i++) {
        // TODO: transaction local storage?
        D_ASSERT(gstate.collection_map.find(source.data_indexes[i]) == gstate.collection_map.end());
        gstate.collection_map[source.data_indexes[i]] = move(source.collections[i]);
    }
}

SinkFinalizeType PhysicalMaterialize::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
        GlobalSinkState &gstate_p) const {
    auto &gstate = (MaterializeGlobalSinkState &)gstate_p;
    D_ASSERT(!gstate.finished);
    for (auto & iter : gstate.collection_map) {
        gstate.result_set.Append(*iter.second);
    }
    gstate.finished = true;
    return SinkFinalizeType::READY;
}

ChunkCollection PhysicalMaterialize::GetResult() {
    D_ASSERT(sink_state != nullptr);
    return move(reinterpret_cast<MaterializeGlobalSinkState*>(sink_state.get())->result_set);
}

} // namespace duckdb
