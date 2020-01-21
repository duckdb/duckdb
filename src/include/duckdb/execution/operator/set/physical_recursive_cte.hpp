//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {
    class PhysicalRecursiveCTE : public PhysicalOperator {
    public:
        PhysicalRecursiveCTE(LogicalOperator &op, unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom);


        std::shared_ptr<ChunkCollection> working_table;
        std::shared_ptr<index_t> iteration;
        ChunkCollection intermediate_table;

    public:
        void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
        unique_ptr<PhysicalOperatorState> GetOperatorState() override;
//        string ExtraRenderInformation() const override;
    };

}; // namespace duckdb
