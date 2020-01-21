//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_cte_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! The PhysicalChunkCollectionScan scans a Chunk Collection
    class PhysicalCTEScan : public PhysicalOperator {
    public:
        PhysicalCTEScan(vector<TypeId> types, PhysicalOperatorType op_type)
                : PhysicalOperator(op_type, types), collection(nullptr) {
        }

        void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
        unique_ptr<PhysicalOperatorState> GetOperatorState() override;

    public:
        // the chunk collection to scan
        ChunkCollection *collection;

        index_t iteration = 0;
        index_t *remote_iteration;
    };

} // namespace duckdb
