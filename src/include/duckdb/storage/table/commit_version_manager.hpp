//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/table_version_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
    struct CommitVersionManager {
    public:
        void DidCommitTransaction(const transaction_t commit_id) {
            if (commit_id == last_commit_id) return;

            last_commit_id = commit_id;
            version++;
        }

        idx_t GetVersion() const {
            return version;
        }

        void SetVersion(idx_t version) {
            this->version = version;
        }
    private:
        idx_t version = 0;
        transaction_t last_commit_id = 0;
    };
}