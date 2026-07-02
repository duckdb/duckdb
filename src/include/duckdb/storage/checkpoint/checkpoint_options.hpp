//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/checkpoint_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/checkpoint_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

struct CheckpointOptions {
	CheckpointOptions()
	    : wal_action(CheckpointWALAction::DONT_DELETE_WAL), action(CheckpointAction::CHECKPOINT_IF_REQUIRED),
	      type(CheckpointType::FULL_CHECKPOINT), transaction_id(MAX_TRANSACTION_ID) {
	}

	CheckpointWALAction wal_action;
	CheckpointAction action;
	CheckpointType type;
	transaction_t transaction_id;
	//! The WAL lock - in case we are holding it during the entire checkpoint.
	//! This is only required if we are doing a checkpoint instead of writing to the WAL
	optional_ptr<lock_guard<mutex>> wal_lock;
};

} // namespace duckdb
