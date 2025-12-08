//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/checkpoint_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/checkpoint_type.hpp"

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
};

} // namespace duckdb
