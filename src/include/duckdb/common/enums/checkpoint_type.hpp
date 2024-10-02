//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/checkpoint_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class CheckpointWALAction {
	//! Delete the WAL file after the checkpoint completes - generally done on shutdown
	DELETE_WAL,
	//! Leave the WAL file alone
	DONT_DELETE_WAL
};

enum class CheckpointAction {
	//! Checkpoint only if a checkpoint is required (i.e. the WAL has data in it that can be flushed)
	CHECKPOINT_IF_REQUIRED,
	//! Always checkpoint regardless of whether or not there is data in the WAL to flush
	ALWAYS_CHECKPOINT
};

enum class CheckpointType {
	//! Full checkpoints involve vacuuming deleted rows and updates
	//! They can only be run if no transaction need to read old data (that would be cleaned up/vacuumed)
	FULL_CHECKPOINT,
	//! Concurrent checkpoints write committed data to disk but do less clean-up
	//! They can be run even when active transactions need to read old data
	CONCURRENT_CHECKPOINT
};

} // namespace duckdb
