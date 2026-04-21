//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/extension_callback.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/storage/checkpoint/checkpoint_options.hpp"

namespace duckdb {
struct DBConfig;
class ClientContext;
class DatabaseInstance;
class ErrorData;

enum class CheckpointTableFlags : uint8_t {
	NONE = 0,
	ROW_GROUPS_DROPPED = 1 << 0,
	ROW_GROUPS_MERGED = 1 << 1,
	ROW_IDS_REMAPPED = 1 << 2,
	INDEXES_REBUILT = 1 << 3
};

inline CheckpointTableFlags operator|(CheckpointTableFlags left, CheckpointTableFlags right) {
	return static_cast<CheckpointTableFlags>(static_cast<uint8_t>(left) | static_cast<uint8_t>(right));
}

inline CheckpointTableFlags &operator|=(CheckpointTableFlags &left, CheckpointTableFlags right) {
	left = left | right;
	return left;
}

inline bool HasCheckpointTableFlag(CheckpointTableFlags flags, CheckpointTableFlags flag) {
	return (static_cast<uint8_t>(flags) & static_cast<uint8_t>(flag)) != 0;
}

struct CheckpointTableEvent {
	idx_t table_oid;
	optional_idx first_affected_old_row_group;
	CheckpointTableFlags flags = CheckpointTableFlags::NONE;
};

struct CheckpointEventInfo {
	CheckpointOptions options;
	vector<CheckpointTableEvent> tables;
};

class ExtensionCallback {
public:
	virtual ~ExtensionCallback() {
	}

	//! Called when a new connection is opened
	virtual void OnConnectionOpened(ClientContext &context) {
	}
	//! Called when a connection is closed
	virtual void OnConnectionClosed(ClientContext &context) {
	}
	//! Called before an extension starts loading
	virtual void OnBeginExtensionLoad(DatabaseInstance &db, const string &name) {
	}
	//! Called after an extension is finished loading
	virtual void OnExtensionLoaded(DatabaseInstance &db, const string &name) {
	}
	//! Called after an extension fails to load loading
	virtual void OnExtensionLoadFail(DatabaseInstance &db, const string &name, const ErrorData &error) {
	}
	//! Called before a checkpoint starts
	virtual void OnCheckpointStart(DatabaseInstance &db, const CheckpointOptions &options) {
	}
	//! Called after a checkpoint ends
	virtual void OnCheckpointEnd(DatabaseInstance &db, const CheckpointEventInfo &info) {
	}

	static void Register(DBConfig &config, shared_ptr<ExtensionCallback> extension);
	static ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>> Iterate(ClientContext &context) {
		return ExtensionCallbackManager::Get(context).ExtensionCallbacks();
	}
	static ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>> Iterate(DatabaseInstance &db) {
		return ExtensionCallbackManager::Get(db).ExtensionCallbacks();
	}
};

} // namespace duckdb
