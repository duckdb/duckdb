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

enum class CheckpointRowGroupLineageKind : uint8_t { DROP, REMAP, MERGE };

struct CheckpointRowGroupLineageEntry {
	CheckpointRowGroupLineageKind kind;
	idx_t old_row_group_index;
	idx_t old_row_group_count;
	optional_idx new_row_group_index;
	idx_t new_row_group_count;
};

struct CheckpointTableEvent {
	idx_t table_oid;
	optional_idx first_affected_old_row_group;
	bool row_ids_remapped = false;
	bool indexes_rebuilt = false;
	vector<CheckpointRowGroupLineageEntry> row_group_lineage;
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

	static void Register(DBConfig &config, shared_ptr<ExtensionCallback> extension);
	static ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>> Iterate(ClientContext &context) {
		return ExtensionCallbackManager::Get(context).ExtensionCallbacks();
	}
	static ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>> Iterate(DatabaseInstance &db) {
		return ExtensionCallbackManager::Get(db).ExtensionCallbacks();
	}
};

class CheckpointCallback {
public:
	virtual ~CheckpointCallback() {
	}

	//! Called before a checkpoint starts
	virtual void OnCheckpointStart(DatabaseInstance &db, const CheckpointOptions &options) {
	}
	//! Called after a checkpoint ends
	virtual void OnCheckpointEnd(DatabaseInstance &db, const CheckpointEventInfo &info) {
	}

	static void Register(DBConfig &config, shared_ptr<CheckpointCallback> extension);
	static ExtensionCallbackIteratorHelper<shared_ptr<CheckpointCallback>> Iterate(DatabaseInstance &db) {
		return ExtensionCallbackManager::Get(db).CheckpointCallbacks();
	}
};

} // namespace duckdb
