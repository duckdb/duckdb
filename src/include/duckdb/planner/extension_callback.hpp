//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/extension_callback.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/extension_callback_manager.hpp"

namespace duckdb {
struct DBConfig;
class ClientContext;
class DatabaseInstance;
class ErrorData;

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
	//! Called when a query error occurs before it is returned to the client.
	//! Extensions can inspect and modify the error (e.g., add hints, rewrite messages).
	//! The query string is provided for context (may be empty).
	virtual void OnError(const ClientContext &context, ErrorData &error, const string &query) {
	}

	static void Register(DBConfig &config, shared_ptr<ExtensionCallback> extension);
	static ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>> Iterate(ClientContext &context) {
		return ExtensionCallbackManager::Get(context).ExtensionCallbacks();
	}
	static ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>> Iterate(const ClientContext &context) {
		return ExtensionCallbackManager::Get(context).ExtensionCallbacks();
	}
	static ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>> Iterate(DatabaseInstance &db) {
		return ExtensionCallbackManager::Get(db).ExtensionCallbacks();
	}
};

} // namespace duckdb
