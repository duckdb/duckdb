//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_callback_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class ExtensionCallback;
class OperatorExtension;
class OptimizerExtension;
class ParserExtension;
class PlannerExtension;
class StorageExtension;
struct ExtensionCallbackRegistry;

template <class T>
class ExtensionCallbackIteratorHelper;

class ExtensionCallbackManager {
public:
	ExtensionCallbackManager();
	~ExtensionCallbackManager();

	static ExtensionCallbackManager &Get(ClientContext &context);
	static ExtensionCallbackManager &Get(DatabaseInstance &db);
	static const ExtensionCallbackManager &Get(const ClientContext &context);

	void Register(ParserExtension extension);
	void Register(PlannerExtension extension);
	void Register(OptimizerExtension extension);
	void Register(shared_ptr<OperatorExtension> extension);
	void Register(const string &name, shared_ptr<StorageExtension> extension);
	void Register(shared_ptr<ExtensionCallback> extension);

	ExtensionCallbackIteratorHelper<shared_ptr<OperatorExtension>> OperatorExtensions() const;
	ExtensionCallbackIteratorHelper<OptimizerExtension> OptimizerExtensions() const;
	ExtensionCallbackIteratorHelper<ParserExtension> ParserExtensions() const;
	ExtensionCallbackIteratorHelper<PlannerExtension> PlannerExtensions() const;
	ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>> ExtensionCallbacks() const;
	optional_ptr<StorageExtension> FindStorageExtension(const string &name) const;
	bool HasParserExtensions() const;

private:
	mutex registry_lock;
	shared_ptr<ExtensionCallbackRegistry> callback_registry;
};

template <class T>
class ExtensionCallbackIteratorHelper {
public:
	ExtensionCallbackIteratorHelper(const vector<T> &vec, shared_ptr<ExtensionCallbackRegistry> callback_registry);
	~ExtensionCallbackIteratorHelper();

private:
	const vector<T> &vec;
	shared_ptr<ExtensionCallbackRegistry> callback_registry;

public:
	typename vector<T>::const_iterator begin() { // NOLINT: match stl API
		return vec.cbegin();
	}
	typename vector<T>::const_iterator end() { // NOLINT: match stl API
		return vec.cend();
	}
};

} // namespace duckdb
