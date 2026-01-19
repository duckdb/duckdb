//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_callback_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
struct ExtensionCallbackRegistry;
class ParserExtension;
class OptimizerExtension;
class OperatorExtension;
class StorageExtension;
class ExtensionCallback;
class ClientContext;

template <class T>
class ExtensionCallbackIteratorHelper;

class ExtensionCallbackManager {
public:
	ExtensionCallbackManager();
	~ExtensionCallbackManager();

	void Register(ParserExtension extension);
	void Register(OptimizerExtension extension);
	void Register(shared_ptr<OperatorExtension> extension);
	void Register(shared_ptr<StorageExtension> extension);
	void Register(shared_ptr<ExtensionCallback> extension);

	static ExtensionCallbackIteratorHelper<shared_ptr<OperatorExtension>> OperatorExtensions(ClientContext &context);

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
