#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/planner/extension_callback.hpp"

namespace duckdb {

struct ExtensionCallbackRegistry {
	//! Extensions made to the parser
	vector<ParserExtension> parser_extensions;
	//! Extensions made to the optimizer
	vector<OptimizerExtension> optimizer_extensions;
	//! Extensions made to binder
	vector<shared_ptr<OperatorExtension>> operator_extensions;
	//! Extensions made to storage
	case_insensitive_map_t<shared_ptr<StorageExtension>> storage_extensions;
	//! Set of callbacks that can be installed by extensions
	vector<shared_ptr<ExtensionCallback>> extension_callbacks;
};

ExtensionCallbackManager::ExtensionCallbackManager() : callback_registry(make_shared_ptr<ExtensionCallbackRegistry>()) {
}
ExtensionCallbackManager::~ExtensionCallbackManager() {
}

void ExtensionCallbackManager::Register(ParserExtension extension) {
	lock_guard<mutex> guard(registry_lock);
	auto new_registry = make_shared_ptr<ExtensionCallbackRegistry>(*callback_registry);
	new_registry->parser_extensions.push_back(std::move(extension));
	callback_registry.atomic_store(new_registry);
}

void ExtensionCallbackManager::Register(OptimizerExtension extension) {
}

void ExtensionCallbackManager::Register(shared_ptr<OperatorExtension> extension) {
}

void ExtensionCallbackManager::Register(shared_ptr<StorageExtension> extension) {
}

void ExtensionCallbackManager::Register(shared_ptr<ExtensionCallback> extension) {
}

template <class T>
ExtensionCallbackIteratorHelper<T>::ExtensionCallbackIteratorHelper(
    const vector<T> &vec, shared_ptr<ExtensionCallbackRegistry> callback_registry)
    : vec(vec), callback_registry(std::move(callback_registry)) {
}

template <class T>
ExtensionCallbackIteratorHelper<T>::~ExtensionCallbackIteratorHelper() {
}

ExtensionCallbackIteratorHelper<shared_ptr<OperatorExtension>>
ExtensionCallbackManager::OperatorExtensions(ClientContext &context) {
	auto &callback_manager = DBConfig::GetConfig(context).GetCallbackManager();
	auto registry = callback_manager.callback_registry.atomic_load();
	auto &operator_extensions = registry->operator_extensions;
	return ExtensionCallbackIteratorHelper<shared_ptr<OperatorExtension>>(operator_extensions, std::move(registry));
}

template class ExtensionCallbackIteratorHelper<shared_ptr<OperatorExtension>>;

} // namespace duckdb
