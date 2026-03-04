#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/planner/planner_extension.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/planner/extension_callback.hpp"

namespace duckdb {

struct ExtensionCallbackRegistry {
	//! Extensions made to the parser
	vector<ParserExtension> parser_extensions;
	//! Extensions made to the planner
	vector<PlannerExtension> planner_extensions;
	//! Extensions made to the optimizer
	vector<OptimizerExtension> optimizer_extensions;
	//! Extensions made to binder
	vector<shared_ptr<OperatorExtension>> operator_extensions;
	//! Extensions made to storage
	case_insensitive_map_t<shared_ptr<StorageExtension>> storage_extensions;
	//! Set of callbacks that can be installed by extensions
	vector<shared_ptr<ExtensionCallback>> extension_callbacks;
};

ExtensionCallbackManager &ExtensionCallbackManager::Get(ClientContext &context) {
	return DBConfig::GetConfig(context).GetCallbackManager();
}

const ExtensionCallbackManager &ExtensionCallbackManager::Get(const ClientContext &context) {
	return DBConfig::GetConfig(context).GetCallbackManager();
}

ExtensionCallbackManager &ExtensionCallbackManager::Get(DatabaseInstance &db) {
	return DBConfig::GetConfig(db).GetCallbackManager();
}

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

void ExtensionCallbackManager::Register(PlannerExtension extension) {
	lock_guard<mutex> guard(registry_lock);
	auto new_registry = make_shared_ptr<ExtensionCallbackRegistry>(*callback_registry);
	new_registry->planner_extensions.push_back(std::move(extension));
	callback_registry.atomic_store(new_registry);
}

void ExtensionCallbackManager::Register(OptimizerExtension extension) {
	lock_guard<mutex> guard(registry_lock);
	auto new_registry = make_shared_ptr<ExtensionCallbackRegistry>(*callback_registry);
	new_registry->optimizer_extensions.push_back(std::move(extension));
	callback_registry.atomic_store(new_registry);
}

void ExtensionCallbackManager::Register(shared_ptr<OperatorExtension> extension) {
	lock_guard<mutex> guard(registry_lock);
	auto new_registry = make_shared_ptr<ExtensionCallbackRegistry>(*callback_registry);
	new_registry->operator_extensions.push_back(std::move(extension));
	callback_registry.atomic_store(new_registry);
}

void ExtensionCallbackManager::Register(const string &name, shared_ptr<StorageExtension> extension) {
	lock_guard<mutex> guard(registry_lock);
	auto new_registry = make_shared_ptr<ExtensionCallbackRegistry>(*callback_registry);
	new_registry->storage_extensions[name] = std::move(extension);
	callback_registry.atomic_store(new_registry);
}

void ExtensionCallbackManager::Register(shared_ptr<ExtensionCallback> extension) {
	lock_guard<mutex> guard(registry_lock);
	auto new_registry = make_shared_ptr<ExtensionCallbackRegistry>(*callback_registry);
	new_registry->extension_callbacks.push_back(std::move(extension));
	callback_registry.atomic_store(new_registry);
}

template <class T>
ExtensionCallbackIteratorHelper<T>::ExtensionCallbackIteratorHelper(
    const vector<T> &vec, shared_ptr<ExtensionCallbackRegistry> callback_registry)
    : vec(vec), callback_registry(std::move(callback_registry)) {
}

template <class T>
ExtensionCallbackIteratorHelper<T>::~ExtensionCallbackIteratorHelper() {
}

ExtensionCallbackIteratorHelper<shared_ptr<OperatorExtension>> ExtensionCallbackManager::OperatorExtensions() const {
	auto registry = callback_registry.atomic_load();
	auto &operator_extensions = registry->operator_extensions;
	return ExtensionCallbackIteratorHelper<shared_ptr<OperatorExtension>>(operator_extensions, std::move(registry));
}

ExtensionCallbackIteratorHelper<OptimizerExtension> ExtensionCallbackManager::OptimizerExtensions() const {
	auto registry = callback_registry.atomic_load();
	auto &optimizer_extensions = registry->optimizer_extensions;
	return ExtensionCallbackIteratorHelper<OptimizerExtension>(optimizer_extensions, std::move(registry));
}

ExtensionCallbackIteratorHelper<ParserExtension> ExtensionCallbackManager::ParserExtensions() const {
	auto registry = callback_registry.atomic_load();
	auto &parser_extensions = registry->parser_extensions;
	return ExtensionCallbackIteratorHelper<ParserExtension>(parser_extensions, std::move(registry));
}

ExtensionCallbackIteratorHelper<PlannerExtension> ExtensionCallbackManager::PlannerExtensions() const {
	auto registry = callback_registry.atomic_load();
	auto &planner_extensions = registry->planner_extensions;
	return ExtensionCallbackIteratorHelper<PlannerExtension>(planner_extensions, std::move(registry));
}

ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>> ExtensionCallbackManager::ExtensionCallbacks() const {
	auto registry = callback_registry.atomic_load();
	auto &extension_callbacks = registry->extension_callbacks;
	return ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>>(extension_callbacks, std::move(registry));
}

optional_ptr<StorageExtension> ExtensionCallbackManager::FindStorageExtension(const string &name) const {
	auto registry = callback_registry.atomic_load();
	auto entry = registry->storage_extensions.find(name);
	if (entry == registry->storage_extensions.end()) {
		return nullptr;
	}
	return entry->second.get();
}

bool ExtensionCallbackManager::HasParserExtensions() const {
	auto registry = callback_registry.atomic_load();
	return !registry->parser_extensions.empty();
}

void OptimizerExtension::Register(DBConfig &config, OptimizerExtension extension) {
	config.GetCallbackManager().Register(std::move(extension));
}

void ParserExtension::Register(DBConfig &config, ParserExtension extension) {
	config.GetCallbackManager().Register(std::move(extension));
}

void PlannerExtension::Register(DBConfig &config, PlannerExtension extension) {
	config.GetCallbackManager().Register(std::move(extension));
}

void OperatorExtension::Register(DBConfig &config, shared_ptr<OperatorExtension> extension) {
	config.GetCallbackManager().Register(std::move(extension));
}

optional_ptr<StorageExtension> StorageExtension::Find(const DBConfig &config, const string &extension_name) {
	return config.GetCallbackManager().FindStorageExtension(extension_name);
}

void ExtensionCallback::Register(DBConfig &config, shared_ptr<ExtensionCallback> extension) {
	config.GetCallbackManager().Register(std::move(extension));
}

void StorageExtension::Register(DBConfig &config, const string &extension_name,
                                shared_ptr<StorageExtension> extension) {
	config.GetCallbackManager().Register(extension_name, std::move(extension));
}

template class ExtensionCallbackIteratorHelper<shared_ptr<ExtensionCallback>>;
template class ExtensionCallbackIteratorHelper<shared_ptr<OperatorExtension>>;
template class ExtensionCallbackIteratorHelper<OptimizerExtension>;
template class ExtensionCallbackIteratorHelper<ParserExtension>;
template class ExtensionCallbackIteratorHelper<PlannerExtension>;

} // namespace duckdb
