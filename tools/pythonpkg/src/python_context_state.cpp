#include "duckdb_python/python_context_state.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PythonContextState::PythonContextState() {
}

PythonContextState &PythonContextState::Get(ClientContext &context) {
	D_ASSERT(context.registered_state.count("python_state"));
	return context.registered_state.at("python_state")->Cast<PythonContextState>();
}

unique_ptr<TableRef> PythonContextState::GetRegisteredObject(const string &name) {
	auto it = registered_objects.find(name);
	if (it == registered_objects.end()) {
		return nullptr;
	}
	return it->second->Copy();
}

void PythonContextState::RegisterObject(const string &name, unique_ptr<TableRef> object) {
	registered_objects[name] = std::move(object);
}

void PythonContextState::UnregisterObject(const string &name) {
	registered_objects.erase(name);
}

} // namespace duckdb
