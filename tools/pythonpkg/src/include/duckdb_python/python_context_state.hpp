//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/python_context_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

class PythonContextState : public ClientContextState {
public:
	PythonContextState();

public:
	static PythonContextState &Get(ClientContext &client);

public:
	unique_ptr<TableRef> GetRegisteredObject(const string &name);
	void RegisterObject(const string &name, unique_ptr<TableRef> object);
	void UnregisterObject(const string &name);

private:
	case_insensitive_map_t<unique_ptr<TableRef>> registered_objects;
};

} // namespace duckdb
