#include "duckdb/function/table_function.hpp"

namespace duckdb {

GlobalTableFunctionState::~GlobalTableFunctionState() {
}

LocalTableFunctionState::~LocalTableFunctionState() {
}

TableFunctionInfo::~TableFunctionInfo() {
}

TableFunction::TableFunction(string name, vector<LogicalType> arguments, table_function_t function,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : SimpleNamedParameterFunction(std::move(name), std::move(arguments)), bind(bind), bind_replace(nullptr),
      init_global(init_global), init_local(init_local), function(function), in_out_function(nullptr),
      in_out_function_final(nullptr), statistics(nullptr), dependency(nullptr), cardinality(nullptr),
      pushdown_complex_filter(nullptr), to_string(nullptr), table_scan_progress(nullptr), get_batch_index(nullptr),
      get_batch_info(nullptr), serialize(nullptr), deserialize(nullptr), projection_pushdown(false),
      filter_pushdown(false), filter_prune(false) {
}

TableFunction::TableFunction(const vector<LogicalType> &arguments, table_function_t function,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : TableFunction(string(), arguments, function, bind, init_global, init_local) {
}
TableFunction::TableFunction()
    : SimpleNamedParameterFunction("", {}), bind(nullptr), bind_replace(nullptr), init_global(nullptr),
      init_local(nullptr), function(nullptr), in_out_function(nullptr), statistics(nullptr), dependency(nullptr),
      cardinality(nullptr), pushdown_complex_filter(nullptr), to_string(nullptr), table_scan_progress(nullptr),
      get_batch_index(nullptr), get_batch_info(nullptr), serialize(nullptr), deserialize(nullptr),
      projection_pushdown(false), filter_pushdown(false), filter_prune(false) {
}

bool TableFunction::Equal(const TableFunction &rhs) const {
	// number of types
	if (this->arguments.size() != rhs.arguments.size()) {
		return false;
	}
	// argument types
	for (idx_t i = 0; i < this->arguments.size(); ++i) {
		if (this->arguments[i] != rhs.arguments[i]) {
			return false;
		}
	}
	// varargs
	if (this->varargs != rhs.varargs) {
		return false;
	}

	return true; // they are equal
}

} // namespace duckdb
