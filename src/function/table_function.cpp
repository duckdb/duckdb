#include "duckdb/function/table_function.hpp"

namespace duckdb {

GlobalTableFunctionState::~GlobalTableFunctionState() {
}

LocalTableFunctionState::~LocalTableFunctionState() {
}

PartitionStatistics::PartitionStatistics() : row_start(0), count(0), count_type(CountType::COUNT_APPROXIMATE) {
}

TableFunctionInfo::~TableFunctionInfo() {
}

TableFunction::TableFunction(string name, vector<LogicalType> arguments, table_function_t function,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : SimpleNamedParameterFunction(std::move(name), std::move(arguments)), bind(bind), bind_replace(nullptr),
      bind_operator(nullptr), init_global(init_global), init_local(init_local), function(function),
      in_out_function(nullptr), in_out_function_final(nullptr), statistics(nullptr), dependency(nullptr),
      cardinality(nullptr), pushdown_complex_filter(nullptr), pushdown_expression(nullptr), to_string(nullptr),
      dynamic_to_string(nullptr), table_scan_progress(nullptr), get_partition_data(nullptr), get_bind_info(nullptr),
      type_pushdown(nullptr), get_multi_file_reader(nullptr), supports_pushdown_type(nullptr),
      get_partition_info(nullptr), get_partition_stats(nullptr), get_virtual_columns(nullptr),
      get_row_id_columns(nullptr), serialize(nullptr), deserialize(nullptr), projection_pushdown(false),
      filter_pushdown(false), filter_prune(false), sampling_pushdown(false), late_materialization(false) {
}

TableFunction::TableFunction(const vector<LogicalType> &arguments, table_function_t function,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : TableFunction(string(), arguments, function, bind, init_global, init_local) {
}

TableFunction::TableFunction() : TableFunction("", {}, nullptr, nullptr, nullptr, nullptr) {
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
