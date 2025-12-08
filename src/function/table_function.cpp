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

TableFunction::TableFunction(string name, const vector<LogicalType> &arguments, table_function_t function_,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : SimpleNamedParameterFunction(std::move(name), arguments), bind(bind), bind_replace(nullptr),
      bind_operator(nullptr), init_global(init_global), init_local(init_local), function(function_),
      in_out_function(nullptr), in_out_function_final(nullptr), statistics(nullptr), dependency(nullptr),
      cardinality(nullptr), pushdown_complex_filter(nullptr), pushdown_expression(nullptr), to_string(nullptr),
      dynamic_to_string(nullptr), table_scan_progress(nullptr), get_partition_data(nullptr), get_bind_info(nullptr),
      type_pushdown(nullptr), get_multi_file_reader(nullptr), supports_pushdown_type(nullptr),
      get_partition_info(nullptr), get_partition_stats(nullptr), get_virtual_columns(nullptr),
      get_row_id_columns(nullptr), set_scan_order(nullptr), serialize(nullptr), deserialize(nullptr),
      projection_pushdown(false), filter_pushdown(false), filter_prune(false), sampling_pushdown(false),
      late_materialization(false) {
}

TableFunction::TableFunction(string name, const vector<LogicalType> &arguments, std::nullptr_t function_,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : SimpleNamedParameterFunction(std::move(name), arguments), bind(bind), bind_replace(nullptr),
      bind_operator(nullptr), init_global(init_global), init_local(init_local), function(nullptr),
      in_out_function(nullptr), in_out_function_final(nullptr), statistics(nullptr), dependency(nullptr),
      cardinality(nullptr), pushdown_complex_filter(nullptr), pushdown_expression(nullptr), to_string(nullptr),
      dynamic_to_string(nullptr), table_scan_progress(nullptr), get_partition_data(nullptr), get_bind_info(nullptr),
      type_pushdown(nullptr), get_multi_file_reader(nullptr), supports_pushdown_type(nullptr),
      get_partition_info(nullptr), get_partition_stats(nullptr), get_virtual_columns(nullptr),
      get_row_id_columns(nullptr), serialize(nullptr), deserialize(nullptr), projection_pushdown(false),
      filter_pushdown(false), filter_prune(false), sampling_pushdown(false), late_materialization(false) {
}

TableFunction::TableFunction(const vector<LogicalType> &arguments, table_function_t function_,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : TableFunction("", arguments, function_, bind, init_global, init_local) {
}

TableFunction::TableFunction(const vector<LogicalType> &arguments, std::nullptr_t function_, table_function_bind_t bind,
                             table_function_init_global_t init_global, table_function_init_local_t init_local)
    : TableFunction("", arguments, function_, bind, init_global, init_local) {
}

TableFunction::TableFunction() : TableFunction("", {}, nullptr, nullptr, nullptr, nullptr) {
}

bool TableFunction::operator==(const TableFunction &rhs) const {
	return name == rhs.name && arguments == rhs.arguments && varargs == rhs.varargs && bind == rhs.bind &&
	       bind_replace == rhs.bind_replace && bind_operator == rhs.bind_operator && init_global == rhs.init_global &&
	       init_local == rhs.init_local && function == rhs.function && in_out_function == rhs.in_out_function &&
	       in_out_function_final == rhs.in_out_function_final && statistics == rhs.statistics &&
	       dependency == rhs.dependency && cardinality == rhs.cardinality &&
	       pushdown_complex_filter == rhs.pushdown_complex_filter && pushdown_expression == rhs.pushdown_expression &&
	       to_string == rhs.to_string && dynamic_to_string == rhs.dynamic_to_string &&
	       table_scan_progress == rhs.table_scan_progress && get_partition_data == rhs.get_partition_data &&
	       get_bind_info == rhs.get_bind_info && type_pushdown == rhs.type_pushdown &&
	       get_multi_file_reader == rhs.get_multi_file_reader && supports_pushdown_type == rhs.supports_pushdown_type &&
	       get_partition_info == rhs.get_partition_info && get_partition_stats == rhs.get_partition_stats &&
	       get_virtual_columns == rhs.get_virtual_columns && get_row_id_columns == rhs.get_row_id_columns &&
	       serialize == rhs.serialize && deserialize == rhs.deserialize &&
	       verify_serialization == rhs.verify_serialization && projection_pushdown == rhs.projection_pushdown &&
	       filter_pushdown == rhs.filter_pushdown && filter_prune == rhs.filter_prune &&
	       sampling_pushdown == rhs.sampling_pushdown && late_materialization == rhs.late_materialization &&
	       global_initialization == rhs.global_initialization;
}

bool TableFunction::operator!=(const TableFunction &rhs) const {
	return !(*this == rhs);
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

bool ExtractSourceResultType(AsyncResultType in, SourceResultType &out) {
	switch (in) {
	case AsyncResultType::IMPLICIT:
	case AsyncResultType::INVALID:
		return false;
	case AsyncResultType::HAVE_MORE_OUTPUT:
		out = SourceResultType::HAVE_MORE_OUTPUT;
		break;
	case AsyncResultType::FINISHED:
		out = SourceResultType::FINISHED;
		break;
	case AsyncResultType::BLOCKED:
		out = SourceResultType::BLOCKED;
		break;
	}
	return true;
}

} // namespace duckdb
