#include "duckdb/function/table_function.hpp"

namespace duckdb {

FunctionOperatorData::~FunctionOperatorData() {
}

TableFunctionInfo::~TableFunctionInfo() {
}

TableFilterCollection::TableFilterCollection(TableFilterSet *table_filters) : table_filters(table_filters) {
}

TableFunction::TableFunction(string name, vector<LogicalType> arguments, table_function_t function,
                             table_function_bind_t bind, table_function_init_t init, table_statistics_t statistics,
                             table_function_cleanup_t cleanup, table_function_dependency_t dependency,
                             table_function_cardinality_t cardinality,
                             table_function_pushdown_complex_filter_t pushdown_complex_filter,
                             table_function_to_string_t to_string, table_function_max_threads_t max_threads,
                             table_function_init_parallel_state_t init_parallel_state,
                             table_function_parallel_t parallel_function, table_function_init_parallel_t parallel_init,
                             table_function_parallel_state_next_t parallel_state_next, bool projection_pushdown,
                             bool filter_pushdown, table_function_progress_t query_progress,
                             table_in_out_function_t in_out_function)
    : SimpleNamedParameterFunction(move(name), move(arguments)), bind(bind), init(init), function(function),
      in_out_function(in_out_function), statistics(statistics), cleanup(cleanup), dependency(dependency),
      cardinality(cardinality), pushdown_complex_filter(pushdown_complex_filter), to_string(to_string),
      max_threads(max_threads), init_parallel_state(init_parallel_state), parallel_function(parallel_function),
      parallel_init(parallel_init), parallel_state_next(parallel_state_next), table_scan_progress(query_progress),
      projection_pushdown(projection_pushdown), filter_pushdown(filter_pushdown) {
}

TableFunction::TableFunction(const vector<LogicalType> &arguments, table_function_t function,
                             table_function_bind_t bind, table_function_init_t init, table_statistics_t statistics,
                             table_function_cleanup_t cleanup, table_function_dependency_t dependency,
                             table_function_cardinality_t cardinality,
                             table_function_pushdown_complex_filter_t pushdown_complex_filter,
                             table_function_to_string_t to_string, table_function_max_threads_t max_threads,
                             table_function_init_parallel_state_t init_parallel_state,
                             table_function_parallel_t parallel_function, table_function_init_parallel_t parallel_init,
                             table_function_parallel_state_next_t parallel_state_next, bool projection_pushdown,
                             bool filter_pushdown, table_function_progress_t query_progress,
                             table_in_out_function_t in_out_function)
    : TableFunction(string(), arguments, function, bind, init, statistics, cleanup, dependency, cardinality,
                    pushdown_complex_filter, to_string, max_threads, init_parallel_state, parallel_function,
                    parallel_init, parallel_state_next, projection_pushdown, filter_pushdown, query_progress,
                    in_out_function) {
}
TableFunction::TableFunction() : SimpleNamedParameterFunction("", {}) {
}

} // namespace duckdb
