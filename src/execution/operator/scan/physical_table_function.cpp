#include "duckdb/execution/operator/scan/physical_table_function.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace std;

namespace duckdb {

void PhysicalTableFunction::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                             PhysicalOperatorState *state) {
	// run main code
	// function.function(context.client, parameters, chunk, bind_data.get());
	// if (chunk.size() == 0) {
	// 	// finished, call clean up
	// 	if (function.final) {
	// 		function.final(context.client, bind_data.get());
	// 	}
	// }
}

string PhysicalTableFunction::ExtraRenderInformation() const {
	return function.name;
}

} // namespace duckdb
