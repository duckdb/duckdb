#include "duckdb/execution/operator/scan/physical_table_function.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

void PhysicalTableFunction::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	// run main code
	function->function.function(context, parameters, chunk, bind_data.get());
	if (chunk.size() == 0) {
		// finished, call clean up
		if (function->function.final) {
			function->function.final(context, bind_data.get());
		}
	}
}
