
#include "execution/operator/schema/physical_create_index.hpp"
#include "execution/expression_executor.hpp"

#include "catalog/catalog_entry/schema_catalog_entry.hpp"

#include "storage/data_table.hpp"

#include "main/client_context.hpp"

#include "storage/order_index.hpp"

using namespace duckdb;
using namespace std;

vector<TypeId> PhysicalCreateIndex::GetTypes() {
	return {TypeId::BIGINT};
}

void PhysicalCreateIndex::_GetChunk(ClientContext &context, DataChunk &chunk,
                                    PhysicalOperatorState *state) {
	chunk.Reset();

	if (state->finished) {
		return;
	}

	if (column_ids.size() == 0) {
		throw NotImplementedException(
		    "CREATE INDEX does not refer to any columns in the base table!");
	}

	auto &schema = *table.schema;
	if (!schema.CreateIndex(context.ActiveTransaction(), info.get())) {
		// index already exists, but error ignored because of CREATE ... IF NOT
		// EXISTS
		return;
	}

	// create the chunk to hold intermediate expression results
	if (expressions.size() > 1) {
		throw NotImplementedException(
		    "Multidimensional indexes not supported yet");
	}

	DataChunk result;
	vector<TypeId> result_types;
	for (auto &expr : expressions) {
		result_types.push_back(expr->return_type);
	}
	result.Initialize(result_types);

	column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);

	ScanStructure ss;
	table.storage->InitializeScan(ss);

	DataChunk intermediate;
	auto types = table.storage->GetTypes(column_ids);
	intermediate.Initialize(types);

	// FIXME: use estimated table size as initial index size
	auto order_index =
	    make_unique<OrderIndex>(*table.storage, column_ids, types, result_types,
	                            move(expressions), STANDARD_VECTOR_SIZE);
	// now we start incrementally building the index
	while (true) {
		intermediate.Reset();

		// scan a new chunk from the table to index
		table.storage->CreateIndexScan(ss, column_ids, intermediate);
		if (intermediate.size() == 0) {
			// finished scanning for index creation
			// release all locks
			break;
		}
		// resolve the expressions for this chunk
		ExpressionExecutor executor(intermediate, context);
		executor.Execute(order_index->expressions, result);

		order_index->Insert(result,
		                    intermediate.data[intermediate.column_count - 1]);
	}
	// after we have finished inserting everything we sort the index
	order_index->Sort();

	table.storage->indexes.push_back(move(order_index));
	table.storage->ReleaseIndexLocks();

	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(0));

	state->finished = true;
}

unique_ptr<PhysicalOperatorState>
PhysicalCreateIndex::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalOperatorState>(nullptr, parent_executor);
}
