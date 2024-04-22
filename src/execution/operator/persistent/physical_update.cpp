#include "duckdb/execution/operator/persistent/physical_update.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/storage/table/update_state.hpp"

namespace duckdb {

PhysicalUpdate::PhysicalUpdate(vector<LogicalType> types, TableCatalogEntry &tableref, DataTable &table,
                               vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
                               vector<unique_ptr<Expression>> bound_defaults,
                               vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality,
                               bool return_chunk)
    : PhysicalOperator(PhysicalOperatorType::UPDATE, std::move(types), estimated_cardinality), tableref(tableref),
      table(table), columns(std::move(columns)), expressions(std::move(expressions)),
      bound_defaults(std::move(bound_defaults)), bound_constraints(std::move(bound_constraints)),
      return_chunk(return_chunk) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class UpdateGlobalState : public GlobalSinkState {
public:
	explicit UpdateGlobalState(ClientContext &context, const vector<LogicalType> &return_types)
	    : updated_count(0), return_collection(context, return_types) {
	}

	mutex lock;
	idx_t updated_count;
	unordered_set<row_t> updated_columns;
	ColumnDataCollection return_collection;
};

class UpdateLocalState : public LocalSinkState {
public:
	UpdateLocalState(ClientContext &context, const vector<unique_ptr<Expression>> &expressions,
	                 const vector<LogicalType> &table_types, const vector<unique_ptr<Expression>> &bound_defaults,
	                 const vector<unique_ptr<BoundConstraint>> &bound_constraints)
	    : default_executor(context, bound_defaults), bound_constraints(bound_constraints) {
		// initialize the update chunk
		auto &allocator = Allocator::Get(context);
		vector<LogicalType> update_types;
		update_types.reserve(expressions.size());
		for (auto &expr : expressions) {
			update_types.push_back(expr->return_type);
		}
		update_chunk.Initialize(allocator, update_types);
		// initialize the mock chunk
		mock_chunk.Initialize(allocator, table_types);
	}

	DataChunk update_chunk;
	DataChunk mock_chunk;
	ExpressionExecutor default_executor;
	unique_ptr<TableDeleteState> delete_state;
	unique_ptr<TableUpdateState> update_state;
	const vector<unique_ptr<BoundConstraint>> &bound_constraints;

	TableDeleteState &GetDeleteState(DataTable &table, TableCatalogEntry &tableref, ClientContext &context) {
		if (!delete_state) {
			delete_state = table.InitializeDelete(tableref, context, bound_constraints);
		}
		return *delete_state;
	}

	TableUpdateState &GetUpdateState(DataTable &table, TableCatalogEntry &tableref, ClientContext &context) {
		if (!update_state) {
			update_state = table.InitializeUpdate(tableref, context, bound_constraints);
		}
		return *update_state;
	}
};

SinkResultType PhysicalUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<UpdateGlobalState>();
	auto &lstate = input.local_state.Cast<UpdateLocalState>();

	DataChunk &update_chunk = lstate.update_chunk;
	DataChunk &mock_chunk = lstate.mock_chunk;

	chunk.Flatten();
	lstate.default_executor.SetChunk(chunk);

	// update data in the base table
	// the row ids are given to us as the last column of the child chunk
	auto &row_ids = chunk.data[chunk.ColumnCount() - 1];
	update_chunk.Reset();
	update_chunk.SetCardinality(chunk);

	for (idx_t i = 0; i < expressions.size(); i++) {
		if (expressions[i]->type == ExpressionType::VALUE_DEFAULT) {
			// default expression, set to the default value of the column
			lstate.default_executor.ExecuteExpression(columns[i].index, update_chunk.data[i]);
		} else {
			D_ASSERT(expressions[i]->type == ExpressionType::BOUND_REF);
			// index into child chunk
			auto &binding = expressions[i]->Cast<BoundReferenceExpression>();
			update_chunk.data[i].Reference(chunk.data[binding.index]);
		}
	}

	lock_guard<mutex> glock(gstate.lock);
	if (update_is_del_and_insert) {
		// index update or update on complex type, perform a delete and an append instead

		// figure out which rows have not yet been deleted in this update
		// this is required since we might see the same row_id multiple times
		// in the case of an UPDATE query that e.g. has joins
		auto row_id_data = FlatVector::GetData<row_t>(row_ids);
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		idx_t update_count = 0;
		for (idx_t i = 0; i < update_chunk.size(); i++) {
			auto row_id = row_id_data[i];
			if (gstate.updated_columns.find(row_id) == gstate.updated_columns.end()) {
				gstate.updated_columns.insert(row_id);
				sel.set_index(update_count++, i);
			}
		}
		if (update_count != update_chunk.size()) {
			// we need to slice here
			update_chunk.Slice(sel, update_count);
		}
		auto &delete_state = lstate.GetDeleteState(table, tableref, context.client);
		table.Delete(delete_state, context.client, row_ids, update_chunk.size());
		// for the append we need to arrange the columns in a specific manner (namely the "standard table order")
		mock_chunk.SetCardinality(update_chunk);
		for (idx_t i = 0; i < columns.size(); i++) {
			mock_chunk.data[columns[i].index].Reference(update_chunk.data[i]);
		}
		table.LocalAppend(tableref, context.client, mock_chunk, bound_constraints);
	} else {
		if (return_chunk) {
			mock_chunk.SetCardinality(update_chunk);
			for (idx_t i = 0; i < columns.size(); i++) {
				mock_chunk.data[columns[i].index].Reference(update_chunk.data[i]);
			}
		}
		auto &update_state = lstate.GetUpdateState(table, tableref, context.client);
		table.Update(update_state, context.client, row_ids, columns, update_chunk);
	}

	if (return_chunk) {
		gstate.return_collection.Append(mock_chunk);
	}

	gstate.updated_count += chunk.size();

	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<UpdateGlobalState>(context, GetTypes());
}

unique_ptr<LocalSinkState> PhysicalUpdate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<UpdateLocalState>(context.client, expressions, table.GetTypes(), bound_defaults,
	                                   bound_constraints);
}

SinkCombineResultType PhysicalUpdate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &state = input.local_state.Cast<UpdateLocalState>();
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this, state.default_executor, "default_executor", 1);
	client_profiler.Flush(context.thread.profiler);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class UpdateSourceState : public GlobalSourceState {
public:
	explicit UpdateSourceState(const PhysicalUpdate &op) {
		if (op.return_chunk) {
			D_ASSERT(op.sink_state);
			auto &g = op.sink_state->Cast<UpdateGlobalState>();
			g.return_collection.InitializeScan(scan_state);
		}
	}

	ColumnDataScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<UpdateSourceState>(*this);
}

SourceResultType PhysicalUpdate::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<UpdateSourceState>();
	auto &g = sink_state->Cast<UpdateGlobalState>();
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.updated_count)));
		return SourceResultType::FINISHED;
	}

	g.return_collection.Scan(state.scan_state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
