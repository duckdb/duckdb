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
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

PhysicalUpdate::PhysicalUpdate(vector<LogicalType> types, TableCatalogEntry &tableref, DataTable &table,
                               vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
                               vector<unique_ptr<Expression>> bound_defaults,
                               vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality,
                               bool return_chunk)
    : PhysicalOperator(PhysicalOperatorType::UPDATE, std::move(types), estimated_cardinality), tableref(tableref),
      table(table), columns(std::move(columns)), expressions(std::move(expressions)),
      bound_defaults(std::move(bound_defaults)), bound_constraints(std::move(bound_constraints)),
      return_chunk(return_chunk), index_update(false) {

	auto &indexes = table.GetDataTableInfo().get()->GetIndexes();
	auto index_columns = indexes.GetRequiredColumns();

	unordered_set<column_t> update_columns;
	for (const auto col : this->columns) {
		update_columns.insert(col.index);
	}

	for (const auto &col : table.Columns()) {
		if (index_columns.find(col.Logical().index) == index_columns.end()) {
			continue;
		}
		if (update_columns.find(col.Physical().index) == update_columns.end()) {
			continue;
		}
		index_update = true;
		break;
	}
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
	unordered_set<row_t> updated_rows;
	ColumnDataCollection return_collection;
};

class UpdateLocalState : public LocalSinkState {
public:
	UpdateLocalState(ClientContext &context, const vector<unique_ptr<Expression>> &expressions,
	                 const vector<LogicalType> &table_types, const vector<unique_ptr<Expression>> &bound_defaults,
	                 const vector<unique_ptr<BoundConstraint>> &bound_constraints)
	    : default_executor(context, bound_defaults), bound_constraints(bound_constraints) {

		// Initialize the update chunk.
		auto &allocator = Allocator::Get(context);
		vector<LogicalType> update_types;
		update_types.reserve(expressions.size());
		for (auto &expr : expressions) {
			update_types.push_back(expr->return_type);
		}
		update_chunk.Initialize(allocator, update_types);

		// Initialize the mock and delete chunk.
		mock_chunk.Initialize(allocator, table_types);
		delete_chunk.Initialize(allocator, table_types);
	}

	DataChunk update_chunk;
	DataChunk mock_chunk;
	DataChunk delete_chunk;
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
	auto &g_state = input.global_state.Cast<UpdateGlobalState>();
	auto &l_state = input.local_state.Cast<UpdateLocalState>();

	chunk.Flatten();
	l_state.default_executor.SetChunk(chunk);

	DataChunk &update_chunk = l_state.update_chunk;
	update_chunk.Reset();
	update_chunk.SetCardinality(chunk);

	for (idx_t i = 0; i < expressions.size(); i++) {
		// Default expression, set to the default value of the column.
		if (expressions[i]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			l_state.default_executor.ExecuteExpression(columns[i].index, update_chunk.data[i]);
			continue;
		}

		D_ASSERT(expressions[i]->GetExpressionType() == ExpressionType::BOUND_REF);
		auto &binding = expressions[i]->Cast<BoundReferenceExpression>();
		update_chunk.data[i].Reference(chunk.data[binding.index]);
	}

	lock_guard<mutex> glock(g_state.lock);
	auto &row_ids = chunk.data[chunk.ColumnCount() - 1];
	DataChunk &mock_chunk = l_state.mock_chunk;

	// Regular in-place update.
	if (!update_is_del_and_insert) {
		if (return_chunk) {
			mock_chunk.SetCardinality(update_chunk);
			for (idx_t i = 0; i < columns.size(); i++) {
				mock_chunk.data[columns[i].index].Reference(update_chunk.data[i]);
			}
		}
		auto &update_state = l_state.GetUpdateState(table, tableref, context.client);
		table.Update(update_state, context.client, row_ids, columns, update_chunk);

		if (return_chunk) {
			g_state.return_collection.Append(mock_chunk);
		}
		g_state.updated_count += chunk.size();
		return SinkResultType::NEED_MORE_INPUT;
	}

	// We update an index or a complex type, so we need to split the UPDATE into DELETE + INSERT.

	// Keep track of the rows that have not yet been deleted in this UPDATE.
	// This is required since we might see the same row_id multiple times, e.g.,
	// during an UPDATE containing joins.
	SelectionVector sel(update_chunk.size());
	idx_t update_count = 0;
	auto row_id_data = FlatVector::GetData<row_t>(row_ids);

	for (idx_t i = 0; i < update_chunk.size(); i++) {
		auto row_id = row_id_data[i];
		if (g_state.updated_rows.find(row_id) == g_state.updated_rows.end()) {
			g_state.updated_rows.insert(row_id);
			sel.set_index(update_count++, i);
		}
	}

	// The update chunk now contains exactly those rows that we are deleting.
	Vector del_row_ids(row_ids);
	if (update_count != update_chunk.size()) {
		update_chunk.Slice(sel, update_count);
		del_row_ids.Slice(row_ids, sel, update_count);
	}

	auto &delete_chunk = index_update ? l_state.delete_chunk : l_state.mock_chunk;
	delete_chunk.Reset();
	delete_chunk.SetCardinality(update_count);

	if (index_update) {
		auto &transaction = DuckTransaction::Get(context.client, table.db);
		vector<StorageIndex> column_ids;
		for (idx_t i = 0; i < table.ColumnCount(); i++) {
			column_ids.emplace_back(i);
		};
		// We need to fetch the previous index keys to add them to the delete index.
		auto fetch_state = ColumnFetchState();
		table.Fetch(transaction, delete_chunk, column_ids, row_ids, update_count, fetch_state);
	}

	auto &delete_state = l_state.GetDeleteState(table, tableref, context.client);
	table.Delete(delete_state, context.client, del_row_ids, update_count);

	// Arrange the columns in the standard table order.
	mock_chunk.SetCardinality(update_count);
	for (idx_t i = 0; i < columns.size(); i++) {
		mock_chunk.data[columns[i].index].Reference(update_chunk.data[i]);
	}

	table.LocalAppend(tableref, context.client, mock_chunk, bound_constraints, del_row_ids, delete_chunk);
	if (return_chunk) {
		g_state.return_collection.Append(mock_chunk);
	}

	g_state.updated_count += chunk.size();
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
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this);
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
