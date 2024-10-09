#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/update_state.hpp"

namespace duckdb {

PhysicalInsert::PhysicalInsert(
    vector<LogicalType> types_p, TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map,
    vector<unique_ptr<Expression>> bound_defaults, vector<unique_ptr<BoundConstraint>> bound_constraints_p,
    vector<unique_ptr<Expression>> set_expressions, vector<PhysicalIndex> set_columns, vector<LogicalType> set_types,
    idx_t estimated_cardinality, bool return_chunk, bool parallel, OnConflictAction action_type,
    unique_ptr<Expression> on_conflict_condition_p, unique_ptr<Expression> do_update_condition_p,
    unordered_set<column_t> conflict_target_p, vector<column_t> columns_to_fetch_p)
    : PhysicalOperator(PhysicalOperatorType::INSERT, std::move(types_p), estimated_cardinality),
      column_index_map(std::move(column_index_map)), insert_table(&table), insert_types(table.GetTypes()),
      bound_defaults(std::move(bound_defaults)), bound_constraints(std::move(bound_constraints_p)),
      return_chunk(return_chunk), parallel(parallel), action_type(action_type),
      set_expressions(std::move(set_expressions)), set_columns(std::move(set_columns)), set_types(std::move(set_types)),
      on_conflict_condition(std::move(on_conflict_condition_p)), do_update_condition(std::move(do_update_condition_p)),
      conflict_target(std::move(conflict_target_p)), columns_to_fetch(std::move(columns_to_fetch_p)) {

	if (action_type == OnConflictAction::THROW) {
		return;
	}

	D_ASSERT(this->set_expressions.size() == this->set_columns.size());

	// One or more columns are referenced from the existing table,
	// we use the 'insert_types' to figure out which types these columns have
	types_to_fetch = vector<LogicalType>(columns_to_fetch.size(), LogicalType::SQLNULL);
	for (idx_t i = 0; i < columns_to_fetch.size(); i++) {
		auto &id = columns_to_fetch[i];
		D_ASSERT(id < insert_types.size());
		types_to_fetch[i] = insert_types[id];
	}
}

PhysicalInsert::PhysicalInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info_p,
                               idx_t estimated_cardinality, bool parallel)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE_AS, op.types, estimated_cardinality), insert_table(nullptr),
      return_chunk(false), schema(&schema), info(std::move(info_p)), parallel(parallel),
      action_type(OnConflictAction::THROW) {
	GetInsertInfo(*info, insert_types, bound_defaults);
}

void PhysicalInsert::GetInsertInfo(const BoundCreateTableInfo &info, vector<LogicalType> &insert_types,
                                   vector<unique_ptr<Expression>> &bound_defaults) {
	auto &create_info = info.base->Cast<CreateTableInfo>();
	for (auto &col : create_info.columns.Physical()) {
		insert_types.push_back(col.GetType());
		bound_defaults.push_back(make_uniq<BoundConstantExpression>(Value(col.GetType())));
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class InsertGlobalState : public GlobalSinkState {
public:
	explicit InsertGlobalState(ClientContext &context, const vector<LogicalType> &return_types, DuckTableEntry &table)
	    : table(table), insert_count(0), initialized(false), return_collection(context, return_types) {
	}

	mutex lock;
	DuckTableEntry &table;
	idx_t insert_count;
	bool initialized;
	LocalAppendState append_state;
	ColumnDataCollection return_collection;
};

class InsertLocalState : public LocalSinkState {
public:
	InsertLocalState(ClientContext &context, const vector<LogicalType> &types,
	                 const vector<unique_ptr<Expression>> &bound_defaults,
	                 const vector<unique_ptr<BoundConstraint>> &bound_constraints)
	    : default_executor(context, bound_defaults), bound_constraints(bound_constraints) {
		insert_chunk.Initialize(Allocator::Get(context), types);
	}

	DataChunk insert_chunk;
	ExpressionExecutor default_executor;
	TableAppendState local_append_state;
	unique_ptr<RowGroupCollection> local_collection;
	optional_ptr<OptimisticDataWriter> writer;
	// Rows that have been updated by a DO UPDATE conflict
	unordered_set<row_t> updated_global_rows;
	// Rows in the transaction-local storage that have been updated by a DO UPDATE conflict
	unordered_set<row_t> updated_local_rows;
	idx_t update_count = 0;
	unique_ptr<ConstraintState> constraint_state;
	const vector<unique_ptr<BoundConstraint>> &bound_constraints;

	ConstraintState &GetConstraintState(DataTable &table, TableCatalogEntry &tableref) {
		if (!constraint_state) {
			constraint_state = table.InitializeConstraintState(tableref, bound_constraints);
		}
		return *constraint_state;
	}
};

unique_ptr<GlobalSinkState> PhysicalInsert::GetGlobalSinkState(ClientContext &context) const {
	optional_ptr<TableCatalogEntry> table;
	if (info) {
		// CREATE TABLE AS
		D_ASSERT(!insert_table);
		auto &catalog = schema->catalog;
		table = &catalog.CreateTable(catalog.GetCatalogTransaction(context), *schema.get_mutable(), *info)
		             ->Cast<TableCatalogEntry>();
	} else {
		D_ASSERT(insert_table);
		D_ASSERT(insert_table->IsDuckTable());
		table = insert_table.get_mutable();
	}
	auto result = make_uniq<InsertGlobalState>(context, GetTypes(), table->Cast<DuckTableEntry>());
	return std::move(result);
}

unique_ptr<LocalSinkState> PhysicalInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<InsertLocalState>(context.client, insert_types, bound_defaults, bound_constraints);
}

void PhysicalInsert::ResolveDefaults(const TableCatalogEntry &table, DataChunk &chunk,
                                     const physical_index_vector_t<idx_t> &column_index_map,
                                     ExpressionExecutor &default_executor, DataChunk &result) {
	chunk.Flatten();
	default_executor.SetChunk(chunk);

	result.Reset();
	result.SetCardinality(chunk);

	if (!column_index_map.empty()) {
		// columns specified by the user, use column_index_map
		for (auto &col : table.GetColumns().Physical()) {
			auto storage_idx = col.StorageOid();
			auto mapped_index = column_index_map[col.Physical()];
			if (mapped_index == DConstants::INVALID_INDEX) {
				// insert default value
				default_executor.ExecuteExpression(storage_idx, result.data[storage_idx]);
			} else {
				// get value from child chunk
				D_ASSERT((idx_t)mapped_index < chunk.ColumnCount());
				D_ASSERT(result.data[storage_idx].GetType() == chunk.data[mapped_index].GetType());
				result.data[storage_idx].Reference(chunk.data[mapped_index]);
			}
		}
	} else {
		// no columns specified, just append directly
		for (idx_t i = 0; i < result.ColumnCount(); i++) {
			D_ASSERT(result.data[i].GetType() == chunk.data[i].GetType());
			result.data[i].Reference(chunk.data[i]);
		}
	}
}

bool AllConflictsMeetCondition(DataChunk &result) {
	result.Flatten();
	auto data = FlatVector::GetData<bool>(result.data[0]);
	for (idx_t i = 0; i < result.size(); i++) {
		if (!data[i]) {
			return false;
		}
	}
	return true;
}

void CheckOnConflictCondition(ExecutionContext &context, DataChunk &conflicts, const unique_ptr<Expression> &condition,
                              DataChunk &result) {
	ExpressionExecutor executor(context.client, *condition);
	result.Initialize(context.client, {LogicalType::BOOLEAN});
	executor.Execute(conflicts, result);
	result.SetCardinality(conflicts.size());
}

static void CombineExistingAndInsertTuples(DataChunk &result, DataChunk &scan_chunk, DataChunk &input_chunk,
                                           ClientContext &client, const PhysicalInsert &op) {
	auto &types_to_fetch = op.types_to_fetch;
	auto &insert_types = op.insert_types;

	if (types_to_fetch.empty()) {
		// We have not scanned the initial table, so we can just duplicate the initial chunk
		result.Initialize(client, input_chunk.GetTypes());
		result.Reference(input_chunk);
		result.SetCardinality(input_chunk);
		return;
	}
	vector<LogicalType> combined_types;
	combined_types.reserve(insert_types.size() + types_to_fetch.size());
	combined_types.insert(combined_types.end(), insert_types.begin(), insert_types.end());
	combined_types.insert(combined_types.end(), types_to_fetch.begin(), types_to_fetch.end());

	result.Initialize(client, combined_types);
	result.Reset();
	// Add the VALUES list
	for (idx_t i = 0; i < insert_types.size(); i++) {
		idx_t col_idx = i;
		auto &other_col = input_chunk.data[i];
		auto &this_col = result.data[col_idx];
		D_ASSERT(other_col.GetType() == this_col.GetType());
		this_col.Reference(other_col);
	}
	// Add the columns from the original conflicting tuples
	for (idx_t i = 0; i < types_to_fetch.size(); i++) {
		idx_t col_idx = i + insert_types.size();
		auto &other_col = scan_chunk.data[i];
		auto &this_col = result.data[col_idx];
		D_ASSERT(other_col.GetType() == this_col.GetType());
		this_col.Reference(other_col);
	}
	// This is guaranteed by the requirement of a conflict target to have a condition or set expressions
	// Only when we have any sort of condition or SET expression that references the existing table is this possible
	// to not be true.
	// We can have a SET expression without a conflict target ONLY if there is only 1 Index on the table
	// In which case this also can't cause a discrepancy between existing tuple count and insert tuple count
	D_ASSERT(input_chunk.size() == scan_chunk.size());
	result.SetCardinality(input_chunk.size());
}

static void CreateUpdateChunk(ExecutionContext &context, DataChunk &chunk, TableCatalogEntry &table, Vector &row_ids,
                              DataChunk &update_chunk, const PhysicalInsert &op) {

	auto &do_update_condition = op.do_update_condition;
	auto &set_types = op.set_types;
	auto &set_expressions = op.set_expressions;
	// Check the optional condition for the DO UPDATE clause, to filter which rows will be updated
	if (do_update_condition) {
		DataChunk do_update_filter_result;
		do_update_filter_result.Initialize(context.client, {LogicalType::BOOLEAN});
		ExpressionExecutor where_executor(context.client, *do_update_condition);
		where_executor.Execute(chunk, do_update_filter_result);
		do_update_filter_result.SetCardinality(chunk.size());
		do_update_filter_result.Flatten();

		ManagedSelection selection(chunk.size());

		auto where_data = FlatVector::GetData<bool>(do_update_filter_result.data[0]);
		for (idx_t i = 0; i < chunk.size(); i++) {
			if (where_data[i]) {
				selection.Append(i);
			}
		}
		if (selection.Count() != selection.Size()) {
			// Not all conflicts met the condition, need to filter out the ones that don't
			chunk.Slice(selection.Selection(), selection.Count());
			chunk.SetCardinality(selection.Count());
			// Also apply this Slice to the to-update row_ids
			row_ids.Slice(selection.Selection(), selection.Count());
		}
	}

	// Execute the SET expressions
	update_chunk.Initialize(context.client, set_types);
	ExpressionExecutor executor(context.client, set_expressions);
	executor.Execute(chunk, update_chunk);
	update_chunk.SetCardinality(chunk);
}

template <bool GLOBAL>
static idx_t PerformOnConflictAction(ExecutionContext &context, DataChunk &chunk, TableCatalogEntry &table,
                                     Vector &row_ids, const PhysicalInsert &op) {

	if (op.action_type == OnConflictAction::NOTHING) {
		return 0;
	}
	auto &set_columns = op.set_columns;

	DataChunk update_chunk;
	CreateUpdateChunk(context, chunk, table, row_ids, update_chunk, op);

	auto &data_table = table.GetStorage();
	// Perform the update, using the results of the SET expressions
	if (GLOBAL) {
		auto update_state = data_table.InitializeUpdate(table, context.client, op.bound_constraints);
		data_table.Update(*update_state, context.client, row_ids, set_columns, update_chunk);
	} else {
		auto &local_storage = LocalStorage::Get(context.client, data_table.db);
		// Perform the update, using the results of the SET expressions
		local_storage.Update(data_table, row_ids, set_columns, update_chunk);
	}
	return update_chunk.size();
}

// TODO: should we use a hash table to keep track of this instead?
template <bool GLOBAL>
static void RegisterUpdatedRows(InsertLocalState &lstate, const Vector &row_ids, idx_t count) {
	// Insert all rows, if any of the rows has already been updated before, we throw an error
	auto data = FlatVector::GetData<row_t>(row_ids);

	// The rowids in the transaction-local ART aren't final yet so we have to separately keep track of the two sets of
	// rowids
	unordered_set<row_t> &updated_rows = GLOBAL ? lstate.updated_global_rows : lstate.updated_local_rows;
	for (idx_t i = 0; i < count; i++) {
		auto result = updated_rows.insert(data[i]);
		if (result.second == false) {
			throw InvalidInputException(
			    "ON CONFLICT DO UPDATE can not update the same row twice in the same command. Ensure that no rows "
			    "proposed for insertion within the same command have duplicate constrained values");
		}
	}
}

static map<idx_t, vector<idx_t>> CheckDistinctness(DataChunk &input, ConflictInfo &info) {
	map<idx_t, vector<idx_t>> conflicts;

	auto &column_ids = info.column_ids;
	ValidityMask valid(input.size());
	if (column_ids.empty()) {
		// FIXME: this happens for "INSERT OR REPLACE" or "ON CONFLICT DO .."" (without specific columns)
		// what we likely need to do is run through all the existing indexes to figure out on which columns they act
		// and use those same column ids here (running multiple times for each distinct column id set)
		return conflicts;
	}

	for (idx_t i = 0; i < input.size(); i++) {
		if (!valid.RowIsValid(i)) {
			// Already a conflict
			continue;
		}

		bool has_conflicts = false;
		for (idx_t j = i + 1; j < input.size(); j++) {
			if (!valid.RowIsValid(j)) {
				// Already a conflict
				continue;
			}
			bool matches = true;
			for (auto &col_idx : column_ids) {
				auto this_row = input.GetValue(col_idx, i);
				auto other_row = input.GetValue(col_idx, j);
				if (this_row.IsNull() || other_row.IsNull()) {
					matches = false;
					break;
				}
				if (this_row != other_row) {
					matches = false;
					break;
				}
			}
			if (matches) {
				auto &row_ids = conflicts[i];
				has_conflicts = true;
				row_ids.push_back(j);
				valid.SetInvalid(j);
			}
		}
		if (has_conflicts) {
			valid.SetInvalid(i);
		}
	}
	return conflicts;
}

template <bool GLOBAL>
static void VerifyOnConflictCondition(ExecutionContext &context, DataChunk &combined_chunk,
                                      const unique_ptr<Expression> &on_conflict_condition, InsertLocalState &lstate,
                                      DataTable &data_table, TableCatalogEntry &table, LocalStorage &local_storage) {
	if (!on_conflict_condition) {
		return;
	}
	DataChunk conflict_condition_result;
	CheckOnConflictCondition(context, combined_chunk, on_conflict_condition, conflict_condition_result);
	bool conditions_met = AllConflictsMeetCondition(conflict_condition_result);
	if (!conditions_met) {
		// Filter out the tuples that did pass the filter, then run the verify again
		ManagedSelection sel(combined_chunk.size());
		auto data = FlatVector::GetData<bool>(conflict_condition_result.data[0]);
		for (idx_t i = 0; i < combined_chunk.size(); i++) {
			if (!data[i]) {
				// Only populate the selection vector with the tuples that did not meet the condition
				sel.Append(i);
			}
		}
		combined_chunk.Slice(sel.Selection(), sel.Count());
		if (GLOBAL) {
			auto &constraint_state = lstate.GetConstraintState(data_table, table);
			data_table.VerifyAppendConstraints(constraint_state, context.client, combined_chunk, nullptr);
		} else {
			DataTable::VerifyUniqueIndexes(local_storage.GetIndexes(data_table), context.client, lstate.insert_chunk,
			                               nullptr);
		}
		throw InternalException("The previous operation was expected to throw but didn't");
	}
}

namespace {

struct ConflictTupleData {
	//! The index at which the conflict is registered in the SelectionVector
	idx_t conflict_idx;
	//! The index of the (to-be-inserted) tuple that has caused the conflict
	idx_t tuple_idx;
};

} // namespace

// Perform an INSERT for inner conflicts, rather than an UPDATE
template <bool GLOBAL>
static idx_t HandleUndetectedInsertConflicts(TableCatalogEntry &table, ExecutionContext &context,
                                             InsertGlobalState &gstate, InsertLocalState &lstate, DataTable &data_table,
                                             const PhysicalInsert &op, vector<pair<idx_t, idx_t>> &conflicts) {
	if (op.action_type == OnConflictAction::NOTHING) {
		return 0;
	}

	DataChunk conflict_chunk; // contains only the conflicting values
	DataChunk scan_chunk;     // contains the value that appeared earlier in the chunk, that caused the conflict
	DataChunk combined_chunk; // contains conflict_chunk + scan_chunk (wide)

	ManagedSelection conflict_sel(conflicts.size());
	ManagedSelection scan_sel(conflicts.size());
	for (auto &entry : conflicts) {
		scan_sel.Append(entry.first);
		conflict_sel.Append(entry.second);
	}

	conflict_chunk.Initialize(context.client, lstate.insert_chunk.GetTypes());
	conflict_chunk.Reference(lstate.insert_chunk);
	conflict_chunk.Slice(conflict_sel.Selection(), conflict_sel.Count());
	conflict_chunk.SetCardinality(conflict_sel.Count());

	if (!op.types_to_fetch.empty()) {
		scan_chunk.Initialize(context.client, op.types_to_fetch);
		for (idx_t i = 0; i < op.columns_to_fetch.size(); i++) {
			scan_chunk.data[i].Reference(lstate.insert_chunk.data[op.columns_to_fetch[i]]);
		}
		scan_chunk.Slice(scan_sel.Selection(), scan_sel.Count());
		scan_chunk.SetCardinality(scan_sel.Count());
	}

	// Splice the Input chunk and the fetched chunk together
	CombineExistingAndInsertTuples(combined_chunk, scan_chunk, conflict_chunk, context.client, op);

	auto &local_storage = LocalStorage::Get(context.client, data_table.db);
	auto &on_conflict_condition = op.on_conflict_condition;
	VerifyOnConflictCondition<GLOBAL>(context, combined_chunk, on_conflict_condition, lstate, data_table, table,
	                                  local_storage);

	DataChunk update_chunk;
	Vector dummy_row_ids(LogicalType::ROW_TYPE, combined_chunk.size());
	CreateUpdateChunk(context, combined_chunk, table, dummy_row_ids, update_chunk, op);

	// The DO UPDATE expressions have run, now we need to recreate the 'lstate.insert_chunk' layout, substituting the
	// existing columns with the updated ones.
	DataChunk insert_chunk;
	insert_chunk.Initialize(context.client, lstate.insert_chunk.GetTypes());
	insert_chunk.Slice(lstate.insert_chunk, conflict_sel.Selection(), conflict_sel.Count());
	for (idx_t i = 0; i < op.set_columns.size(); i++) {
		auto idx = op.set_columns[i].index;
		insert_chunk.data[idx].Reference(update_chunk.data[i]);
	}
	insert_chunk.SetCardinality(update_chunk.size());

	auto &storage = table.GetStorage();
	// FIXME: this messes with the insertion order, probably want a better solution than this
	// maybe an idea would be to provide a set of row_ids this should create
	// which would create gaps that we fill with the other LocalAppend ??

	// FIXME: this is problematic, since we perform this insert from 'HandleInsertConflicts', we have the possibility of
	// violating a unique constraint if the data is not present in GLOBAL=true but IS present in GLOBAL=false (or vice
	// versa), then this insert can cause: "Constraint Error: PRIMARY KEY or UNIQUE constraint violated: duplicate key
	// ..."
	storage.LocalAppend(gstate.append_state, table, context.client, insert_chunk, true);
	return insert_chunk.size();
}

template <bool GLOBAL>
static idx_t HandleInsertConflicts(TableCatalogEntry &table, ExecutionContext &context, InsertGlobalState &gstate,
                                   InsertLocalState &lstate, DataTable &data_table, const PhysicalInsert &op) {
	auto &types_to_fetch = op.types_to_fetch;
	auto &on_conflict_condition = op.on_conflict_condition;
	auto &conflict_target = op.conflict_target;
	auto &columns_to_fetch = op.columns_to_fetch;

	auto &local_storage = LocalStorage::Get(context.client, data_table.db);

	ConflictInfo conflict_info(conflict_target);

	auto inner_conflicts = CheckDistinctness(lstate.insert_chunk, conflict_info);

	ConflictManager conflict_manager(VerifyExistenceType::APPEND, lstate.insert_chunk.size(), &conflict_info);
	if (GLOBAL) {
		auto &constraint_state = lstate.GetConstraintState(data_table, table);
		data_table.VerifyAppendConstraints(constraint_state, context.client, lstate.insert_chunk, &conflict_manager);
	} else {
		DataTable::VerifyUniqueIndexes(local_storage.GetIndexes(data_table), context.client, lstate.insert_chunk,
		                               &conflict_manager);
	}

	conflict_manager.Finalize();

	idx_t affected_tuples = 0;
	if (conflict_manager.ConflictCount() == 0) {
		if (inner_conflicts.empty()) {
			// No conflicts found, 0 updates performed
			return 0;
		}
		vector<pair<idx_t, idx_t>> filtered_inner_conflicts;
		// There are only conflicts within the to-be-inserted-tuples
		ValidityMask seen(lstate.insert_chunk.size());
		for (auto &it : inner_conflicts) {
			seen.SetInvalid(it.first);
			for (auto &idx : it.second) {
				seen.SetInvalid(idx);
			}
			filtered_inner_conflicts.emplace_back(it.first, it.second.back());
		}
		D_ASSERT(!seen.AllValid());

		ManagedSelection remaining_tuples(lstate.insert_chunk.size());
		for (idx_t i = 0; i < lstate.insert_chunk.size(); i++) {
			if (seen.RowIsValid(i)) {
				remaining_tuples.Append(i);
			}
		}
		affected_tuples = HandleUndetectedInsertConflicts<GLOBAL>(table, context, gstate, lstate, data_table, op,
		                                                          filtered_inner_conflicts);
		lstate.insert_chunk.Slice(remaining_tuples.Selection(), remaining_tuples.Count());
		lstate.insert_chunk.SetCardinality(remaining_tuples.Count());
		return affected_tuples;
	}

	auto &row_ids = conflict_manager.RowIds();

	ManagedSelection remaining_conflicts(conflict_manager.ConflictCount());
	ManagedSelection all_conflicts(lstate.insert_chunk.size());
	if (!inner_conflicts.empty()) {
		// The input data is not all distinct on the relevant column_ids

		vector<pair<idx_t, idx_t>> filtered_inner_conflicts;
		// There are existing conflicts that need to be filtered, so we dont update the same row multiple times
		unordered_map<idx_t, idx_t> index_conflicts;
		for (idx_t i = 0; i < conflict_manager.ConflictCount(); i++) {
			index_conflicts[conflict_manager.Conflicts()[i]] = i;
		}

		map<idx_t, ConflictTupleData> remaining_conflict_map;
		for (auto &it : inner_conflicts) {
			auto entry = index_conflicts.find(it.first);
			if (entry == index_conflicts.end()) {
				// This conflict was not found by the INDEX constraint check
				filtered_inner_conflicts.emplace_back(it.first, it.second.back());
			} else {
#ifdef DEBUG
				for (auto &idx : it.second) {
					// If the first conflict was found, the others should also be found
					D_ASSERT(index_conflicts.count(idx));
				}
#endif
				remaining_conflict_map[entry->first] = ConflictTupleData {/*.conflict_idx =*/entry->second,
				                                                          /*.tuple_idx =*/it.second.back()};
			}
		}

		unordered_set<idx_t> flattened_conflicts;
		for (auto &it : inner_conflicts) {
			flattened_conflicts.insert(it.first);
			for (auto &idx : it.second) {
				flattened_conflicts.insert(idx);
			}
		}

		// The 'inner_conflicts' only contain conflicts within the insert chunk
		// this does not include conflicts on distinct values that *only* conflict with existing data
		// i.e if '2' exists in the table, and '2' does not occur more than once in the inserted data, 'inner_conflicts'
		// would not find it
		for (auto entry : index_conflicts) {
			if (!flattened_conflicts.count(entry.first)) {
				remaining_conflict_map[entry.first] = ConflictTupleData {/*.conflict_idx =*/entry.second,
				                                                         /*.tuple_idx =*/entry.first};
			}
		}

		// Combine the index conflicts with the inner conflicts, this will form the 'all_conflicts' selection vector
		set<idx_t> all_conflicts_set;
		for (auto &entry : index_conflicts) {
			all_conflicts_set.insert(entry.first);
		}
		for (auto &entry : flattened_conflicts) {
			all_conflicts_set.insert(entry);
		}
		for (auto &entry : all_conflicts_set) {
			all_conflicts.Append(entry);
		}

		ManagedSelection row_id_selvec(remaining_conflict_map.size());
		for (auto &entry : remaining_conflict_map) {
			auto &conflict_data = entry.second;
			row_id_selvec.Append(conflict_data.conflict_idx);
			remaining_conflicts.Append(conflict_data.tuple_idx);
		}

		D_ASSERT(row_id_selvec.Count() == remaining_conflicts.Count());
		row_ids.Slice(row_id_selvec.Selection(), row_id_selvec.Count());
		row_ids.Flatten(row_id_selvec.Count());

		affected_tuples += HandleUndetectedInsertConflicts<GLOBAL>(table, context, gstate, lstate, data_table, op,
		                                                           filtered_inner_conflicts);
	} else {
		for (idx_t i = 0; i < conflict_manager.ConflictCount(); i++) {
			remaining_conflicts.Append(conflict_manager.Conflicts()[i]);
		}
		for (idx_t i = 0; i < conflict_manager.ConflictCount(); i++) {
			all_conflicts.Append(conflict_manager.Conflicts()[i]);
		}
	}

	DataChunk conflict_chunk; // contains only the conflicting values
	DataChunk scan_chunk;     // contains the original values, that caused the conflict
	DataChunk combined_chunk; // contains conflict_chunk + scan_chunk (wide)

	// Filter out everything but the (remaining) conflicting rows
	conflict_chunk.Initialize(context.client, lstate.insert_chunk.GetTypes());
	conflict_chunk.Reference(lstate.insert_chunk);
	conflict_chunk.Slice(remaining_conflicts.Selection(), remaining_conflicts.Count());
	conflict_chunk.SetCardinality(remaining_conflicts.Count());

	// Holds the pins for the fetched rows
	unique_ptr<ColumnFetchState> fetch_state;
	if (!types_to_fetch.empty()) {
		D_ASSERT(scan_chunk.size() == 0);
		// When these values are required for the conditions or the SET expressions,
		// then we scan the existing table for the conflicting tuples, using the rowids
		scan_chunk.Initialize(context.client, types_to_fetch);
		fetch_state = make_uniq<ColumnFetchState>();
		if (GLOBAL) {
			auto &transaction = DuckTransaction::Get(context.client, table.catalog);
			data_table.Fetch(transaction, scan_chunk, columns_to_fetch, row_ids, remaining_conflicts.Count(),
			                 *fetch_state);
		} else {
			local_storage.FetchChunk(data_table, row_ids, remaining_conflicts.Count(), columns_to_fetch, scan_chunk,
			                         *fetch_state);
		}
	}

	// Splice the Input chunk and the fetched chunk together
	CombineExistingAndInsertTuples(combined_chunk, scan_chunk, conflict_chunk, context.client, op);

	VerifyOnConflictCondition<GLOBAL>(context, combined_chunk, on_conflict_condition, lstate, data_table, table,
	                                  local_storage);

	RegisterUpdatedRows<GLOBAL>(lstate, row_ids, combined_chunk.size());

	affected_tuples += PerformOnConflictAction<GLOBAL>(context, combined_chunk, table, row_ids, op);

	// Remove the conflicting tuples from the insert chunk
	SelectionVector sel_vec(lstate.insert_chunk.size());
	idx_t new_size = SelectionVector::Inverted(all_conflicts.Selection(), sel_vec, all_conflicts.Count(),
	                                           lstate.insert_chunk.size());
	lstate.insert_chunk.Slice(sel_vec, new_size);
	lstate.insert_chunk.SetCardinality(new_size);
	return affected_tuples;
}

idx_t PhysicalInsert::OnConflictHandling(TableCatalogEntry &table, ExecutionContext &context, InsertGlobalState &gstate,
                                         InsertLocalState &lstate) const {
	auto &data_table = table.GetStorage();
	if (action_type == OnConflictAction::THROW) {
		auto &constraint_state = lstate.GetConstraintState(data_table, table);
		data_table.VerifyAppendConstraints(constraint_state, context.client, lstate.insert_chunk, nullptr);
		return 0;
	}
	// Check whether any conflicts arise, and if they all meet the conflict_target + condition
	// If that's not the case - We throw the first error
	idx_t updated_tuples = 0;
	updated_tuples += HandleInsertConflicts<true>(table, context, gstate, lstate, data_table, *this);
	// Also check the transaction-local storage+ART so we can detect conflicts within this transaction
	updated_tuples += HandleInsertConflicts<false>(table, context, gstate, lstate, data_table, *this);

	return updated_tuples;
}

SinkResultType PhysicalInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<InsertGlobalState>();
	auto &lstate = input.local_state.Cast<InsertLocalState>();

	auto &table = gstate.table;
	auto &storage = table.GetStorage();
	PhysicalInsert::ResolveDefaults(table, chunk, column_index_map, lstate.default_executor, lstate.insert_chunk);

	if (!parallel) {
		if (!gstate.initialized) {
			storage.InitializeLocalAppend(gstate.append_state, table, context.client, bound_constraints);
			gstate.initialized = true;
		}

		if (action_type != OnConflictAction::NOTHING && return_chunk) {
			// If the action is UPDATE or REPLACE, we will always create either an APPEND or an INSERT
			// for NOTHING we don't create either an APPEND or an INSERT for the tuple
			// so it should not be added to the RETURNING chunk
			gstate.return_collection.Append(lstate.insert_chunk);
		}
		idx_t updated_tuples = OnConflictHandling(table, context, gstate, lstate);
		if (action_type == OnConflictAction::NOTHING && return_chunk) {
			// Because we didn't add to the RETURNING chunk yet
			// we add the tuples that did not get filtered out now
			gstate.return_collection.Append(lstate.insert_chunk);
		}
		gstate.insert_count += lstate.insert_chunk.size();
		gstate.insert_count += updated_tuples;
		storage.LocalAppend(gstate.append_state, table, context.client, lstate.insert_chunk, true);

		// We finalize the local append to write the segment node count.
		if (action_type != OnConflictAction::THROW) {
			storage.FinalizeLocalAppend(gstate.append_state);
			gstate.initialized = false;
		}

	} else {
		D_ASSERT(!return_chunk);
		// parallel append
		if (!lstate.local_collection) {
			lock_guard<mutex> l(gstate.lock);
			auto table_info = storage.GetDataTableInfo();
			auto &block_manager = TableIOManager::Get(storage).GetBlockManagerForRowData();
			lstate.local_collection = make_uniq<RowGroupCollection>(std::move(table_info), block_manager, insert_types,
			                                                        NumericCast<idx_t>(MAX_ROW_ID));
			lstate.local_collection->InitializeEmpty();
			lstate.local_collection->InitializeAppend(lstate.local_append_state);
			lstate.writer = &gstate.table.GetStorage().CreateOptimisticWriter(context.client);
		}
		OnConflictHandling(table, context, gstate, lstate);

		auto new_row_group = lstate.local_collection->Append(lstate.insert_chunk, lstate.local_append_state);
		if (new_row_group) {
			lstate.writer->WriteNewRowGroup(*lstate.local_collection);
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<InsertGlobalState>();
	auto &lstate = input.local_state.Cast<InsertLocalState>();
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this);
	client_profiler.Flush(context.thread.profiler);

	if (!parallel || !lstate.local_collection) {
		return SinkCombineResultType::FINISHED;
	}

	// parallel append: finalize the append
	TransactionData tdata(0, 0);
	lstate.local_collection->FinalizeAppend(tdata, lstate.local_append_state);

	auto append_count = lstate.local_collection->GetTotalRows();

	lock_guard<mutex> lock(gstate.lock);
	gstate.insert_count += append_count;
	if (append_count < Storage::ROW_GROUP_SIZE) {
		// we have few rows - append to the local storage directly
		auto &table = gstate.table;
		auto &storage = table.GetStorage();
		storage.InitializeLocalAppend(gstate.append_state, table, context.client, bound_constraints);
		auto &transaction = DuckTransaction::Get(context.client, table.catalog);
		lstate.local_collection->Scan(transaction, [&](DataChunk &insert_chunk) {
			storage.LocalAppend(gstate.append_state, table, context.client, insert_chunk);
			return true;
		});
		storage.FinalizeLocalAppend(gstate.append_state);
	} else {
		// we have written rows to disk optimistically - merge directly into the transaction-local storage
		gstate.table.GetStorage().LocalMerge(context.client, *lstate.local_collection);
		gstate.table.GetStorage().FinalizeOptimisticWriter(context.client, *lstate.writer);
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<InsertGlobalState>();
	if (!parallel && gstate.initialized) {
		auto &table = gstate.table;
		auto &storage = table.GetStorage();
		storage.FinalizeLocalAppend(gstate.append_state);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class InsertSourceState : public GlobalSourceState {
public:
	explicit InsertSourceState(const PhysicalInsert &op) {
		if (op.return_chunk) {
			D_ASSERT(op.sink_state);
			auto &g = op.sink_state->Cast<InsertGlobalState>();
			g.return_collection.InitializeScan(scan_state);
		}
	}

	ColumnDataScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<InsertSourceState>(*this);
}

SourceResultType PhysicalInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<InsertSourceState>();
	auto &insert_gstate = sink_state->Cast<InsertGlobalState>();
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(insert_gstate.insert_count)));
		return SourceResultType::FINISHED;
	}

	insert_gstate.return_collection.Scan(state.scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
