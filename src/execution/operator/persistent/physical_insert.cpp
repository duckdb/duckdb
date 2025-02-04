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
#include "duckdb/function/create_sort_key.hpp"

namespace duckdb {

PhysicalInsert::PhysicalInsert(
    vector<LogicalType> types_p, TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map,
    vector<unique_ptr<Expression>> bound_defaults, vector<unique_ptr<BoundConstraint>> bound_constraints_p,
    vector<unique_ptr<Expression>> set_expressions, vector<PhysicalIndex> set_columns, vector<LogicalType> set_types,
    idx_t estimated_cardinality, bool return_chunk, bool parallel, OnConflictAction action_type,
    unique_ptr<Expression> on_conflict_condition_p, unique_ptr<Expression> do_update_condition_p,
    unordered_set<column_t> conflict_target_p, vector<column_t> columns_to_fetch_p, bool update_is_del_and_insert)
    : PhysicalOperator(PhysicalOperatorType::INSERT, std::move(types_p), estimated_cardinality),
      column_index_map(std::move(column_index_map)), insert_table(&table), insert_types(table.GetTypes()),
      bound_defaults(std::move(bound_defaults)), bound_constraints(std::move(bound_constraints_p)),
      return_chunk(return_chunk), parallel(parallel), action_type(action_type),
      set_expressions(std::move(set_expressions)), set_columns(std::move(set_columns)), set_types(std::move(set_types)),
      on_conflict_condition(std::move(on_conflict_condition_p)), do_update_condition(std::move(do_update_condition_p)),
      conflict_target(std::move(conflict_target_p)), update_is_del_and_insert(update_is_del_and_insert) {

	if (action_type == OnConflictAction::THROW) {
		return;
	}

	D_ASSERT(this->set_expressions.size() == this->set_columns.size());

	// One or more columns are referenced from the existing table,
	// we use the 'insert_types' to figure out which types these columns have
	types_to_fetch = vector<LogicalType>(columns_to_fetch_p.size(), LogicalType::SQLNULL);
	for (idx_t i = 0; i < columns_to_fetch_p.size(); i++) {
		auto &id = columns_to_fetch_p[i];
		D_ASSERT(id < insert_types.size());
		types_to_fetch[i] = insert_types[id];
		columns_to_fetch.emplace_back(id);
	}
}

PhysicalInsert::PhysicalInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info_p,
                               idx_t estimated_cardinality, bool parallel)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE_AS, op.types, estimated_cardinality), insert_table(nullptr),
      return_chunk(false), schema(&schema), info(std::move(info_p)), parallel(parallel),
      action_type(OnConflictAction::THROW), update_is_del_and_insert(false) {
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

InsertGlobalState::InsertGlobalState(ClientContext &context, const vector<LogicalType> &return_types,
                                     DuckTableEntry &table)
    : table(table), insert_count(0), initialized(false), return_collection(context, return_types) {
}

InsertLocalState::InsertLocalState(ClientContext &context, const vector<LogicalType> &types,
                                   const vector<unique_ptr<Expression>> &bound_defaults,
                                   const vector<unique_ptr<BoundConstraint>> &bound_constraints)
    : default_executor(context, bound_defaults), bound_constraints(bound_constraints) {

	auto &allocator = Allocator::Get(context);
	insert_chunk.Initialize(allocator, types);
	update_chunk.Initialize(allocator, types);
	append_chunk.Initialize(allocator, types);
}

ConstraintState &InsertLocalState::GetConstraintState(DataTable &table, TableCatalogEntry &table_ref) {
	if (!constraint_state) {
		constraint_state = table.InitializeConstraintState(table_ref, bound_constraints);
	}
	return *constraint_state;
}

TableDeleteState &InsertLocalState::GetDeleteState(DataTable &table, TableCatalogEntry &table_ref,
                                                   ClientContext &context) {
	if (!delete_state) {
		delete_state = table.InitializeDelete(table_ref, context, bound_constraints);
	}
	return *delete_state;
}

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
			row_ids.Flatten(selection.Count());
		}
	}

	if (chunk.size() == 0) {
		auto initialize = vector<bool>(set_types.size(), false);
		update_chunk.Initialize(context.client, set_types, initialize, chunk.size());
		update_chunk.SetCardinality(chunk);
		return;
	}

	// Execute the SET expressions.
	update_chunk.Initialize(context.client, set_types, chunk.size());
	ExpressionExecutor executor(context.client, set_expressions);
	executor.Execute(chunk, update_chunk);
	update_chunk.SetCardinality(chunk);
}

template <bool GLOBAL>
static idx_t PerformOnConflictAction(InsertLocalState &lstate, InsertGlobalState &gstate, ExecutionContext &context,
                                     DataChunk &chunk, TableCatalogEntry &table, Vector &row_ids,
                                     const PhysicalInsert &op) {
	// Early-out, if we do nothing on conflicting rows.
	if (op.action_type == OnConflictAction::NOTHING) {
		return 0;
	}

	auto &set_columns = op.set_columns;
	DataChunk update_chunk;
	CreateUpdateChunk(context, chunk, table, row_ids, update_chunk, op);
	auto &data_table = table.GetStorage();

	if (update_chunk.size() == 0) {
		// Nothing to do
		return update_chunk.size();
	}

	// Arrange the columns in the standard table order.
	DataChunk &append_chunk = lstate.append_chunk;
	append_chunk.SetCardinality(update_chunk);
	for (idx_t i = 0; i < append_chunk.ColumnCount(); i++) {
		append_chunk.data[i].Reference(chunk.data[i]);
	}
	for (idx_t i = 0; i < set_columns.size(); i++) {
		append_chunk.data[set_columns[i].index].Reference(update_chunk.data[i]);
	}

	// Perform the UPDATE on the (global) storage.
	if (!op.update_is_del_and_insert) {
		if (!op.parallel && op.return_chunk) {
			gstate.return_collection.Append(append_chunk);
		}

		if (GLOBAL) {
			auto update_state = data_table.InitializeUpdate(table, context.client, op.bound_constraints);
			data_table.Update(*update_state, context.client, row_ids, set_columns, update_chunk);
			return update_chunk.size();
		}
		auto &local_storage = LocalStorage::Get(context.client, data_table.db);
		if (gstate.initialized) {
			// Flush the data first, it might be referenced by the Update
			data_table.FinalizeLocalAppend(gstate.append_state);
			gstate.initialized = false;
		}
		local_storage.Update(data_table, row_ids, set_columns, update_chunk);
		return update_chunk.size();
	}

	if (GLOBAL) {
		auto &delete_state = lstate.GetDeleteState(data_table, table, context.client);
		data_table.Delete(delete_state, context.client, row_ids, update_chunk.size());
	} else {
		auto &local_storage = LocalStorage::Get(context.client, data_table.db);
		local_storage.Delete(data_table, row_ids, update_chunk.size());
	}

	if (!op.parallel && op.return_chunk) {
		gstate.return_collection.Append(append_chunk);
	}
	data_table.LocalAppend(table, context.client, append_chunk, op.bound_constraints, row_ids, append_chunk);
	return update_chunk.size();
}

// TODO: should we use a hash table to keep track of this instead?
static void RegisterUpdatedRows(InsertLocalState &lstate, const Vector &row_ids, idx_t count) {
	// Insert all rows, if any of the rows has already been updated before, we throw an error
	auto data = FlatVector::GetData<row_t>(row_ids);

	auto &updated_rows = lstate.updated_rows;
	for (idx_t i = 0; i < count; i++) {
		auto result = updated_rows.insert(data[i]);
		if (result.second == false) {
			// This is following postgres behavior:
			throw InvalidInputException(
			    "ON CONFLICT DO UPDATE can not update the same row twice in the same command. Ensure that no rows "
			    "proposed for insertion within the same command have duplicate constrained values");
		}
	}
}

static void CheckDistinctnessInternal(ValidityMask &valid, vector<reference<Vector>> &sort_keys, idx_t count,
                                      map<idx_t, vector<idx_t>> &result) {
	for (idx_t i = 0; i < count; i++) {
		bool has_conflicts = false;
		for (idx_t j = i + 1; j < count; j++) {
			if (!valid.RowIsValid(j)) {
				// Already a conflict
				continue;
			}
			bool matches = true;
			for (auto &sort_key : sort_keys) {
				auto &this_row = FlatVector::GetData<string_t>(sort_key.get())[i];
				auto &other_row = FlatVector::GetData<string_t>(sort_key.get())[j];
				if (this_row != other_row) {
					matches = false;
					break;
				}
			}
			if (matches) {
				auto &row_ids = result[i];
				has_conflicts = true;
				row_ids.push_back(j);
				valid.SetInvalid(j);
			}
		}
		if (has_conflicts) {
			valid.SetInvalid(i);
		}
	}
}

static void PrepareSortKeys(DataChunk &input, unordered_map<column_t, unique_ptr<Vector>> &sort_keys,
                            const unordered_set<column_t> &column_ids) {
	OrderModifiers order_modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
	for (auto &it : column_ids) {
		auto &sort_key = sort_keys[it];
		if (sort_key != nullptr) {
			continue;
		}
		auto &column = input.data[it];
		sort_key = make_uniq<Vector>(LogicalType::BLOB);
		CreateSortKeyHelpers::CreateSortKey(column, input.size(), order_modifiers, *sort_key);
	}
}

static map<idx_t, vector<idx_t>> CheckDistinctness(DataChunk &input, ConflictInfo &info,
                                                   unordered_set<BoundIndex *> &matched_indexes) {
	map<idx_t, vector<idx_t>> conflicts;
	unordered_map<idx_t, unique_ptr<Vector>> sort_keys;
	//! Register which rows have already caused a conflict
	ValidityMask valid(input.size());

	auto &column_ids = info.column_ids;
	if (column_ids.empty()) {
		for (auto index : matched_indexes) {
			auto &index_column_ids = index->GetColumnIdSet();
			PrepareSortKeys(input, sort_keys, index_column_ids);
			vector<reference<Vector>> columns;
			for (auto &idx : index_column_ids) {
				columns.push_back(*sort_keys[idx]);
			}
			CheckDistinctnessInternal(valid, columns, input.size(), conflicts);
		}
	} else {
		PrepareSortKeys(input, sort_keys, column_ids);
		vector<reference<Vector>> columns;
		for (auto &idx : column_ids) {
			columns.push_back(*sort_keys[idx]);
		}
		CheckDistinctnessInternal(valid, columns, input.size(), conflicts);
	}
	return conflicts;
}

template <bool GLOBAL>
static void VerifyOnConflictCondition(ExecutionContext &context, DataChunk &combined_chunk,
                                      const unique_ptr<Expression> &on_conflict_condition,
                                      ConstraintState &constraint_state, DataChunk &tuples, DataTable &data_table,
                                      LocalStorage &local_storage) {
	if (!on_conflict_condition) {
		return;
	}
	DataChunk conflict_condition_result;
	CheckOnConflictCondition(context, combined_chunk, on_conflict_condition, conflict_condition_result);
	bool conditions_met = AllConflictsMeetCondition(conflict_condition_result);
	if (conditions_met) {
		return;
	}

	// We need to throw. Filter all tuples that passed, and verify again with those that violate the constraint.
	ManagedSelection sel(combined_chunk.size());
	auto data = FlatVector::GetData<bool>(conflict_condition_result.data[0]);
	for (idx_t i = 0; i < combined_chunk.size(); i++) {
		if (!data[i]) {
			// This tuple did not meet the condition.
			sel.Append(i);
		}
	}
	combined_chunk.Slice(sel.Selection(), sel.Count());

	// Verify and throw.
	if (GLOBAL) {
		data_table.VerifyAppendConstraints(constraint_state, context.client, combined_chunk, nullptr, nullptr);
		throw InternalException("VerifyAppendConstraints was expected to throw but didn't");
	}

	auto &indexes = local_storage.GetIndexes(data_table);
	auto storage = local_storage.GetStorage(data_table);
	DataTable::VerifyUniqueIndexes(indexes, storage, tuples, nullptr);
	throw InternalException("VerifyUniqueIndexes was expected to throw but didn't");
}

template <bool GLOBAL>
static idx_t HandleInsertConflicts(TableCatalogEntry &table, ExecutionContext &context, InsertLocalState &lstate,
                                   InsertGlobalState &gstate, DataChunk &tuples, const PhysicalInsert &op) {
	auto &types_to_fetch = op.types_to_fetch;
	auto &on_conflict_condition = op.on_conflict_condition;
	auto &conflict_target = op.conflict_target;
	auto &columns_to_fetch = op.columns_to_fetch;
	auto &data_table = table.GetStorage();

	auto &local_storage = LocalStorage::Get(context.client, data_table.db);

	ConflictInfo conflict_info(conflict_target);
	ConflictManager conflict_manager(VerifyExistenceType::APPEND, tuples.size(), &conflict_info);
	if (GLOBAL) {
		auto &constraint_state = lstate.GetConstraintState(data_table, table);
		auto storage = local_storage.GetStorage(data_table);
		data_table.VerifyAppendConstraints(constraint_state, context.client, tuples, storage, &conflict_manager);
	} else {
		auto &indexes = local_storage.GetIndexes(data_table);
		auto storage = local_storage.GetStorage(data_table);
		DataTable::VerifyUniqueIndexes(indexes, storage, tuples, &conflict_manager);
	}

	conflict_manager.Finalize();
	if (conflict_manager.ConflictCount() == 0) {
		// No conflicts found, 0 updates performed
		return 0;
	}
	idx_t affected_tuples = 0;

	auto &conflicts = conflict_manager.Conflicts();
	auto &row_ids = conflict_manager.RowIds();

	DataChunk conflict_chunk; // contains only the conflicting values
	DataChunk scan_chunk;     // contains the original values, that caused the conflict
	DataChunk combined_chunk; // contains conflict_chunk + scan_chunk (wide)

	// Filter out everything but the conflicting rows
	conflict_chunk.Initialize(context.client, tuples.GetTypes());
	conflict_chunk.Reference(tuples);
	conflict_chunk.Slice(conflicts.Selection(), conflicts.Count());
	conflict_chunk.SetCardinality(conflicts.Count());

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
			data_table.Fetch(transaction, scan_chunk, columns_to_fetch, row_ids, conflicts.Count(), *fetch_state);
		} else {
			local_storage.FetchChunk(data_table, row_ids, conflicts.Count(), columns_to_fetch, scan_chunk,
			                         *fetch_state);
		}
	}

	// Splice the Input chunk and the fetched chunk together
	CombineExistingAndInsertTuples(combined_chunk, scan_chunk, conflict_chunk, context.client, op);

	auto &constraint_state = lstate.GetConstraintState(data_table, table);
	VerifyOnConflictCondition<GLOBAL>(context, combined_chunk, on_conflict_condition, constraint_state, tuples,
	                                  data_table, local_storage);

	if (&tuples == &lstate.update_chunk) {
		// Allow updating duplicate rows for the 'update_chunk'
		RegisterUpdatedRows(lstate, row_ids, combined_chunk.size());
	}

	affected_tuples += PerformOnConflictAction<GLOBAL>(lstate, gstate, context, combined_chunk, table, row_ids, op);

	// Remove the conflicting tuples from the insert chunk
	SelectionVector sel_vec(tuples.size());
	idx_t new_size = SelectionVector::Inverted(conflicts.Selection(), sel_vec, conflicts.Count(), tuples.size());
	tuples.Slice(sel_vec, new_size);
	tuples.SetCardinality(new_size);
	return affected_tuples;
}

idx_t PhysicalInsert::OnConflictHandling(TableCatalogEntry &table, ExecutionContext &context, InsertGlobalState &gstate,
                                         InsertLocalState &lstate) const {
	auto &data_table = table.GetStorage();
	auto &local_storage = LocalStorage::Get(context.client, data_table.db);

	if (action_type == OnConflictAction::THROW) {
		auto &constraint_state = lstate.GetConstraintState(data_table, table);
		auto storage = local_storage.GetStorage(data_table);
		data_table.VerifyAppendConstraints(constraint_state, context.client, lstate.insert_chunk, storage, nullptr);
		return 0;
	}

	ConflictInfo conflict_info(conflict_target);

	auto &global_indexes = data_table.GetDataTableInfo()->GetIndexes();
	auto &local_indexes = local_storage.GetIndexes(data_table);

	unordered_set<BoundIndex *> matched_indexes;
	if (conflict_info.column_ids.empty()) {
		// We care about every index that applies to the table if no ON CONFLICT (...) target is given
		global_indexes.Scan([&](Index &index) {
			if (!index.IsUnique()) {
				return false;
			}
			if (conflict_info.ConflictTargetMatches(index)) {
				D_ASSERT(index.IsBound());
				auto &bound_index = index.Cast<BoundIndex>();
				matched_indexes.insert(&bound_index);
			}
			return false;
		});
		local_indexes.Scan([&](Index &index) {
			if (!index.IsUnique()) {
				return false;
			}
			if (conflict_info.ConflictTargetMatches(index)) {
				D_ASSERT(index.IsBound());
				auto &bound_index = index.Cast<BoundIndex>();
				matched_indexes.insert(&bound_index);
			}
			return false;
		});
	}

	auto inner_conflicts = CheckDistinctness(lstate.insert_chunk, conflict_info, matched_indexes);
	idx_t count = lstate.insert_chunk.size();
	if (!inner_conflicts.empty()) {
		// We have at least one inner conflict, filter it out
		ManagedSelection sel_vec(count);
		ValidityMask not_a_conflict(count);
		set<idx_t> last_occurrences_of_conflict;
		for (idx_t i = 0; i < count; i++) {
			auto it = inner_conflicts.find(i);
			if (it != inner_conflicts.end()) {
				auto &conflicts = it->second;
				auto conflict_it = conflicts.begin();
				for (; conflict_it != conflicts.end();) {
					auto &idx = *conflict_it;
					not_a_conflict.SetInvalid(idx);
					conflict_it++;
					if (conflict_it == conflicts.end()) {
						last_occurrences_of_conflict.insert(idx);
					}
				}
			}
			if (not_a_conflict.RowIsValid(i)) {
				sel_vec.Append(i);
			}
		}
		if (action_type == OnConflictAction::UPDATE) {
			if (do_update_condition) {
				//! See https://github.com/duckdblabs/duckdb-internal/issues/4090 for context
				throw NotImplementedException("Inner conflicts detected with a conditional DO UPDATE on-conflict "
				                              "action, not fully implemented yet");
			}
			ManagedSelection last_occurrences(last_occurrences_of_conflict.size());
			for (auto &idx : last_occurrences_of_conflict) {
				last_occurrences.Append(idx);
			}

			lstate.update_chunk.Reference(lstate.insert_chunk);
			lstate.update_chunk.Slice(last_occurrences.Selection(), last_occurrences.Count());
			lstate.update_chunk.SetCardinality(last_occurrences.Count());
		}

		lstate.insert_chunk.Slice(sel_vec.Selection(), sel_vec.Count());
		lstate.insert_chunk.SetCardinality(sel_vec.Count());
	}

	// Check whether any conflicts arise, and if they all meet the conflict_target + condition
	// If that's not the case - We throw the first error
	idx_t updated_tuples = 0;
	updated_tuples += HandleInsertConflicts<true>(table, context, lstate, gstate, lstate.insert_chunk, *this);
	// Also check the transaction-local storage+ART so we can detect conflicts within this transaction
	updated_tuples += HandleInsertConflicts<false>(table, context, lstate, gstate, lstate.insert_chunk, *this);

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

		idx_t updated_tuples = OnConflictHandling(table, context, gstate, lstate);

		gstate.insert_count += lstate.insert_chunk.size();
		gstate.insert_count += updated_tuples;
		if (!parallel && return_chunk) {
			gstate.return_collection.Append(lstate.insert_chunk);
		}
		storage.LocalAppend(gstate.append_state, context.client, lstate.insert_chunk, true);
		if (action_type == OnConflictAction::UPDATE && lstate.update_chunk.size() != 0) {
			(void)HandleInsertConflicts<true>(table, context, lstate, gstate, lstate.update_chunk, *this);
			(void)HandleInsertConflicts<false>(table, context, lstate, gstate, lstate.update_chunk, *this);
			// All of the tuples should have been turned into an update, leaving the chunk empty afterwards
			D_ASSERT(lstate.update_chunk.size() == 0);
		}
	} else {
		//! FIXME: can't we enable this by using a BatchedDataCollection ?
		D_ASSERT(!return_chunk);
		// parallel append
		if (!lstate.local_collection) {
			lock_guard<mutex> l(gstate.lock);
			auto table_info = storage.GetDataTableInfo();
			auto &io_manager = TableIOManager::Get(table.GetStorage());
			lstate.local_collection = make_uniq<RowGroupCollection>(std::move(table_info), io_manager, insert_types,
			                                                        NumericCast<idx_t>(MAX_ROW_ID));
			lstate.local_collection->InitializeEmpty();
			lstate.local_collection->InitializeAppend(lstate.local_append_state);
			lstate.writer = &gstate.table.GetStorage().CreateOptimisticWriter(context.client);
		}
		OnConflictHandling(table, context, gstate, lstate);
		D_ASSERT(action_type != OnConflictAction::UPDATE);

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

	auto &table = gstate.table;
	auto &storage = table.GetStorage();
	const idx_t row_group_size = storage.GetRowGroupSize();

	// parallel append: finalize the append
	TransactionData tdata(0, 0);
	lstate.local_collection->FinalizeAppend(tdata, lstate.local_append_state);

	auto append_count = lstate.local_collection->GetTotalRows();

	lock_guard<mutex> lock(gstate.lock);
	gstate.insert_count += append_count;
	if (append_count < row_group_size) {
		// we have few rows - append to the local storage directly
		storage.InitializeLocalAppend(gstate.append_state, table, context.client, bound_constraints);
		auto &transaction = DuckTransaction::Get(context.client, table.catalog);
		lstate.local_collection->Scan(transaction, [&](DataChunk &insert_chunk) {
			storage.LocalAppend(gstate.append_state, context.client, insert_chunk, false);
			return true;
		});
		storage.FinalizeLocalAppend(gstate.append_state);
	} else {
		// we have written rows to disk optimistically - merge directly into the transaction-local storage
		lstate.writer->WriteLastRowGroup(*lstate.local_collection);
		lstate.writer->FinalFlush();
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
