#include "duckdb/execution/operator/schema/physical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/execution/index/index_type.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

PhysicalCreateIndex::PhysicalCreateIndex(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table_p,
                                         const vector<column_t> &column_ids, unique_ptr<CreateIndexInfo> info,
                                         vector<unique_ptr<Expression>> unbound_expressions_p,
                                         idx_t estimated_cardinality, IndexType index_type,
                                         unique_ptr<IndexBuildBindData> bind_data,
                                         unique_ptr<AlterTableInfo> alter_table_info)

    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_INDEX, op.types, estimated_cardinality),
      table(table_p.Cast<DuckTableEntry>()), info(std::move(info)),
      unbound_expressions(std::move(unbound_expressions_p)), index_type(std::move(index_type)),
      bind_data(std::move(bind_data)), alter_table_info(std::move(alter_table_info)) {
	// Convert the logical column ids to physical column ids.
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}

	for (idx_t i = 0; i < unbound_expressions.size(); ++i) {
		auto &expr = unbound_expressions[i];
		indexed_column_types.push_back(expr->GetReturnType());
		indexed_columns.push_back(i);
	}

	// Row id is always last
	rowid_column.push_back(unbound_expressions.size());

	// Pre-build the BoundForeignKeyConstraint and full column type list for FK verification.
	if (this->alter_table_info && this->alter_table_info->IsAddForeignKey()) {
		auto &constraint_info = this->alter_table_info->Cast<AddConstraintInfo>();
		auto &fk = constraint_info.constraint->Cast<ForeignKeyConstraint>();
		physical_index_set_t pk_key_set(fk.info.pk_keys.begin(), fk.info.pk_keys.end());
		physical_index_set_t fk_key_set(fk.info.fk_keys.begin(), fk.info.fk_keys.end());
		bound_fk = make_uniq<BoundForeignKeyConstraint>(fk.info, std::move(pk_key_set), std::move(fk_key_set));
	}
}

//---------------------------------------------------------------------------------------------------------------------
// Sink
//---------------------------------------------------------------------------------------------------------------------
class CreateIndexGlobalSinkState : public GlobalSinkState {
public:
	unique_ptr<IndexBuildGlobalState> gstate;
};

unique_ptr<GlobalSinkState> PhysicalCreateIndex::GetGlobalSinkState(ClientContext &context) const {
	auto gstate = make_uniq<CreateIndexGlobalSinkState>();

	IndexBuildInitGlobalStateInput global_state_input {bind_data.get(),     context,    table, *info,
	                                                   unbound_expressions, storage_ids};
	gstate->gstate = index_type.build_global_init(global_state_input);

	return std::move(gstate);
}

class CreateIndexLocalSinkState : public LocalSinkState {
public:
	unique_ptr<IndexBuildLocalState> lstate;
	DataChunk key_chunk;
	DataChunk row_chunk;
};

unique_ptr<LocalSinkState> PhysicalCreateIndex::GetLocalSinkState(ExecutionContext &context) const {
	auto lstate = make_uniq<CreateIndexLocalSinkState>();

	IndexBuildInitLocalStateInput local_state_input {bind_data.get(), context.client,      table,
	                                                 *info,           unbound_expressions, storage_ids};
	lstate->lstate = index_type.build_local_init(local_state_input);

	lstate->key_chunk.InitializeEmpty(indexed_column_types);
	lstate->row_chunk.InitializeEmpty({LogicalType::ROW_TYPE});

	return std::move(lstate);
}

SinkResultType PhysicalCreateIndex::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CreateIndexGlobalSinkState>();
	auto &lstate = input.local_state.Cast<CreateIndexLocalSinkState>();

	// FIXME: use unified format instead of Flatten
	chunk.Flatten();

	// Reference the key columns and rowid column
	lstate.key_chunk.ReferenceColumns(chunk, indexed_columns);
	lstate.row_chunk.ReferenceColumns(chunk, rowid_column);

	// Check for NULLs, if we are creating a PRIMARY KEY.
	// FIXME: Later, we want to ensure that we skip the NULL check for any non-PK alter.
	if (alter_table_info && !bound_fk) {
		for (idx_t i = 0; i < lstate.key_chunk.ColumnCount(); i++) {
			if (VectorOperations::HasNull(lstate.key_chunk.data[i])) {
				throw ConstraintException("NOT NULL constraint failed: %s", info->GetIndexName());
			}
		}
	}

		// Verify FK referential integrity for ALTER TABLE ADD FOREIGN KEY.
	// TODO: Does it verify local data? Add test to check this!
	if (bound_fk) {
		// CreateIndexScan uses TABLE_SCAN_OMIT_PERMANENTLY_DELETED, which still surfaces
		// rows that the current transaction has locally deleted. Skip them here so FK
		// verification only sees rows that will exist post-commit. The deleted rows must
		// still enter the index build below so commit-time DELETE can remove them.
		auto &transaction = DuckTransaction::Get(context.client, table.catalog);
		auto &data_storage = table.GetStorage();
		auto row_ids = FlatVector::GetData<row_t>(lstate.row_chunk.data[0]);
		SelectionVector visible_sel(STANDARD_VECTOR_SIZE);
		idx_t visible_count = 0;
		for (idx_t i = 0; i < lstate.row_chunk.size(); i++) {
			if (data_storage.CanFetch(transaction, row_ids[i])) {
				visible_sel.set_index(visible_count++, i);
			}
		}

		if (visible_count > 0) {
			// key_chunk has FK columns at indices 0..N-1 (projected), but VerifyForeignKeyConstraint
			// uses fk_keys physical indices to address the chunk. Build a correctly-indexed chunk.
			vector<LogicalType> fk_table_types;
			for (auto &col : table.GetColumns().Physical()) {
				fk_table_types.emplace_back(col.Type());
			}
			DataChunk full_chunk;
			full_chunk.InitializeEmpty(fk_table_types);
			DataChunk verify_keys;
			verify_keys.InitializeEmpty(indexed_column_types);
			if (visible_count == lstate.key_chunk.size()) {
				for (idx_t i = 0; i < bound_fk->info.fk_keys.size(); i++) {
					full_chunk.data[bound_fk->info.fk_keys[i].index].Reference(lstate.key_chunk.data[i]);
				}
				full_chunk.SetCardinality(visible_count);
			} else {
				for (idx_t i = 0; i < bound_fk->info.fk_keys.size(); i++) {
					verify_keys.data[i].Slice(lstate.key_chunk.data[i], visible_sel, visible_count);
					full_chunk.data[bound_fk->info.fk_keys[i].index].Reference(verify_keys.data[i]);
				}
				full_chunk.SetCardinality(visible_count);
			}
			table.GetStorage().VerifyFKReferentialIntegrity(*bound_fk, context.client, full_chunk);
		}
	}

	// Sink into the index
	IndexBuildSinkInput sink_input {bind_data.get(), *gstate.gstate, *lstate.lstate, table, *info};
	index_type.build_sink(sink_input, lstate.key_chunk, lstate.row_chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateIndex::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateIndexGlobalSinkState>();
	auto &lstate = input.local_state.Cast<CreateIndexLocalSinkState>();

	IndexBuildCombineInput combine_input {bind_data.get(), *gstate.gstate, *lstate.lstate, table, *info};
	index_type.build_combine(combine_input);

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCreateIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CreateIndexGlobalSinkState>();

	// For ALTER TABLE ADD FOREIGN KEY: the upstream scan uses is_create_index and only
	// sees committed rows. Verify any locally-appended FK rows against the referenced
	// PK table here, while the transaction is still active.
	if (bound_fk) {
		auto &storage = table.GetStorage();
		auto &local_storage = LocalStorage::Get(context, table.catalog);
		auto local = local_storage.GetStorage(storage);
		if (local) {
			vector<LogicalType> fk_table_types;
			for (auto &col : table.GetColumns().Physical()) {
				fk_table_types.emplace_back(col.Type());
			}
			vector<StorageIndex> fk_key_columns;
			for (auto &fk_key : bound_fk->info.fk_keys) {
				fk_key_columns.emplace_back(fk_key.index);
			}
			auto &transaction = DuckTransaction::Get(context, table.catalog);
			auto &collection = local->GetCollection();
			DataChunk full_chunk;
			full_chunk.InitializeEmpty(fk_table_types);
			for (auto &local_chunk : collection.Chunks(transaction, fk_key_columns)) {
				full_chunk.Reset();
				for (idx_t i = 0; i < bound_fk->info.fk_keys.size(); i++) {
					full_chunk.data[bound_fk->info.fk_keys[i].index].Reference(local_chunk.data[i]);
				}
				full_chunk.SetCardinality(local_chunk.size());
				storage.VerifyFKReferentialIntegrity(*bound_fk, context, full_chunk);
			}
		}
	}

	// Finalize the index
	IndexBuildFinalizeInput finalize_input {*gstate.gstate};
	auto bound_index = index_type.build_finalize(finalize_input);

	// Vacuum excess memory and verify.
	bound_index->Vacuum();

	bound_index->Verify();

	D_ASSERT(!bound_index->ToString(true).empty());

	bound_index->VerifyAllocations();

	auto &storage = table.GetStorage();
	if (!storage.IsMainTable()) {
		throw TransactionException(
		    "Transaction conflict: cannot add an index to a table that has been altered or dropped");
	}

	auto &schema = table.schema;
	info->column_ids = storage_ids;

	if (!alter_table_info) {
		// Ensure that the index does not yet exist in the catalog.
		auto entry =
		    schema.GetEntry(schema.GetCatalogTransaction(context), CatalogType::INDEX_ENTRY, info->GetIndexName());
		if (entry) {
			if (info->on_conflict != OnCreateConflict::IGNORE_ON_CONFLICT) {
				throw CatalogException("Index with name %s already exists!", info->GetIndexName());
			}
			// IF NOT EXISTS on existing index. We are done.
			return SinkFinalizeType::READY;
		}

		auto index_entry = schema.CreateIndex(schema.GetCatalogTransaction(context), *info, table).get();
		D_ASSERT(index_entry);
		auto &index = index_entry->Cast<DuckIndexEntry>();
		index.initial_index_size = bound_index->GetInMemorySize();

	} else {
		// Ensure that there are no other indexes with that name on this table.
		const auto &indexes = storage.GetDataTableInfo()->GetIndexes();
		if (indexes.Contains(info->GetIndexName())) {
			throw CatalogException("an index with that name already exists for this table: %s",
			                       SQLIdentifier(info->GetIndexName()));
		}

		auto &catalog = Catalog::GetCatalog(context, info->GetQualifiedName().Catalog());
		catalog.Alter(context, *alter_table_info);
	}

	// Add the index to the storage.
	storage.AddIndex(std::move(bound_index));

	return SinkFinalizeType::READY;
}

} // namespace duckdb
