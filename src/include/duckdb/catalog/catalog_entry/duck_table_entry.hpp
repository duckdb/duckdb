//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/duck_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"

namespace duckdb {

struct AddConstraintInfo;

//! A table catalog entry
class DuckTableEntry : public TableCatalogEntry {
public:
	//! Create a TableCatalogEntry and initialize storage for it
	DuckTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, BoundCreateTableInfo &info,
	               shared_ptr<DataTable> inherited_storage = nullptr);

public:
	unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo &info) override;
	unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction, AlterInfo &info) override;
	void UndoAlter(ClientContext &context, AlterInfo &info) override;
	void Rollback(CatalogEntry &prev_entry) override;

	//! Returns the underlying storage of the table
	DataTable &GetStorage() override;

	//! Get statistics of a column (physical or virtual) within the table
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	unique_ptr<BlockingSample> GetSample() override;

	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	void SetAsRoot() override;

	void CommitAlter(string &column_name);
	void CommitDrop();

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	vector<ColumnSegmentInfo> GetColumnSegmentInfo() override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	bool IsDuckTable() const override {
		return true;
	}

private:
	unique_ptr<CatalogEntry> RenameColumn(ClientContext &context, RenameColumnInfo &info);
	unique_ptr<CatalogEntry> AddColumn(ClientContext &context, AddColumnInfo &info);
	unique_ptr<CatalogEntry> RemoveColumn(ClientContext &context, RemoveColumnInfo &info);
	unique_ptr<CatalogEntry> SetDefault(ClientContext &context, SetDefaultInfo &info);
	unique_ptr<CatalogEntry> ChangeColumnType(ClientContext &context, ChangeColumnTypeInfo &info);
	unique_ptr<CatalogEntry> SetNotNull(ClientContext &context, SetNotNullInfo &info);
	unique_ptr<CatalogEntry> DropNotNull(ClientContext &context, DropNotNullInfo &info);
	unique_ptr<CatalogEntry> AddForeignKeyConstraint(optional_ptr<ClientContext> context, AlterForeignKeyInfo &info);
	unique_ptr<CatalogEntry> DropForeignKeyConstraint(ClientContext &context, AlterForeignKeyInfo &info);
	unique_ptr<CatalogEntry> SetColumnComment(ClientContext &context, SetColumnCommentInfo &info);
	unique_ptr<CatalogEntry> AddConstraint(ClientContext &context, AddConstraintInfo &info);

	void UpdateConstraintsOnColumnDrop(const LogicalIndex &removed_index, const vector<LogicalIndex> &adjusted_indices,
	                                   const RemoveColumnInfo &info, CreateTableInfo &create_info,
	                                   const vector<unique_ptr<BoundConstraint>> &bound_constraints, bool is_generated);

private:
	//! A reference to the underlying storage unit used for this table
	shared_ptr<DataTable> storage;
	//! Manages dependencies of the individual columns of the table
	ColumnDependencyManager column_dependency_manager;
};
} // namespace duckdb
