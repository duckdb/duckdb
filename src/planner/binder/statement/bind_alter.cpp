#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bind_statement_helper.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_alter.hpp"

namespace duckdb {

	unique_ptr<LogicalOperator> DuckCatalog::BindAlterAddForeignKey(Binder &binder, TableCatalogEntry &table_entry,
                                                           unique_ptr<LogicalOperator> plan,
                                                           unique_ptr<CreateIndexInfo> create_info,
                                                           unique_ptr<AlterTableInfo> alter_info) {
	// TODO: ZZZ Seems no need this func?
	D_ASSERT(plan->type == LogicalOperatorType::LOGICAL_GET);
	IndexBinder index_binder(binder, binder.context);
	return index_binder.BindCreateIndex(binder.context, std::move(create_info), table_entry, std::move(plan),
	                                    std::move(alter_info));
}

unique_ptr<LogicalOperator> DuckCatalog::BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
                                                           unique_ptr<LogicalOperator> plan,
                                                           unique_ptr<CreateIndexInfo> create_info,
                                                           unique_ptr<AlterTableInfo> alter_info) {
	D_ASSERT(plan->type == LogicalOperatorType::LOGICAL_GET);
	IndexBinder index_binder(binder, binder.context);
	return index_binder.BindCreateIndex(binder.context, std::move(create_info), table_entry, std::move(plan),
	                                    std::move(alter_info));
}

void Binder::BindParsedForeignKeyConstraint(ForeignKeyConstraint& fk, SchemaCatalogEntry &schema, TableCatalogEntry &table)
{
	// TODO: ZZZ. Fill ForeignKeyConstraint info.
	// auto &fk = cond->Cast<ForeignKeyConstraint>();
	if (fk.info.type != ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
		return;
	}
	if (!fk.info.pk_keys.empty() && !fk.info.fk_keys.empty()) {
		return;
	}
	D_ASSERT(fk.info.pk_keys.empty());
	D_ASSERT(fk.info.fk_keys.empty());
	FindForeignKeyIndexes(table.GetColumns(), fk.fk_columns, fk.info.fk_keys);

	// Resolve the self-reference.
	if (StringUtil::CIEquals(table.name, fk.info.table)) {
		fk.info.type = ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE;
		// Need table constraints!
		FindMatchingPrimaryKeyColumns(table.GetColumns(), table.GetConstraints(), fk);
		// Need table constraints!
		FindForeignKeyIndexes(table.GetColumns(), fk.pk_columns, fk.info.pk_keys);
		CheckForeignKeyTypes(table.GetColumns(), table.GetColumns(), fk);
		return;
	}

	// Resolve the table reference.
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, fk.info.table);
	auto table_entry = entry_retriever.GetEntry(INVALID_CATALOG, fk.info.schema, table_lookup);
	if (table_entry->type == CatalogType::VIEW_ENTRY) {
		throw BinderException("cannot reference a VIEW with a FOREIGN KEY");
	}

	auto &pk_table_entry_ptr = table_entry->Cast<TableCatalogEntry>();
	if (&pk_table_entry_ptr.schema != &schema) {
		throw BinderException("Creating foreign keys across different schemas or catalogs is not supported");
	}
	FindMatchingPrimaryKeyColumns(pk_table_entry_ptr.GetColumns(), pk_table_entry_ptr.GetConstraints(), fk);
	FindForeignKeyIndexes(pk_table_entry_ptr.GetColumns(), fk.pk_columns, fk.info.pk_keys);
	CheckForeignKeyTypes(pk_table_entry_ptr.GetColumns(), table.GetColumns(), fk);
	if (pk_table_entry_ptr.IsDuckTable()) {
		auto &storage = pk_table_entry_ptr.GetStorage();

		if (!storage.HasForeignKeyIndex(fk.info.pk_keys, ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE)) {
			auto fk_column_names = StringUtil::Join(fk.pk_columns, ",");
			throw BinderException("Failed to create foreign key on %s(%s): no UNIQUE or PRIMARY KEY constraint "
					"present on these columns",
					pk_table_entry_ptr.name, fk_column_names);
		}
	}

	D_ASSERT(fk.info.pk_keys.size() == fk.info.fk_keys.size());
	D_ASSERT(fk.info.pk_keys.size() == fk.pk_columns.size());
	D_ASSERT(fk.info.fk_keys.size() == fk.fk_columns.size());
}
BoundStatement Binder::BindAlterAddForeignKey(BoundStatement &result, CatalogEntry &entry,
                                         unique_ptr<AlterInfo> alter_info) {
	// TODO: ZZZ How to bind FK?
	// Copy from BindAlterAddIndex, modify if needed
	auto &table_info = alter_info->Cast<AlterTableInfo>();
	auto &constraint_info = table_info.Cast<AddConstraintInfo>();
	auto &table = entry.Cast<TableCatalogEntry>();
	auto &column_list = table.GetColumns();

	// FK is NOT UniqueConstraint
	// auto bound_constraint = BindUniqueConstraint(*constraint_info.constraint, table_info.name, column_list);
	// auto &bound_unique = bound_constraint->Cast<BoundUniqueConstraint>();
	// TODO: ZZZ. SetCatalogLookupCallback() Do we need this here? Leave it for now.
	// TODO: ZZZ parse the PK list first! Check BindCreateTableConstraints for ref.
	// BindParsedForeignKeyConstraint(*constraint_info.constraint, table_info.schema);
	// void Binder::BindParsedForeignKeyConstraint(ForeignKeyConstraint& fk, SchemaCatalogEntry &schema) Or
	// void Binder::BindParsedForeignKeyConstraint(ForeignKeyConstraint& fk, TableCatalogEntry &table)
	BindParsedForeignKeyConstraint(constraint_info.constraint->Cast<ForeignKeyConstraint>(), table.schema, table);
	auto &fk = constraint_info.constraint->Cast<ForeignKeyConstraint>();


	// Create the CreateIndexInfo.
	auto create_index_info = make_uniq<CreateIndexInfo>();
	create_index_info->table = table_info.name;
	create_index_info->index_type = ART::TYPE_NAME;
	create_index_info->constraint_type = IndexConstraintType::FOREIGN;

	// physical_index type: vector<PhysicalIndex> keys
	// Need to get such info for FKs from BoundForeignKeyConstraint
	// Check the def of BoundForeignKeyConstraint
	// It's ForeignKeyInfo --> vector<PhysicalIndex> fk_keys;
	for (const auto &physical_index : fk.info.fk_keys) {
		auto &col = column_list.GetColumn(physical_index);
		unique_ptr<ParsedExpression> parsed = make_uniq<ColumnRefExpression>(col.GetName(), table_info.name);
		create_index_info->expressions.push_back(parsed->Copy());
		create_index_info->parsed_expressions.push_back(parsed->Copy());
	}

	// TODO: ZZZ index name
	// Need FK index name
	auto index_name = fk.GetName(table_info.name);
	create_index_info->index_name = index_name;
	D_ASSERT(!create_index_info->index_name.empty());

	// Plan the table scan.
	TableDescription table_description(table_info.catalog, table_info.schema, table_info.name);
	auto table_ref = make_uniq<BaseTableRef>(table_description);
	auto bound_table = Bind(*table_ref);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("can only add an index to a base table");
	}
	auto plan = CreatePlan(*bound_table);
	auto &get = plan->Cast<LogicalGet>();
	get.names = column_list.GetColumnNames();

	auto alter_table_info = unique_ptr_cast<AlterInfo, AlterTableInfo>(std::move(alter_info));
	result.plan = table.catalog.BindAlterAddForeignKey(*this, table, std::move(plan), std::move(create_index_info),
	                                              std::move(alter_table_info));
	return std::move(result);
}
BoundStatement Binder::BindAlterAddIndex(BoundStatement &result, CatalogEntry &entry,
                                         unique_ptr<AlterInfo> alter_info) {
	if (entry.type != CatalogType::TABLE_ENTRY) {
		throw BinderException("Cannot execute the `ALTER TABLE` statement on `%s`, only `Table` entries are accepted.",
		                      CatalogTypeToString(entry.type));
	}
	auto &table_info = alter_info->Cast<AlterTableInfo>();
	auto &constraint_info = table_info.Cast<AddConstraintInfo>();
	auto &table = entry.Cast<TableCatalogEntry>();
	auto &column_list = table.GetColumns();

	auto bound_constraint =
	    BindUniqueConstraint(*constraint_info.constraint, table_info.GetQualifiedName().Name(), column_list);
	auto &bound_unique = bound_constraint->Cast<BoundUniqueConstraint>();

	// Create the CreateIndexInfo.
	auto create_index_info = make_uniq<CreateIndexInfo>();
	create_index_info->table = table_info.GetQualifiedName().Name();
	create_index_info->index_type = ART::TYPE_NAME;
	create_index_info->constraint_type = IndexConstraintType::PRIMARY;

	for (const auto &physical_index : bound_unique.keys) {
		auto &col = column_list.GetColumn(physical_index);
		unique_ptr<ParsedExpression> parsed =
		    make_uniq<ColumnRefExpression>(col.GetName(), table_info.GetQualifiedName().Name());
		create_index_info->expressions.push_back(parsed->Copy());
		create_index_info->parsed_expressions.push_back(parsed->Copy());
	}

	auto unique_constraint = constraint_info.constraint->Cast<UniqueConstraint>();
	auto index_name = unique_constraint.GetName(table_info.GetQualifiedName().Name());
	create_index_info->SetIndexName(index_name);
	D_ASSERT(!create_index_info->GetIndexName().empty());

	// Plan the table scan.
	TableDescription table_description(table_info.GetQualifiedName());
	auto table_ref = make_uniq<BaseTableRef>(table_description);
	auto bound_table = Bind(*table_ref);
	if (bound_table.plan->type != LogicalOperatorType::LOGICAL_GET) {
		throw BinderException("can only add an index to a base table");
	}
	auto &get = bound_table.plan->Cast<LogicalGet>();
	get.names = StringsToIdentifiers(column_list.GetColumnNames());

	auto alter_table_info = unique_ptr_cast<AlterInfo, AlterTableInfo>(std::move(alter_info));
	result.plan = table.catalog.BindAlterAddIndex(*this, table, std::move(bound_table.plan),
	                                              std::move(create_index_info), std::move(alter_table_info));
	return std::move(result);
}

static void BindAlterTypes(Binder &binder, AlterStatement &stmt) {
	if (stmt.info->type == AlterType::ALTER_TABLE) {
		auto &table_info = stmt.info->Cast<AlterTableInfo>();
		switch (table_info.alter_table_type) {
		case AlterTableType::ADD_COLUMN: {
			auto &add_info = table_info.Cast<AddColumnInfo>();
			binder.BindLogicalType(add_info.new_column.TypeMutable());
		} break;
		case AlterTableType::ADD_FIELD: {
			auto &add_info = table_info.Cast<AddFieldInfo>();
			binder.BindLogicalType(add_info.new_field.TypeMutable());
		} break;
		case AlterTableType::ALTER_COLUMN_TYPE: {
			auto &alter_column_info = table_info.Cast<ChangeColumnTypeInfo>();
			binder.BindLogicalType(alter_column_info.target_type);
		} break;
		default:
			break;
		}
	}
}

BoundStatement Binder::Bind(AlterStatement &stmt) {
	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};

	// Special handling for ALTER DATABASE - doesn't use schema binding
	if (stmt.info->type == AlterType::ALTER_DATABASE) {
		auto &properties = GetStatementProperties();
		properties.return_type = StatementReturnType::NOTHING;
		properties.RegisterDBModify(Catalog::GetSystemCatalog(context), context, DatabaseModificationType::ALTER_TABLE);
		result.plan = make_uniq<LogicalAlter>(std::move(stmt.info));
		return result;
	}

	// resolve the (possibly nested) catalog/schema qualification of the altered entry
	stmt.info->SetQualifiedName(BindTableName(stmt.info->GetQualifiedName()));

	optional_ptr<CatalogEntry> entry;
	if (stmt.info->type == AlterType::SET_COLUMN_COMMENT) {
		// Extra step for column comments: They can alter a table or a view, and we resolve that here.
		auto &info = stmt.info->Cast<SetColumnCommentInfo>();
		entry = info.TryResolveCatalogEntry(entry_retriever);
		if (entry && info.catalog_entry_type == CatalogType::VIEW_ENTRY) {
			// when running SET COLUMN on a VIEW - ensure the view is bound
			auto &view = entry->Cast<ViewCatalogEntry>();
			view.BindView(context);
		}
	} else {
		// For any other ALTER, we retrieve the catalog entry directly.
		EntryLookupInfo lookup_info(stmt.info->GetCatalogType(), stmt.info->GetQualifiedName());
		entry = entry_retriever.GetEntry(lookup_info, stmt.info->if_not_found);
	}

	auto &properties = GetStatementProperties();
	properties.return_type = StatementReturnType::NOTHING;
	if (!entry) {
		// Bind types in this binder
		BindAlterTypes(*this, stmt);

		result.plan = make_uniq<LogicalAlter>(std::move(stmt.info));
		return result;
	}

	D_ASSERT(!entry->deleted);
	auto &catalog = entry->ParentCatalog();

	// Bind types in the same catalog as the entry
	auto type_binder = Binder::CreateBinder(context, *this);
	type_binder->SetSearchPath(catalog, stmt.info->GetQualifiedName().Schema());

	BindAlterTypes(*type_binder, stmt);

	if (catalog.IsSystemCatalog()) {
		throw BinderException("Can not comment on System Catalog entries");
	}
	if (!entry->temporary) {
		// We can only alter temporary tables and views in read-only mode.
		properties.RegisterDBModify(catalog, context, DatabaseModificationType::ALTER_TABLE);
	}
	stmt.info->SetQualifiedName(entry->ParentSchema().GetQualifiedName(stmt.info->GetQualifiedName().Name()));

	if (stmt.info->IsAddPrimaryKey()) {
		return BindAlterAddIndex(result, *entry, std::move(stmt.info));
	}
	
		if (stmt.info->IsAddForeignKey()) {
		return BindAlterAddForeignKey(result, *entry, std::move(stmt.info));
	}
		result.plan = make_uniq<LogicalAlter>(std::move(stmt.info));
		return result;

}

} // namespace duckdb
