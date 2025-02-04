#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> DuckCatalog::BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
                                                           unique_ptr<LogicalOperator> plan,
                                                           unique_ptr<CreateIndexInfo> create_info,
                                                           unique_ptr<AlterTableInfo> alter_info) {
	D_ASSERT(plan->type == LogicalOperatorType::LOGICAL_GET);
	IndexBinder index_binder(binder, binder.context);
	return index_binder.BindCreateIndex(binder.context, std::move(create_info), table_entry, std::move(plan),
	                                    std::move(alter_info));
}

BoundStatement Binder::BindAlterAddIndex(BoundStatement &result, CatalogEntry &entry,
                                         unique_ptr<AlterInfo> alter_info) {
	auto &table_info = alter_info->Cast<AlterTableInfo>();
	auto &constraint_info = table_info.Cast<AddConstraintInfo>();
	auto &table = entry.Cast<TableCatalogEntry>();
	auto &column_list = table.GetColumns();

	auto bound_constraint = BindUniqueConstraint(*constraint_info.constraint, table_info.name, column_list);
	auto &bound_unique = bound_constraint->Cast<BoundUniqueConstraint>();

	// Create the CreateIndexInfo.
	auto create_index_info = make_uniq<CreateIndexInfo>();
	create_index_info->table = table_info.name;
	create_index_info->index_type = ART::TYPE_NAME;
	create_index_info->constraint_type = IndexConstraintType::PRIMARY;

	for (const auto &physical_index : bound_unique.keys) {
		auto &col = column_list.GetColumn(physical_index);
		unique_ptr<ParsedExpression> parsed = make_uniq<ColumnRefExpression>(col.GetName(), table_info.name);
		create_index_info->expressions.push_back(parsed->Copy());
		create_index_info->parsed_expressions.push_back(parsed->Copy());
	}

	auto unique_constraint = constraint_info.constraint->Cast<UniqueConstraint>();
	auto index_name = unique_constraint.GetName(table_info.name);
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
	result.plan = table.catalog.BindAlterAddIndex(*this, table, std::move(plan), std::move(create_index_info),
	                                              std::move(alter_table_info));
	return std::move(result);
}

BoundStatement Binder::Bind(AlterStatement &stmt) {
	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	BindSchemaOrCatalog(stmt.info->catalog, stmt.info->schema);

	optional_ptr<CatalogEntry> entry;
	if (stmt.info->type == AlterType::SET_COLUMN_COMMENT) {
		// Extra step for column comments: They can alter a table or a view, and we resolve that here.
		auto &info = stmt.info->Cast<SetColumnCommentInfo>();
		entry = info.TryResolveCatalogEntry(entry_retriever);

	} else {
		// For any other ALTER, we retrieve the catalog entry directly.
		entry = entry_retriever.GetEntry(stmt.info->GetCatalogType(), stmt.info->catalog, stmt.info->schema,
		                                 stmt.info->name, stmt.info->if_not_found);
	}

	auto &properties = GetStatementProperties();
	properties.return_type = StatementReturnType::NOTHING;
	if (!entry) {
		result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_ALTER, std::move(stmt.info));
		return result;
	}

	D_ASSERT(!entry->deleted);
	auto &catalog = entry->ParentCatalog();
	if (catalog.IsSystemCatalog()) {
		throw BinderException("Can not comment on System Catalog entries");
	}
	if (!entry->temporary) {
		// We can only alter temporary tables and views in read-only mode.
		properties.RegisterDBModify(catalog, context);
	}
	stmt.info->catalog = catalog.GetName();
	stmt.info->schema = entry->ParentSchema().name;

	if (!stmt.info->IsAddPrimaryKey()) {
		result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_ALTER, std::move(stmt.info));
		return result;
	}

	return BindAlterAddIndex(result, *entry, std::move(stmt.info));
}

BoundStatement Binder::Bind(TransactionStatement &stmt) {
	auto &properties = GetStatementProperties();

	// Transaction statements do not require a valid transaction.
	properties.requires_valid_transaction = stmt.info->type == TransactionType::BEGIN_TRANSACTION;

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_TRANSACTION, std::move(stmt.info));
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
