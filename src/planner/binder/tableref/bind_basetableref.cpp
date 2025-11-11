#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace duckdb {

static bool TryLoadExtensionForReplacementScan(ClientContext &context, const string &table_name) {
	auto lower_name = StringUtil::Lower(table_name);
	auto &dbconfig = DBConfig::GetConfig(context);

	if (!dbconfig.options.autoload_known_extensions) {
		return false;
	}

	for (const auto &entry : EXTENSION_FILE_POSTFIXES) {
		if (StringUtil::EndsWith(lower_name, entry.name)) {
			ExtensionHelper::AutoLoadExtension(context, entry.extension);
			return true;
		}
	}

	for (const auto &entry : EXTENSION_FILE_CONTAINS) {
		if (StringUtil::Contains(lower_name, entry.name)) {
			ExtensionHelper::AutoLoadExtension(context, entry.extension);
			return true;
		}
	}

	return false;
}

BoundStatement Binder::BindWithReplacementScan(ClientContext &context, BaseTableRef &ref) {
	auto &config = DBConfig::GetConfig(context);
	if (!context.config.use_replacement_scans) {
		return BoundStatement();
	}
	for (auto &scan : config.replacement_scans) {
		ReplacementScanInput input(ref.catalog_name, ref.schema_name, ref.table_name);
		auto replacement_function = scan.function(context, input, scan.data.get());
		if (!replacement_function) {
			continue;
		}
		if (!ref.alias.empty()) {
			// user-provided alias overrides the default alias
			replacement_function->alias = ref.alias;
		} else if (replacement_function->alias.empty()) {
			// if the replacement scan itself did not provide an alias we use the table name
			replacement_function->alias = ref.table_name;
		}
		if (replacement_function->type == TableReferenceType::TABLE_FUNCTION) {
			auto &table_function = replacement_function->Cast<TableFunctionRef>();
			table_function.column_name_alias = ref.column_name_alias;
		} else if (replacement_function->type == TableReferenceType::SUBQUERY) {
			auto &subquery = replacement_function->Cast<SubqueryRef>();
			subquery.column_name_alias = ref.column_name_alias;
		} else {
			auto select_node = make_uniq<SelectNode>();
			select_node->select_list.push_back(make_uniq<StarExpression>());
			select_node->from_table = std::move(replacement_function);
			auto select_stmt = make_uniq<SelectStatement>();
			select_stmt->node = std::move(select_node);
			auto subquery = make_uniq<SubqueryRef>(std::move(select_stmt));
			subquery->column_name_alias = ref.column_name_alias;
			replacement_function = std::move(subquery);
		}
		if (GetBindingMode() == BindingMode::EXTRACT_REPLACEMENT_SCANS) {
			AddReplacementScan(ref.table_name, replacement_function->Copy());
		}
		return Bind(*replacement_function);
	}
	return BoundStatement();
}

unique_ptr<BoundAtClause> Binder::BindAtClause(optional_ptr<AtClause> at_clause) {
	if (!at_clause) {
		return nullptr;
	}
	ConstantBinder binder(*this, context, "AT clause");
	auto expr = binder.Bind(at_clause->ExpressionMutable());
	auto val = ExpressionExecutor::EvaluateScalar(context, *expr);
	return make_uniq<BoundAtClause>(at_clause->Unit(), std::move(val));
}

vector<CatalogSearchEntry> Binder::GetSearchPath(Catalog &catalog, const string &schema_name) {
	vector<CatalogSearchEntry> view_search_path;
	auto &catalog_name = catalog.GetName();
	if (!schema_name.empty()) {
		view_search_path.emplace_back(catalog_name, schema_name);
	}
	auto default_schema = catalog.GetDefaultSchema();
	if (schema_name.empty() && schema_name != default_schema) {
		view_search_path.emplace_back(catalog.GetName(), default_schema);
	}
	return view_search_path;
}

static vector<LogicalType> ExchangeAllNullTypes(const vector<LogicalType> &types) {
	vector<LogicalType> result = types;
	for (auto &type : result) {
		if (ExpressionBinder::ContainsNullType(type)) {
			type = ExpressionBinder::ExchangeNullType(type);
		}
	}
	return result;
}

BoundStatement Binder::Bind(BaseTableRef &ref) {
	QueryErrorContext error_context(ref.query_location);
	// CTEs and views are also referred to using BaseTableRefs, hence need to distinguish here
	// check if the table name refers to a CTE

	// CTE name should never be qualified (i.e. schema_name should be empty)
	// unless we want to refer to the recurring table of "using key".
	BindingAlias binding_alias(ref.schema_name, ref.table_name);
	auto ctebinding = GetCTEBinding(binding_alias);
	if (ctebinding && ctebinding->CanBeReferenced()) {
		ctebinding->Reference();

		// There is a CTE binding in the BindContext.
		// This can only be the case if there is a recursive CTE,
		// or a materialized CTE present.
		auto index = GenerateTableIndex();

		auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
		auto names = BindContext::AliasColumnNames(alias, ctebinding->GetColumnNames(), ref.column_name_alias);

		bind_context.AddGenericBinding(index, alias, names, ctebinding->GetColumnTypes());

		bool is_recurring = ref.schema_name == "recurring";

		BoundStatement result;
		result.types = ctebinding->GetColumnTypes();
		result.names = names;
		result.plan =
		    make_uniq<LogicalCTERef>(index, ctebinding->GetIndex(), result.types, std::move(names), is_recurring);
		return result;
	}

	// not a CTE
	// extract a table or view from the catalog
	auto at_clause = BindAtClause(ref.at_clause);
	auto entry_at_clause = at_clause ? at_clause.get() : entry_retriever.GetAtClause();
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, ref.table_name, entry_at_clause, error_context);
	BindSchemaOrCatalog(entry_retriever, ref.catalog_name, ref.schema_name);
	auto table_or_view =
	    entry_retriever.GetEntry(ref.catalog_name, ref.schema_name, table_lookup, OnEntryNotFound::RETURN_NULL);
	// we still didn't find the table
	if (GetBindingMode() == BindingMode::EXTRACT_NAMES || GetBindingMode() == BindingMode::EXTRACT_QUALIFIED_NAMES) {
		if (!table_or_view || table_or_view->type == CatalogType::TABLE_ENTRY) {
			// if we are in EXTRACT_NAMES or EXTRACT_QUALIFIED_NAMES, we create a dummy table ref
			if (GetBindingMode() == BindingMode::EXTRACT_QUALIFIED_NAMES) {
				AddTableName(ref.ToString());
			} else {
				AddTableName(ref.table_name);
			}

			// add a bind context entry
			auto table_index = GenerateTableIndex();
			auto ref_alias = ref.alias.empty() ? ref.table_name : ref.alias;
			vector<LogicalType> types {LogicalType::INTEGER};
			vector<string> names {"__dummy_col" + to_string(table_index)};
			bind_context.AddGenericBinding(table_index, ref_alias, names, types);

			BoundStatement result;
			result.types = std::move(types);
			result.names = std::move(names);
			result.plan = make_uniq<LogicalDummyScan>(table_index);
			return result;
		}
	}
	if (!table_or_view) {
		// table could not be found: try to bind a replacement scan
		// Try replacement scan bind
		auto replacement_scan_bind_result = BindWithReplacementScan(context, ref);
		if (replacement_scan_bind_result.plan) {
			return replacement_scan_bind_result;
		}

		// Try autoloading an extension, then retry the replacement scan bind
		auto full_path = ReplacementScan::GetFullPath(ref.catalog_name, ref.schema_name, ref.table_name);
		auto extension_loaded = TryLoadExtensionForReplacementScan(context, full_path);
		if (extension_loaded) {
			replacement_scan_bind_result = BindWithReplacementScan(context, ref);
			if (replacement_scan_bind_result.plan) {
				return replacement_scan_bind_result;
			}
		}
		auto &config = DBConfig::GetConfig(context);
		if (context.config.use_replacement_scans && config.options.enable_external_access &&
		    ExtensionHelper::IsFullPath(full_path)) {
			auto &fs = FileSystem::GetFileSystem(context);
			if (fs.FileExists(full_path)) {
				throw BinderException(
				    "No extension found that is capable of reading the file \"%s\"\n* If this file is a supported file "
				    "format you can explicitly use the reader functions, such as read_csv, read_json or read_parquet",
				    full_path);
			}
		}

		// if we found a CTE that cannot be referenced that means that there is a circular reference
		if (ctebinding) {
			D_ASSERT(!ctebinding->CanBeReferenced());
			throw BinderException(error_context,
			                      "Circular reference to CTE \"%s\", use WITH RECURSIVE to "
			                      "use recursive CTEs.",
			                      ref.table_name);
		}
		// could not find an alternative: bind again to get the error
		// note: this will always throw when using DuckDB as a catalog, but a second look-up might succeed
		// in catalogs that do not have transactional DDL
		table_or_view =
		    entry_retriever.GetEntry(ref.catalog_name, ref.schema_name, table_lookup, OnEntryNotFound::THROW_EXCEPTION);
	}

	switch (table_or_view->type) {
	case CatalogType::TABLE_ENTRY: {
		// base table
		auto table_index = GenerateTableIndex();
		auto &table = table_or_view->Cast<TableCatalogEntry>();

		auto &properties = GetStatementProperties();
		properties.RegisterDBRead(table.ParentCatalog(), context);

		unique_ptr<FunctionData> bind_data;
		auto scan_function = table.GetScanFunction(context, bind_data, table_lookup);
		if (bind_data && !bind_data->SupportStatementCache()) {
			SetAlwaysRequireRebind();
		}
		// TODO: bundle the type and name vector in a struct (e.g PackedColumnMetadata)
		vector<LogicalType> table_types;
		vector<string> table_names;
		vector<TableColumnType> table_categories;

		vector<LogicalType> return_types;
		vector<string> return_names;
		for (auto &col : table.GetColumns().Logical()) {
			table_types.push_back(col.Type());
			table_names.push_back(col.Name());
			return_types.push_back(col.Type());
			return_names.push_back(col.Name());
		}
		table_names = BindContext::AliasColumnNames(ref.table_name, table_names, ref.column_name_alias);

		virtual_column_map_t virtual_columns;
		if (scan_function.get_virtual_columns) {
			virtual_columns = scan_function.get_virtual_columns(context, bind_data.get());
		} else {
			virtual_columns = table.GetVirtualColumns();
		}
		auto logical_get =
		    make_uniq<LogicalGet>(table_index, scan_function, std::move(bind_data), std::move(return_types),
		                          std::move(return_names), std::move(virtual_columns));
		auto table_entry = logical_get->GetTable();
		auto &col_ids = logical_get->GetMutableColumnIds();
		if (!table_entry) {
			bind_context.AddBaseTable(table_index, ref.alias, table_names, table_types, col_ids, ref.table_name);
		} else {
			bind_context.AddBaseTable(table_index, ref.alias, table_names, table_types, col_ids, *table_entry);
		}
		BoundStatement result;
		result.types = table_types;
		result.names = table_names;
		result.plan = std::move(logical_get);
		return result;
	}
	case CatalogType::VIEW_ENTRY: {
		// the node is a view: get the query that the view represents
		auto &view_catalog_entry = table_or_view->Cast<ViewCatalogEntry>();
		// We need to use a new binder for the view that doesn't reference any CTEs
		// defined for this binder so there are no collisions between the CTEs defined
		// for the view and for the current query
		auto view_binder = Binder::CreateBinder(context, this, BinderType::VIEW_BINDER);
		view_binder->can_contain_nulls = true;

		// The view may contain CTEs, but maybe only in the cte_map, so we need create CTE nodes for them
		auto query = view_catalog_entry.GetQuery().Copy();
		SubqueryRef subquery(unique_ptr_cast<SQLStatement, SelectStatement>(std::move(query)));

		subquery.alias = ref.alias;
		// construct view names by first (1) taking the view aliases, (2) adding the view names, then (3) applying
		// subquery aliases
		vector<string> view_names = view_catalog_entry.aliases;
		for (idx_t n = view_names.size(); n < view_catalog_entry.names.size(); n++) {
			view_names.push_back(view_catalog_entry.names[n]);
		}
		subquery.column_name_alias = BindContext::AliasColumnNames(ref.table_name, view_names, ref.column_name_alias);

		// when binding a view, we always look into the catalog/schema where the view is stored first
		auto view_search_path =
		    GetSearchPath(view_catalog_entry.ParentCatalog(), view_catalog_entry.ParentSchema().name);
		view_binder->entry_retriever.SetSearchPath(std::move(view_search_path));
		// propagate the AT clause through the view
		view_binder->entry_retriever.SetAtClause(entry_at_clause);

		// bind the child subquery
		view_binder->AddBoundView(view_catalog_entry);
		auto bound_child = view_binder->Bind(subquery);
		if (!view_binder->correlated_columns.empty()) {
			throw BinderException("Contents of view were altered - view bound correlated columns");
		}

		// verify that the types and names match up with the expected types and names if the view has type info defined
		if (GetBindingMode() != BindingMode::EXTRACT_NAMES &&
		    GetBindingMode() != BindingMode::EXTRACT_QUALIFIED_NAMES && view_catalog_entry.HasTypes()) {
			// we bind the view subquery and the original view with different "can_contain_nulls",
			// but we don't want to throw an error when SQLNULL does not match up with INTEGER,
			// so we exchange all SQLNULL with INTEGER here before comparing
			auto bound_types = ExchangeAllNullTypes(bound_child.types);
			auto view_types = ExchangeAllNullTypes(view_catalog_entry.types);
			if (bound_types != view_types) {
				auto actual_types = StringUtil::ToString(bound_types, ", ");
				auto expected_types = StringUtil::ToString(view_types, ", ");
				throw BinderException(
				    "Contents of view were altered: types don't match! Expected [%s], but found [%s] instead",
				    expected_types, actual_types);
			}
			if (bound_child.names.size() == view_catalog_entry.names.size() &&
			    bound_child.names != view_catalog_entry.names) {
				auto actual_names = StringUtil::Join(bound_child.names, ", ");
				auto expected_names = StringUtil::Join(view_catalog_entry.names, ", ");
				throw BinderException(
				    "Contents of view were altered: names don't match! Expected [%s], but found [%s] instead",
				    expected_names, actual_names);
			}
		}
		bind_context.AddView(bound_child.plan->GetRootIndex(), subquery.alias, subquery, bound_child,
		                     view_catalog_entry);
		return bound_child;
	}
	default:
		throw InternalException("Catalog entry type");
	}
}
} // namespace duckdb
