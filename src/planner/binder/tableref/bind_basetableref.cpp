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
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_cteref.hpp"
#include "duckdb/planner/tableref/bound_dummytableref.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"

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

unique_ptr<BoundTableRef> Binder::BindWithReplacementScan(ClientContext &context, BaseTableRef &ref) {
	auto &config = DBConfig::GetConfig(context);
	if (!context.config.use_replacement_scans) {
		return nullptr;
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
			throw InternalException("Replacement scan should return either a table function or a subquery");
		}
		if (GetBindingMode() == BindingMode::EXTRACT_REPLACEMENT_SCANS) {
			AddReplacementScan(ref.table_name, replacement_function->Copy());
		}
		return Bind(*replacement_function);
	}
	return nullptr;
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

unique_ptr<BoundTableRef> Binder::Bind(BaseTableRef &ref) {
	QueryErrorContext error_context(ref.query_location);
	// CTEs and views are also referred to using BaseTableRefs, hence need to distinguish here
	// check if the table name refers to a CTE

	// CTE name should never be qualified (i.e. schema_name should be empty)
	vector<reference<CommonTableExpressionInfo>> found_ctes;
	if (ref.schema_name.empty()) {
		found_ctes = FindCTE(ref.table_name, ref.table_name == alias);
	}

	if (!found_ctes.empty()) {
		// Check if there is a CTE binding in the BindContext
		bool circular_cte = false;
		for (auto found_cte : found_ctes) {
			auto &cte = found_cte.get();
			auto ctebinding = bind_context.GetCTEBinding(ref.table_name);
			if (ctebinding && (cte.query->node->type == QueryNodeType::RECURSIVE_CTE_NODE ||
			                   cte.materialized == CTEMaterialize::CTE_MATERIALIZE_ALWAYS)) {
				// There is a CTE binding in the BindContext.
				// This can only be the case if there is a recursive CTE,
				// or a materialized CTE present.
				auto index = GenerateTableIndex();
				auto materialized = cte.materialized;
				if (materialized == CTEMaterialize::CTE_MATERIALIZE_DEFAULT) {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
					materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
#else
					materialized = CTEMaterialize::CTE_MATERIALIZE_NEVER;
#endif
				}
				auto result = make_uniq<BoundCTERef>(index, ctebinding->index, materialized);
				auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
				auto names = BindContext::AliasColumnNames(alias, ctebinding->names, ref.column_name_alias);

				bind_context.AddGenericBinding(index, alias, names, ctebinding->types);
				// Update references to CTE
				auto cteref = bind_context.cte_references[ref.table_name];
				(*cteref)++;

				result->types = ctebinding->types;
				result->bound_columns = std::move(names);
				return std::move(result);
			} else {
				if (CTEIsAlreadyBound(cte)) {
					// remember error state
					circular_cte = true;
					// retry with next candidate CTE
					continue;
				}

				// If we have found a materialized CTE, but no corresponding CTE binding,
				// something is wrong.
				if (cte.materialized == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
					throw BinderException(
					    "There is a WITH item named \"%s\", but it cannot be referenced from this part of the query.",
					    ref.table_name);
				}

				// Move CTE to subquery and bind recursively
				SubqueryRef subquery(unique_ptr_cast<SQLStatement, SelectStatement>(cte.query->Copy()));
				subquery.alias = ref.alias.empty() ? ref.table_name : ref.alias;
				subquery.column_name_alias = cte.aliases;
				for (idx_t i = 0; i < ref.column_name_alias.size(); i++) {
					if (i < subquery.column_name_alias.size()) {
						subquery.column_name_alias[i] = ref.column_name_alias[i];
					} else {
						subquery.column_name_alias.push_back(ref.column_name_alias[i]);
					}
				}
				return Bind(subquery, &found_cte.get());
			}
		}
		if (circular_cte) {
			auto replacement_scan_bind_result = BindWithReplacementScan(context, ref);
			if (replacement_scan_bind_result) {
				return replacement_scan_bind_result;
			}

			throw BinderException(
			    "Circular reference to CTE \"%s\", There are two possible solutions. \n1. use WITH RECURSIVE to "
			    "use recursive CTEs. \n2. If "
			    "you want to use the TABLE name \"%s\" the same as the CTE name, please explicitly add "
			    "\"SCHEMA\" before table name. You can try \"main.%s\" (main is the duckdb default schema)",
			    ref.table_name, ref.table_name, ref.table_name);
		}
	}
	// not a CTE
	// extract a table or view from the catalog
	BindSchemaOrCatalog(ref.catalog_name, ref.schema_name);
	auto table_or_view = entry_retriever.GetEntry(CatalogType::TABLE_ENTRY, ref.catalog_name, ref.schema_name,
	                                              ref.table_name, OnEntryNotFound::RETURN_NULL, error_context);
	// we still didn't find the table
	if (GetBindingMode() == BindingMode::EXTRACT_NAMES) {
		if (!table_or_view || table_or_view->type == CatalogType::TABLE_ENTRY) {
			// if we are in EXTRACT_NAMES, we create a dummy table ref
			AddTableName(ref.table_name);

			// add a bind context entry
			auto table_index = GenerateTableIndex();
			auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
			vector<LogicalType> types {LogicalType::INTEGER};
			vector<string> names {"__dummy_col" + to_string(table_index)};
			bind_context.AddGenericBinding(table_index, alias, names, types);
			return make_uniq_base<BoundTableRef, BoundEmptyTableRef>(table_index);
		}
	}
	if (!table_or_view) {
		// table could not be found: try to bind a replacement scan
		// Try replacement scan bind
		auto replacement_scan_bind_result = BindWithReplacementScan(context, ref);
		if (replacement_scan_bind_result) {
			return replacement_scan_bind_result;
		}

		// Try autoloading an extension, then retry the replacement scan bind
		auto full_path = ReplacementScan::GetFullPath(ref.catalog_name, ref.schema_name, ref.table_name);
		auto extension_loaded = TryLoadExtensionForReplacementScan(context, full_path);
		if (extension_loaded) {
			replacement_scan_bind_result = BindWithReplacementScan(context, ref);
			if (replacement_scan_bind_result) {
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

		// could not find an alternative: bind again to get the error
		(void)entry_retriever.GetEntry(CatalogType::TABLE_ENTRY, ref.catalog_name, ref.schema_name, ref.table_name,
		                               OnEntryNotFound::THROW_EXCEPTION, error_context);
		throw InternalException("Catalog::GetEntry should have thrown an exception above");
	}

	switch (table_or_view->type) {
	case CatalogType::TABLE_ENTRY: {
		// base table: create the BoundBaseTableRef node
		auto table_index = GenerateTableIndex();
		auto &table = table_or_view->Cast<TableCatalogEntry>();

		auto &properties = GetStatementProperties();
		properties.RegisterDBRead(table.ParentCatalog(), context);

		unique_ptr<FunctionData> bind_data;
		auto scan_function = table.GetScanFunction(context, bind_data);
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

		auto logical_get =
		    make_uniq<LogicalGet>(table_index, scan_function, std::move(bind_data), std::move(return_types),
		                          std::move(return_names), table.GetRowIdType());
		auto table_entry = logical_get->GetTable();
		auto &col_ids = logical_get->GetMutableColumnIds();
		if (!table_entry) {
			bind_context.AddBaseTable(table_index, ref.alias, table_names, table_types, col_ids, ref.table_name);
		} else {
			bind_context.AddBaseTable(table_index, ref.alias, table_names, table_types, col_ids, *table_entry);
		}
		return make_uniq_base<BoundTableRef, BoundBaseTableRef>(table, std::move(logical_get));
	}
	case CatalogType::VIEW_ENTRY: {
		// the node is a view: get the query that the view represents
		auto &view_catalog_entry = table_or_view->Cast<ViewCatalogEntry>();
		// We need to use a new binder for the view that doesn't reference any CTEs
		// defined for this binder so there are no collisions between the CTEs defined
		// for the view and for the current query
		auto view_binder = Binder::CreateBinder(context, this, BinderType::VIEW_BINDER);
		view_binder->can_contain_nulls = true;
		SubqueryRef subquery(unique_ptr_cast<SQLStatement, SelectStatement>(view_catalog_entry.query->Copy()));
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
		// bind the child subquery
		view_binder->AddBoundView(view_catalog_entry);
		auto bound_child = view_binder->Bind(subquery);
		if (!view_binder->correlated_columns.empty()) {
			throw BinderException("Contents of view were altered - view bound correlated columns");
		}

		D_ASSERT(bound_child->type == TableReferenceType::SUBQUERY);
		// verify that the types and names match up with the expected types and names
		auto &bound_subquery = bound_child->Cast<BoundSubqueryRef>();
		if (GetBindingMode() != BindingMode::EXTRACT_NAMES) {
			// we bind the view subquery and the original view with different "can_contain_nulls",
			// but we don't want to throw an error when SQLNULL does not match up with INTEGER,
			// so we exchange all SQLNULL with INTEGER here before comparing
			auto bound_types = ExchangeAllNullTypes(bound_subquery.subquery->types);
			auto view_types = ExchangeAllNullTypes(view_catalog_entry.types);
			if (bound_types != view_types) {
				auto actual_types = StringUtil::ToString(bound_types, ", ");
				auto expected_types = StringUtil::ToString(view_types, ", ");
				throw BinderException(
				    "Contents of view were altered: types don't match! Expected [%s], but found [%s] instead",
				    expected_types, actual_types);
			}
			if (bound_subquery.subquery->names.size() == view_catalog_entry.names.size() &&
			    bound_subquery.subquery->names != view_catalog_entry.names) {
				auto actual_names = StringUtil::Join(bound_subquery.subquery->names, ", ");
				auto expected_names = StringUtil::Join(view_catalog_entry.names, ", ");
				throw BinderException(
				    "Contents of view were altered: names don't match! Expected [%s], but found [%s] instead",
				    expected_names, actual_names);
			}
		}
		bind_context.AddView(bound_subquery.subquery->GetRootIndex(), subquery.alias, subquery,
		                     *bound_subquery.subquery, view_catalog_entry);
		return bound_child;
	}
	default:
		throw InternalException("Catalog entry type");
	}
}
} // namespace duckdb
