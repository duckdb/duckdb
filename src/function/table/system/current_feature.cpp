#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/feature_query.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

static optional_ptr<FeatureCatalogEntry> LookupFeature(ClientContext &context, const string &feature_name) {
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		auto entry = schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::FEATURE_ENTRY,
		                                   feature_name);
		if (entry) {
			return &entry->Cast<FeatureCatalogEntry>();
		}
	}
	return nullptr;
}

static unique_ptr<TableRef> CurrentFeatureBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto feature_name = input.inputs[0].GetValue<string>();
	auto feature_entry = LookupFeature(context, feature_name);

	if (!feature_entry) {
		throw CatalogException("Feature \"%s\" does not exist", feature_name);
	}

	if (feature_entry->current_version < 1) {
		throw CatalogException("Feature \"%s\" has not been refreshed yet — run REFRESH FEATURE %s first", feature_name,
		                       feature_name);
	}

	// Resolve to the current snapshot in the denormalized store: filter the store table to the current
	// version's rows and hide the internal bookkeeping columns.
	auto store_table = make_uniq<BaseTableRef>();
	store_table->catalog_name = feature_entry->ParentCatalog().GetName();
	store_table->schema_name = feature_entry->ParentSchema().name;
	store_table->table_name = feature_name + "__v" + duckdb::to_string(feature_entry->current_version);

	auto star = make_uniq<StarExpression>();
	star->exclude_list.insert(QualifiedColumnName(FEATURE_VERSION_COLUMN));
	star->exclude_list.insert(QualifiedColumnName(FEATURE_TIMESTAMP_COLUMN));

	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(std::move(star));
	select->from_table = std::move(store_table);
	select->where_clause = make_uniq<ComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>(FEATURE_VERSION_COLUMN),
	    make_uniq<ConstantExpression>(Value::BIGINT(feature_entry->current_version)));

	auto statement = make_uniq<SelectStatement>();
	statement->node = std::move(select);
	return make_uniq<SubqueryRef>(std::move(statement));
}

void CurrentFeatureFun::RegisterFunction(BuiltinFunctions &set) {
	auto current_feature = TableFunction("current_feature", {LogicalType::VARCHAR}, nullptr);
	current_feature.bind_replace = CurrentFeatureBindReplace;
	set.AddFunction(current_feature);
}

} // namespace duckdb
