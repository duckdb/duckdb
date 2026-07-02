#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

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
		throw CatalogException("Feature \"%s\" has not been refreshed yet — run REFRESH FEATURE %s first",
		                       feature_name, feature_name);
	}

	auto result = make_uniq<BaseTableRef>();
	result->catalog_name = feature_entry->ParentCatalog().GetName();
	result->schema_name = feature_entry->ParentSchema().name;
	result->table_name = feature_name + "__v" + duckdb::to_string(feature_entry->current_version);
	return std::move(result);
}

void CurrentFeatureFun::RegisterFunction(BuiltinFunctions &set) {
	auto current_feature = TableFunction("current_feature", {LogicalType::VARCHAR}, nullptr);
	current_feature.bind_replace = CurrentFeatureBindReplace;
	set.AddFunction(current_feature);
}

} // namespace duckdb
