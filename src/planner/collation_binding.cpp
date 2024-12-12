#include "duckdb/planner/collation_binding.hpp"
#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

bool PushVarcharCollation(ClientContext &context, unique_ptr<Expression> &source, const LogicalType &sql_type,
                          CollationType type) {
	if (sql_type.id() != LogicalTypeId::VARCHAR) {
		// only VARCHAR columns require collation
		return false;
	}
	// replace default collation with system collation
	auto str_collation = StringType::GetCollation(sql_type);
	string collation;
	if (str_collation.empty()) {
		collation = DBConfig::GetConfig(context).options.collation;
	} else {
		collation = str_collation;
	}
	collation = StringUtil::Lower(collation);
	// bind the collation
	if (collation.empty() || collation == "binary" || collation == "c" || collation == "posix") {
		// no collation or binary collation: skip
		return false;
	}
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto splits = StringUtil::Split(StringUtil::Lower(collation), ".");
	vector<reference<CollateCatalogEntry>> entries;
	unordered_set<string> collations;
	for (auto &collation_argument : splits) {
		if (collations.count(collation_argument)) {
			// we already applied this collation
			continue;
		}
		auto &collation_entry = catalog.GetEntry<CollateCatalogEntry>(context, DEFAULT_SCHEMA, collation_argument);
		if (collation_entry.combinable) {
			entries.insert(entries.begin(), collation_entry);
		} else {
			if (!entries.empty() && !entries.back().get().combinable) {
				throw BinderException("Cannot combine collation types \"%s\" and \"%s\"", entries.back().get().name,
				                      collation_entry.name);
			}
			entries.push_back(collation_entry);
		}
		collations.insert(collation_argument);
	}
	for (auto &entry : entries) {
		auto &collation_entry = entry.get();
		if (!collation_entry.combinable && type == CollationType::COMBINABLE_COLLATIONS) {
			// not a combinable collation - ignore
			return false;
		}
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(source));

		FunctionBinder function_binder(context);
		auto function = function_binder.BindScalarFunction(collation_entry.function, std::move(children));
		source = std::move(function);
	}
	return true;
}

bool PushTimeTZCollation(ClientContext &context, unique_ptr<Expression> &source, const LogicalType &sql_type,
                         CollationType) {
	if (sql_type.id() != LogicalTypeId::TIME_TZ) {
		return false;
	}

	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &function_entry =
	    catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "timetz_byte_comparable");
	if (function_entry.functions.Size() != 1) {
		throw InternalException("timetz_byte_comparable should only have a single overload");
	}
	auto &scalar_function = function_entry.functions.GetFunctionReferenceByOffset(0);
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(source));

	FunctionBinder function_binder(context);
	auto function = function_binder.BindScalarFunction(scalar_function, std::move(children));
	source = std::move(function);
	return true;
}

bool PushIntervalCollation(ClientContext &context, unique_ptr<Expression> &source, const LogicalType &sql_type,
                           CollationType) {
	if (sql_type.id() != LogicalTypeId::INTERVAL) {
		return false;
	}

	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &function_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "normalized_interval");
	if (function_entry.functions.Size() != 1) {
		throw InternalException("normalized_interval should only have a single overload");
	}
	auto &scalar_function = function_entry.functions.GetFunctionReferenceByOffset(0);
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(source));

	FunctionBinder function_binder(context);
	auto function = function_binder.BindScalarFunction(scalar_function, std::move(children));
	source = std::move(function);
	return true;
}

// timetz_byte_comparable
CollationBinding::CollationBinding() {
	RegisterCollation(CollationCallback(PushVarcharCollation));
	RegisterCollation(CollationCallback(PushTimeTZCollation));
	RegisterCollation(CollationCallback(PushIntervalCollation));
}

void CollationBinding::RegisterCollation(CollationCallback callback) {
	collations.push_back(callback);
}

bool CollationBinding::PushCollation(ClientContext &context, unique_ptr<Expression> &source,
                                     const LogicalType &sql_type, CollationType type) const {
	for (auto &collation : collations) {
		if (collation.try_push_collation(context, source, sql_type, type)) {
			// successfully pushed a collation
			return true;
		}
	}
	return false;
}

} // namespace duckdb
