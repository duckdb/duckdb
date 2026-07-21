#include "duckdb/planner/collation_binding.hpp"
#include "duckdb/catalog/catalog_entry/collate_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {
constexpr const char *CollateCatalogEntry::Name;

static vector<string> GetVarcharCollationFunctions(ClientContext &context, const LogicalType &sql_type,
                                                   CollationType type) {
	vector<string> result;
	if (sql_type.id() != LogicalTypeId::VARCHAR) {
		// only VARCHAR columns require collation
		return result;
	}
	// replace default collation with system collation
	auto str_collation = StringType::GetCollation(sql_type);
	string collation;
	if (str_collation.empty()) {
		collation = Settings::Get<DefaultCollationSetting>(context);
	} else {
		collation = str_collation;
	}
	collation = StringUtil::Lower(collation);
	// bind the collation
	if (collation.empty() || collation == "binary" || collation == "c" || collation == "posix") {
		// no collation or binary collation: skip
		return result;
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
		auto &collation_entry = catalog.GetEntry<CollateCatalogEntry>(
		    context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier(collation_argument)));
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
			// not a combinable collation - only apply the (preceding) combinable collations
			break;
		}
		result.push_back(collation_entry.function.GetName().GetIdentifierName());
	}
	return result;
}

static vector<string> GetTimeTZCollationFunctions(ClientContext &, const LogicalType &sql_type, CollationType) {
	if (sql_type.id() != LogicalTypeId::TIME_TZ) {
		return vector<string>();
	}
	return {"timetz_byte_comparable"};
}

static vector<string> GetBitStringCollationFunctions(ClientContext &, const LogicalType &sql_type, CollationType) {
	if (sql_type.id() != LogicalTypeId::BIT) {
		return vector<string>();
	}
	return {"bitstring_byte_comparable"};
}

static vector<string> GetIntervalCollationFunctions(ClientContext &, const LogicalType &sql_type, CollationType) {
	if (sql_type.id() != LogicalTypeId::INTERVAL) {
		return vector<string>();
	}
	return {"normalized_interval"};
}

static vector<string> GetVariantCollationFunctions(ClientContext &, const LogicalType &sql_type, CollationType) {
	if (sql_type.id() != LogicalTypeId::VARIANT) {
		return vector<string>();
	}
	return {"variant_comparator"};
}

CollationBinding::CollationBinding() {
	RegisterCollation(CollationCallback(GetVarcharCollationFunctions));
	RegisterCollation(CollationCallback(GetTimeTZCollationFunctions));
	RegisterCollation(CollationCallback(GetBitStringCollationFunctions));
	RegisterCollation(CollationCallback(GetIntervalCollationFunctions));
	RegisterCollation(CollationCallback(GetVariantCollationFunctions));
}

void CollationBinding::RegisterCollation(CollationCallback callback) {
	collations.push_back(callback);
}

//! Binds the scalar function with the given name (looked up from the system catalog) around "source".
static unique_ptr<Expression> ApplyCollationFunction(ClientContext &context, const string &function_name,
                                                     unique_ptr<Expression> source) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &function_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier(function_name)));
	auto source_alias = source->GetAlias();
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(source));

	FunctionBinder function_binder(context);
	ErrorData error;
	auto function = function_binder.BindScalarFunction(function_entry, std::move(children), error);
	if (!function) {
		error.Throw();
	}
	function->SetAlias(source_alias);
	return function;
}

//! Pushes a collation into a LIST/ARRAY type by wrapping the source in a list_transform that applies the collation to
//! every element via a lambda. Returns false if the elements do not require collation, or if list_transform is not
//! available (i.e. the core_functions extension is not loaded).
static bool PushNestedCollation(ClientContext &context, unique_ptr<Expression> &source, const LogicalType &child_type,
                                CollationType type, const CollationBinding &binding) {
	// build a lambda body that applies the collation to the lambda parameter (a reference to the child element)
	unique_ptr<Expression> lambda_body = make_uniq<BoundReferenceExpression>(child_type, idx_t(0));
	if (!binding.PushCollation(context, lambda_body, child_type, type)) {
		// the child type does not require collation - nothing to push
		return false;
	}

	auto &catalog = Catalog::GetSystemCatalog(context);
	auto list_transform = catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), "list_transform"),
	    OnEntryNotFound::RETURN_NULL);
	if (!list_transform) {
		// list_transform is not available - cannot push the collation into the list
		return false;
	}

	auto bound_lambda =
	    make_uniq<BoundLambdaExpression>(ExpressionType::LAMBDA, LogicalType::LAMBDA, std::move(lambda_body), 1);
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(source));
	children.push_back(std::move(bound_lambda));

	FunctionBinder function_binder(context);
	ErrorData error;
	auto function = function_binder.BindScalarFunction(*list_transform, std::move(children), error);
	if (!function) {
		error.Throw();
	}
	// the lambda expression is consumed by the bind - remove it from the children
	auto &bound_function = function->Cast<BoundFunctionExpression>();
	bound_function.GetChildrenMutable().erase_at(1);
	source = std::move(function);
	return true;
}

bool CollationBinding::PushCollation(ClientContext &context, unique_ptr<Expression> &source,
                                     const LogicalType &sql_type, CollationType type) const {
	if (sql_type.id() == LogicalTypeId::LIST) {
		return PushNestedCollation(context, source, ListType::GetChildType(sql_type), type, *this);
	}
	if (sql_type.id() == LogicalTypeId::ARRAY) {
		return PushNestedCollation(context, source, ArrayType::GetChildType(sql_type), type, *this);
	}
	for (auto &collation : collations) {
		auto functions = collation.get_collation_functions(context, sql_type, type);
		if (functions.empty()) {
			continue;
		}
		// successfully retrieved the collation functions - apply them to the source expression
		for (auto &function_name : functions) {
			source = ApplyCollationFunction(context, function_name, std::move(source));
		}
		return true;
	}
	return false;
}

} // namespace duckdb
