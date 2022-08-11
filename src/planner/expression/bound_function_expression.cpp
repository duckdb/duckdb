#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/parser/expression_util.hpp"

namespace duckdb {

BoundFunctionExpression::BoundFunctionExpression(LogicalType return_type, ScalarFunction bound_function,
                                                 vector<unique_ptr<Expression>> arguments,
                                                 unique_ptr<FunctionData> bind_info, bool is_operator)
    : Expression(ExpressionType::BOUND_FUNCTION, ExpressionClass::BOUND_FUNCTION, move(return_type)),
      function(move(bound_function)), children(move(arguments)), bind_info(move(bind_info)), is_operator(is_operator) {
	D_ASSERT(!function.name.empty());
}

bool BoundFunctionExpression::HasSideEffects() const {
	return function.side_effects == FunctionSideEffects::HAS_SIDE_EFFECTS ? true : Expression::HasSideEffects();
}

bool BoundFunctionExpression::IsFoldable() const {
	// functions with side effects cannot be folded: they have to be executed once for every row
	return function.side_effects == FunctionSideEffects::HAS_SIDE_EFFECTS ? false : Expression::IsFoldable();
}

string BoundFunctionExpression::ToString() const {
	return FunctionExpression::ToString<BoundFunctionExpression, Expression>(*this, string(), function.name,
	                                                                         is_operator);
}
bool BoundFunctionExpression::PropagatesNullValues() const {
	return function.null_handling == FunctionNullHandling::SPECIAL_HANDLING ? false
	                                                                        : Expression::PropagatesNullValues();
}

hash_t BoundFunctionExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(result, function.Hash());
}

bool BoundFunctionExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundFunctionExpression *)other_p;
	if (other->function != function) {
		return false;
	}
	if (!ExpressionUtil::ListEquals(children, other->children)) {
		return false;
	}
	if (!FunctionData::Equals(bind_info.get(), other->bind_info.get())) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundFunctionExpression::Copy() {
	vector<unique_ptr<Expression>> new_children;
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	unique_ptr<FunctionData> new_bind_info = bind_info ? bind_info->Copy() : nullptr;

	auto copy = make_unique<BoundFunctionExpression>(return_type, function, move(new_children), move(new_bind_info),
	                                                 is_operator);
	copy->CopyProperties(*this);
	return move(copy);
}

void BoundFunctionExpression::Verify() const {
	D_ASSERT(!function.name.empty());
}

void BoundFunctionExpression::Serialize(FieldWriter &writer) const {
	D_ASSERT(!function.name.empty());
	D_ASSERT(return_type == function.return_type);
	writer.WriteString(function.name);
	writer.WriteField(is_operator);
	writer.WriteSerializable(return_type);
	writer.WriteRegularSerializableList(function.arguments);

	writer.WriteSerializableList(children);

	bool serialize = function.serialize;
	writer.WriteField(serialize);
	if (serialize) {
		function.serialize(writer, bind_info.get(), function);
	}
}

unique_ptr<Expression> BoundFunctionExpression::Deserialize(ExpressionDeserializationState &state,
                                                            FieldReader &reader) {
	auto name = reader.ReadRequired<string>();
	auto is_operator = reader.ReadRequired<bool>();
	auto return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto arguments = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto children = reader.ReadRequiredSerializableList<Expression>(state.gstate);

	// TODO this is duplicated in logical_get and bound_aggregate_expression more or less, make it a template or so

	auto &context = state.gstate.context;
	auto &catalog = Catalog::GetCatalog(context);
	auto func_catalog = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, name);

	if (!func_catalog || func_catalog->type != CatalogType::SCALAR_FUNCTION_ENTRY) {
		throw InternalException("Cant find catalog entry for function %s", name);
	}

	auto functions = (ScalarFunctionCatalogEntry *)func_catalog;
	auto function = functions->functions.GetFunctionByArguments(arguments);
	unique_ptr<FunctionData> bind_info;

	// sometimes the bind changes those, not sure if we should generically set those
	function.return_type = move(return_type);
	function.arguments = move(arguments);
	auto has_deserialize = reader.ReadRequired<bool>();
	if (has_deserialize) {
		if (!function.deserialize) {
			throw SerializationException("Function requires deserialization but no deserialization function for %s",
			                             function.name);
		}
		bind_info = function.deserialize(context, reader, function);
	} else if (function.bind) {
		bind_info = function.bind(context, function, children);
	}
	return_type = function.return_type;

	return make_unique<BoundFunctionExpression>(move(return_type), move(function), move(children), move(bind_info),
	                                            is_operator);
}
} // namespace duckdb
