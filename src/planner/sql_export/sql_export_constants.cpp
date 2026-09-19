#include "duckdb/planner/sql_export/bound_expression_sql_exporter_internal.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/function/scalar/generic_common.hpp"

namespace duckdb {
namespace bound_expression_sql_export {

static bool HasUnsupportedVariantKeys(const VariantNode &node) {
	// Struct literals require nonempty, case-insensitively unique keys; inspect before the lossy STRUCT conversion.
	if (node.GetTypeId() == VariantLogicalType::OBJECT) {
		identifier_set_t names;
		for (auto &child : node.GetObjectChildren()) {
			if (child.key.GetSize() == 0 || !names.insert(Identifier(child.key.GetString())).second ||
			    HasUnsupportedVariantKeys(child.value)) {
				return true;
			}
		}
	} else if (node.GetTypeId() == VariantLogicalType::ARRAY) {
		for (auto child : node.GetArrayChildren()) {
			if (HasUnsupportedVariantKeys(child)) {
				return true;
			}
		}
	}
	return false;
}

static bool HasUnsupportedVariantKeys(const Value &value) {
	if (value.IsNull()) {
		return false;
	}
	optional_ptr<const vector<Value>> children;
	switch (value.type().id()) {
	case LogicalTypeId::VARIANT: {
		Vector vector(value, count_t(1));
		VariantIterator iterator(vector);
		return HasUnsupportedVariantKeys(iterator.Root(0));
	}
	case LogicalTypeId::STRUCT:
		children = StructValue::GetChildren(value);
		break;
	case LogicalTypeId::LIST:
		children = ListValue::GetChildren(value);
		break;
	case LogicalTypeId::ARRAY:
		children = ArrayValue::GetChildren(value);
		break;
	case LogicalTypeId::MAP:
		children = MapValue::GetChildren(value);
		break;
	case LogicalTypeId::UNION:
		return HasUnsupportedVariantKeys(UnionValue::GetValue(value));
	default:
		return false;
	}
	for (auto &child : *children) {
		if (HasUnsupportedVariantKeys(child)) {
			return true;
		}
	}
	return false;
}

static unique_ptr<ParsedExpression> SystemFunction(const string &name, vector<unique_ptr<ParsedExpression>> arguments) {
	return make_uniq<FunctionExpression>(QualifiedName("system", "main", Identifier(name)), std::move(arguments));
}

static unique_ptr<ParsedExpression> UnarySystemFunction(const string &name, unique_ptr<ParsedExpression> argument) {
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(std::move(argument));
	return SystemFunction(name, std::move(arguments));
}

static unique_ptr<ParsedExpression> BinarySystemFunction(const string &name, unique_ptr<ParsedExpression> left,
                                                         unique_ptr<ParsedExpression> right) {
	vector<unique_ptr<ParsedExpression>> arguments;
	arguments.push_back(std::move(left));
	arguments.push_back(std::move(right));
	return SystemFunction(name, std::move(arguments));
}

static unique_ptr<ParsedExpression> IntervalSQLConstant(const interval_t &value) {
	auto months = UnarySystemFunction("to_months", ConstantExpression::FromValue(Value::INTEGER(value.months)));
	auto days = UnarySystemFunction("to_days", ConstantExpression::FromValue(Value::INTEGER(value.days)));
	auto micros = UnarySystemFunction("to_microseconds", ConstantExpression::FromValue(Value::BIGINT(value.micros)));
	return BinarySystemFunction("add", BinarySystemFunction("add", std::move(months), std::move(days)),
	                            std::move(micros));
}

bool BoundExpressionSQLExportState::RequiresConstantConstructor(const LogicalType &type) {
	if (HasNestedCollation(type)) {
		return true;
	}
	return TypeVisitor::Contains(type, [](const LogicalType &child) {
		return child.IsAggregateState() || child.id() == LogicalTypeId::TYPE ||
		       (child.id() == LogicalTypeId::GEOMETRY && GeoType::HasCRS(child));
	});
}

BoundExpressionSQLExportResult
BoundExpressionSQLExportState::ExportNestedConstant(const LogicalType &type, optional_ptr<const Value> value,
                                                    const LogicalPlanVerificationPath &path) {
	if (type.id() == LogicalTypeId::TYPE) {
		if (!value || value->IsNull()) {
			return BoundExpressionSQLExportResult::Success(SQLCast(type, ConstantExpression::FromValue(Value())));
		}
		auto witness = ExportNestedConstant(TypeValue::GetType(*value), nullptr, path);
		if (witness.HasError()) {
			return witness;
		}
		return BoundExpressionSQLExportResult::Success(UnarySystemFunction("get_type", std::move(witness.GetValue())));
	}
	if (type.IsAggregateState() || type.id() == LogicalTypeId::GEOMETRY ||
	    (IsSQLRepresentableType(type) && !RequiresConstantConstructor(type))) {
		auto constant = BoundConstantExpression(value ? *value : Value(type));
		constant.SetReturnType(type);
		return Export(constant, path);
	}
	const bool empty_list =
	    value && !value->IsNull() && type.id() == LogicalTypeId::LIST && ListValue::GetChildren(*value).empty();
	if (value && (value->IsNull() || empty_list)) {
		auto witness = ExportNestedConstant(type, nullptr, path);
		if (witness.HasError()) {
			return witness;
		}
		auto result = make_uniq<CaseExpression>();
		result->CaseChecksMutable().push_back(
		    {ConstantExpression::FromValue(Value::BOOLEAN(false)), std::move(witness.GetValue())});
		result->ElseMutable() =
		    ConstantExpression::FromValue(empty_list ? Value::LIST(LogicalType::SQLNULL, {}) : Value());
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}
	if (type.id() == LogicalTypeId::MAP) {
		vector<Value> keys, values;
		if (value) {
			for (auto &entry : MapValue::GetChildren(*value)) {
				auto &children = StructValue::GetChildren(entry);
				keys.push_back(children[0]);
				values.push_back(children[1]);
			}
		}
		auto key_list = Value::LIST(MapType::KeyType(type), std::move(keys));
		auto value_list = Value::LIST(MapType::ValueType(type), std::move(values));
		auto left = ExportNestedConstant(key_list.type(), key_list, path);
		if (left.HasError()) {
			return left;
		}
		auto right = ExportNestedConstant(value_list.type(), value_list, path);
		if (right.HasError()) {
			return right;
		}
		auto result = BinarySystemFunction("map", std::move(left.GetValue()), std::move(right.GetValue()));
		if (type.HasAlias()) {
			result = SQLCast(type, std::move(result));
		}
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}
	vector<FunctionArgument> arguments;
	child_list_t<LogicalType> child_types;
	optional_ptr<const vector<Value>> children;
	string function;
	switch (type.id()) {
	case LogicalTypeId::TUPLE:
	case LogicalTypeId::STRUCT:
		function = type.id() == LogicalTypeId::TUPLE || StructType::IsUnnamed(type) ? "row" : "struct_pack";
		child_types = StructType::GetChildTypes(type);
		if (value) {
			children = StructValue::GetChildren(*value);
		}
		break;
	case LogicalTypeId::LIST:
		function = "list_value";
		if (value) {
			children = ListValue::GetChildren(*value);
		}
		child_types.resize(children ? children->size() : 1, {Identifier(), ListType::GetChildType(type)});
		break;
	case LogicalTypeId::ARRAY:
		function = "array_value";
		if (value) {
			children = ArrayValue::GetChildren(*value);
		}
		child_types.resize(ArrayType::GetSize(type), {Identifier(), ArrayType::GetChildType(type)});
		break;
	default:
		return Failure(
		    UnsupportedFeature(path, "nested_constant_type", "The nested value type has no SQL constructor"));
	}
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto child =
		    ExportNestedConstant(child_types[i].second, children ? &(*children)[i] : nullptr, ChildPath(path, i));
		if (child.HasError()) {
			return child;
		}
		arguments.emplace_back(function == "struct_pack" ? child_types[i].first : Identifier(),
		                       std::move(child.GetValue()));
	}
	unique_ptr<ParsedExpression> result =
	    make_uniq<FunctionExpression>(QualifiedName("system", "main", Identifier(function)), std::move(arguments));
	if (type.HasAlias()) {
		result = SQLCast(type, std::move(result));
	}
	return BoundExpressionSQLExportResult::Success(std::move(result));
}

BoundExpressionSQLExportResult
BoundExpressionSQLExportState::CastToConstructedType(const LogicalType &type, unique_ptr<ParsedExpression> child,
                                                     const LogicalPlanVerificationPath &path) {
	auto target = ExportNestedConstant(type, nullptr, path);
	if (target.HasError()) {
		return target;
	}
	return BoundExpressionSQLExportResult::Success(
	    BinarySystemFunction("cast_to_type", std::move(child), std::move(target.GetValue())));
}

BoundExpressionSQLExportResult
BoundExpressionSQLExportState::RestoreResultType(const LogicalType &type, unique_ptr<ParsedExpression> result,
                                                 const LogicalPlanVerificationPath &path) {
	if (RequiresConstantConstructor(type)) {
		return CastToConstructedType(type, std::move(result), path);
	}
	return BoundExpressionSQLExportResult::Success(SQLCast(type, std::move(result)));
}

BoundExpressionSQLExportResult BoundExpressionSQLExportState::ExportConstant(const BoundConstantExpression &expression,
                                                                             const LogicalPlanVerificationPath &path) {
	D_ASSERT(expression.GetExpressionType() == ExpressionType::VALUE_CONSTANT);
	auto &return_type = expression.GetReturnType();
	auto &value = expression.GetValue();
	D_ASSERT(return_type == value.type());
	if (!IsSQLValueType(return_type)) {
		return Failure(InternalExpressionInvariant(path, expression, "Bound constant has an unexportable type"));
	}
	if (TypeVisitor::Contains(return_type, LogicalTypeId::VARIANT) && HasUnsupportedVariantKeys(value)) {
		return Failure(UnsupportedFeature(path, "variant_literal",
		                                  "VARIANT object keys cannot be represented by a struct literal"));
	}
	if (return_type.IsAggregateState()) {
		auto storage_type = return_type.WithAlias("").WithExtensionInfo(nullptr);
		Vector source(value, count_t(1));
		Vector storage(storage_type, 1);
		storage.Reinterpret(source);
		auto child = ExportConstant(BoundConstantExpression(storage.GetValue(0)), path);
		if (child.HasError()) {
			return child;
		}
		auto result = ExportAggregateFunction::StateToSQL(return_type, std::move(child.GetValue()));
		if (!result) {
			return Failure(UnsupportedFeature(path, "aggregate_state_parameters",
			                                  "Aggregate state SQL parameters are not representable"));
		}
		return BoundExpressionSQLExportResult::Success(std::move(result));
	}
	if (return_type.id() != LogicalTypeId::GEOMETRY &&
	    (!IsSQLRepresentableType(return_type) || RequiresConstantConstructor(return_type))) {
		return ExportNestedConstant(return_type, value, path);
	}
	unique_ptr<ParsedExpression> result;
	if (!value.IsNull() && return_type.id() == LogicalTypeId::INTERVAL) {
		return BoundExpressionSQLExportResult::Success(IntervalSQLConstant(IntervalValue::Get(value)));
	}
	if (return_type.id() == LogicalTypeId::GEOMETRY) {
		if (value.IsNull()) {
			auto geometry = GeoType::HasCRS(return_type) ? Value("GEOMETRYCOLLECTION EMPTY") : Value();
			result = SQLCast(LogicalType::GEOMETRY(), ConstantExpression::FromValue(geometry));
		} else {
			result = UnarySystemFunction("st_geomfromwkb",
			                             ConstantExpression::FromValue(Value::BLOB_RAW(StringValue::Get(value))));
		}
		if (GeoType::HasCRS(return_type)) {
			vector<unique_ptr<ParsedExpression>> arguments;
			arguments.push_back(std::move(result));
			arguments.push_back(ConstantExpression::FromValue(Value(GeoType::GetCRS(return_type).GetDefinition())));
			result = make_uniq<FunctionExpression>(QualifiedName("system", "main", "st_setcrs"), std::move(arguments));
			if (value.IsNull()) {
				auto typed_null = make_uniq<CaseExpression>();
				typed_null->CaseChecksMutable().push_back(
				    {ConstantExpression::FromValue(Value::BOOLEAN(false)), std::move(result)});
				typed_null->ElseMutable() = ConstantExpression::FromValue(Value());
				result = std::move(typed_null);
			}
			return BoundExpressionSQLExportResult::Success(std::move(result));
		}
	} else {
		result = ConstantExpression::FromValue(value.WithType(SQLCastType(return_type)));
	}
	if (return_type.id() != LogicalTypeId::SQLNULL &&
	    (result->GetExpressionClass() != ExpressionClass::CAST ||
	     !result->Cast<CastExpression>().TargetType().Equals(
	         *TypeExpression::FromLogicalType(SQLCastType(return_type))))) {
		result = SQLCast(return_type, std::move(result));
	}
	return BoundExpressionSQLExportResult::Success(std::move(result));
}

} // namespace bound_expression_sql_export
} // namespace duckdb
