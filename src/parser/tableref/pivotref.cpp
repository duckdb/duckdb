#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/expression_util.hpp"
#include "duckdb/common/limits.hpp"

#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// PivotColumn
//===--------------------------------------------------------------------===//
string PivotColumn::ToString() const {
	string result;
	if (!unpivot_names.empty()) {
		D_ASSERT(pivot_expressions.empty());
		// unpivot
		if (unpivot_names.size() == 1) {
			result += KeywordHelper::WriteOptionallyQuoted(unpivot_names[0]);
		} else {
			result += "(";
			for (idx_t n = 0; n < unpivot_names.size(); n++) {
				if (n > 0) {
					result += ", ";
				}
				result += KeywordHelper::WriteOptionallyQuoted(unpivot_names[n]);
			}
			result += ")";
		}
	} else if (!pivot_expressions.empty()) {
		// pivot
		result += "(";
		for (idx_t n = 0; n < pivot_expressions.size(); n++) {
			if (n > 0) {
				result += ", ";
			}
			result += pivot_expressions[n]->ToString();
		}
		result += ")";
	}
	result += " IN ";
	if (pivot_enum.empty()) {
		result += "(";
		for (idx_t e = 0; e < entries.size(); e++) {
			auto &entry = entries[e];
			if (e > 0) {
				result += ", ";
			}
			if (entry.expr) {
				D_ASSERT(entry.values.empty());
				result += entry.expr->ToString();
			} else if (entry.values.size() == 1) {
				result += entry.values[0].ToSQLString();
			} else {
				result += "(";
				for (idx_t v = 0; v < entry.values.size(); v++) {
					if (v > 0) {
						result += ", ";
					}
					result += entry.values[v].ToSQLString();
				}
				result += ")";
			}
			if (!entry.alias.empty()) {
				result += " AS " + KeywordHelper::WriteOptionallyQuoted(entry.alias);
			}
		}
		result += ")";
	} else {
		result += KeywordHelper::WriteOptionallyQuoted(pivot_enum);
	}
	return result;
}

bool PivotColumnEntry::Equals(const PivotColumnEntry &other) const {
	if (alias != other.alias) {
		return false;
	}
	if (values.size() != other.values.size()) {
		return false;
	}
	for (idx_t i = 0; i < values.size(); i++) {
		if (!Value::NotDistinctFrom(values[i], other.values[i])) {
			return false;
		}
	}
	return true;
}

bool PivotColumn::Equals(const PivotColumn &other) const {
	if (!ExpressionUtil::ListEquals(pivot_expressions, other.pivot_expressions)) {
		return false;
	}
	if (other.unpivot_names != unpivot_names) {
		return false;
	}
	if (other.pivot_enum != pivot_enum) {
		return false;
	}
	if (other.entries.size() != entries.size()) {
		return false;
	}
	for (idx_t i = 0; i < entries.size(); i++) {
		if (!entries[i].Equals(other.entries[i])) {
			return false;
		}
	}
	return true;
}

PivotColumn PivotColumn::Copy() const {
	PivotColumn result;
	for (auto &expr : pivot_expressions) {
		result.pivot_expressions.push_back(expr->Copy());
	}
	result.unpivot_names = unpivot_names;
	for (auto &entry : entries) {
		result.entries.push_back(entry.Copy());
	}
	result.pivot_enum = pivot_enum;
	return result;
}

//===--------------------------------------------------------------------===//
// PivotColumnEntry
//===--------------------------------------------------------------------===//
PivotColumnEntry PivotColumnEntry::Copy() const {
	PivotColumnEntry result;
	result.values = values;
	result.expr = expr ? expr->Copy() : nullptr;
	result.alias = alias;
	return result;
}

static bool TryFoldConstantForBackwardsCompatability(const ParsedExpression &expr, Value &value) {
	switch (expr.GetExpressionType()) {
	case ExpressionType::FUNCTION: {
		auto &function = expr.Cast<FunctionExpression>();
		if (function.function_name == "struct_pack") {
			unordered_set<string> unique_names;
			child_list_t<Value> values;
			values.reserve(function.children.size());
			for (const auto &child : function.children) {
				if (!unique_names.insert(child->GetAlias()).second) {
					return false;
				}
				Value child_value;
				if (!TryFoldConstantForBackwardsCompatability(*child, child_value)) {
					return false;
				}
				values.emplace_back(child->GetAlias(), std::move(child_value));
			}
			value = Value::STRUCT(std::move(values));
			return true;
		} else if (function.function_name == "list_value") {
			vector<Value> values;
			values.reserve(function.children.size());
			for (const auto &child : function.children) {
				Value child_value;
				if (!TryFoldConstantForBackwardsCompatability(*child, child_value)) {
					return false;
				}
				values.emplace_back(std::move(child_value));
			}

			// figure out child type
			LogicalType child_type(LogicalTypeId::SQLNULL);
			for (auto &child_value : values) {
				child_type = LogicalType::ForceMaxLogicalType(child_type, child_value.type());
			}

			// finally create the list
			value = Value::LIST(child_type, values);
			return true;
		} else if (function.function_name == "map") {
			Value keys;
			if (!TryFoldConstantForBackwardsCompatability(*function.children[0], keys)) {
				return false;
			}

			Value values;
			if (!TryFoldConstantForBackwardsCompatability(*function.children[1], values)) {
				return false;
			}

			vector<Value> keys_unpacked = ListValue::GetChildren(keys);
			vector<Value> values_unpacked = ListValue::GetChildren(values);

			value = Value::MAP(ListType::GetChildType(keys.type()), ListType::GetChildType(values.type()),
			                   keys_unpacked, values_unpacked);
			return true;
		} else {
			return false;
		}
	}
	case ExpressionType::VALUE_CONSTANT: {
		auto &constant = expr.Cast<ConstantExpression>();
		value = constant.value;
		return true;
	}
	case ExpressionType::OPERATOR_CAST: {
		auto &cast = expr.Cast<CastExpression>();
		Value dummy_value;
		if (!TryFoldConstantForBackwardsCompatability(*cast.child, dummy_value)) {
			return false;
		}

		// Try to default bind cast
		LogicalType cast_type;
		try {
			cast_type = UnboundType::TryDefaultBind(cast.cast_type);
		} catch (...) {
			return false;
		}

		if (cast_type == LogicalType::INVALID || cast_type == LogicalTypeId::UNBOUND) {
			return false;
		}

		string error_message;
		if (!dummy_value.DefaultTryCastAs(cast_type, value, &error_message)) {
			return false;
		}
		return true;
	}
	default:
		return false;
	}
}

static bool TryFoldForBackwardsCompatibility(const unique_ptr<ParsedExpression> &expr, vector<Value> &values) {
	if (!expr) {
		return true;
	}

	switch (expr->GetExpressionType()) {
	case ExpressionType::COLUMN_REF: {
		auto &colref = expr->Cast<ColumnRefExpression>();
		if (colref.IsQualified()) {
			return false;
		}
		values.emplace_back(colref.GetColumnName());
		return true;
	}
	case ExpressionType::FUNCTION: {
		auto &function = expr->Cast<FunctionExpression>();
		if (function.function_name != "row") {
			return false;
		}
		for (auto &child : function.children) {
			if (!TryFoldForBackwardsCompatibility(child, values)) {
				return false;
			}
		}
		return true;
	}
	default: {
		Value val;
		if (!TryFoldConstantForBackwardsCompatability(*expr, val)) {
			return false;
		}
		values.push_back(std::move(val));
		return true;
	}
	}
}

void PivotColumnEntry::Serialize(Serializer &serializer) const {
	if (serializer.ShouldSerialize(7) || !expr) {
		serializer.WritePropertyWithDefault<vector<Value>>(100, "values", values);
		serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(101, "star_expr", expr);
		serializer.WritePropertyWithDefault<string>(102, "alias", alias);
	} else {
		// We used to only support constant values in pivot entries, and folded expressions in the
		// transformer. So we need to seriaize in a backwards compatible way here by trying to fold
		// the expression back to constant values.
		vector<Value> dummy_values;
		if (!TryFoldForBackwardsCompatibility(expr, dummy_values)) {
			throw SerializationException(
			    "Cannot serialize arbitrary expression pivot entries when targeting database storage version '%s'",
			    serializer.GetOptions().serialization_compatibility.duckdb_version);
			;
		}
		serializer.WritePropertyWithDefault<vector<Value>>(100, "values", dummy_values);
		serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(101, "star_expr", nullptr);
		serializer.WritePropertyWithDefault<string>(102, "alias", alias);
	}
}

PivotColumnEntry PivotColumnEntry::Deserialize(Deserializer &deserializer) {
	PivotColumnEntry result;
	deserializer.ReadPropertyWithDefault<vector<Value>>(100, "values", result.values);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(101, "star_expr", result.expr);
	deserializer.ReadPropertyWithDefault<string>(102, "alias", result.alias);
	return result;
}

//===--------------------------------------------------------------------===//
// PivotRef
//===--------------------------------------------------------------------===//
string PivotRef::ToString() const {
	string result;
	result = source->ToString();
	if (!aggregates.empty()) {
		// pivot
		result += " PIVOT (";
		for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
			if (aggr_idx > 0) {
				result += ", ";
			}
			result += aggregates[aggr_idx]->ToString();
			if (!aggregates[aggr_idx]->GetAlias().empty()) {
				result += " AS " + KeywordHelper::WriteOptionallyQuoted(aggregates[aggr_idx]->GetAlias());
			}
		}
	} else {
		// unpivot
		result += " UNPIVOT ";
		if (include_nulls) {
			result += "INCLUDE NULLS ";
		}
		result += "(";
		if (unpivot_names.size() == 1) {
			result += KeywordHelper::WriteOptionallyQuoted(unpivot_names[0]);
		} else {
			result += "(";
			for (idx_t n = 0; n < unpivot_names.size(); n++) {
				if (n > 0) {
					result += ", ";
				}
				result += KeywordHelper::WriteOptionallyQuoted(unpivot_names[n]);
			}
			result += ")";
		}
	}
	result += " FOR";
	for (auto &pivot : pivots) {
		result += " ";
		result += pivot.ToString();
	}
	if (!groups.empty()) {
		result += " GROUP BY ";
		for (idx_t i = 0; i < groups.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += groups[i];
		}
	}
	result += ")";
	if (!alias.empty()) {
		result += " AS " + KeywordHelper::WriteOptionallyQuoted(alias);
		if (!column_name_alias.empty()) {
			result += "(";
			for (idx_t i = 0; i < column_name_alias.size(); i++) {
				if (i > 0) {
					result += ", ";
				}
				result += KeywordHelper::WriteOptionallyQuoted(column_name_alias[i]);
			}
			result += ")";
		}
	}
	return result;
}

bool PivotRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<PivotRef>();
	if (!source->Equals(*other.source)) {
		return false;
	}
	if (!ParsedExpression::ListEquals(aggregates, other.aggregates)) {
		return false;
	}
	if (pivots.size() != other.pivots.size()) {
		return false;
	}
	for (idx_t i = 0; i < pivots.size(); i++) {
		if (!pivots[i].Equals(other.pivots[i])) {
			return false;
		}
	}
	if (unpivot_names != other.unpivot_names) {
		return false;
	}
	if (alias != other.alias) {
		return false;
	}
	if (groups != other.groups) {
		return false;
	}
	if (include_nulls != other.include_nulls) {
		return false;
	}
	return true;
}

unique_ptr<TableRef> PivotRef::Copy() {
	auto copy = make_uniq<PivotRef>();
	copy->source = source->Copy();
	for (auto &aggr : aggregates) {
		copy->aggregates.push_back(aggr->Copy());
	}
	copy->unpivot_names = unpivot_names;
	for (auto &entry : pivots) {
		copy->pivots.push_back(entry.Copy());
	}
	copy->groups = groups;
	copy->column_name_alias = column_name_alias;
	copy->include_nulls = include_nulls;
	copy->alias = alias;
	return std::move(copy);
}

} // namespace duckdb
