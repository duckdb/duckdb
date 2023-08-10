#include "duckdb/parser/tableref/pivotref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

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
			if (entry.star_expr) {
				D_ASSERT(entry.values.empty());
				result += entry.star_expr->ToString();
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

void PivotColumn::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteSerializableList(pivot_expressions);
	writer.WriteList<string>(unpivot_names);
	writer.WriteRegularSerializableList(entries);
	writer.WriteString(pivot_enum);
	writer.Finalize();
}

PivotColumn PivotColumn::Deserialize(Deserializer &source) {
	PivotColumn result;
	FieldReader reader(source);
	result.pivot_expressions = reader.ReadRequiredSerializableList<ParsedExpression>();
	result.unpivot_names = reader.ReadRequiredList<string>();
	result.entries = reader.ReadRequiredSerializableList<PivotColumnEntry, PivotColumnEntry>();
	result.pivot_enum = reader.ReadRequired<string>();
	reader.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// PivotColumnEntry
//===--------------------------------------------------------------------===//
PivotColumnEntry PivotColumnEntry::Copy() const {
	PivotColumnEntry result;
	result.values = values;
	result.star_expr = star_expr ? star_expr->Copy() : nullptr;
	result.alias = alias;
	return result;
}

void PivotColumnEntry::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteRegularSerializableList(values);
	writer.WriteOptional(star_expr);
	writer.WriteString(alias);
	writer.Finalize();
}

void PivotColumnEntry::FormatSerialize(FormatSerializer &serializer) const {
	serializer.WriteProperty(100, "values", values);
	serializer.WriteOptionalProperty(101, "star_expr", star_expr);
	serializer.WriteProperty(102, "alias", alias);
}

PivotColumnEntry PivotColumnEntry::Deserialize(Deserializer &source) {
	PivotColumnEntry result;
	FieldReader reader(source);
	result.values = reader.ReadRequiredSerializableList<Value, Value>();
	result.star_expr = reader.ReadOptional<ParsedExpression>(nullptr);
	result.alias = reader.ReadRequired<string>();
	reader.Finalize();
	return result;
}

PivotColumnEntry PivotColumnEntry::FormatDeserialize(FormatDeserializer &source) {
	PivotColumnEntry result;
	source.ReadProperty(100, "values", result.values);
	source.ReadOptionalProperty(101, "star_expr", result.star_expr);
	source.ReadProperty(102, "alias", result.alias);
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
			if (!aggregates[aggr_idx]->alias.empty()) {
				result += " AS " + KeywordHelper::WriteOptionallyQuoted(aggregates[aggr_idx]->alias);
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

void PivotRef::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*source);
	writer.WriteSerializableList(aggregates);
	writer.WriteList<string>(unpivot_names);
	writer.WriteRegularSerializableList(pivots);
	writer.WriteList<string>(groups);
	writer.WriteList<string>(column_name_alias);
	writer.WriteField<bool>(include_nulls);
}

unique_ptr<TableRef> PivotRef::Deserialize(FieldReader &reader) {
	auto result = make_uniq<PivotRef>();
	result->source = reader.ReadRequiredSerializable<TableRef>();
	result->aggregates = reader.ReadRequiredSerializableList<ParsedExpression>();
	result->unpivot_names = reader.ReadRequiredList<string>();
	result->pivots = reader.ReadRequiredSerializableList<PivotColumn, PivotColumn>();
	result->groups = reader.ReadRequiredList<string>();
	result->column_name_alias = reader.ReadRequiredList<string>();
	result->include_nulls = reader.ReadRequired<bool>();
	return std::move(result);
}

} // namespace duckdb
