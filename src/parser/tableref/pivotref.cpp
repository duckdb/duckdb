#include "duckdb/parser/tableref/pivotref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string PivotColumn::ToString() const {
	string result;
	result += " FOR";
	result = KeywordHelper::WriteOptionallyQuoted(name);
	result += " IN ";
	if (pivot_enum.empty()) {
		result += "(";
		for (idx_t i = 0; i < values.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += values[i].ToSQLString();
		}
		result += ")";
	} else {
		result += KeywordHelper::WriteOptionallyQuoted(pivot_enum);
	}
	return result;
}

string PivotRef::ToString() const {
	string result;
	result = source->ToString() + " PIVOT (";
	result += aggregate->ToString();

	for (auto &pivot : pivots) {
		result += " ";
		result += pivot.ToString();
	}
	result += ")";
	if (!alias.empty()) {
		result += " AS " + KeywordHelper::WriteOptionallyQuoted(alias);
	}
	return result;
}

bool PivotRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto other = (PivotRef *)other_p;
	if (!source->Equals(other->source.get())) {
		return false;
	}
	if (!aggregate->Equals(other->aggregate.get())) {
		return false;
	}
	if (pivots.size() != other->pivots.size()) {
		return false;
	}
	for (idx_t i = 0; i < pivots.size(); i++) {
		if (pivots[i].name != other->pivots[i].name) {
			return false;
		}
		if (pivots[i].values != other->pivots[i].values) {
			return false;
		}
	}
	if (alias != other->alias) {
		return false;
	}
	return true;
}

unique_ptr<TableRef> PivotRef::Copy() {
	auto copy = make_unique<PivotRef>();
	copy->source = source->Copy();
	copy->aggregate = aggregate->Copy();
	copy->pivots = pivots;
	copy->alias = alias;
	return std::move(copy);
}

void PivotRef::Serialize(FieldWriter &writer) const {
	throw InternalException("FIXME: serialize pivot");
}

unique_ptr<TableRef> PivotRef::Deserialize(FieldReader &reader) {
	throw InternalException("FIXME: deserialize pivot");
}

} // namespace duckdb
