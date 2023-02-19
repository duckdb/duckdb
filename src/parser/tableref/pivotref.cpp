#include "duckdb/parser/tableref/pivotref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string PivotColumn::ToString() const {
	string result;
	result += " FOR";
	if (names.size() == 1) {
		result += KeywordHelper::WriteOptionallyQuoted(names[0]);
	} else {
		result += "(";
		for (idx_t n = 0; n < names.size(); n++) {
			if (n > 0) {
				result += ", ";
			}
			result += KeywordHelper::WriteOptionallyQuoted(names[n]);
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
			if (entry.values.size() == 1) {
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
	if (values != other.values) {
		return false;
	}
	return true;
}

bool PivotColumn::Equals(const PivotColumn &other) const {
	if (other.names != names) {
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
		}
	} else {
		// unpivot
		result += " UNPIVOT (";
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
	if (aggregates.size() != other->aggregates.size()) {
		return false;
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		if (!BaseExpression::Equals(aggregates[i].get(), other->aggregates[i].get())) {
			return false;
		}
	}
	if (pivots.size() != other->pivots.size()) {
		return false;
	}
	for (idx_t i = 0; i < pivots.size(); i++) {
		if (!pivots[i].Equals(other->pivots[i])) {
			return false;
		}
	}
	if (unpivot_names != other->unpivot_names) {
		return false;
	}
	if (alias != other->alias) {
		return false;
	}
	if (groups != other->groups) {
		return false;
	}
	return true;
}

unique_ptr<TableRef> PivotRef::Copy() {
	auto copy = make_unique<PivotRef>();
	copy->source = source->Copy();
	for (auto &aggr : aggregates) {
		copy->aggregates.push_back(aggr->Copy());
	}
	copy->unpivot_names = unpivot_names;
	copy->pivots = pivots;
	copy->groups = groups;
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
