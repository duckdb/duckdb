#include "duckdb/parser/tableref.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

string TableRef::BaseToString(string result) const {
	vector<string> column_name_alias;
	return BaseToString(std::move(result), column_name_alias);
}

string TableRef::BaseToString(string result, const vector<string> &column_name_alias) const {
	if (!alias.empty()) {
		result += StringUtil::Format(" AS %s", SQLIdentifier(alias));
	}
	if (!column_name_alias.empty()) {
		D_ASSERT(!alias.empty());
		result += "(";
		for (idx_t i = 0; i < column_name_alias.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += KeywordHelper::WriteOptionallyQuoted(column_name_alias[i]);
		}
		result += ")";
	}
	if (sample) {
		result += " TABLESAMPLE " + EnumUtil::ToString(sample->method);
		result += "(" + sample->sample_size.ToString() + " " + string(sample->is_percentage ? "PERCENT" : "ROWS") + ")";
		if (sample->seed >= 0) {
			result += "REPEATABLE (" + to_string(sample->seed) + ")";
		}
	}

	return result;
}

bool TableRef::Equals(const TableRef &other) const {
	if (type != other.type) {
		return false;
	}
	if (alias != other.alias) {
		// FIXME: CIEquals?
		return false;
	}
	if (!SampleOptions::Equals(sample.get(), other.sample.get())) {
		return false;
	}
	if (column_name_alias.size() != other.column_name_alias.size()) {
		return false;
	}
	D_ASSERT(column_name_alias.size() == column_type_hint.size());
	for (idx_t i = 0; i < column_name_alias.size(); i++) {
		if (!StringUtil::CIEquals(column_name_alias[i], other.column_name_alias[i])) {
			return false;
		}
		if (column_type_hint[i] != other.column_type_hint[i]) {
			return false;
		}
	}
	return true;
}

void TableRef::CopyProperties(TableRef &target) const {
	D_ASSERT(type == target.type);
	target.alias = alias;
	target.query_location = query_location;
	target.sample = sample ? sample->Copy() : nullptr;
	target.external_dependency = external_dependency;
	target.column_name_alias = column_name_alias;
	target.column_type_hint = column_type_hint;
}

void TableRef::Print() {
	Printer::Print(ToString());
}

bool TableRef::Equals(const unique_ptr<TableRef> &left, const unique_ptr<TableRef> &right) {
	if (left.get() == right.get()) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return left->Equals(*right);
}

} // namespace duckdb
