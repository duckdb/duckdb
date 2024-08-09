#include "duckdb/parser/tableref.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

string TableRef::BaseToString(string result, const vector<string> &column_name_alias,
                              const vector<LogicalType> &column_type_hint) const {
	if (!alias.empty() || !column_name_alias.empty()) {
		result += " AS ";
	}
	if (!alias.empty()) {
		result += StringUtil::Format("%s", SQLIdentifier(alias));
	}
	if (!column_name_alias.empty()) {
		D_ASSERT(column_type_hint.empty() || column_name_alias.size() == column_type_hint.size());
		result += "(";
		for (idx_t i = 0; i < column_name_alias.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			string column_definition;
			column_definition += KeywordHelper::WriteOptionallyQuoted(column_name_alias[i]);
			if (!column_type_hint.empty()) {
				auto &type_hint = column_type_hint[i];
				column_definition += " ";
				column_definition += type_hint.ToString();
			}
			result += column_definition;
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

string TableRef::BaseToString(string result, const vector<string> &column_name_alias) const {
	vector<LogicalType> empty_types;

	return BaseToString(std::move(result), column_name_alias, empty_types);
}

string TableRef::BaseToString(string result) const {
	vector<string> empty_names;
	vector<LogicalType> empty_types;

	return BaseToString(std::move(result), empty_names, empty_types);
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
	return true;
}

void TableRef::CopyProperties(TableRef &target) const {
	D_ASSERT(type == target.type);
	target.alias = alias;
	target.query_location = query_location;
	target.sample = sample ? sample->Copy() : nullptr;
	target.external_dependency = external_dependency;
	target.column_name_alias = column_name_alias;
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
