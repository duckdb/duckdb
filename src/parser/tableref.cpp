#include "duckdb/parser/tableref.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

string TableRef::BaseToString(string result) const {
	vector<string> column_name_alias;
	return BaseToString(std::move(result), column_name_alias);
}

string TableRef::AliasToString(const vector<string> &column_name_alias) const {
	string result;
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
	return result;
}

string TableRef::SampleToString() const {
	string result;
	if (sample) {
		result += " TABLESAMPLE " + EnumUtil::ToString(sample->method);
		result += "(" + sample->sample_size.ToString() + " " + string(sample->is_percentage ? "PERCENT" : "ROWS") + ")";
		if (sample->seed.IsValid()) {
			result += "REPEATABLE (" + to_string(sample->seed.GetIndex()) + ")";
		}
	}
	return result;
}

string TableRef::BaseToString(string result, const vector<string> &column_name_alias) const {
	result += AliasToString(column_name_alias);
	result += SampleToString();
	return result;
}

bool TableRef::Equals(const TableRef &other) const {
	return type == other.type && alias == other.alias && SampleOptions::Equals(sample.get(), other.sample.get());
}

void TableRef::CopyProperties(TableRef &target) const {
	D_ASSERT(type == target.type);
	target.alias = alias;
	target.query_location = query_location;
	target.sample = sample ? sample->Copy() : nullptr;
	target.external_dependency = external_dependency;
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
