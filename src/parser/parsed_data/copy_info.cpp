#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

unique_ptr<CopyInfo> CopyInfo::Copy() const {
	auto result = make_uniq<CopyInfo>();
	result->catalog = catalog;
	result->schema = schema;
	result->table = table;
	result->select_list = select_list;
	result->file_path = file_path;
	result->is_from = is_from;
	result->format = format;
	result->options = options;
	if (select_statement) {
		result->select_statement = select_statement->Copy();
	}
	return result;
}

string CopyInfo::CopyOptionsToString(const string &format, const case_insensitive_map_t<vector<Value>> &options) {
	if (format.empty() && options.empty()) {
		return string();
	}
	string result;

	result += " (";
	vector<string> stringified;
	if (!format.empty()) {
		stringified.push_back(StringUtil::Format(" FORMAT %s", format));
	}
	for (auto &opt : options) {
		auto &name = opt.first;
		auto &values = opt.second;

		auto option = name + " ";
		if (values.empty()) {
			// Options like HEADER don't need an explicit value
			// just providing the name already sets it to true
			stringified.push_back(option);
		} else if (values.size() == 1) {
			stringified.push_back(option + values[0].ToSQLString());
		} else {
			vector<string> sub_values;
			for (auto &val : values) {
				sub_values.push_back(val.ToSQLString());
			}
			stringified.push_back(option + "( " + StringUtil::Join(sub_values, ", ") + " )");
		}
	}
	result += StringUtil::Join(stringified, ", ");
	result += " )";
	return result;
}

string CopyInfo::TablePartToString() const {
	string result;

	D_ASSERT(!table.empty());
	result += QualifierToString(catalog, schema, table);

	// (c1, c2, ..)
	if (!select_list.empty()) {
		vector<string> options;
		for (auto &option : select_list) {
			options.push_back(KeywordHelper::WriteOptionallyQuoted(option));
		}
		result += " (";
		result += StringUtil::Join(options, ", ");
		result += " )";
	}
	return result;
}

string CopyInfo::ToString() const {
	string result = "";
	result += "COPY ";
	if (is_from) {
		D_ASSERT(!select_statement);
		result += TablePartToString();
		result += " FROM";
		result += StringUtil::Format(" %s", SQLString(file_path));
		result += CopyOptionsToString(format, options);
	} else {
		if (select_statement) {
			// COPY (select-node) TO ...
			result += "(" + select_statement->ToString() + ")";
		} else {
			result += TablePartToString();
		}
		result += " TO ";
		result += StringUtil::Format("%s", SQLString(file_path));
		result += CopyOptionsToString(format, options);
	}
	result += ";";
	return result;
}

} // namespace duckdb
