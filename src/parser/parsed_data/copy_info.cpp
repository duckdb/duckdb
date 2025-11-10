#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

CopyInfo::CopyInfo()
    : ParseInfo(TYPE), catalog(INVALID_CATALOG), schema(DEFAULT_SCHEMA), is_from(false), is_format_auto_detected(true) {
}

unique_ptr<CopyInfo> CopyInfo::Copy() const {
	auto result = make_uniq<CopyInfo>();
	result->catalog = catalog;
	result->schema = schema;
	result->table = table;
	result->select_list = select_list;
	result->file_path_expression = file_path_expression ? file_path_expression->Copy() : nullptr;
	result->file_path = file_path;
	result->is_from = is_from;
	result->format = format;
	result->is_format_auto_detected = is_format_auto_detected;
	for (auto &entry : parsed_options) {
		result->parsed_options[entry.first] = entry.second ? entry.second->Copy() : nullptr;
	}
	result->options = options;
	if (select_statement) {
		result->select_statement = select_statement->Copy();
	}
	return result;
}

string CopyInfo::CopyOptionsToString() const {
	// We only output the format if there is a format, and it was manually set.
	const bool output_format = !format.empty() && !is_format_auto_detected;
	if (!output_format && options.empty() && parsed_options.empty()) {
		return string();
	}
	string result;

	result += " (";
	vector<string> stringified;
	if (!format.empty() && !is_format_auto_detected) {
		stringified.push_back(StringUtil::Format(" FORMAT %s", format));
	}
	for (auto &opt : parsed_options) {
		auto &name = opt.first;
		auto &expr = opt.second;
		string option_string = name;
		if (expr) {
			option_string += " " + expr->ToString();
		}
		stringified.push_back(option_string);
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
	} else {
		if (select_statement) {
			// COPY (select-node) TO ...
			result += "(" + select_statement->ToString() + ")";
		} else {
			result += TablePartToString();
		}
		result += " TO ";
	}
	if (file_path_expression && file_path.empty()) {
		result += "(";
		result += file_path_expression->ToString();
		result += ")";
	} else {
		result += StringUtil::Format(" %s", SQLString(file_path));
	}
	result += CopyOptionsToString();
	result += ";";
	return result;
}

} // namespace duckdb
