#include "duckdb/parser/statement/copy_statement.hpp"

namespace duckdb {

CopyStatement::CopyStatement() : SQLStatement(StatementType::COPY_STATEMENT), info(make_uniq<CopyInfo>()) {
}

CopyStatement::CopyStatement(const CopyStatement &other) : SQLStatement(other), info(other.info->Copy()) {
	if (other.select_statement) {
		select_statement = other.select_statement->Copy();
	}
}

string CopyStatement::CopyOptionsToString(const string &format,
                                          const case_insensitive_map_t<vector<Value>> &options) const {
	if (format.empty() && options.empty()) {
		return string();
	}
	string result;

	result += " (";
	if (!format.empty()) {
		result += " FORMAT ";
		result += format;
	}
	for (auto it = options.begin(); it != options.end(); it++) {
		if (!format.empty() || it != options.begin()) {
			result += ", ";
		}
		auto &name = it->first;
		auto &values = it->second;

		result += name + " ";
		if (values.empty()) {
			// Options like HEADER don't need an explicit value
			// just providing the name already sets it to true
		} else if (values.size() == 1) {
			result += values[0].ToSQLString();
		} else {
			result += "( ";
			for (idx_t i = 0; i < values.size(); i++) {
				if (i) {
					result += ", ";
				}
				result += values[i].ToSQLString();
			}
			result += " )";
		}
	}
	result += " )";
	return result;
}

// COPY table-name (c1, c2, ..)
string TablePart(const CopyInfo &info) {
	string result;

	if (!info.catalog.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(info.catalog) + ".";
	}
	if (!info.schema.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(info.schema) + ".";
	}
	D_ASSERT(!info.table.empty());
	result += KeywordHelper::WriteOptionallyQuoted(info.table);

	// (c1, c2, ..)
	if (!info.select_list.empty()) {
		result += " (";
		for (idx_t i = 0; i < info.select_list.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += KeywordHelper::WriteOptionallyQuoted(info.select_list[i]);
		}
		result += " )";
	}
	return result;
}

string CopyStatement::ToString() const {
	string result;

	result += "COPY ";
	if (info->is_from) {
		D_ASSERT(!select_statement);
		result += TablePart(*info);
		result += " FROM";
		result += StringUtil::Format(" %s", SQLString(info->file_path));
		result += CopyOptionsToString(info->format, info->options);
	} else {
		if (select_statement) {
			// COPY (select-node) TO ...
			result += "(" + select_statement->ToString() + ")";
		} else {
			result += TablePart(*info);
		}
		result += " TO ";
		result += StringUtil::Format("%s", SQLString(info->file_path));
		result += CopyOptionsToString(info->format, info->options);
	}
	return result;
}

unique_ptr<SQLStatement> CopyStatement::Copy() const {
	return unique_ptr<CopyStatement>(new CopyStatement(*this));
}

} // namespace duckdb
