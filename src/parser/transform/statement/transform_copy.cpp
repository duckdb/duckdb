#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/star_expression.hpp"
#include "parser/statement/copy_statement.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/tableref/basetableref.hpp"
#include "parser/transformer.hpp"
#include "common/string_util.hpp"

#include <cstring>

using namespace duckdb;
using namespace postgres;
using namespace std;

static ExternalFileFormat StringToExternalFileFormat(const string &str) {
	auto upper = StringUtil::Upper(str);
	if (upper == "CSV") {
		return ExternalFileFormat::CSV;
	}
	throw ConversionException("No ExternalFileFormat for input '%s'", upper.c_str());
}

unique_ptr<CopyStatement> Transformer::TransformCopy(Node *node) {
	const string kDelimiterTok = "delimiter";
	const string kFormatTok = "format";
	const string kQuoteTok = "quote";
	const string kEscapeTok = "escape";
	const string kHeaderTok = "header";

	CopyStmt *stmt = reinterpret_cast<CopyStmt *>(node);
	assert(stmt);
	auto result = make_unique<CopyStatement>();
	auto &info = *result->info;
	info.file_path = stmt->filename;
	info.is_from = stmt->is_from;

	if (stmt->attlist) {
		for (auto n = stmt->attlist->head; n != nullptr; n = n->next) {
			auto target = reinterpret_cast<ResTarget *>(n->data.ptr_value);
			if (target->name) {
				info.select_list.push_back(string(target->name));
			}
		}
	}

	if (stmt->relation) {
		auto ref = TransformRangeVar(stmt->relation);
		if (info.is_from) {
			// copy file into table
			auto &table = *reinterpret_cast<BaseTableRef *>(ref.get());
			info.table = table.table_name;
			info.schema = table.schema_name;
		} else {
			// copy table into file, generate SELECT * FROM table;
			auto statement = make_unique<SelectNode>();
			statement->from_table = move(ref);
			if (stmt->attlist) {
				for (index_t i = 0; i < info.select_list.size(); i++)
					statement->select_list.push_back(make_unique<ColumnRefExpression>(info.select_list[i]));
			} else {
				statement->select_list.push_back(make_unique<StarExpression>());
			}
			result->select_statement = move(statement);
		}
	} else {
		result->select_statement = TransformSelectNode((SelectStmt *)stmt->query);
	}

	// Handle options
	if (stmt->options) {
		ListCell *cell = nullptr;
		for_each_cell(cell, stmt->options->head) {
			auto *def_elem = reinterpret_cast<DefElem *>(cell->data.ptr_value);

			if (def_elem->defname == kDelimiterTok) {
				// delimiter
				auto *delimiter_val = reinterpret_cast<postgres::Value *>(def_elem->arg);
				index_t delim_len = strlen(delimiter_val->val.str);
				info.delimiter = '\0';
				char *delim_cstr = delimiter_val->val.str;
				if (delim_len == 1) {
					info.delimiter = delim_cstr[0];
				}
				if (delim_len == 2 && delim_cstr[0] == '\\' && delim_cstr[1] == 't') {
					info.delimiter = '\t';
				}
				if (info.delimiter == '\0') {
					throw Exception("Could not interpret DELIMITER option");
				}
			} else if (def_elem->defname == kFormatTok) {
				// format
				auto *format_val = reinterpret_cast<postgres::Value *>(def_elem->arg);
				info.format = StringToExternalFileFormat(format_val->val.str);
			} else if (def_elem->defname == kQuoteTok) {
				// quote
				auto *quote_val = reinterpret_cast<postgres::Value *>(def_elem->arg);
				info.quote = *quote_val->val.str;
			} else if (def_elem->defname == kEscapeTok) {
				// escape
				auto *escape_val = reinterpret_cast<postgres::Value *>(def_elem->arg);
				info.escape = *escape_val->val.str;
			} else if (def_elem->defname == kHeaderTok) {
				auto *header_val = reinterpret_cast<postgres::Value *>(def_elem->arg);
				assert(header_val->type == T_Integer);
				info.header = header_val->val.ival == 1 ? true : false;
			} else {
				throw ParserException("Unsupported COPY option: %s", def_elem->defname);
			}
		}
	}

	return result;
}
