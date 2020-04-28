#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

#include <cstring>

using namespace duckdb;
using namespace std;

static ExternalFileFormat StringToExternalFileFormat(const string &str) {
	auto upper = StringUtil::Upper(str);
	return ExternalFileFormat::CSV;
}

void SetControlString(PGDefElem *def_elem, string option, string option_example, string &info_str) {
	auto *val = (PGValue *)(def_elem->arg);
	if (!val || val->type != T_PGString) {
		throw ParserException("Unsupported parameter type for " + option + ": expected e.g. " + option_example);
	}
	info_str = val->val.str;
}

void SubstringDetection(string &str_1, string &str_2, string name_str_1, string name_str_2) {
	if (str_1.find(str_2) != string::npos || str_2.find(str_1) != std::string::npos) {
		throw Exception("COPY " + name_str_1 + " must not appear in the " + name_str_2 +
		                " specification and vice versa");
	}
}

void HandleOptions(PGCopyStmt *stmt, CopyInfo &info) {
	// option names
	const string kDelimiterTok = "delimiter";
	const string kFormatTok = "format";
	const string kQuoteTok = "quote";
	const string kEscapeTok = "escape";
	const string kHeaderTok = "header";
	const string kNullTok = "null";
	const string kForceQuoteTok = "force_quote";
	const string kForceNotNullTok = "force_not_null";
	const string kEncodingTok = "encoding";

	PGListCell *cell = nullptr;

	// iterate over each option
	for_each_cell(cell, stmt->options->head) {
		auto *def_elem = reinterpret_cast<PGDefElem *>(cell->data.ptr_value);

		if (StringUtil::StartsWith(def_elem->defname, "delim") || StringUtil::StartsWith(def_elem->defname, "sep")) {
			// delimiter
			SetControlString(def_elem, "DELIMITER", "DELIMITER ','", info.delimiter);

		} else if (def_elem->defname == kFormatTok) {
			// format
			auto *format_val = (PGValue *)(def_elem->arg);
			if (!format_val || format_val->type != T_PGString) {
				throw ParserException("Unsupported parameter type for FORMAT: expected e.g. FORMAT 'csv', 'csv_auto'");
			}

			if (StringUtil::Upper(format_val->val.str) != "CSV" &&
			    StringUtil::Upper(format_val->val.str) != "CSV_AUTO") {
				throw Exception("Copy is only supported for .CSV-files, FORMAT 'csv'");
			}

			info.format = StringToExternalFileFormat("CSV");

			if (StringUtil::Upper(format_val->val.str) == "CSV_AUTO") {
				info.auto_detect = true;
			}

		} else if (def_elem->defname == kQuoteTok) {
			// quote
			SetControlString(def_elem, "QUOTE", "QUOTE '\"'", info.quote);
			if (info.quote.length() == 0) {
				throw Exception("QUOTE must not be empty");
			}

		} else if (def_elem->defname == kEscapeTok) {
			// escape
			SetControlString(def_elem, "ESCAPE", "ESCAPE '\"'", info.escape);
			if (info.escape.length() == 0) {
				throw Exception("ESCAPE must not be empty");
			}

		} else if (def_elem->defname == kHeaderTok) {
			// header
			auto *header_val = (PGValue *)(def_elem->arg);
			if (!header_val) {
				info.header = true;
				continue;
			}
			switch (header_val->type) {
			case T_PGInteger:
				info.header = header_val->val.ival == 1 ? true : false;
				break;
			case T_PGString: {
				auto val = duckdb::Value(string(header_val->val.str));
				info.header = val.CastAs(TypeId::BOOL).value_.boolean;
				break;
			}
			default:
				throw ParserException("Unsupported parameter type for HEADER: expected e.g. HEADER 1");
			}

		} else if (def_elem->defname == kNullTok) {
			// null
			SetControlString(def_elem, "NULL", "NULL 'null'", info.null_str);

		} else if (def_elem->defname == kForceQuoteTok) {
			// force quote
			// only for COPY ... TO ...
			if (info.is_from) {
				throw Exception("The FORCE_QUOTE option is only for COPY ... TO ...");
			}

			auto *force_quote_val = def_elem->arg;
			if (!force_quote_val || (force_quote_val->type != T_PGAStar && force_quote_val->type != T_PGList)) {
				throw ParserException("Unsupported parameter type for FORCE_QUOTE: expected e.g. FORCE_QUOTE *");
			}

			// * option (all columns)
			if (force_quote_val->type == T_PGAStar) {
				info.quote_all = true;
			}

			// list of columns
			if (force_quote_val->type == T_PGList) {
				auto column_list = (PGList *)(force_quote_val);
				for (auto c = column_list->head; c != NULL; c = lnext(c)) {
					auto target = (PGResTarget *)(c->data.ptr_value);
					info.force_quote_list.push_back(string(target->name));
				}
			}

		} else if (def_elem->defname == kForceNotNullTok) {
			// force not null
			// only for COPY ... FROM ...
			if (!info.is_from) {
				throw Exception("The FORCE_NOT_NULL option is only for COPY ... FROM ...");
			}

			auto *force_not_null_val = def_elem->arg;
			if (!force_not_null_val || force_not_null_val->type != T_PGList) {
				throw ParserException("Unsupported parameter type for FORCE_NOT_NULL: expected e.g. FORCE_NOT_NULL *");
			}

			auto column_list = (PGList *)(force_not_null_val);
			for (auto c = column_list->head; c != NULL; c = lnext(c)) {
				auto target = (PGResTarget *)(c->data.ptr_value);
				info.force_not_null_list.push_back(string(target->name));
			}

		} else if (def_elem->defname == kEncodingTok) {
			// encoding
			auto *encoding_val = (PGValue *)(def_elem->arg);
			if (!encoding_val || encoding_val->type != T_PGString) {
				throw ParserException("Unsupported parameter type for ENCODING: expected e.g. ENCODING 'UTF-8'");
			}
			if (StringUtil::Upper(encoding_val->val.str) != "UTF8" &&
			    StringUtil::Upper(encoding_val->val.str) != "UTF-8") {
				throw Exception("Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'");
			}

		} else {
			throw ParserException("Unsupported COPY option: %s", def_elem->defname);
		}
	}
}

unique_ptr<CopyStatement> Transformer::TransformCopy(PGNode *node) {
	auto stmt = reinterpret_cast<PGCopyStmt *>(node);
	assert(stmt);
	auto result = make_unique<CopyStatement>();
	auto &info = *result->info;

	// get file_path and is_from
	info.file_path = stmt->filename;
	info.is_from = stmt->is_from;

	// get select_list
	if (stmt->attlist) {
		for (auto n = stmt->attlist->head; n != nullptr; n = n->next) {
			auto target = reinterpret_cast<PGResTarget *>(n->data.ptr_value);
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
				for (idx_t i = 0; i < info.select_list.size(); i++)
					statement->select_list.push_back(make_unique<ColumnRefExpression>(info.select_list[i]));
			} else {
				statement->select_list.push_back(make_unique<StarExpression>());
			}
			result->select_statement = move(statement);
		}
	} else {
		result->select_statement = TransformSelectNode((PGSelectStmt *)stmt->query);
	}

	// handle options, when no option were given, try auto detect
	if (stmt->options) {
		HandleOptions(stmt, info);
	}

	// the default character of the ESCAPE option is the same as the QUOTE character
	if (info.escape == "") {
		info.escape = info.quote;
	}
	// escape and delimiter must not be substrings of each other
	SubstringDetection(info.delimiter, info.escape, "DELIMITER", "ESCAPE");
	// delimiter and quote must not be substrings of each other
	SubstringDetection(info.quote, info.delimiter, "DELIMITER", "QUOTE");
	// escape and quote must not be substrings of each other (but can be the same)
	if (info.quote != info.escape) {
		SubstringDetection(info.quote, info.escape, "QUOTE", "ESCAPE");
	}
	// null string and delimiter must not be substrings of each other
	if (info.null_str != "") {
		SubstringDetection(info.delimiter, info.null_str, "DELIMITER", "NULL");
	}

	return result;
}
