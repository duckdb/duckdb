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
	const string kNullTok = "null";
	const string kForceQuoteTok = "force_quote";
	const string kForceNotNullTok = "force_not_null";
	const string kEncodingTok = "encoding";

	CopyStmt *stmt = reinterpret_cast<CopyStmt *>(node);
	assert(stmt);
	auto result = make_unique<CopyStatement>();
	auto &info = *result->info;

	// get file_path and is_from
	info.file_path = stmt->filename;
	info.is_from = stmt->is_from;

	// get select_list
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
			// get table and schema
			info.table = table.table_name;
			info.schema = table.schema_name;
		} else {
			// copy table into file, generate SELECT * FROM table;
			auto statement = make_unique<SelectNode>();
			statement->from_table = move(ref);
			if (stmt->attlist) {
				for (index_t i = 0; i < info.select_list.size(); i++) {
					statement->select_list.push_back(make_unique<ColumnRefExpression>(info.select_list[i]));
				}
			} else {
				statement->select_list.push_back(make_unique<StarExpression>());
			}
			result->select_statement = move(statement);
		}
	} else {
		result->select_statement = TransformSelectNode((SelectStmt *)stmt->query);
	}

	// handle options
	if (stmt->options) {
		ListCell *cell = nullptr;
		for_each_cell(cell, stmt->options->head) {
			auto *def_elem = reinterpret_cast<DefElem *>(cell->data.ptr_value);

			if (StringUtil::StartsWith(def_elem->defname, "delim") ||
			    StringUtil::StartsWith(def_elem->defname, "sep")) {

				// delimiter
				auto *delimiter_val = (postgres::Value *)(def_elem->arg);
				if (!delimiter_val || delimiter_val->type != T_String) {
					throw ParserException("Unsupported parameter type for DELIMITER: expected e.g. DELIMITER ','");
				}
				info.delimiter = delimiter_val->val.str;

			} else if (def_elem->defname == kFormatTok) {

				// format
				auto *format_val = (postgres::Value *)(def_elem->arg);
				if (!format_val || format_val->type != T_String) {
					throw ParserException("Unsupported parameter type for FORMAT: expected e.g. FORMAT 'csv'");
				}
				if (StringUtil::Upper(format_val->val.str) != "CSV") {
					throw Exception("Copy is only supported for .CSV-files, FORMAT 'csv'");
				}
				info.format = StringToExternalFileFormat(format_val->val.str);

			} else if (def_elem->defname == kQuoteTok) {

				// quote
				auto *quote_val = (postgres::Value *)(def_elem->arg);
				if (!quote_val || quote_val->type != T_String) {
					throw ParserException("Unsupported parameter type for QUOTE: expected e.g. QUOTE '\"'");
				}
				info.quote = quote_val->val.str;

				if (info.quote.length() == 0) {
					throw Exception("QUOTE must not be empty");
				}

			} else if (def_elem->defname == kEscapeTok) {

				// escape
				auto *escape_val = (postgres::Value *)(def_elem->arg);
				if (!escape_val || escape_val->type != T_String) {
					throw ParserException("Unsupported parameter type for ESCAPE: expected e.g. ESCAPE '\"'");
				}

				info.escape = escape_val->val.str;

				if (info.escape.length() == 0) {
					throw Exception("ESCAPE must not be empty");
				}

			} else if (def_elem->defname == kHeaderTok) {

				// header
				auto *header_val = (postgres::Value *)(def_elem->arg);
				if (!header_val) {
					info.header = true;
					continue;
				}
				switch (header_val->type) {
				case T_Integer:
					info.header = header_val->val.ival == 1 ? true : false;
					break;
				case T_String: {
					auto val = duckdb::Value(string(header_val->val.str));
					info.header = val.CastAs(TypeId::BOOLEAN).value_.boolean;
					break;
				}
				default:
					throw ParserException("Unsupported parameter type for HEADER: expected e.g. HEADER 1");
				}

			} else if (def_elem->defname == kNullTok) {

				// null
				auto *null_val = (postgres::Value *)(def_elem->arg);
				if (!null_val || null_val->type != T_String) {
					throw ParserException("Unsupported parameter type for NULL: expected e.g. NULL 'null'");
				}
				info.null_str = null_val->val.str;

			} else if (def_elem->defname == kForceQuoteTok) {

				// force quote
				// only for COPY ... TO ...
				if(info.is_from) {
					throw Exception("The FORCE_QUOTE option is only for COPY ... TO ...");
				}

				auto *quote_val = def_elem->arg;
				if (!quote_val || (quote_val->type != T_A_Star && quote_val->type != T_List)) {
					throw ParserException("Unsupported parameter type for FORCE_QUOTE: expected e.g. FORCE_QUOTE *");
				}

				// * option (all columns)
				if (quote_val->type == T_A_Star) {
					info.quote_all = true;
				}

				// list of columns
				if (quote_val->type == T_List) {
					auto column_list = (postgres::List*)(quote_val);
					for (ListCell *c = column_list->head; c != NULL; c = lnext(c)) {
						ResTarget *target = (ResTarget *)(c->data.ptr_value);
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
				if (!force_not_null_val->type || force_not_null_val->type != T_List) {
					throw ParserException("Unsupported parameter type for FORCE_NOT_NULL: expected e.g. FORCE_NOT_NULL *");
				}

				auto column_list = (postgres::List*)(force_not_null_val);
				for (ListCell *c = column_list->head; c != NULL; c = lnext(c)) {
					ResTarget *target = (ResTarget *)(c->data.ptr_value);
					info.force_not_null_list.push_back(string(target->name));
				}

			} else if (def_elem->defname == kEncodingTok) {

				// encoding
				auto *encoding_val = (postgres::Value *)(def_elem->arg);
				if (!encoding_val || encoding_val->type != T_String) {
					throw ParserException("Unsupported parameter type for ENCODING: expected e.g. ENCODING 'UTF-8'");
				}
				if (StringUtil::Upper(encoding_val->val.str) != "UTF8" && StringUtil::Upper(encoding_val->val.str) != "UTF-8") {
					throw Exception("Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'");
				}

			}

			else {
				throw ParserException("Unsupported COPY option: %s", def_elem->defname);
			}
		}
	}

	// the default character of the ESCAPE option is the same as the QUOTE character
	if (info.escape == "") {
		info.escape = info.quote;
	}

	// dependencies must be checked afterwards
	// the null string must not contain the delimiter
	if (info.null_str.find(info.delimiter) != string::npos) {
		throw Exception("COPY delimiter must not appear in the NULL specification");
	}
	// delimiter and quote must be different
	if (info.delimiter == info.quote) {
		throw Exception("COPY delimiter and quote must be different");
	}

	return result;
}
