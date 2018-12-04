
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/star_expression.hpp"
#include "parser/statement/copy_statement.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/tableref/basetableref.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<CopyStatement> Transformer::TransformCopy(Node *node) {
	const string kDelimiterTok = "delimiter";
	const string kFormatTok = "format";
	const string kQuoteTok = "quote";
	const string kEscapeTok = "escape";

	CopyStmt *stmt = reinterpret_cast<CopyStmt *>(node);
	assert(stmt);
	auto result = make_unique<CopyStatement>();
	result->file_path = stmt->filename;
	result->is_from = stmt->is_from;

	if (stmt->attlist) {
		for (auto n = stmt->attlist->head; n != nullptr; n = n->next) {
			auto target = reinterpret_cast<ResTarget *>(n->data.ptr_value);
			if (target->name) {
				result->select_list.push_back(string(target->name));
			}
		}
	}

	if (stmt->relation) {
		auto ref = TransformRangeVar(stmt->relation);
		if (result->is_from) {
			// copy file into table
			auto &table = *reinterpret_cast<BaseTableRef *>(ref.get());
			result->table = table.table_name;
			result->schema = table.schema_name;
		} else {
			// copy table into file, generate SELECT * FROM table;
			auto statement = make_unique<SelectNode>();
			statement->from_table = move(ref);
			if (stmt->attlist) {
				for (size_t i = 0; i < result->select_list.size(); i++)
					statement->select_list.push_back(
					    make_unique<ColumnRefExpression>(
					        result->select_list[i]));
			} else {
				statement->select_list.push_back(make_unique<StarExpression>());
			}
			result->select_statement = move(statement);
		}
	} else {
		result->select_statement = TransformSelectNode((SelectStmt*)stmt->query);
	}

	// Handle options
	if (stmt->options) {
		ListCell *cell = nullptr;
		for_each_cell(cell, stmt->options->head) {
			auto *def_elem = reinterpret_cast<DefElem *>(cell->data.ptr_value);

			// Check delimiter
			if (def_elem->defname == kDelimiterTok) {
				auto *delimiter_val = reinterpret_cast<value *>(def_elem->arg);
				result->delimiter = *delimiter_val->val.str;
			}

			// Check format
			if (def_elem->defname == kFormatTok) {
				auto *format_val = reinterpret_cast<value *>(def_elem->arg);
				result->format =
				    StringToExternalFileFormat(format_val->val.str);
			}

			// Check quote
			if (def_elem->defname == kQuoteTok) {
				auto *quote_val = reinterpret_cast<value *>(def_elem->arg);
				result->quote = *quote_val->val.str;
			}

			// Check escape
			if (def_elem->defname == kEscapeTok) {
				auto *escape_val = reinterpret_cast<value *>(def_elem->arg);
				result->escape = *escape_val->val.str;
			}
		}
	}

	return result;
}
