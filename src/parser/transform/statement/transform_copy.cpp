#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
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

unique_ptr<CopyStatement> Transformer::TransformCopy(PGNode *node) {
	auto stmt = reinterpret_cast<PGCopyStmt *>(node);
	assert(stmt);
	auto result = make_unique<CopyStatement>();
	auto &info = *result->info;

	// get file_path and is_from
	info.file_path = stmt->filename;
	info.is_from = stmt->is_from;

	info.format = "parquet";

	// get select_list
	if (stmt->attlist) {
		for (auto n = stmt->attlist->head; n != nullptr; n = n->next) {
			auto target = reinterpret_cast<PGResTarget *>(n->data.ptr_value);
			if (target->name) {
				info.select_list.push_back(string(target->name));
			}
			// FIXME
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
		PGListCell *cell = nullptr;

		// iterate over each option
		for_each_cell(cell, stmt->options->head) {
			auto *def_elem = reinterpret_cast<PGDefElem *>(cell->data.ptr_value);
			if (!def_elem->arg) {
				info.options[def_elem->defname] = vector<Value>();
				continue;
			}
			switch (def_elem->arg->type) {
			case T_PGList: {
				auto column_list = (PGList *)(def_elem->arg);
				for (auto c = column_list->head; c != NULL; c = lnext(c)) {
					auto target = (PGResTarget *)(c->data.ptr_value);
					info.options[def_elem->defname].push_back(Value(target->name));
				}
				break;
			}
			case T_PGAStar:
				info.options[def_elem->defname].push_back(Value("*"));
				break;
			default:
				info.options[def_elem->defname].push_back(TransformValue(*((PGValue *)def_elem->arg))->value);
				break;
			}
		}
	}

	return result;
}
