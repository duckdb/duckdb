#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

vector<unique_ptr<ParsedExpression>> Transformer::TransformIndexParameters(duckdb_libpgquery::PGList &list,
                                                                           const string &relation_name) {
	vector<unique_ptr<ParsedExpression>> expressions;
	for (auto cell = list.head; cell != nullptr; cell = cell->next) {
		auto index_element = PGPointerCast<duckdb_libpgquery::PGIndexElem>(cell->data.ptr_value);
		if (index_element->collation) {
			throw NotImplementedException("Index with collation not supported yet!");
		}
		if (index_element->opclass) {
			throw NotImplementedException("Index with opclass not supported yet!");
		}

		if (index_element->name) {
			// create a column reference expression
			expressions.push_back(make_uniq<ColumnRefExpression>(index_element->name, relation_name));
		} else {
			// parse the index expression
			D_ASSERT(index_element->expr);
			expressions.push_back(TransformExpression(index_element->expr));
		}
	}
	return expressions;
}

unique_ptr<CreateStatement> Transformer::TransformCreateIndex(duckdb_libpgquery::PGIndexStmt &stmt) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateIndexInfo>();
	if (stmt.unique) {
		info->constraint_type = IndexConstraintType::UNIQUE;
	} else {
		info->constraint_type = IndexConstraintType::NONE;
	}

	info->on_conflict = TransformOnConflict(stmt.onconflict);

	info->expressions = TransformIndexParameters(*stmt.indexParams, stmt.relation->relname);

	auto index_type_name = StringUtil::Upper(string(stmt.accessMethod));

	if (index_type_name == "ART") {
		info->index_type = IndexType::ART;
	} else {
		info->index_type = IndexType::EXTENSION;
	}

	info->index_type_name = index_type_name;

	if (stmt.relation->schemaname) {
		info->schema = stmt.relation->schemaname;
	}
	if (stmt.relation->catalogname) {
		info->catalog = stmt.relation->catalogname;
	}
	info->table = stmt.relation->relname;
	if (stmt.idxname) {
		info->index_name = stmt.idxname;
	} else {
		throw NotImplementedException("Index without a name not supported yet!");
	}

	// Parse the options list
	if (stmt.options) {
		duckdb_libpgquery::PGListCell *cell;
		for_each_cell(cell, stmt.options->head) {
			auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
			Value val;
			if (def_elem->arg) {
				val = TransformValue(*PGPointerCast<duckdb_libpgquery::PGValue>(def_elem->arg))->value;
			} else {
				val = Value::BOOLEAN(true);
			}
			info->options[StringUtil::Lower(def_elem->defname)] = std::move(val);
		}
	}

	for (auto &expr : info->expressions) {
		info->parsed_expressions.emplace_back(expr->Copy());
	}
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
