
#include "common/assert.hpp"

#include "catalog/column_catalog.hpp"

#include "parser/tableref/tableref_list.hpp"
#include "parser/transform.hpp"

#include "parser/expression/expression_list.hpp"

using namespace std;

namespace duckdb {

bool TransformGroupBy(List *group,
                      vector<unique_ptr<AbstractExpression>> &result) {
	if (!group) {
		return false;
	}

	for (auto node = group->head; node != nullptr; node = node->next) {
		Node *n = reinterpret_cast<Node *>(node->data.ptr_value);
		result.push_back(TransformExpression(n));
	}
	return true;
}

bool TransformOrderBy(List *order, OrderByDescription &result) {
	if (!order) {
		return false;
	}

	for (auto node = order->head; node != nullptr; node = node->next) {
		Node *temp = reinterpret_cast<Node *>(node->data.ptr_value);
		if (temp->type == T_SortBy) {
			OrderByNode ordernode;
			SortBy *sort = reinterpret_cast<SortBy *>(temp);
			Node *target = sort->node;
			if (sort->sortby_dir == SORTBY_ASC ||
			    sort->sortby_dir == SORTBY_DEFAULT)
				ordernode.type = OrderType::ASCENDING;
			else if (sort->sortby_dir == SORTBY_DESC)
				ordernode.type = OrderType::DESCENDING;
			else
				throw NotImplementedException("Unimplemented order by type");
			ordernode.expression = TransformExpression(target);
			if (ordernode.expression->type == ExpressionType::VALUE_CONSTANT) {
				auto constant = reinterpret_cast<ConstantExpression *>(
				    ordernode.expression.get());
				if (TypeIsIntegral(constant->value.type)) {
					ordernode.expression = make_unique<ColumnRefExpression>(
					    TypeId::INVALID, constant->value.GetNumericValue());
				} else {
					// order by non-integral constant, continue
					continue;
				}
			}
			result.orders.push_back(
			    OrderByNode(ordernode.type, move(ordernode.expression)));
		} else {
			throw NotImplementedException("ORDER BY list member type %d\n",
			                              temp->type);
		}
	}
	return true;
}

unique_ptr<SelectStatement> TransformSelect(Node *node) {
	SelectStmt *stmt = reinterpret_cast<SelectStmt *>(node);
	switch (stmt->op) {
	case SETOP_NONE: {
		auto result = make_unique<SelectStatement>();
		result->select_distinct = stmt->distinctClause != NULL ? true : false;
		if (!TransformExpressionList(stmt->targetList, result->select_list)) {
			return nullptr;
		}
		result->from_table = TransformFrom(stmt->fromClause);
		TransformGroupBy(stmt->groupClause, result->groupby.groups);
		result->groupby.having = TransformExpression(stmt->havingClause);
		TransformOrderBy(stmt->sortClause, result->orderby);
		result->where_clause = TransformExpression(stmt->whereClause);
		if (stmt->limitCount) {
			result->limit.limit =
			    reinterpret_cast<A_Const *>(stmt->limitCount)->val.val.ival;
			result->limit.offset =
			    !stmt->limitOffset
			        ? 0
			        : reinterpret_cast<A_Const *>(stmt->limitOffset)
			              ->val.val.ival;
		}
		return result;
	}
	case SETOP_UNION: {
		auto result = TransformSelect((Node *)stmt->larg);
		if (!result) {
			return nullptr;
		}
		auto right = TransformSelect((Node *)stmt->rarg);
		if (!right) {
			return nullptr;
		}
		result->union_select = move(right);
		return result;
	}
	case SETOP_INTERSECT:
	case SETOP_EXCEPT:
	default:
		throw NotImplementedException("A_Expr not implemented!");
	}
}

unique_ptr<CreateStatement> TransformCreate(Node *node) {
	CreateStmt *stmt = reinterpret_cast<CreateStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateStatement>();
	if (stmt->inhRelations) {
		throw NotImplementedException("inherited relations not implemented");
	}
	RangeVar *relation;
	assert(stmt->relation);

	if (stmt->relation->schemaname) {
		result->schema = stmt->relation->schemaname;
	}
	result->table = stmt->relation->relname;
	assert(stmt->tableElts);
	ListCell *c;

	for (c = stmt->tableElts->head; c != NULL; c = lnext(c)) {
		ColumnDef *cdef = (ColumnDef *)c->data.ptr_value;
		if (cdef->type != T_ColumnDef) {
			throw NotImplementedException("Can only handle basic columns");
		}
		char *name = (reinterpret_cast<value *>(
		                  cdef->typeName->names->tail->data.ptr_value)
		                  ->val.str);
		auto centry = ColumnCatalogEntry(
		    cdef->colname, TransformStringToTypeId(name), cdef->is_not_null);
		result->columns.push_back(centry);
	}
	return result;
}

unique_ptr<InsertStatement> TransformInsert(Node *node) {
	InsertStmt *stmt = reinterpret_cast<InsertStmt *>(node);
	assert(stmt);

	auto select_stmt = reinterpret_cast<SelectStmt *>(stmt->selectStmt);
	if (select_stmt->fromClause) {
		throw NotImplementedException("Can only handle basic inserts");
	}

	auto result = make_unique<InsertStatement>();
	assert(select_stmt->valuesLists);

	if (stmt->cols) {
		for (ListCell *c = stmt->cols->head; c != NULL; c = lnext(c)) {
			ResTarget *target = (ResTarget *)(c->data.ptr_value);
			result->columns.push_back(std::string(target->name));
		}
	}

	if (!TransformValueList(select_stmt->valuesLists, result->values)) {
		throw Exception("Failed to transform value list");
	}

	auto ref = TransformRangeVar(stmt->relation);
	auto &table = *reinterpret_cast<BaseTableRef *>(ref.get());
	result->table = table.table_name;
	result->schema = table.schema_name;
	return result;
}

unique_ptr<CopyStatement> TransformCopy(Node *node) {
	const string kDelimiterTok = "delimiter";
	const string kFormatTok = "format";
	const string kQuoteTok = "quote";
	const string kEscapeTok = "escape";

	CopyStmt *stmt = reinterpret_cast<CopyStmt *>(node);
	assert(stmt);
	auto result = make_unique<CopyStatement>();
	result->file_path = stmt->filename;
	result->is_from = stmt->is_from;

	if (stmt->relation) {
		auto ref = TransformRangeVar(stmt->relation);
		if (result->is_from) {
			// copy file into table
			auto &table = *reinterpret_cast<BaseTableRef *>(ref.get());
			result->table = table.table_name;
			result->schema = table.schema_name;
		} else {
			// copy table into file, generate SELECT * FROM table;
			auto statement = make_unique<SelectStatement>();
			statement->from_table = move(ref);
			statement->select_list.push_back(
			    make_unique<ColumnRefExpression>());
			result->select_stmt = move(statement);
		}
	} else {
		result->select_stmt = TransformSelect(stmt->query);
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

} // namespace duckdb
