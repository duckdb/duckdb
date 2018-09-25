
#include "common/assert.hpp"

#include "catalog/column_definition.hpp"
#include "parser/parser.hpp"

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
			    sort->sortby_dir == SORTBY_DEFAULT) {
				ordernode.type = OrderType::ASCENDING;
			} else if (sort->sortby_dir == SORTBY_DESC) {
				ordernode.type = OrderType::DESCENDING;
			} else {
				throw NotImplementedException("Unimplemented order by type");
			}
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
		auto top = TransformSelect((Node *)stmt->larg);
		if (!top) {
			return nullptr;
		}
		auto bottom = TransformSelect((Node *)stmt->rarg);
		if (!bottom) {
			return nullptr;
		}
		SelectStatement *top_ptr = top.get();
		// top may already have a union_select
		// we need to find the rightmost union child in the top chain and add
		// bottom there
		while (top_ptr->union_select) {
			top_ptr = top_ptr->union_select.get();
		}
		assert(top_ptr);

		top_ptr->union_select = move(bottom);
		top_ptr->select_distinct = !stmt->all;
		return top;
	}
	case SETOP_EXCEPT:
	case SETOP_INTERSECT:
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
		auto centry = ColumnDefinition(
		    cdef->colname, TransformStringToTypeId(name), cdef->is_not_null);
		result->columns.push_back(centry);
	}
	return result;
}

unique_ptr<DropStatement> TransformDrop(Node *node) {
	DropStmt *stmt = reinterpret_cast<DropStmt *>(node);
	assert(stmt);
	auto result = make_unique<DropStatement>();
	if (stmt->objects->length != 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}

	switch (stmt->removeType) {
	case OBJECT_TABLE: {
		auto table_list =
		    reinterpret_cast<List *>(stmt->objects->head->data.ptr_value);
		if (table_list->length == 2) {
			result->schema =
			    reinterpret_cast<value *>(table_list->head->data.ptr_value)
			        ->val.str;
			result->table = reinterpret_cast<value *>(
			                    table_list->head->next->data.ptr_value)
			                    ->val.str;
		} else {
			result->table =
			    reinterpret_cast<value *>(table_list->head->data.ptr_value)
			        ->val.str;
		}
	} break;
	default:
		throw NotImplementedException("Cannot drop this type yet");
	}
	return result;
}

unique_ptr<InsertStatement> TransformInsert(Node *node) {
	InsertStmt *stmt = reinterpret_cast<InsertStmt *>(node);
	assert(stmt);

	auto result = make_unique<InsertStatement>();

	// first check if there are any columns specified
	if (stmt->cols) {
		for (ListCell *c = stmt->cols->head; c != NULL; c = lnext(c)) {
			ResTarget *target = (ResTarget *)(c->data.ptr_value);
			result->columns.push_back(std::string(target->name));
		}
	}

	auto select_stmt = reinterpret_cast<SelectStmt *>(stmt->selectStmt);
	if (select_stmt->fromClause) {
		// insert from select statement
		result->select_statement = TransformSelect(stmt->selectStmt);
	} else {
		// simple set of values
		assert(select_stmt->valuesLists);

		// transform the insert list
		auto list = select_stmt->valuesLists;
		for (auto value_list = list->head; value_list != NULL;
		     value_list = value_list->next) {
			List *target = (List *)(value_list->data.ptr_value);

			vector<unique_ptr<AbstractExpression>> insert_values;
			if (!TransformExpressionList(target, insert_values)) {
				throw Exception("Could not parse expression list!");
			}
			if (result->values.size() > 0) {
				if (result->values[0].size() != insert_values.size()) {
					throw Exception(
					    "Insert VALUES lists must all be the same length");
				}
			}
			result->values.push_back(move(insert_values));
		}
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
			auto statement = make_unique<SelectStatement>();
			statement->from_table = move(ref);
			if (stmt->attlist) {
				for (size_t i = 0; i < result->select_list.size(); i++)
					statement->select_list.push_back(
					    make_unique<ColumnRefExpression>(
					        result->select_list[i]));
			} else {
				statement->select_list.push_back(
				    make_unique<ColumnRefExpression>());
			}
			result->select_statement = move(statement);
		}
	} else {
		result->select_statement = TransformSelect(stmt->query);
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

unique_ptr<TransactionStatement> TransformTransaction(Node *node) {
	TransactionStmt *stmt = reinterpret_cast<TransactionStmt *>(node);
	assert(stmt);
	switch (stmt->kind) {
	case TRANS_STMT_BEGIN:
		return make_unique<TransactionStatement>(
		    TransactionType::BEGIN_TRANSACTION);
	case TRANS_STMT_COMMIT:
		return make_unique<TransactionStatement>(TransactionType::COMMIT);
	case TRANS_STMT_ROLLBACK:
		return make_unique<TransactionStatement>(TransactionType::ROLLBACK);
	default:
		throw NotImplementedException("Transaction type %d not implemented yet",
		                              stmt->kind);
	}
}

unique_ptr<DeleteStatement> TransformDelete(Node *node) {
	DeleteStmt *stmt = reinterpret_cast<DeleteStmt *>(node);
	assert(stmt);
	auto result = make_unique<DeleteStatement>();

	result->condition = TransformExpression(stmt->whereClause);
	result->table = TransformRangeVar(stmt->relation);
	if (result->table->ref_type != TableReferenceType::BASE_TABLE) {
		throw Exception("Can only delete from base tables!");
	}
	return result;
}

unique_ptr<UpdateStatement> TransformUpdate(Node *node) {
	UpdateStmt *stmt = reinterpret_cast<UpdateStmt *>(node);
	assert(stmt);

	auto result = make_unique<UpdateStatement>();

	result->table = TransformRangeVar(stmt->relation);
	result->condition = TransformExpression(stmt->whereClause);

	List *root = stmt->targetList;
	for (auto cell = root->head; cell != NULL; cell = cell->next) {
		auto target = (ResTarget *)(cell->data.ptr_value);
		result->columns.push_back(target->name);
		result->expressions.push_back(TransformExpression(target->val));
	}
	return result;
}

} // namespace duckdb
