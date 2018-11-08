
#include "common/assert.hpp"

#include "parser/column_definition.hpp"

#include "parser/parser.hpp"
#include "parser/tableref/list.hpp"
#include "parser/transform.hpp"

#include "parser/constraint.hpp"

#include "parser/constraints/list.hpp"
#include "parser/expression/list.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

unique_ptr<Constraint> TransformConstraint(postgres::ListCell *cell) {
	auto constraint =
	    reinterpret_cast<postgres::Constraint *>(cell->data.ptr_value);
	switch (constraint->contype) {
	case postgres::CONSTR_UNIQUE:
	case postgres::CONSTR_PRIMARY: {
		auto type = constraint->contype == postgres::CONSTR_PRIMARY
		                ? ConstraintType::PRIMARY_KEY
		                : ConstraintType::UNIQUE;
		vector<string> columns;
		for (auto kc = constraint->keys->head; kc; kc = kc->next) {
			columns.push_back(
			    string(reinterpret_cast<postgres::value *>(kc->data.ptr_value)
			               ->val.str));
		}
		return make_unique<ParsedConstraint>(type, columns);
	}
	default:
		throw NotImplementedException("Constraint type not handled yet!");
	}
}

unique_ptr<Constraint> TransformConstraint(postgres::ListCell *cell,
                                           ColumnDefinition column,
                                           size_t index) {
	auto constraint =
	    reinterpret_cast<postgres::Constraint *>(cell->data.ptr_value);
	assert(constraint);
	switch (constraint->contype) {
	case postgres::CONSTR_NOTNULL:
		return make_unique<NotNullConstraint>(index);
	case postgres::CONSTR_CHECK: {
		auto expression = TransformExpression(constraint->raw_expr);
		if (expression->HasSubquery()) {
			throw ParserException("subqueries prohibited in CHECK constraints");
		}
		if (expression->IsAggregate()) {
			throw ParserException("aggregates prohibited in CHECK constraints");
		}
		return make_unique<CheckConstraint>(
		    TransformExpression(constraint->raw_expr));
	}
	case postgres::CONSTR_PRIMARY:
		return make_unique<ParsedConstraint>(ConstraintType::PRIMARY_KEY,
		                                     index);
	case postgres::CONSTR_UNIQUE:
		return make_unique<ParsedConstraint>(ConstraintType::UNIQUE, index);
	case postgres::CONSTR_FOREIGN:
	case postgres::CONSTR_DEFAULT:
	default:
		throw NotImplementedException("Constraint not implemented!");
	}
}

using namespace postgres;

bool TransformGroupBy(List *group, vector<unique_ptr<Expression>> &result) {
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
		stmt->larg->sortClause = stmt->sortClause;
		stmt->larg->limitOffset = stmt->limitOffset;
		stmt->larg->limitCount = stmt->limitCount;
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

static unique_ptr<DropTableStatement> TransformDropTable(DropStmt *stmt) {
	auto result = make_unique<DropTableStatement>();
	auto &info = *result->info.get();

	info.cascade = stmt->behavior == DropBehavior::DROP_CASCADE;
	auto table_list =
	    reinterpret_cast<List *>(stmt->objects->head->data.ptr_value);
	if (table_list->length == 2) {
		info.schema =
		    reinterpret_cast<value *>(table_list->head->data.ptr_value)
		        ->val.str;
		info.table =
		    reinterpret_cast<value *>(table_list->head->next->data.ptr_value)
		        ->val.str;
	} else {
		info.table = reinterpret_cast<value *>(table_list->head->data.ptr_value)
		                 ->val.str;
	}
	info.if_exists = stmt->missing_ok;
	return result;
}

static unique_ptr<DropSchemaStatement> TransformDropSchema(DropStmt *stmt) {
	auto result = make_unique<DropSchemaStatement>();
	auto &info = *result->info.get();

	info.cascade = stmt->behavior == DropBehavior::DROP_CASCADE;
	info.if_exists = stmt->missing_ok;
	for (auto cell = stmt->objects->head; cell != nullptr; cell = cell->next) {
		auto table_list = reinterpret_cast<List *>(cell->data.ptr_value);
		auto schema_value =
		    reinterpret_cast<value *>(table_list->head->data.ptr_value);
		info.schema = schema_value->val.str;
		break;
	}
	return result;
}

unique_ptr<SQLStatement> TransformDrop(Node *node) {
	DropStmt *stmt = reinterpret_cast<DropStmt *>(node);
	assert(stmt);
	if (stmt->objects->length != 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}

	switch (stmt->removeType) {
	case OBJECT_TABLE:
		return TransformDropTable(stmt);
	case OBJECT_SCHEMA:
		return TransformDropSchema(stmt);
	default:
		throw NotImplementedException("Cannot drop this type yet");
	}
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

			vector<unique_ptr<Expression>> insert_values;
			if (!TransformExpressionList(target, insert_values)) {
				throw ParserException("Could not parse expression list!");
			}
			if (result->values.size() > 0) {
				if (result->values[0].size() != insert_values.size()) {
					throw ParserException(
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

unique_ptr<CreateTableStatement> TransformCreateTable(Node *node) {
	auto stmt = reinterpret_cast<CreateStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateTableStatement>();
	auto &info = *result->info.get();

	if (stmt->inhRelations) {
		throw NotImplementedException("inherited relations not implemented");
	}
	assert(stmt->relation);

	if (stmt->relation->schemaname) {
		info.schema = stmt->relation->schemaname;
	}
	info.table = stmt->relation->relname;
	info.if_not_exists = stmt->if_not_exists;
	assert(stmt->tableElts);

	for (auto c = stmt->tableElts->head; c != NULL; c = lnext(c)) {
		auto node = reinterpret_cast<Node *>(c->data.ptr_value);
		switch (node->type) {
		case T_ColumnDef: {
			auto cdef = (ColumnDef *)c->data.ptr_value;
			char *name = (reinterpret_cast<value *>(
			                  cdef->typeName->names->tail->data.ptr_value)
			                  ->val.str);
			auto centry =
			    ColumnDefinition(cdef->colname, TransformStringToTypeId(name));

			if (cdef->constraints) {
				for (auto constr = cdef->constraints->head; constr != nullptr;
				     constr = constr->next) {
					info.constraints.push_back(TransformConstraint(
					    constr, centry, info.columns.size()));
				}
			}
			info.columns.push_back(centry);
			break;
		}
		case T_Constraint: {
			info.constraints.push_back(TransformConstraint(c));
			break;
		}
		default:
			throw NotImplementedException("ColumnDef type not handled yet");
		}
	}
	return result;
}

unique_ptr<CreateSchemaStatement> TransformCreateSchema(Node *node) {
	auto stmt = reinterpret_cast<CreateSchemaStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateSchemaStatement>();
	auto &info = *result->info.get();

	assert(stmt->schemaname);
	info.schema = stmt->schemaname;
	info.if_not_exists = stmt->if_not_exists;

	if (stmt->authrole) {
		auto authrole = reinterpret_cast<Node *>(stmt->authrole);
		switch (authrole->type) {
		case T_RoleSpec:
		default:
			throw NotImplementedException("Authrole not implemented yet!");
		}
	}

	if (stmt->schemaElts) {
		// schema elements
		for (auto cell = stmt->schemaElts->head; cell != nullptr;
		     cell = cell->next) {
			auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
			switch (node->type) {
			case T_CreateStmt:
			case T_ViewStmt:
			default:
				throw NotImplementedException(
				    "Schema element not supported yet!");
			}
		}
	}

	return result;
}


unique_ptr<AlterTableStatement> TransformAlter(Node *node) {
	auto stmt = reinterpret_cast<AlterTableStmt *>(node);
	assert(stmt);
	assert(stmt->relation);

	auto result = make_unique<AlterTableStatement>();
	auto &info = *result->info.get();
	auto new_alter_cmd = make_unique<AlterTableCmd>();
	result->table = TransformRangeVar(stmt->relation);

	info.table = stmt->relation->relname;

	// first we check the type of ALTER
	for (auto c = stmt->cmds->head; c != NULL; c = c->next) {
		auto command = reinterpret_cast<AlterTableCmd *>(lfirst(c));
		//TODO: Include more options for command->subtype
		switch (command->subtype) {
			case AT_AddColumn: {
                auto cdef = (ColumnDef *)command->def;
                char *name = (reinterpret_cast<value *>(
                        cdef->typeName->names->tail->data.ptr_value)
                        ->val.str);
                auto centry =
                        ColumnDefinition(cdef->colname, TransformStringToTypeId(name));
                info.new_columns.push_back(centry);
                break;
            }
			case AT_DropColumn:
			default:
				throw NotImplementedException(
				    "ALTER TABLE option not supported yet!");
		}
	}

	return result;
}

unique_ptr<CreateIndexStatement> TransformCreateIndex(Node *node){
	IndexStmt *stmt = reinterpret_cast<IndexStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateIndexStatement>();
	auto &info = *result->info.get();

    info.unique = stmt->unique;

	for (auto cell = stmt->indexParams->head; cell != nullptr;
		 cell = cell->next) {
		char *index_attr =
				reinterpret_cast<IndexElem *>(cell->data.ptr_value)->name;
        info.indexed_columns.push_back(std::string(index_attr));
	}

    info.index_type = StringToIndexType(std::string(stmt->accessMethod));
    info.table = stmt->relation->relname;
	if (stmt->relation->schemaname) {
        info.schema = stmt->relation->schemaname;
	}
    if (stmt->idxname){
        info.index_name = stmt->idxname;
    }
    else{
        throw NotImplementedException(
                "Index wout a name not supported yet!");
    }
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
				statement->select_list.push_back(make_unique<StarExpression>());
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
	case TRANS_STMT_START:
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
	if (result->table->type != TableReferenceType::BASE_TABLE) {
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
