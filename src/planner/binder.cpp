#include "planner/binder.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/constraints/list.hpp"
#include "parser/expression/list.hpp"
#include "parser/query_node/list.hpp"
#include "parser/statement/list.hpp"
#include "parser/tableref/list.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(SQLStatement &statement) {
	switch (statement.type) {
	case StatementType::SELECT:
		Bind((SelectStatement &)statement);
		break;
	case StatementType::INSERT:
		Bind((InsertStatement &)statement);
		break;
	case StatementType::COPY:
		Bind((CopyStatement &)statement);
		break;
	case StatementType::DELETE:
		Bind((DeleteStatement &)statement);
		break;
	case StatementType::UPDATE:
		Bind((UpdateStatement &)statement);
		break;
	case StatementType::ALTER:
		Bind((AlterTableStatement &)statement);
		break;
	case StatementType::CREATE_TABLE:
		Bind((CreateTableStatement &)statement);
		break;
	case StatementType::CREATE_VIEW:
		Bind((CreateViewStatement &)statement);
		break;
	default:
		assert(statement.type == StatementType::CREATE_INDEX);
		Bind((CreateIndexStatement &)statement);
		break;
	}
}

void Binder::Bind(QueryNode &node) {
	if (node.type == QueryNodeType::SELECT_NODE) {
		Bind((SelectNode &)node);
	} else {
		assert(node.type == QueryNodeType::SET_OPERATION_NODE);
		Bind((SetOperationNode &)node);
	}
}

void Binder::Visit(CheckConstraint &constraint) {
	SQLNodeVisitor::Visit(constraint);
	constraint.expression->ResolveType();
	if (constraint.expression->return_type == TypeId::INVALID) {
		throw BinderException("Could not resolve type of constraint!");
	}
	// the CHECK constraint should always return an INTEGER value
	if (constraint.expression->return_type != TypeId::INTEGER) {
		constraint.expression = make_unique<CastExpression>(TypeId::INTEGER, move(constraint.expression));
	}
}

unique_ptr<Expression> Binder::VisitReplace(ColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) {
	assert(!expr.column_name.empty());
	// individual column reference
	// resolve to either a base table or a subquery expression
	if (expr.table_name.empty()) {
		// no table name: find a binding that contains this
		expr.table_name = bind_context->GetMatchingBinding(expr.column_name);
	}
	return bind_context->BindColumn(expr);
}

unique_ptr<Expression> Binder::VisitReplace(FunctionExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// lookup the function in the catalog
	auto func = context.db.catalog.GetScalarFunction(context.ActiveTransaction(), expr.schema, expr.function_name);
	// if found, construct the BoundFunctionExpression
	auto func_expr = unique_ptr_cast<Expression, FunctionExpression>(move(*expr_ptr));
	return make_unique<BoundFunctionExpression>(move(func_expr), func);
}

unique_ptr<Expression> Binder::VisitReplace(SubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) {
	assert(bind_context);

	Binder binder(context, this);
	binder.bind_context->parent = bind_context.get();
	// the subquery may refer to CTEs from the parent query
	binder.CTE_bindings = CTE_bindings;

	binder.Bind(*expr.subquery);
	auto &select_list = expr.subquery->GetSelectList();
	if (select_list.size() < 1) {
		throw BinderException("Subquery has no projections");
	}
	if (select_list[0]->return_type == TypeId::INVALID) {
		throw BinderException("Subquery has no type");
	}
	if (expr.subquery_type == SubqueryType::IN && select_list.size() != 1) {
		throw BinderException("Subquery returns %zu columns - expected 1", select_list.size());
	}
	auto result = make_unique<BoundSubqueryExpression>();
	result->return_type = expr.subquery_type == SubqueryType::EXISTS ? TypeId::BOOLEAN : select_list[0]->return_type;
	result->context = move(binder.bind_context);
	result->is_correlated = result->context->GetMaxDepth() > 0;
	result->subquery = move(expr.subquery);
	result->subquery_type = expr.subquery_type;
	result->alias = expr.alias;
	return result;
}

// CTEs and views are also referred to using BaseTableRefs, hence need to distinguish here
unique_ptr<TableRef> Binder::Visit(BaseTableRef &expr) {
	auto cte = FindCTE(expr.table_name);
	if (cte) {
		auto subquery = make_unique<SubqueryRef>(move(cte));
		subquery->alias = expr.alias.empty() ? expr.table_name : expr.alias;
		AcceptChild(&subquery);
		return move(subquery);
	}

	auto table_or_view =
	    context.db.catalog.GetTableOrView(context.ActiveTransaction(), expr.schema_name, expr.table_name);
	switch (table_or_view->type) {
	case CatalogType::TABLE:
		bind_context->AddBaseTable(expr.alias.empty() ? expr.table_name : expr.alias,
		                           (TableCatalogEntry *)table_or_view);
		break;
	case CatalogType::VIEW: {
		auto view_catalog_entry = (ViewCatalogEntry *)table_or_view;
		auto subquery = make_unique<SubqueryRef>(view_catalog_entry->query->Copy());

		subquery->alias = expr.alias.empty() ? expr.table_name : expr.alias;

		// if we have subquery aliases we need to set them for the subquery. However, there may be non-aliased result
		// cols from the subquery. Those are returned as well, but are not renamed.
		auto &select_list = subquery->subquery->GetSelectList();
		if (view_catalog_entry->aliases.size() > 0) {
			subquery->column_name_alias.resize(select_list.size());
			for (size_t col_idx = 0; col_idx < select_list.size(); col_idx++) {
				if (col_idx < view_catalog_entry->aliases.size()) {
					subquery->column_name_alias[col_idx] = view_catalog_entry->aliases[col_idx];
				} else {
					subquery->column_name_alias[col_idx] = select_list[col_idx]->GetName();
				}
			}
		}
		AcceptChild(&subquery);

		return move(subquery);
		break;
	}
	default:
		throw NotImplementedException("Catalog entry type");
	}
	return nullptr;
}

unique_ptr<TableRef> Binder::Visit(CrossProductRef &expr) {
	AcceptChild(&expr.left);
	AcceptChild(&expr.right);
	return nullptr;
}

unique_ptr<TableRef> Binder::Visit(JoinRef &expr) {
	AcceptChild(&expr.left);
	AcceptChild(&expr.right);
	VisitExpression(&expr.condition);
	expr.condition->ResolveType();
	return nullptr;
}

unique_ptr<TableRef> Binder::Visit(SubqueryRef &expr) {
	Binder binder(context, this);
	binder.Bind(*expr.subquery);
	expr.context = move(binder.bind_context);

	bind_context->AddSubquery(expr.alias, expr);
	return nullptr;
}

unique_ptr<TableRef> Binder::Visit(TableFunction &expr) {
	auto function_definition = (FunctionExpression *)expr.function.get();
	auto function = context.db.catalog.GetTableFunction(context.ActiveTransaction(), function_definition);
	bind_context->AddTableFunction(expr.alias.empty() ? function_definition->function_name : expr.alias, function);
	return nullptr;
}

void Binder::AddCTE(const string &name, QueryNode *cte) {
	assert(cte);
	assert(!name.empty());
	auto entry = CTE_bindings.find(name);
	if (entry != CTE_bindings.end()) {
		throw BinderException("Duplicate CTE \"%s\" in query!", name.c_str());
	}
	CTE_bindings[name] = cte;
}

unique_ptr<QueryNode> Binder::FindCTE(const string &name) {
	auto entry = CTE_bindings.find(name);
	if (entry == CTE_bindings.end()) {
		if (parent) {
			return parent->FindCTE(name);
		}
		return nullptr;
	}
	return entry->second->Copy();
}
