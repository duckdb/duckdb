#include "planner/binder.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/constraints/list.hpp"
#include "parser/expression/list.hpp"
#include "parser/query_node/list.hpp"
#include "parser/statement/list.hpp"
#include "parser/tableref/list.hpp"
#include "planner/expression_binder/where_binder.hpp"

using namespace duckdb;
using namespace std;

Binder::Binder(ClientContext &context, Binder *parent) :
	context(context),
	parent(!parent ? nullptr : (parent->parent ? parent->parent : parent)),
	bound_tables(0) {
}
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
	case StatementType::EXECUTE:
		// do nothing
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
		bind_context.AddBaseTable(GenerateTableIndex(), expr.alias.empty() ? expr.table_name : expr.alias,
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
	WhereBinder binder(*this, context);
	binder.BindAndResolveType(&expr.condition);
	return nullptr;
}

unique_ptr<TableRef> Binder::Visit(SubqueryRef &expr) {
	expr.binder = make_unique<Binder>(context, this);
	expr.binder->Bind(*expr.subquery);
	bind_context.AddSubquery(GenerateTableIndex(), expr.alias, expr);

	MoveCorrelatedExpressions(*expr.binder);
	return nullptr;
}

unique_ptr<TableRef> Binder::Visit(TableFunction &expr) {
	auto function_definition = (FunctionExpression *)expr.function.get();
	auto function = context.db.catalog.GetTableFunction(context.ActiveTransaction(), function_definition);
	bind_context.AddTableFunction(GenerateTableIndex(),
	                              expr.alias.empty() ? function_definition->function_name : expr.alias, function);
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

size_t Binder::GenerateTableIndex() {
	if (parent) {
		return parent->GenerateTableIndex();
	}
	return bound_tables++;
}

void Binder::PushExpressionBinder(ExpressionBinder *binder) {
	GetActiveBinders().push_back(binder);
}

void Binder::PopExpressionBinder() {
	assert(HasActiveBinder());
	GetActiveBinders().pop_back();
}

void Binder::SetActiveBinder(ExpressionBinder *binder) {
	assert(HasActiveBinder());
	GetActiveBinders().back() = binder;
}

ExpressionBinder *Binder::GetActiveBinder() {
	return GetActiveBinders().back();
}

bool Binder::HasActiveBinder() {
	return GetActiveBinders().size() > 0;
}

vector<ExpressionBinder *> &Binder::GetActiveBinders() {
	if (parent) {
		return parent->GetActiveBinders();
	}
	return active_binders;
}

void Binder::MoveCorrelatedExpressions(Binder &other) {
	MergeCorrelatedColumns(other.correlated_columns);
	other.correlated_columns.clear();
}

void Binder::MergeCorrelatedColumns(vector<CorrelatedColumnInfo> &other) {
	for (size_t i = 0; i < other.size(); i++) {
		AddCorrelatedColumn(other[i]);
	}
}

void Binder::AddCorrelatedColumn(CorrelatedColumnInfo info) {
	// we only add correlated columns to the list if they are not already there
	if (std::find(correlated_columns.begin(), correlated_columns.end(), info) == correlated_columns.end()) {
		correlated_columns.push_back(info);
	}
}
