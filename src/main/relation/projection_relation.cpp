#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/relation_binder.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/parser/expression/table_star_expression.hpp"

namespace duckdb {

ProjectionRelation::ProjectionRelation(shared_ptr<Relation> child_p, vector<unique_ptr<ParsedExpression>> parsed_expressions, vector<string> aliases) :
	Relation(child_p->context, RelationType::PROJECTION), expressions(move(parsed_expressions)), aliases(move(aliases)), child(move(child_p)) {
	// bind the expressions
	context.TryBindRelation(*this, this->columns);
}

BoundStatement ProjectionRelation::Bind(Binder &binder) {
	BoundStatement result;

	// first bind the children with an empty binder
	Binder child_binder(context, &binder);
	auto root = child->Bind(child_binder);

	// copy the select list and handle any STAR expressions
	vector<unique_ptr<ParsedExpression>> new_select_list;
	for (auto &select_element : expressions) {
		if (select_element->GetExpressionType() == ExpressionType::STAR) {
			// * statement, expand to all columns from the FROM clause
			child_binder.bind_context.GenerateAllColumnExpressions(new_select_list);
		} else if (select_element->GetExpressionType() == ExpressionType::TABLE_STAR) {
			auto table_star = (TableStarExpression *)select_element.get();
			child_binder.bind_context.GenerateAllColumnExpressions(new_select_list, table_star->relation_name);
		} else {
			// regular statement, add it to the list
			new_select_list.push_back(select_element->Copy());
		}
	}
	if (aliases.size() != 0 && new_select_list.size() != aliases.size()) {
		throw ParserException("Aliases list length must match expression list length!");
	}
	// now bind the expressions
	RelationBinder expr_binder(child_binder, context, "PROJECTION");
	vector<unique_ptr<Expression>> bound_expressions;
	for(idx_t i = 0; i < new_select_list.size(); i++) {
		auto &expr = new_select_list[i];
		SQLType result_type;
		string name = aliases.size() == 0 ? expr->GetName() : aliases[i];

		auto bound_expr = expr_binder.Bind(expr, &result_type);

		result.types.push_back(result_type);
		result.names.push_back(name);
		bound_expressions.push_back(move(bound_expr));
	}

	// create the logical projection node
	auto proj_index = binder.GenerateTableIndex();
	auto proj = make_unique<LogicalProjection>(proj_index, move(bound_expressions));
	proj->AddChild(move(root.plan));

	result.plan = move(proj);

	// add the nodes of the projection to the current bind context so they can be bound
	binder.bind_context.AddGenericBinding(proj_index, "projection", columns);
	return result;
}

const vector<ColumnDefinition> &ProjectionRelation::Columns() {
	return columns;
}

string ProjectionRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Projection [";
	for(idx_t i = 0; i < expressions.size(); i++) {
		if (i != 0) {
			str += ", ";
		}
		str += expressions[i]->ToString();
	}
	str += "]\n";
	return str + child->ToString(depth + 1);;
}

}