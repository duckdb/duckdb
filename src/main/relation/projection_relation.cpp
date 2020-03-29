#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ProjectionRelation::ProjectionRelation(shared_ptr<Relation> child_p, vector<unique_ptr<ParsedExpression>> parsed_expressions, vector<string> aliases) :
	Relation(child_p->context, RelationType::PROJECTION), child(move(child_p)) {
	if (aliases.size() != 0 && parsed_expressions.size() != aliases.size()) {
		throw ParserException("Aliases list length must match expression list length!");
	}
	// copy the expressions into this projection
	for(idx_t i = 0; i < parsed_expressions.size(); i++) {
		this->expressions.push_back(parsed_expressions[i]->Copy());
	}
	// bind the expressions
	context.BindExpressions(child->Columns(), move(parsed_expressions), this->columns);
	if (aliases.size() > 0) {
		assert(aliases.size() == this->columns.size());
		// apply the aliases
		for(idx_t i = 0; i < aliases.size(); i++) {
			columns[i].name = aliases[i];
		}
	}
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