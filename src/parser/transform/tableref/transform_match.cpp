#include "duckdb/parser/tableref/matchref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

unique_ptr<PathElement> Transformer::TransformPathElement(duckdb_libpgquery::PGPathElement *element) {
	//! Vertex or edge pattern
	auto result = make_uniq<PathElement>(PGQPathReferenceType::PATH_ELEMENT);
	switch (element->match_type) {
	case duckdb_libpgquery::PG_MATCH_VERTEX:
		result->match_type = PGQMatchType::MATCH_VERTEX;
		break;
	case duckdb_libpgquery::PG_MATCH_EDGE_ANY:
		result->match_type = PGQMatchType::MATCH_EDGE_ANY;
		break;
	case duckdb_libpgquery::PG_MATCH_EDGE_LEFT:
		result->match_type = PGQMatchType::MATCH_EDGE_LEFT;
		break;
	case duckdb_libpgquery::PG_MATCH_EDGE_RIGHT:
		result->match_type = PGQMatchType::MATCH_EDGE_RIGHT;
		break;
	case duckdb_libpgquery::PG_MATCH_EDGE_LEFT_RIGHT:
		result->match_type = PGQMatchType::MATCH_EDGE_LEFT_RIGHT;
		break;
	default:
		throw InternalException("Unrecognized match type detected");
	}
	if (!element->label_expr) {
		throw ConstraintException("All patterns must bind to a label");
	}
	auto label_expression = reinterpret_cast<duckdb_libpgquery::PGLabelTest *>(element->label_expr);
	std::string label_name = StringUtil::Lower(label_expression->name);
	result->label = label_name;
	if (!element->element_var) {
		throw ConstraintException("All patterns must bind to a variable, %s is missing a variable", result->label);
	}
	result->variable_binding = element->element_var;
	return result;
}

unique_ptr<SubPath> Transformer::TransformSubPathElement(duckdb_libpgquery::PGSubPath *root,
                                                         unique_ptr<PathPattern> &path_pattern) {
	auto result = make_uniq<SubPath>(PGQPathReferenceType::SUBPATH);

	result->where_clause = TransformExpression(root->where_clause);
	if (root->lower > root->upper) {
		throw ConstraintException("Lower bound greater than upper bound");
	}
	result->lower = root->lower;
	result->upper = root->upper;
	result->single_bind = root->single_bind;
	if (root->path_var) {
		result->path_variable = root->path_var;
	}
	switch (root->mode) {
	case duckdb_libpgquery::PG_PATHMODE_NONE:
		result->path_mode = PGQPathMode::NONE;
		break;
	case duckdb_libpgquery::PG_PATHMODE_WALK:
		result->path_mode = PGQPathMode::WALK;
		break;
	case duckdb_libpgquery::PG_PATHMODE_SIMPLE:
		result->path_mode = PGQPathMode::SIMPLE;
		break;
	case duckdb_libpgquery::PG_PATHMODE_TRAIL:
		result->path_mode = PGQPathMode::TRAIL;
		break;
	case duckdb_libpgquery::PG_PATHMODE_ACYCLIC:
		result->path_mode = PGQPathMode::ACYCLIC;
		break;
	}
	if (result->path_mode > PGQPathMode::WALK) {
		throw NotImplementedException("Path modes other than WALK have not been implemented yet.");
	}
	if (result->upper == 1 << 30 && path_pattern->all && result->path_mode <= PGQPathMode::WALK) {
		throw ConstraintException("ALL unbounded with path mode WALK is not possible as this "
		                          "could lead to infinite results. Consider specifying an upper bound or"
		                          " path mode other than WALK");
	}

	//! Path sequence
	for (auto node = root->path->head; node != nullptr; node = lnext(node)) {
		// Parse path element
		auto path_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
		if (path_node->type == duckdb_libpgquery::T_PGPathElement) {
			auto element = reinterpret_cast<duckdb_libpgquery::PGPathElement *>(path_node);
			auto path_element = TransformPathElement(element);
			result->path_list.push_back(std::move(path_element));
		} else if (path_node->type == duckdb_libpgquery::T_PGSubPath) {
			auto subpath = reinterpret_cast<duckdb_libpgquery::PGSubPath *>(path_node);
			auto subpath_element = TransformSubPathElement(subpath, path_pattern);
			result->path_list.push_back(std::move(subpath_element));
		}
	}
	return result;
}

unique_ptr<PathPattern> Transformer::TransformPath(duckdb_libpgquery::PGPathPattern *root) {
	auto result = make_uniq<PathPattern>();
	result->all = root->all;
	result->shortest = root->shortest;
	result->group = root->group;
	result->topk = root->topk;
	if (result->all && result->shortest) {
		throw NotImplementedException("ALL SHORTEST has not been implemented yet.");
	}
	if (result->topk > 1) {
		throw NotImplementedException("TopK has not been implemented yet.");
	}
	if (result->group) {
		throw NotImplementedException("GROUP has not been implemented yet.");
	}
	//! Path sequence
	for (auto node = root->path->head; node != nullptr; node = lnext(node)) {
		// Parse path element
		auto path_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
		if (path_node->type == duckdb_libpgquery::T_PGPathElement) {
			auto element = reinterpret_cast<duckdb_libpgquery::PGPathElement *>(path_node);
			auto path_element = TransformPathElement(element);
			result->path_elements.push_back(std::move(path_element));
		} else if (path_node->type == duckdb_libpgquery::T_PGSubPath) {
			auto subpath = reinterpret_cast<duckdb_libpgquery::PGSubPath *>(path_node);
			auto subpath_element = TransformSubPathElement(subpath, result);
			result->path_elements.push_back(std::move(subpath_element));
		} else {
			throw NotImplementedException("Path node type " + NodetypeToString(path_node->type) + " not recognized");
		}
	}

	return result;
}

unique_ptr<TableRef> Transformer::TransformMatch(duckdb_libpgquery::PGMatchClause &root) {
	auto match_info = make_uniq<MatchExpression>();
	match_info->pg_name = root.pg_name; // Name of the property graph to bind to
	string alias_name;
	if (root.graph_table) {
		alias_name = TransformQualifiedName(*root.graph_table).name;
	}
	match_info->alias = alias_name.empty() ? "unnamed_graphtable" : alias_name;

	if (root.where_clause) {
		match_info->where_clause = TransformExpression(root.where_clause);
	}

	for (auto node = root.paths->head; node != nullptr; node = lnext(node)) {
		auto path = reinterpret_cast<duckdb_libpgquery::PGPathPattern *>(node->data.ptr_value);
		auto transformed_path = TransformPath(path);
		match_info->path_patterns.push_back(std::move(transformed_path));
	}

	TransformExpressionList(*root.columns, match_info->column_list);

	auto children = vector<unique_ptr<ParsedExpression>>();
	children.push_back(std::move(match_info));
	auto result = make_uniq<TableFunctionRef>();
	result->function = make_uniq<FunctionExpression>("duckpgq_match", std::move(children));

	return std::move(result);
}

} // namespace duckdb
