#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/expression/star_expression.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformJoin(duckdb_libpgquery::PGJoinExpr &root) {
	auto result = make_uniq<JoinRef>(JoinRefType::REGULAR);
	switch (root.jointype) {
	case duckdb_libpgquery::PG_JOIN_INNER: {
		result->type = JoinType::INNER;
		break;
	}
	case duckdb_libpgquery::PG_JOIN_LEFT: {
		result->type = JoinType::LEFT;
		break;
	}
	case duckdb_libpgquery::PG_JOIN_FULL: {
		result->type = JoinType::OUTER;
		break;
	}
	case duckdb_libpgquery::PG_JOIN_RIGHT: {
		result->type = JoinType::RIGHT;
		break;
	}
	case duckdb_libpgquery::PG_JOIN_SEMI: {
		result->type = JoinType::SEMI;
		break;
	}
	case duckdb_libpgquery::PG_JOIN_ANTI: {
		result->type = JoinType::ANTI;
		break;
	}
	case duckdb_libpgquery::PG_JOIN_POSITION: {
		result->ref_type = JoinRefType::POSITIONAL;
		break;
	}
	default:
		throw NotImplementedException("Join type %d not supported\n", root.jointype);
	}

	// Check the type of the left and right argument before transforming.
	result->left = TransformTableRefNode(*root.larg);
	result->right = TransformTableRefNode(*root.rarg);

	switch (root.joinreftype) {
	case duckdb_libpgquery::PG_JOIN_NATURAL:
		result->ref_type = JoinRefType::NATURAL;
		break;
	case duckdb_libpgquery::PG_JOIN_ASOF:
		result->ref_type = JoinRefType::ASOF;
		break;
	default:
		break;
	}

	SetQueryLocation(*result, root.location);
	if (root.usingClause && root.usingClause->length > 0) {
		// usingClause is a list of strings.
		for (auto node = root.usingClause->head; node != nullptr; node = node->next) {
			auto target = PGPointerCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
			D_ASSERT(target->type == duckdb_libpgquery::T_PGString);
			auto value = PGCast<duckdb_libpgquery::PGValue>(*target.get());
			result->using_columns.push_back(string(value.val.str));
		}
		return std::move(result);
	}

	// Check if this is a cross product.
	if (!root.quals && result->using_columns.empty() && result->ref_type == JoinRefType::REGULAR) {
		result->ref_type = JoinRefType::CROSS;
	}
	result->condition = TransformExpression(root.quals);

	if (root.alias) {
		// This is a join with an alias, so we wrap it in a subquery.
		auto select_node = make_uniq<SelectNode>();
		select_node->select_list.push_back(make_uniq<StarExpression>());
		select_node->from_table = std::move(result);

		auto select = make_uniq<SelectStatement>();
		select->node = std::move(select_node);

		auto subquery = make_uniq<SubqueryRef>(std::move(select));
		SetQueryLocation(*subquery, root.location);

		// Apply the alias to the subquery.
		subquery->alias = TransformAlias(root.alias, subquery->column_name_alias);
		return std::move(subquery);
	}
	return std::move(result);
}

} // namespace duckdb
