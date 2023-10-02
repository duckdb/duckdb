#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/transformer.hpp"

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
	default: {
		throw NotImplementedException("Join type %d not supported\n", root.jointype);
	}
	}

	// Check the type of left arg and right arg before transform
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
	result->query_location = root.location;

	if (root.usingClause && root.usingClause->length > 0) {
		// usingClause is a list of strings
		for (auto node = root.usingClause->head; node != nullptr; node = node->next) {
			auto target = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
			D_ASSERT(target->type == duckdb_libpgquery::T_PGString);
			auto column_name = string(reinterpret_cast<duckdb_libpgquery::PGValue *>(target)->val.str);
			result->using_columns.push_back(column_name);
		}
		return std::move(result);
	}

	if (!root.quals && result->using_columns.empty() && result->ref_type == JoinRefType::REGULAR) { // CROSS PRODUCT
		result->ref_type = JoinRefType::CROSS;
	}
	result->condition = TransformExpression(root.quals);
	return std::move(result);
}

} // namespace duckdb
