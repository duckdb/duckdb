#include <string>
#include <utility>

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "nodes/nodes.hpp"
#include "nodes/parsenodes.hpp"
#include "pg_definitions.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateTableAs(duckdb_libpgquery::PGCreateTableAsStmt &stmt) {
	if (stmt.relkind == duckdb_libpgquery::PG_OBJECT_MATVIEW) {
		throw NotImplementedException("Materialized view not implemented");
	}
	if (stmt.is_select_into) {
		throw NotImplementedException("Unimplemented features for CREATE TABLE as");
	}
	if (stmt.query->type != duckdb_libpgquery::T_PGSelectStmt) {
		throw ParserException("CREATE TABLE AS requires a SELECT clause");
	}

	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTableInfo>();
	auto qname = TransformQualifiedName(*stmt.into->rel);
	if (qname.name.empty()) {
		throw ParserException("Empty table name not supported");
	}
	auto query = TransformSelectStmt(*stmt.query, false);

	vector<unique_ptr<ParsedExpression>> partition_keys;
	if (stmt.into->partition_list) {
		TransformExpressionList(*stmt.into->partition_list, partition_keys);
	}
	info->partition_keys = std::move(partition_keys);

	vector<unique_ptr<ParsedExpression>> order_keys;
	if (stmt.into->sort_list) {
		TransformExpressionList(*stmt.into->sort_list, order_keys);
	}
	info->sort_keys = std::move(order_keys);

	if (stmt.into->options) {
		TransformTableOptions(info->options, *stmt.into->options);
	}

	// push a LIMIT 0 if 'WITH NO DATA' is specified
	if (stmt.into->skipData) {
		auto limit_modifier = make_uniq<LimitModifier>();
		limit_modifier->limit = make_uniq<ConstantExpression>(Value::BIGINT(0));
		auto limit_node = make_uniq<SelectNode>();
		limit_node->modifiers.push_back(std::move(limit_modifier));
		limit_node->from_table = make_uniq<SubqueryRef>(std::move(query));
		limit_node->select_list.push_back(make_uniq<StarExpression>());
		auto limit_stmt = make_uniq<SelectStatement>();
		limit_stmt->node = std::move(limit_node);
		query = std::move(limit_stmt);
	}

	if (stmt.into->colNames) {
		auto cols = TransformStringList(stmt.into->colNames);
		for (idx_t i = 0; i < cols.size(); i++) {
			// We really don't know the type of the columns during parsing, so we just use UNKNOWN
			info->columns.AddColumn(ColumnDefinition(cols[i], LogicalType::UNKNOWN));
		}
	}

	info->catalog = qname.catalog;
	info->schema = qname.schema;
	info->table = qname.name;
	info->on_conflict = TransformOnConflict(stmt.onconflict);
	info->temporary =
	    stmt.into->rel->relpersistence == duckdb_libpgquery::PGPostgresRelPersistence::PG_RELPERSISTENCE_TEMP;
	info->query = std::move(query);
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
