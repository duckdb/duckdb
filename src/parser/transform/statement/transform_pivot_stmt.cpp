#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/multi_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

void Transformer::AddPivotEntry(string enum_name, unique_ptr<TableRef> source, string column_name) {
	if (parent) {
		parent->AddPivotEntry(std::move(enum_name), std::move(source), std::move(column_name));
		return;
	}
	auto result = make_unique<CreatePivotEntry>();
	result->enum_name = std::move(enum_name);
	result->source = std::move(source);
	result->column_name = std::move(column_name);

	pivot_entries.push_back(std::move(result));
}

bool Transformer::HasPivotEntries() {
	return !pivot_entries.empty();
}

unique_ptr<SQLStatement> GenerateCreateEnumStmt(string column_name, unique_ptr<TableRef> source, string enum_name) {
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateTypeInfo>();

	info->temporary = true;
	info->internal = false;
	info->catalog = INVALID_CATALOG;
	info->schema = INVALID_SCHEMA;
	info->name = enum_name;

	// generate the query that will result in the enum creation
	auto select_node = make_unique<SelectNode>();
	auto columnref = make_unique<ColumnRefExpression>(column_name);
	auto cast = make_unique<CastExpression>(LogicalType::VARCHAR, columnref->Copy());
	select_node->select_list.push_back(std::move(cast));
	select_node->from_table = std::move(source);

	// order by the column
	auto modifier = make_unique<OrderModifier>();
	modifier->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, std::move(columnref));
	select_node->modifiers.push_back(std::move(modifier));

	auto select = make_unique<SelectStatement>();
	select->node = std::move(select_node);
	info->query = std::move(select);
	info->type = LogicalType::INVALID;

	result->info = std::move(info);
	return std::move(result);
}

// unique_ptr<SQLStatement> GenerateDropEnumStmt(string enum_name) {
//	auto result = make_unique<DropStatement>();
//	result->info->if_exists = true;
//	result->info->schema = INVALID_SCHEMA;
//	result->info->catalog = INVALID_CATALOG;
//	result->info->name = std::move(enum_name);
//	result->info->type = CatalogType::TYPE_ENTRY;
//	return std::move(result);
//}

unique_ptr<SQLStatement> Transformer::CreatePivotStatement(unique_ptr<SQLStatement> statement) {
	auto result = make_unique<MultiStatement>();
	for (auto &pivot : pivot_entries) {
		result->statements.push_back(
		    GenerateCreateEnumStmt(std::move(pivot->column_name), std::move(pivot->source), pivot->enum_name));
	}
	result->statements.push_back(std::move(statement));
	// FIXME: drop the types again!?
	//	for(auto &pivot : pivot_entries) {
	//		result->statements.push_back(GenerateDropEnumStmt(std::move(pivot->enum_name)));
	//	}
	return std::move(result);
}

unique_ptr<QueryNode> Transformer::TransformPivotStatement(duckdb_libpgquery::PGPivotStmt *pivot) {
	auto source = TransformTableRefNode(pivot->source);
	auto aggregate = TransformExpression(pivot->aggr);
	auto columns = TransformStringList(pivot->columns);

	auto uuid = UUID::ToString(UUID::GenerateRandomUUID());
	// generate CREATE TYPE statements for each of the columns
	vector<string> enum_names;
	for (idx_t c = 0; c < columns.size(); c++) {
		auto enum_name = "__pivot_enum_" + uuid + std::to_string(c);
		AddPivotEntry(enum_name, source->Copy(), columns[c]);
		enum_names.push_back(std::move(enum_name));
	}

	// generate the actual query, including the pivot
	auto select_node = make_unique<SelectNode>();
	select_node->select_list.push_back(make_unique<StarExpression>());

	auto pivot_ref = make_unique<PivotRef>();
	pivot_ref->source = std::move(source);
	pivot_ref->aggregate = std::move(aggregate);
	if (pivot->rows) {
		pivot_ref->rows = TransformStringList(pivot->rows);
	}
	for (idx_t c = 0; c < columns.size(); c++) {
		PivotColumn col;
		col.name = columns[c];
		col.pivot_enum = enum_names[c];
		pivot_ref->pivots.push_back(col);
	}
	select_node->from_table = std::move(pivot_ref);
	return std::move(select_node);
}

} // namespace duckdb
