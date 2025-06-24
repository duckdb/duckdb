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
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

void Transformer::AddPivotEntry(string enum_name, unique_ptr<SelectNode> base, unique_ptr<ParsedExpression> column,
                                unique_ptr<QueryNode> subquery, bool has_parameters) {
	if (parent) {
		parent->AddPivotEntry(std::move(enum_name), std::move(base), std::move(column), std::move(subquery),
		                      has_parameters);
		return;
	}
	auto result = make_uniq<CreatePivotEntry>();
	result->enum_name = std::move(enum_name);
	result->base = std::move(base);
	result->column = std::move(column);
	result->subquery = std::move(subquery);
	result->has_parameters = has_parameters;

	pivot_entries.push_back(std::move(result));
}

bool Transformer::HasPivotEntries() {
	return !GetPivotEntries().empty();
}

idx_t Transformer::PivotEntryCount() {
	return GetPivotEntries().size();
}

vector<unique_ptr<Transformer::CreatePivotEntry>> &Transformer::GetPivotEntries() {
	if (parent) {
		return parent->GetPivotEntries();
	}
	return pivot_entries;
}

void Transformer::PivotEntryCheck(const string &type) {
	auto &entries = GetPivotEntries();
	if (!entries.empty()) {
		throw ParserException(
		    "PIVOT statements with pivot elements extracted from the data cannot be used in %ss.\nIn order to use "
		    "PIVOT in a %s the PIVOT values must be manually specified, e.g.:\nPIVOT ... ON %s IN (val1, val2, ...)",
		    type, type, entries[0]->column->ToString());
	}
}
unique_ptr<SQLStatement> Transformer::GenerateCreateEnumStmt(unique_ptr<CreatePivotEntry> entry) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTypeInfo>();

	info->temporary = true;
	info->internal = false;
	info->catalog = INVALID_CATALOG;
	info->schema = INVALID_SCHEMA;
	info->name = std::move(entry->enum_name);
	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;

	// generate the query that will result in the enum creation
	unique_ptr<QueryNode> subselect;
	if (!entry->subquery) {
		auto select_node = std::move(entry->base);
		auto columnref = entry->column->Copy();
		auto cast = make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(columnref));
		select_node->select_list.push_back(std::move(cast));

		auto is_not_null =
		    make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(entry->column));
		select_node->where_clause = std::move(is_not_null);

		// order by the column
		select_node->modifiers.push_back(make_uniq<DistinctModifier>());
		auto modifier = make_uniq<OrderModifier>();
		modifier->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT,
		                              make_uniq<ConstantExpression>(Value::INTEGER(1)));
		select_node->modifiers.push_back(std::move(modifier));
		subselect = std::move(select_node);
	} else {
		subselect = std::move(entry->subquery);
	}

	auto select = make_uniq<SelectStatement>();
	select->node = TransformMaterializedCTE(std::move(subselect));
	info->query = std::move(select);
	info->type = LogicalType::INVALID;

	result->info = std::move(info);
	return std::move(result);
}

// unique_ptr<SQLStatement> GenerateDropEnumStmt(string enum_name) {
//	auto result = make_uniq<DropStatement>();
//	result->info->if_exists = true;
//	result->info->schema = INVALID_SCHEMA;
//	result->info->catalog = INVALID_CATALOG;
//	result->info->name = std::move(enum_name);
//	result->info->type = CatalogType::TYPE_ENTRY;
//	return std::move(result);
//}

unique_ptr<SQLStatement> Transformer::CreatePivotStatement(unique_ptr<SQLStatement> statement) {
	auto result = make_uniq<MultiStatement>();
	for (auto &pivot : pivot_entries) {
		if (pivot->has_parameters) {
			throw ParserException(
			    "PIVOT statements with pivot elements extracted from the data cannot have parameters in their source.\n"
			    "In order to use parameters the PIVOT values must be manually specified, e.g.:\n"
			    "PIVOT ... ON %s IN (val1, val2, ...)",
			    pivot->column->ToString());
		}
		result->statements.push_back(GenerateCreateEnumStmt(std::move(pivot)));
	}
	result->statements.push_back(std::move(statement));
	// FIXME: drop the types again!?
	//	for(auto &pivot : pivot_entries) {
	//		result->statements.push_back(GenerateDropEnumStmt(std::move(pivot->enum_name)));
	//	}
	return std::move(result);
}

unique_ptr<QueryNode> Transformer::TransformPivotStatement(duckdb_libpgquery::PGSelectStmt &select) {
	auto pivot = select.pivot;
	auto current_param_count = ParamCount();
	auto source = TransformTableRefNode(*pivot->source);
	auto next_param_count = ParamCount();
	bool has_parameters = next_param_count > current_param_count;

	auto select_node = make_uniq<SelectNode>();
	// handle the CTEs
	if (select.withClause) {
		TransformCTE(*PGPointerCast<duckdb_libpgquery::PGWithClause>(select.withClause), select_node->cte_map);
	}
	if (!pivot->columns) {
		// no pivot columns - not actually a pivot
		select_node->from_table = std::move(source);
		if (pivot->groups) {
			auto groups = TransformStringList(pivot->groups);
			GroupingSet set;
			for (idx_t gr = 0; gr < groups.size(); gr++) {
				auto &group = groups[gr];
				auto colref = make_uniq<ColumnRefExpression>(group);
				select_node->select_list.push_back(colref->Copy());
				select_node->groups.group_expressions.push_back(std::move(colref));
				set.insert(gr);
			}
			select_node->groups.grouping_sets.push_back(std::move(set));
		}
		if (pivot->aggrs) {
			TransformExpressionList(*pivot->aggrs, select_node->select_list);
		}
		// transform order by/limit modifiers
		TransformModifiers(select, *select_node);
		return std::move(select_node);
	}

	// generate CREATE TYPE statements for each of the columns that do not have an IN list
	bool is_pivot = !pivot->unpivots;
	auto columns = TransformPivotList(*pivot->columns, is_pivot);
	for (idx_t c = 0; c < columns.size(); c++) {
		auto &col = columns[c];
		if (!col.pivot_enum.empty() || !col.entries.empty()) {
			continue;
		}
		if (col.pivot_expressions.size() != 1) {
			throw InternalException("PIVOT statement with multiple names in pivot entry!?");
		}
		auto enum_name = "__pivot_enum_" + UUID::ToString(UUID::GenerateRandomUUID());

		auto new_select = make_uniq<SelectNode>();
		ExtractCTEsRecursive(new_select->cte_map);
		new_select->from_table = source->Copy();
		AddPivotEntry(enum_name, std::move(new_select), col.pivot_expressions[0]->Copy(), std::move(col.subquery),
		              has_parameters);
		col.pivot_enum = enum_name;
	}

	// generate the actual query, including the pivot
	select_node->select_list.push_back(make_uniq<StarExpression>());

	auto pivot_ref = make_uniq<PivotRef>();
	pivot_ref->source = std::move(source);
	if (pivot->unpivots) {
		pivot_ref->unpivot_names = TransformStringList(pivot->unpivots);
	} else {
		if (pivot->aggrs) {
			TransformExpressionList(*pivot->aggrs, pivot_ref->aggregates);
		} else {
			// pivot but no aggregates specified - push a count star
			vector<unique_ptr<ParsedExpression>> children;
			auto function = make_uniq<FunctionExpression>("count_star", std::move(children));
			pivot_ref->aggregates.push_back(std::move(function));
		}
	}
	if (pivot->groups) {
		pivot_ref->groups = TransformStringList(pivot->groups);
	}
	pivot_ref->pivots = std::move(columns);
	SetQueryLocation(*pivot_ref, pivot->location);
	select_node->from_table = std::move(pivot_ref);
	// transform order by/limit modifiers
	TransformModifiers(select, *select_node);

	return std::move(select_node);
}

} // namespace duckdb
