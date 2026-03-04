#include "transformer/peg_transformer.hpp"

#include "duckdb/parser/statement/multi_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"

namespace duckdb {

void PEGTransformer::ParamTypeCheck(PreparedParamType last_type, PreparedParamType new_type) {
	// Mixing positional/auto-increment and named parameters is not supported
	if (last_type == PreparedParamType::INVALID) {
		return;
	}
	if (last_type == PreparedParamType::NAMED) {
		if (new_type != PreparedParamType::NAMED) {
			throw NotImplementedException("Mixing named and positional parameters is not supported yet");
		}
	}
	if (last_type != PreparedParamType::NAMED) {
		if (new_type == PreparedParamType::NAMED) {
			throw NotImplementedException("Mixing named and positional parameters is not supported yet");
		}
	}
}

bool PEGTransformer::GetParam(const string &identifier, idx_t &index, PreparedParamType type) {
	ParamTypeCheck(last_param_type, type);
	auto entry = named_parameter_map.find(identifier);
	if (entry == named_parameter_map.end()) {
		return false;
	}
	index = entry->second;
	return true;
}

void PEGTransformer::SetParam(const string &identifier, idx_t index, PreparedParamType type) {
	ParamTypeCheck(last_param_type, type);
	last_param_type = type;
	D_ASSERT(!named_parameter_map.count(identifier));
	named_parameter_map[identifier] = index;
}

void PEGTransformer::ClearParameters() {
	prepared_statement_parameter_index = 0;
	named_parameter_map.clear();
}

void PEGTransformer::Clear() {
	ClearParameters();
	pivot_entries.clear();
	stored_cte_map.clear();
}

idx_t PEGTransformer::ParamCount() const {
	return prepared_statement_parameter_index;
}

void PEGTransformer::SetParamCount(idx_t new_count) {
	prepared_statement_parameter_index = new_count;
}

unique_ptr<SQLStatement> PEGTransformer::GenerateCreateEnumStmt(unique_ptr<CreatePivotEntry> entry) {
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
	select->node = std::move(subselect);
	info->query = std::move(select);
	info->type = LogicalType::INVALID;

	result->info = std::move(info);
	return std::move(result);
}

unique_ptr<SQLStatement> PEGTransformer::CreatePivotStatement(unique_ptr<SQLStatement> statement) {
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
	result->stmt_location = statement->stmt_location;
	result->stmt_length = statement->stmt_length;
	result->statements.push_back(std::move(statement));
	return std::move(result);
}

void PEGTransformer::PivotEntryCheck(const string &type) {
	if (!pivot_entries.empty()) {
		throw ParserException(
		    "PIVOT statements with pivot elements extracted from the data cannot be used in %ss.\nIn order to use "
		    "PIVOT in a %s the PIVOT values must be manually specified, e.g.:\nPIVOT ... ON %s IN (val1, val2, ...)",
		    type, type, pivot_entries[0]->column->ToString());
	}
}

void PEGTransformer::ExtractCTEsRecursive(CommonTableExpressionMap &cte_map) {
	// Traverse the stack from the most recent scope back to the global scope
	// Use reverse iterator if you push new scopes to the back
	for (auto it = stored_cte_map.rbegin(); it != stored_cte_map.rend(); ++it) {
		auto &current_scope = it->get();
		for (auto &entry : current_scope.map) {
			// Check if this CTE name is already in our result map
			if (cte_map.map.find(entry.first) == cte_map.map.end()) {
				cte_map.map[entry.first] = entry.second->Copy();
			}
		}
	}
}

} // namespace duckdb
