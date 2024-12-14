#include "duckdb/common/enums/optimizer_type.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

struct DefaultOptimizerType {
	const char *name;
	OptimizerType type;
};

static const DefaultOptimizerType internal_optimizer_types[] = {
    {"expression_rewriter", OptimizerType::EXPRESSION_REWRITER},
    {"filter_pullup", OptimizerType::FILTER_PULLUP},
    {"filter_pushdown", OptimizerType::FILTER_PUSHDOWN},
    {"empty_result_pullup", OptimizerType::EMPTY_RESULT_PULLUP},
    {"cte_filter_pusher", OptimizerType::CTE_FILTER_PUSHER},
    {"regex_range", OptimizerType::REGEX_RANGE},
    {"in_clause", OptimizerType::IN_CLAUSE},
    {"join_order", OptimizerType::JOIN_ORDER},
    {"deliminator", OptimizerType::DELIMINATOR},
    {"unnest_rewriter", OptimizerType::UNNEST_REWRITER},
    {"unused_columns", OptimizerType::UNUSED_COLUMNS},
    {"statistics_propagation", OptimizerType::STATISTICS_PROPAGATION},
    {"common_subexpressions", OptimizerType::COMMON_SUBEXPRESSIONS},
    {"common_aggregate", OptimizerType::COMMON_AGGREGATE},
    {"column_lifetime", OptimizerType::COLUMN_LIFETIME},
    {"limit_pushdown", OptimizerType::LIMIT_PUSHDOWN},
    {"top_n", OptimizerType::TOP_N},
    {"build_side_probe_side", OptimizerType::BUILD_SIDE_PROBE_SIDE},
    {"compressed_materialization", OptimizerType::COMPRESSED_MATERIALIZATION},
    {"duplicate_groups", OptimizerType::DUPLICATE_GROUPS},
    {"reorder_filter", OptimizerType::REORDER_FILTER},
    {"sampling_pushdown", OptimizerType::SAMPLING_PUSHDOWN},
    {"join_filter_pushdown", OptimizerType::JOIN_FILTER_PUSHDOWN},
    {"extension", OptimizerType::EXTENSION},
    {"materialized_cte", OptimizerType::MATERIALIZED_CTE},
    {"sum_rewriter", OptimizerType::SUM_REWRITER},
    {"late_materialization", OptimizerType::LATE_MATERIALIZATION},
    {nullptr, OptimizerType::INVALID}};

string OptimizerTypeToString(OptimizerType type) {
	for (idx_t i = 0; internal_optimizer_types[i].name; i++) {
		if (internal_optimizer_types[i].type == type) {
			return internal_optimizer_types[i].name;
		}
	}
	throw InternalException("Invalid optimizer type");
}

vector<string> ListAllOptimizers() {
	vector<string> result;
	for (idx_t i = 0; internal_optimizer_types[i].name; i++) {
		result.push_back(internal_optimizer_types[i].name);
	}
	return result;
}

OptimizerType OptimizerTypeFromString(const string &str) {
	for (idx_t i = 0; internal_optimizer_types[i].name; i++) {
		if (internal_optimizer_types[i].name == str) {
			return internal_optimizer_types[i].type;
		}
	}
	// optimizer not found, construct candidate list
	vector<string> optimizer_names;
	for (idx_t i = 0; internal_optimizer_types[i].name; i++) {
		optimizer_names.emplace_back(internal_optimizer_types[i].name);
	}
	throw ParserException("Optimizer type \"%s\" not recognized\n%s", str,
	                      StringUtil::CandidatesErrorMessage(optimizer_names, str, "Candidate optimizers"));
}

} // namespace duckdb
