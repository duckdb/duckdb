#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

struct DefaultOptimizerType {
	const char *name;
	OptimizerType type;
};

static DefaultOptimizerType internal_optimizer_types[] = {
    {"expression_rewriter", OptimizerType::EXPRESSION_REWRITER},
    {"filter_pullup", OptimizerType::FILTER_PULLUP},
    {"filter_pushdown", OptimizerType::FILTER_PUSHDOWN},
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
    {"top_n", OptimizerType::TOP_N},
    {"compressed_materialization", OptimizerType::COMPRESSED_MATERIALIZATION},
    {"duplicate_groups", OptimizerType::DUPLICATE_GROUPS},
    {"reorder_filter", OptimizerType::REORDER_FILTER},
    {"extension", OptimizerType::EXTENSION},
    {nullptr, OptimizerType::INVALID}};

string OptimizerTypeToString(OptimizerType type) {
	for (idx_t i = 0; internal_optimizer_types[i].name; i++) {
		if (internal_optimizer_types[i].type == type) {
			return internal_optimizer_types[i].name;
		}
	}
	throw InternalException("Invalid optimizer type");
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
