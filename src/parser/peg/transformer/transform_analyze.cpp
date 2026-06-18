#include "duckdb/parser/peg/ast/analyze_target.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/vacuum_statement.hpp"

namespace duckdb {
unique_ptr<SQLStatement> PEGTransformerFactory::TransformAnalyzeStatement(PEGTransformer &transformer,
                                                                          const optional<bool> &analyze_verbose,
                                                                          optional<AnalyzeTarget> analyze_target) {
	VacuumOptions vacuum_options;
	vacuum_options.analyze = true;
	auto result = make_uniq<VacuumStatement>(vacuum_options);
	if (analyze_verbose) {
		throw NotImplementedException("ANALYZE VERBOSE is not implemented yet");
	}
	if (analyze_target && analyze_target->ref) {
		result->info->columns = analyze_target->columns;
		result->info->ref = std::move(analyze_target->ref);
		result->info->has_table = true;
	}
	return std::move(result);
}

AnalyzeTarget PEGTransformerFactory::TransformAnalyzeTarget(PEGTransformer &transformer,
                                                            unique_ptr<BaseTableRef> base_table_name,
                                                            const optional<vector<string>> &name_list) {
	AnalyzeTarget result;
	result.ref = std::move(base_table_name);
	if (name_list) {
		result.columns = StringsToIdentifiers(*name_list);
	}
	return result;
}

bool PEGTransformerFactory::TransformAnalyzeVerbose(PEGTransformer &transformer) {
	return true;
}
} // namespace duckdb
