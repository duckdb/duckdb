#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/vacuum_statement.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformVacuumStatement(PEGTransformer &transformer,
                                                                         const VacuumOptions &vacuum_options,
                                                                         AnalyzeTarget analyze_target) {
	auto result = make_uniq<VacuumStatement>(vacuum_options);
	if (analyze_target.ref) {
		result->info->columns = analyze_target.columns;
		result->info->ref = std::move(analyze_target.ref);
		result->info->has_table = true;
	}
	return std::move(result);
}

VacuumOptions PEGTransformerFactory::TransformVacuumLegacyOptions(PEGTransformer &transformer, const string &opt_full,
                                                                  const string &opt_freeze, const string &opt_verbose,
                                                                  const string &opt_analyze) {
	VacuumOptions options;
	options.vacuum = true;
	options.analyze = !opt_analyze.empty();
	if (!opt_full.empty()) {
		throw NotImplementedException("FULL is not yet implemented");
	}
	if (!opt_freeze.empty()) {
		throw NotImplementedException("FREEZE is not yet implemented");
	}
	if (!opt_verbose.empty()) {
		throw NotImplementedException("VERBOSE is not yet implemented");
	}
	return options;
}

VacuumOptions PEGTransformerFactory::TransformVacuumParensOptions(PEGTransformer &transformer,
                                                                  const vector<string> &vacuum_option) {
	VacuumOptions options;
	options.vacuum = true;
	for (auto &option : vacuum_option) {
		if (StringUtil::CIEquals(option, "disable_page_skipping")) {
			throw NotImplementedException("Disable Page Skipping vacuum option");
		}
		if (StringUtil::CIEquals(option, "freeze")) {
			throw NotImplementedException("FREEZE is not yet implemented");
		}
		if (StringUtil::CIEquals(option, "full")) {
			throw NotImplementedException("FULL is not yet implemented");
		}
		if (StringUtil::CIEquals(option, "verbose")) {
			throw NotImplementedException("VERBOSE is not yet implemented");
		}
		if (StringUtil::CIEquals(option, "analyze")) {
			options.analyze = true;
		}
	}
	return options;
}

vector<string> PEGTransformerFactory::TransformNameList(PEGTransformer &transformer, const vector<string> &col_id) {
	vector<string> result;
	for (auto &colid : col_id) {
		result.push_back(colid);
	}
	return result;
}

string PEGTransformerFactory::TransformOptAnalyze(PEGTransformer &transformer) {
	return "analyze";
}

string PEGTransformerFactory::TransformOptFull(PEGTransformer &transformer) {
	return "full";
}

string PEGTransformerFactory::TransformOptFreeze(PEGTransformer &transformer) {
	return "freeze";
}

string PEGTransformerFactory::TransformOptVerbose(PEGTransformer &transformer) {
	return "verbose";
}

} // namespace duckdb
