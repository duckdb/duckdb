#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformVacuumStatement(PEGTransformer &transformer,
                                                                         const VacuumOptions &vacuum_options,
                                                                         AnalyzeTarget analyze_target) {
	// SereneDB extension: VACUUM (REFRESH_*) / VACUUM (COMPACT_*) / VACUUM
	// (SYNC_STATS) / VACUUM (COMPACT) lower to a PRAGMA call instead of the
	// standard VacuumStatement. Parameters are positional: option, name,
	// schema, catalog.
	if (!vacuum_options.serenedb_pragma_option.empty()) {
		auto pragma = make_uniq<PragmaStatement>();
		pragma->info->name = "serenedb_vacuum";
		pragma->info->parameters.push_back(make_uniq<ConstantExpression>(Value(vacuum_options.serenedb_pragma_option)));
		if (analyze_target.ref) {
			auto &base_ref = analyze_target.ref->Cast<BaseTableRef>();
			pragma->info->parameters.push_back(make_uniq<ConstantExpression>(Value(base_ref.table_name)));
			pragma->info->parameters.push_back(make_uniq<ConstantExpression>(Value(base_ref.schema_name)));
			pragma->info->parameters.push_back(make_uniq<ConstantExpression>(Value(base_ref.catalog_name)));
		}
		return std::move(pragma);
	}

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
	static constexpr const char *kSerenedbOptions[] = {
	    "refresh_database", "refresh_schema",    "refresh_table",       "refresh_index",  "refresh_all",
	    "compact_database", "compact_schema",    "compact_table",       "compact_index",  "compact_all",
	    "sync_stats_table", "sync_stats_schema", "sync_stats_database", "sync_stats_all", "compact_rocksdb",
	};
	VacuumOptions options;
	options.vacuum = true;
	bool any_standard_option = false;
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
			any_standard_option = true;
			continue;
		}
		const char *matched = nullptr;
		for (const auto *candidate : kSerenedbOptions) {
			if (StringUtil::CIEquals(option, candidate)) {
				matched = candidate;
				break;
			}
		}
		if (!matched) {
			continue;
		}
		if (!options.serenedb_pragma_option.empty()) {
			throw ParserException("VACUUM accepts at most one SereneDB option (got both '%s' and '%s')",
			                      options.serenedb_pragma_option, matched);
		}
		options.serenedb_pragma_option = matched;
	}
	if (!options.serenedb_pragma_option.empty() && any_standard_option) {
		throw ParserException("VACUUM SereneDB option '%s' cannot be combined with standard VACUUM options",
		                      options.serenedb_pragma_option);
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

// VacuumOption <- OptAnalyze / OptFreeze / OptFull / OptVerbose / Identifier
// Choice body: the auto-generated wrapper extracts the chosen alternative as a
// ParseResult, and we dispatch on its type. The keyword alternatives produce
// named ParseResults that round-trip through Transform<string>; the Identifier
// alternative produces an IdentifierParseResult with empty .name, which the
// dispatcher cannot look up, so we read it directly here.
string PEGTransformerFactory::TransformVacuumOption(PEGTransformer &transformer, ParseResult &choice_result) {
	if (choice_result.type == ParseResultType::IDENTIFIER) {
		return choice_result.Cast<IdentifierParseResult>().identifier;
	}
	return transformer.Transform<string>(choice_result);
}

} // namespace duckdb
