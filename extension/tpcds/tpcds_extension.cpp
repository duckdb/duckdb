// The tpcds extension, written against DuckDB's stable C++ API.

// the CMake build sets the name, package builds do not
#ifndef DUCKDB_EXTENSION_NAME
#define DUCKDB_EXTENSION_NAME tpcds
#endif
#include "duckdb_cpp_extension.hpp"
#include "dsdgen.hpp"

#include <algorithm>
#include <cctype>
#include <memory>
#include <optional>
#include <string>

using namespace duckdb::cxx;

namespace {

std::string Lower(std::string value) {
	std::transform(value.begin(), value.end(), value.begin(),
	               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
	return value;
}

Value CastArgument(Factory &factory, const Value &value, const char *type) {
	return value.Cast(factory, factory.ParseType(type));
}

std::string StringArgument(Factory &factory, const Value &value) {
	auto text = CastArgument(factory, value, "VARCHAR").Get<varchar_t>();
	return std::string(text.data(), text.size());
}

//! The `**kwargs` arguments of a call, the only ones a caller can omit without a default value
template <class F>
void ForEachNamedArgument(FunctionBindInput &input, const char *function_name, F &&handle) {
	auto counts = input.GetArgumentCounts();
	for (idx_t i = counts.NamedOffset(); i < counts.Total(); i++) {
		auto name = Lower(input.GetArgName(i));
		if (!handle(name, input.GetConstantArgument(i))) {
			throw InvalidInputException("Invalid named parameter \"" + name + "\" for function " + function_name);
		}
	}
}

//===--------------------------------------------------------------------===//
// dsdgen
//===--------------------------------------------------------------------===//
struct DSDGenGlobalState {
	std::unique_ptr<tpcds::DSDGenGenerator> generator;
};

void DsdgenBind(TableFunction::BindInput &input) {
	auto &factory = input.GetContext().GetFactory();
	tpcds::DSDGenOptions options;
	ForEachNamedArgument(input, "dsdgen", [&](const std::string &name, const Value &value) {
		if (value.IsNull()) {
			throw InvalidInputException("Cannot use NULL as function argument");
		}
		if (name == "sf") {
			options.sf = CastArgument(factory, value, "DOUBLE").Get<double>();
		} else if (name == "catalog") {
			options.catalog = StringArgument(factory, value);
		} else if (name == "schema") {
			options.schema = StringArgument(factory, value);
		} else if (name == "suffix") {
			options.suffix = StringArgument(factory, value);
		} else if (name == "overwrite") {
			options.overwrite = CastArgument(factory, value, "BOOLEAN").Get<bool>();
		} else if (name == "keys") {
			options.keys = CastArgument(factory, value, "BOOLEAN").Get<bool>();
		} else {
			return false;
		}
		return true;
	});
	tpcds::DSDGenWrapper::ValidateScaleFactor(options.sf);
	input.AddResultColumn("Success", factory.ParseType("BOOLEAN"));
	// The result is empty, but the scan does the real work.
	input.SetCardinality(1000, false);
	// with no rows to keep in order, the threads scanning dsdgen generate in parallel
	input.SetOrderPreservation(OrderPreservation::NO_ORDER);
	input.SetBindData<tpcds::DSDGenOptions>(std::move(options));
}

void DsdgenInitGlobal(TableFunction::InitGlobalInput &input) {
	auto &options = input.GetBindData<tpcds::DSDGenOptions>();
	DSDGenGlobalState state;
	state.generator = tpcds::DSDGenGenerator::Create(input.GetContext(), options);
	auto max_threads = state.generator->MaxThreads();
	input.SetGlobalState<DSDGenGlobalState>(std::move(state));
	input.SetMaxThreads(max_threads);
}

void DsdgenExec(TableFunction::ExecInput &input) {
	auto &state = input.GetGlobalState<DSDGenGlobalState>();
	input.GetOutputChunk().GetVector(0).SetSize(0);
	// hand the thread back between steps, which keeps the progress observable
	if (state.generator->Generate(input.GetContext())) {
		input.Yield();
	}
}

void DsdgenProgress(TableFunction::ProgressInput &input) {
	auto state = input.TryGetGlobalState<DSDGenGlobalState>();
	input.SetProgress(state && state->generator ? state->generator->Progress() : 0.0);
}

//===--------------------------------------------------------------------===//
// tpcds_queries and tpcds_answers
//===--------------------------------------------------------------------===//
struct TPCDSQueriesBindData {
	//! Without a scale factor the stored queries are returned unchanged
	std::optional<double> sf;

	std::string GetQuery(int query) const {
		return sf ? tpcds::DSDGenWrapper::GetQuery(query, *sf) : tpcds::DSDGenWrapper::GetQuery(query);
	}
	//! Equal bind data lets the optimizer share identical scans
	bool operator==(const TPCDSQueriesBindData &other) const {
		return sf == other.sf;
	}
};

struct TPCDSScanState {
	idx_t offset = 0;
};

void TPCDSScanInit(TableFunction::InitGlobalInput &input) {
	input.SetGlobalState<TPCDSScanState>();
}

//! The scale factor is either the positional argument or the named "sf" parameter
void TPCDSQueryBind(TableFunction::BindInput &input) {
	auto &factory = input.GetContext().GetFactory();
	TPCDSQueriesBindData bind_data;
	auto counts = input.GetArgumentCounts();
	if (counts.positional_variadic > 1) {
		throw InvalidInputException("tpcds_queries: takes at most one positional argument, the scale factor");
	}
	auto set_scale_factor = [&](const Value &value) {
		if (bind_data.sf) {
			throw InvalidInputException(
			    "tpcds_queries: the scale factor can be given as a positional argument or as \"sf\", not both");
		}
		if (value.IsNull()) {
			throw InvalidInputException("tpcds_queries: cannot use NULL as scale factor");
		}
		bind_data.sf = CastArgument(factory, value, "DOUBLE").Get<double>();
		// validates the scale factor
		bind_data.GetQuery(1);
	};
	if (counts.positional_variadic == 1) {
		set_scale_factor(input.GetConstantArgument(0));
	}
	ForEachNamedArgument(input, "tpcds_queries", [&](const std::string &name, const Value &value) {
		if (name != "sf") {
			return false;
		}
		set_scale_factor(value);
		return true;
	});
	input.AddResultColumn("query_nr", factory.ParseType("INTEGER"));
	input.AddResultColumn("query", factory.ParseType("VARCHAR"));
	input.SetCardinality(tpcds::DSDGenWrapper::QueriesCount(), true);
	input.SetBindData<TPCDSQueriesBindData>(std::move(bind_data));
}

void TPCDSQueryExec(TableFunction::ExecInput &input) {
	auto &bind_data = input.GetBindData<TPCDSQueriesBindData>();
	auto &state = input.GetGlobalState<TPCDSScanState>();
	auto chunk = input.GetOutputChunk();
	auto query_nr = chunk.GetVector(0);
	auto query = chunk.GetVector(1);
	auto count = std::min<idx_t>(tpcds::DSDGenWrapper::QueriesCount() - state.offset, chunk.GetCapacity());
	query_nr.SetSize(count);
	query.SetSize(count);
	auto query_nr_data = query_nr.GetDataMutable<int32_t>();
	for (idx_t row = 0; row < count; row++) {
		auto nr = static_cast<int32_t>(state.offset + row + 1);
		query_nr_data[row] = nr;
		query.AssignString(row, bind_data.GetQuery(nr));
	}
	state.offset += count;
}

static const double TPCDS_ANSWER_SCALE_FACTORS[] = {0.01, 1};

void TPCDSQueryAnswerBind(TableFunction::BindInput &input) {
	auto &factory = input.GetContext().GetFactory();
	input.AddResultColumn("query_nr", factory.ParseType("INTEGER"));
	input.AddResultColumn("scale_factor", factory.ParseType("DOUBLE"));
	input.AddResultColumn("answer", factory.ParseType("VARCHAR"));
	input.SetCardinality(tpcds::DSDGenWrapper::QueriesCount() * 2, true);
}

void TPCDSQueryAnswerExec(TableFunction::ExecInput &input) {
	auto &state = input.GetGlobalState<TPCDSScanState>();
	idx_t queries = tpcds::DSDGenWrapper::QueriesCount();
	idx_t total_answers = queries * (sizeof(TPCDS_ANSWER_SCALE_FACTORS) / sizeof(double));
	auto chunk = input.GetOutputChunk();
	auto query_nr = chunk.GetVector(0);
	auto scale_factor = chunk.GetVector(1);
	auto answer = chunk.GetVector(2);
	auto count = std::min<idx_t>(total_answers - state.offset, chunk.GetCapacity());
	query_nr.SetSize(count);
	scale_factor.SetSize(count);
	answer.SetSize(count);
	auto query_nr_data = query_nr.GetDataMutable<int32_t>();
	auto scale_factor_data = scale_factor.GetDataMutable<double>();
	for (idx_t row = 0; row < count; row++) {
		auto offset = state.offset + row;
		auto query = static_cast<int32_t>(offset % queries + 1);
		auto sf = TPCDS_ANSWER_SCALE_FACTORS[offset / queries];
		query_nr_data[row] = query;
		scale_factor_data[row] = sf;
		answer.AssignString(row, tpcds::DSDGenWrapper::GetAnswer(sf, query));
	}
	state.offset += count;
}

//===--------------------------------------------------------------------===//
// PRAGMA tpcds
//===--------------------------------------------------------------------===//
std::string PragmaTpcdsQuery(PragmaFunction::QueryInput &input) {
	auto &factory = input.GetContext().GetFactory();
	auto query_nr = input.GetConstantArgument(0);
	if (query_nr.IsNull()) {
		throw InvalidInputException("Cannot use NULL as argument for the TPC-DS query number");
	}
	auto index = CastArgument(factory, query_nr, "BIGINT").Get<int64_t>();
	if (index <= 0 || index > static_cast<int64_t>(tpcds::DSDGenWrapper::QueriesCount())) {
		throw InvalidInputException("Out of range TPC-DS query number " + std::to_string(index));
	}
	std::optional<double> sf;
	ForEachNamedArgument(input, "tpcds", [&](const std::string &name, const Value &value) {
		if (name != "sf") {
			return false;
		}
		if (value.IsNull()) {
			throw InvalidInputException("Cannot use NULL as scale factor for the TPC-DS query");
		}
		sf = CastArgument(factory, value, "DOUBLE").Get<double>();
		return true;
	});
	auto query = static_cast<int>(index);
	return sf ? tpcds::DSDGenWrapper::GetQuery(query, *sf) : tpcds::DSDGenWrapper::GetQuery(query);
}

} // namespace

DUCKDB_CPP_EXTENSION_ENTRYPOINT(Extension &extension, Context &context) {
	auto &factory = context.GetFactory();
	auto any = factory.CreateType(LogicalTypeId::ANY);

	auto dsdgen = TableFunction::Create(factory);
	// creates and fills tables per execution
	dsdgen.SetName("dsdgen")
	    .SetStability(FunctionStability::VOLATILE)
	    .WithSignature([&](FunctionSignature &sig) { sig.AddKwargs("options", any); })
	    .WithDocs([](FunctionDocs &docs) {
		    docs.SetDescription("Creates the TPC-DS tables and fills them with the data of a scale factor. Options: "
		                        "sf, catalog, schema, suffix, overwrite and keys");
	    })
	    .SetBindCallback(DsdgenBind)
	    .SetInitGlobalCallback(DsdgenInitGlobal)
	    .SetExecCallback(DsdgenExec)
	    .SetProgressCallback(DsdgenProgress);
	extension.Register(dsdgen);

	// the TPC-DS queries, optionally parameterized for a scale factor
	auto queries = TableFunction::Create(factory);
	queries.SetName("tpcds_queries")
	    .SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY)
	    .WithSignature([&](FunctionSignature &sig) {
		    sig.AddArgs("sf", factory.ParseType("DOUBLE"));
		    sig.AddKwargs("options", any);
	    })
	    .SetBindCallback(TPCDSQueryBind)
	    .SetInitGlobalCallback(TPCDSScanInit)
	    .SetExecCallback(TPCDSQueryExec);
	extension.Register(queries);

	// the answers of the TPC-DS queries
	auto answers = TableFunction::Create(factory);
	answers.SetName("tpcds_answers")
	    .SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY)
	    .SetBindCallback(TPCDSQueryAnswerBind)
	    .SetInitGlobalCallback(TPCDSScanInit)
	    .SetExecCallback(TPCDSQueryAnswerExec);
	extension.Register(answers);

	// PRAGMA tpcds(n) runs TPC-DS query n
	auto pragma = PragmaFunction::Create(factory);
	pragma.SetName("tpcds")
	    .WithSignature([&](FunctionSignature &sig) {
		    sig.AddParameter("query_nr", factory.ParseType("BIGINT"), FunctionParameterKind::POSITIONAL_ONLY);
		    sig.AddKwargs("options", any);
	    })
	    .SetQueryCallback(PragmaTpcdsQuery);
	extension.Register(pragma);
}
