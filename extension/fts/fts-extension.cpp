#define DUCKDB_EXTENSION_MAIN
#include "fts-extension.hpp"
#include "fts_indexing.hpp"
#include "libstemmer.h"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

static void stem_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];
	auto &stemmer_vector = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    input_vector, stemmer_vector, result, args.size(), [&](string_t input, string_t stemmer) {
		    auto input_data = input.GetDataUnsafe();
		    auto input_size = input.GetSize();

		    if (stemmer.GetString() == "none") {
			    auto output = StringVector::AddString(result, input_data, input_size);
			    return output;
		    }

		    struct sb_stemmer *s = sb_stemmer_new(stemmer.GetString().c_str(), "UTF_8");
		    if (s == 0) {
			    const char **stemmers = sb_stemmer_list();
			    size_t n_stemmers = 27;
			    throw Exception(StringUtil::Format(
			        "Unrecognized stemmer '%s'. Supported stemmers are: ['%s'], or use 'none' for no stemming",
			        stemmer.GetString(),
			        StringUtil::Join(stemmers, n_stemmers, "', '", [](const char *st) { return st; })));
		    }

		    auto output_data = (char *)sb_stemmer_stem(s, (const sb_symbol *)input_data, input_size);
		    auto output_size = sb_stemmer_length(s);
		    auto output = StringVector::AddString(result, output_data, output_size);

		    sb_stemmer_delete(s);
		    return output;
	    });
}

void FTSExtension::Load(DuckDB &db) {
	ScalarFunction stem_func("stem", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, stem_function);
	CreateScalarFunctionInfo stem_info(stem_func);

	auto create_fts_index_func = PragmaFunction::PragmaCall(
	    "create_fts_index", create_fts_index_query, {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR);
	create_fts_index_func.named_parameters["stemmer"] = LogicalType::VARCHAR;
	create_fts_index_func.named_parameters["stopwords"] = LogicalType::VARCHAR;
	create_fts_index_func.named_parameters["ignore"] = LogicalType::VARCHAR;
	create_fts_index_func.named_parameters["strip_accents"] = LogicalType::BOOLEAN;
	create_fts_index_func.named_parameters["lower"] = LogicalType::BOOLEAN;
	create_fts_index_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	CreatePragmaFunctionInfo create_fts_index_info(create_fts_index_func);

	auto drop_fts_index_func =
	    PragmaFunction::PragmaCall("drop_fts_index", drop_fts_index_query, {LogicalType::VARCHAR});
	CreatePragmaFunctionInfo drop_fts_index_info(drop_fts_index_func);

	Connection conn(db);
	conn.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*conn.context);
	catalog.CreateFunction(*conn.context, &stem_info);
	catalog.CreatePragmaFunction(*conn.context, &create_fts_index_info);
	catalog.CreatePragmaFunction(*conn.context, &drop_fts_index_info);
	conn.Commit();
}

std::string FTSExtension::Name() {
	return "fts";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void fts_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::FTSExtension>();
}

DUCKDB_EXTENSION_API const char *fts_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
