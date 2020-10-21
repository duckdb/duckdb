#include "fts-extension.hpp"
#include "libstemmer.h"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

static void stem_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];
	auto &stemmer_vector = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, string_t, true>(
	    input_vector, stemmer_vector, result, args.size(), [&](string_t input, string_t stemmer) {
		    struct sb_stemmer *s = sb_stemmer_new(stemmer.GetData(), CHAR_ENC);
		    if (s == 0) {
			    const char **stemmers = sb_stemmer_list();
			    size_t n_stemmers = 27;

			    string error_message = "unrecognized stemmer. Supported stemmers are ";
			    error_message += StringUtil::Join(stemmers, n_stemmers, ", ", [](const char *st) { return st; });
			    throw Exception(error_message);
		    }

		    auto input_data = input.GetData();
		    auto input_size = input.GetSize();
		    auto output_data = (char *)sb_stemmer_stem(s, (const sb_symbol *)input_data, input_size);
		    auto output_size = sb_stemmer_length(s);

		    auto output = StringVector::AddString(result, output_data, output_size);
		    sb_stemmer_delete(s);

		    return output;
	    });
}

static void pre_process() {
	// strip accents
	// lowercase
	// replace non [a-z] with ' '
	// string split \s+
	// unnest
	// remove empty strings
	// stem

	// function call to create index should be something like
	// create_fts_index(tablename, docid_column, document_body_column1, document_body_column2, etc...)
	// test_indexing.test has the first steps
}

void FTSExtension::Load(DuckDB &db) {
	ScalarFunction stem_func("stem", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, stem_function);
	CreateScalarFunctionInfo stem_info(stem_func);

	Connection conn(db);
	conn.context->transaction.BeginTransaction();

	db.catalog->CreateFunction(*conn.context, &stem_info);

	conn.context->transaction.Commit();
}

} // namespace duckdb
