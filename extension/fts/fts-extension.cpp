#include "fts-extension.hpp"
#include "fts5_tokenize.h"

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

static void stem_function(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t, true>(args.data[0], result, args.size(), [&](string_t input) {
        auto input_data = input.GetData();
        auto input_size = input.GetSize();

        // size is at most the input size: alloc it
        auto output_data = unique_ptr<char[]>{new char[input_size + 1]};
        idx_t output_size;

        if(input_size > FTS5_PORTER_MAX_TOKEN || input_size < 3) {
            // we do not stem tokens longer than the limit, or smaller than 3
            memcpy(output_data.get(), input_data, input_size);
            output_size = input_size;
        } else {
            output_size = fts5PorterCb(output_data.get(), input_data, input_size);
            output_data[output_size] = '\0';
        }

        return StringVector::AddString(result, output_data.get(), output_size);
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
	ScalarFunction stem_func("stem", {LogicalType::VARCHAR}, LogicalType::VARCHAR, stem_function);
	CreateScalarFunctionInfo stem_info(stem_func);

	Connection conn(db);
	conn.context->transaction.BeginTransaction();

	db.catalog->CreateFunction(*conn.context, &stem_info);

	conn.context->transaction.Commit();
}

} // namespace duckdb
