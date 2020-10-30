#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<CreateStatement> Transformer::TransformCreateFunction(PGNode *node) {
    auto stmt = reinterpret_cast<PGCreateFunctionStmt *>(node);
    assert(stmt);
}

} // namespace duckdb
