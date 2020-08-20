#include "duckdb/parser/statement/export_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<ExportStatement> Transformer::TransformExport(PGNode *node) {
	PGExportStmt *stmt = reinterpret_cast<PGExportStmt *>(node);
	auto info = make_unique<CopyInfo>();
	info->file_path = stmt->filename;
	info->format = "csv";
	// handle export options
	TransformCopyOptions(*info, stmt->options);

	return make_unique<ExportStatement>(move(info));
}

} // namespace duckdb
