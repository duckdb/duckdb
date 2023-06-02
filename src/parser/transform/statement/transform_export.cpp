#include "duckdb/parser/statement/export_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ExportStatement> Transformer::TransformExport(duckdb_libpgquery::PGExportStmt &stmt) {
	auto info = make_uniq<CopyInfo>();
	info->file_path = stmt.filename;
	info->format = "csv";
	info->is_from = false;
	// handle export options
	TransformCopyOptions(*info, stmt.options);

	auto result = make_uniq<ExportStatement>(std::move(info));
	if (stmt.database) {
		result->database = stmt.database;
	}
	return result;
}

} // namespace duckdb
