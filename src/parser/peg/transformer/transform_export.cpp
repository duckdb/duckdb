#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/statement/export_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformExportStatement(string export_source, string file_path,
                                                                         vector<GenericCopyOption> options) {
	auto info = make_uniq<CopyInfo>();
	info->file_path = std::move(file_path);
	info->format = "csv";
	info->is_from = false;

	for (auto &option : options) {
		if (option.name == "format") {
			info->format = option.children[0].GetValue<string>();
			info->is_format_auto_detected = false;
		} else if (option.expression) {
			info->parsed_options[StringUtil::Upper(option.name)] = std::move(option.expression);
		} else {
			info->options[StringUtil::Upper(option.name)] = option.children;
		}
	}

	auto result = make_uniq<ExportStatement>(std::move(info));
	result->database = export_source;
	return std::move(result);
}

string PEGTransformerFactory::TransformExportSource(string catalog_name) {
	return catalog_name;
}

} // namespace duckdb
