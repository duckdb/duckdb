#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/statement/export_statement.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformExportStatement(PEGTransformer &transformer, const string &export_source,
                                                const string &string_literal,
                                                const vector<GenericCopyOption> &generic_copy_option_list) {
	auto info = make_uniq<CopyInfo>();
	info->file_path = string_literal;
	info->format = "csv";
	info->is_from = false;

	for (const auto &option : generic_copy_option_list) {
		if (option.name == "format") {
			if (option.children.empty() && !option.expression) {
				throw InvalidInputException("Unknown format specified");
			}
			if (option.expression) {
				if (option.expression->GetExpressionClass() != ExpressionClass::CONSTANT) {
					throw ParserException(
					    "Unsupported parameter type for FORMAT: expected e.g. FORMAT 'csv', 'parquet'");
				}
				auto const_expr = option.expression->Cast<ConstantExpression>();
				info->format = const_expr.GetValue().GetValue<string>();
			} else {
				info->format = option.children[0].GetValue<string>();
			}
			info->is_format_auto_detected = false;
		} else if (option.expression) {
			info->parsed_options[StringUtil::Upper(option.name.GetIdentifierName())] = option.expression->Copy();
		} else {
			info->options[StringUtil::Upper(option.name.GetIdentifierName())] = option.children;
		}
	}

	auto result = make_uniq<ExportStatement>(std::move(info));
	result->database = export_source;
	return std::move(result);
}

string PEGTransformerFactory::TransformExportSource(PEGTransformer &transformer, const Identifier &catalog_name) {
	return catalog_name.GetIdentifierName();
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformImportStatement(PEGTransformer &transformer,
                                                                         const string &string_literal) {
	auto result = make_uniq<PragmaStatement>();
	result->info->name = "import_database";
	result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(string_literal)));
	return std::move(result);
}

} // namespace duckdb
