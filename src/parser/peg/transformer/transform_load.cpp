#include "duckdb/parser/statement/load_statement.hpp"
#include "duckdb/parser/statement/update_extensions_statement.hpp"
#include "duckdb/parser/parsed_data/update_extensions_info.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/peg/ast/extension_repository_info.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformLoadStatement(PEGTransformer &transformer,
                                                                       const Identifier &col_id_or_string,
                                                                       const optional<Identifier> &extension_alias) {
	auto result = make_uniq<LoadStatement>();
	auto info = make_uniq<LoadInfo>();
	info->repo_is_alias = false;
	info->filename = col_id_or_string.GetIdentifierName();
	if (extension_alias) {
		info->alias = *extension_alias;
		info->load_type = LoadType::LOAD_AS;
	} else {
		info->load_type = LoadType::LOAD;
	}
	result->info = std::move(info);
	return std::move(result);
}

Identifier PEGTransformerFactory::TransformExtensionAlias(PEGTransformer &transformer, const Identifier &identifier) {
	return identifier;
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformInstallStatement(
    PEGTransformer &transformer, const bool &has_result, const QualifiedName &identifier_or_string_literal,
    const optional<ExtensionRepositoryInfo> &from_source, const optional<string> &version_number) {
	auto result = make_uniq<LoadStatement>();
	auto info = make_uniq<LoadInfo>();
	info->load_type = has_result ? LoadType::FORCE_INSTALL : LoadType::INSTALL;
	info->filename = identifier_or_string_literal.Name().GetIdentifierName();
	info->repo_is_alias = false;
	if (from_source) {
		info->repository = from_source->name.GetIdentifierName();
		info->repo_is_alias = from_source->repository_is_alias;
	}
	if (version_number) {
		info->version = *version_number;
	}
	result->info = std::move(info);
	return std::move(result);
}

ExtensionRepositoryInfo PEGTransformerFactory::TransformFromSourceIdentifier(PEGTransformer &transformer,
                                                                             const Identifier &identifier) {
	ExtensionRepositoryInfo result;
	result.name = identifier;
	result.repository_is_alias = true;
	return result;
}

ExtensionRepositoryInfo PEGTransformerFactory::TransformFromSourceString(PEGTransformer &transformer,
                                                                         const string &string_literal) {
	ExtensionRepositoryInfo result;
	result.name = Identifier(string_literal);
	result.repository_is_alias = false;
	return result;
}

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformUpdateExtensionsStatement(PEGTransformer &transformer,
                                                          const optional<vector<Identifier>> &identifier) {
	auto result = make_uniq<UpdateExtensionsStatement>();
	auto info = make_uniq<UpdateExtensionsInfo>();
	if (identifier) {
		info->extensions_to_update = *identifier;
	}
	result->info = std::move(info);
	return std::move(result);
}

string PEGTransformerFactory::TransformVersionNumber(PEGTransformer &transformer,
                                                     const QualifiedName &identifier_or_string_literal) {
	return identifier_or_string_literal.Name().GetIdentifierName();
}

} // namespace duckdb
