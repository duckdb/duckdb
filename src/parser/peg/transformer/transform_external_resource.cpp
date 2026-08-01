#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/peg/ast/generic_copy_option.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/external_resource_statement.hpp"

namespace duckdb {

//! Fold `(k v, ...)` create params onto the statement. A bare flag binds to boolean true, mirroring
//! ATTACH/CONNECT TO EXTERNAL RESOURCE option handling.
static void ApplyOptions(const optional<vector<GenericCopyOption>> &options, ExternalResourceStatement &stmt) {
	if (!options) {
		return;
	}
	for (const auto &opt : *options) {
		if (!opt.expression && opt.children.empty()) {
			stmt.options[opt.name.GetIdentifierName()] = make_uniq<ConstantExpression>(Value(true));
		} else {
			stmt.options[opt.name.GetIdentifierName()] = opt.GetFirstChildOrExpression();
		}
	}
}

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformCreateExternalResourceStmt(PEGTransformer &transformer, const string &string_literal,
                                                           const optional<Identifier> &attach_alias,
                                                           const optional<vector<GenericCopyOption>> &attach_options) {
	auto result = make_uniq<ExternalResourceStatement>(ExternalResourceOperation::CREATE);
	result->type = string_literal;
	if (attach_alias) {
		result->name = Identifier(*attach_alias);
	}
	ApplyOptions(attach_options, *result);
	return std::move(result);
}

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformRegisterExternalResourceStmt(PEGTransformer &transformer, const string &string_literal,
                                                             const optional<Identifier> &attach_alias,
                                                             unique_ptr<ParsedExpression> expression) {
	auto result = make_uniq<ExternalResourceStatement>(ExternalResourceOperation::REGISTER);
	result->type = string_literal;
	if (attach_alias) {
		result->name = Identifier(*attach_alias);
	}
	result->handle = std::move(expression);
	return std::move(result);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformDestroyExternalResourceStmt(PEGTransformer &transformer,
                                                                                     const Identifier &col_id) {
	auto result = make_uniq<ExternalResourceStatement>(ExternalResourceOperation::DESTROY);
	result->name = col_id;
	return std::move(result);
}

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformShowExternalResourcesStmt(PEGTransformer &transformer,
                                                          const optional<bool> &show_all_modifier) {
	auto result = make_uniq<ExternalResourceStatement>(ExternalResourceOperation::SHOW);
	result->all = show_all_modifier.has_value();
	return std::move(result);
}

bool PEGTransformerFactory::TransformShowAllModifier(PEGTransformer &transformer) {
	return true;
}

} // namespace duckdb
