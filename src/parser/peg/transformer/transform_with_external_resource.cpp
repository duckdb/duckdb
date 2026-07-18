#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/peg/ast/generic_copy_option.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/external_resource_options.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"
#include "duckdb/parser/statement/connect_statement.hpp"

namespace duckdb {

// `(ATTACH|CONNECT) TO [NEW] EXTERNAL RESOURCE <resource> [(opts)] ...` produces a normal
// ATTACH/CONNECT carrying an ExternalResourceOptions. `NEW '<type>' (opts)` provisions a fresh
// resource this attachment owns (deleter bound, DETACH/DISCONNECT reaps it); a bare `<name>` references
// an already-registered resource it only borrows.

//! NEW TEMPORARY EXTERNAL RESOURCE '<type>' [(create opts)] — provision a fresh resource.
unique_ptr<ExternalResourceOptions> PEGTransformerFactory::TransformExternalResourceCreateClause(
    PEGTransformer &transformer, const string &string_literal,
    const optional<vector<GenericCopyOption>> &external_resource_options) {
	auto result = make_uniq<ExternalResourceOptions>();
	result->provider = string_literal;
	if (external_resource_options) {
		for (const auto &opt : *external_resource_options) {
			if (!opt.expression && opt.children.empty()) {
				// Bare flag (e.g. `(SPOT)`): boolean true, mirroring ATTACH/CONNECT option handling.
				result->parsed_params[opt.name.GetIdentifierName()] = make_uniq<ConstantExpression>(Value(true));
			} else {
				result->parsed_params[opt.name.GetIdentifierName()] = opt.GetFirstChildOrExpression();
			}
		}
	}
	return result;
}

//! EXTERNAL RESOURCE <name> — reference an already-registered resource.
unique_ptr<ExternalResourceOptions>
PEGTransformerFactory::TransformExternalResourceReferenceClause(PEGTransformer &transformer, const Identifier &col_id) {
	auto result = make_uniq<ExternalResourceOptions>();
	result->reference_name = col_id.GetIdentifierName();
	return result;
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformAttachToExternalResource(
    PEGTransformer &transformer, unique_ptr<ExternalResourceOptions> external_resource_source,
    const optional<Identifier> &attach_alias, const optional<vector<GenericCopyOption>> &attach_options) {
	auto result = make_uniq<AttachStatement>();
	auto info = make_uniq<AttachInfo>();
	info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	if (attach_alias) {
		info->name = Identifier(*attach_alias);
	}
	info->external_resource = std::move(external_resource_source);
	if (attach_options) {
		SplitGenericOptions(*attach_options, info->parsed_options, info->options, "ATTACH");
	}
	result->info = std::move(info);
	return std::move(result);
}

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformConnectToExternalResource(PEGTransformer &transformer,
                                                          unique_ptr<ExternalResourceOptions> external_resource_source,
                                                          const optional<vector<GenericCopyOption>> &attach_options) {
	auto result = make_uniq<ConnectStatement>();
	auto info = make_uniq<ConnectInfo>();
	info->external_resource = std::move(external_resource_source);
	if (attach_options) {
		SplitGenericOptions(*attach_options, info->parsed_options, info->options, "CONNECT");
	}
	result->info = std::move(info);
	return std::move(result);
}

} // namespace duckdb
