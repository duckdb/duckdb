#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/connect_statement.hpp"
#include "duckdb/parser/statement/disconnect_statement.hpp"

namespace duckdb {

unique_ptr<ConnectInfo> PEGTransformerFactory::TransformLocalSessionTarget(PEGTransformer &transformer) {
	auto result = make_uniq<ConnectInfo>();
	result->target_is_local = true;
	return result;
}

unique_ptr<ConnectInfo> PEGTransformerFactory::TransformStringSessionTarget(PEGTransformer &transformer,
                                                                            const string &string_literal) {
	auto result = make_uniq<ConnectInfo>();
	result->name = Identifier(string_literal);
	result->name_is_string_literal = true;
	return result;
}

unique_ptr<ConnectInfo> PEGTransformerFactory::TransformCatalogSessionTarget(PEGTransformer &transformer,
                                                                             const Identifier &catalog_name) {
	auto result = make_uniq<ConnectInfo>();
	result->name = catalog_name;
	return result;
}

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformConnectStatement(PEGTransformer &transformer,
                                                 optional<unique_ptr<ConnectInfo>> session_target) {
	auto info = make_uniq<ConnectInfo>();
	if (session_target) {
		info = std::move(*session_target);
	}
	auto result = make_uniq<ConnectStatement>();
	result->info = std::move(info);
	return std::move(result);
}

// DisconnectStatement <- 'DISCONNECT'
unique_ptr<SQLStatement> PEGTransformerFactory::TransformDisconnectStatement(PEGTransformer &transformer) {
	auto result = make_uniq<DisconnectStatement>();
	result->info = make_uniq<DisconnectInfo>();
	return std::move(result);
}

} // namespace duckdb
