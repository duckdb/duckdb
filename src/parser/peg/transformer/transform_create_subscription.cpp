#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

// CreateSubscriptionStatement <- 'CREATE' 'SUBSCRIPTION' Identifier SubscriptionConnection?
// SubscriptionConnection      <- 'CONNECTION' StringLiteral SubscriptionPublication?
// SubscriptionPublication     <- 'PUBLICATION' List(Identifier)
//
// Transforms to:  PRAGMA create_subscription('sub_name')
//                 PRAGMA create_subscription('sub_name', connection := 'connstr')
//                 PRAGMA create_subscription('sub_name', connection := 'connstr', publications := 'pub1,pub2')
//
// The pragma handler itself is not yet implemented (catalog work is out of scope here).
// Parsing must succeed so that higher-level code can detect the statement type.
unique_ptr<SQLStatement> PEGTransformerFactory::TransformCreateSubscriptionStatement(PEGTransformer &transformer,
                                                                                     ParseResult &parse_result) {
	// Layout of the list children (literals are counted as positions):
	//  0: 'CREATE'
	//  1: 'SUBSCRIPTION'
	//  2: Identifier  (subscription name)
	//  3: SubscriptionConnection?
	auto &list_pr = parse_result.Cast<ListParseResult>();

	auto sub_name = list_pr.Child<IdentifierParseResult>(2).identifier;

	auto result = make_uniq<PragmaStatement>();
	result->info->name = "create_subscription";
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value(sub_name)));

	auto &conn_opt = list_pr.Child<OptionalParseResult>(3);
	if (conn_opt.HasResult()) {
		// SubscriptionConnection <- 'CONNECTION' StringLiteral SubscriptionPublication?
		// List children:
		//  0: 'CONNECTION'
		//  1: StringLiteral (connection string)
		//  2: SubscriptionPublication?
		auto &conn_list = conn_opt.GetResult().Cast<ListParseResult>();
		auto connection_str = conn_list.Child<StringLiteralParseResult>(1).result;
		result->info->named_parameters["connection"] = make_uniq<ConstantExpression>(Value(connection_str));

		auto &pub_opt = conn_list.Child<OptionalParseResult>(2);
		if (pub_opt.HasResult()) {
			// SubscriptionPublication <- 'PUBLICATION' List(Identifier)
			// List children:
			//  0: 'PUBLICATION'
			//  1: the List(...) inline expansion — a ListParseResult of Identifier results
			auto &pub_list = pub_opt.GetResult().Cast<ListParseResult>();
			auto pub_results = ExtractParseResultsFromList(pub_list.Child<ListParseResult>(1));
			string publications_value;
			for (idx_t i = 0; i < pub_results.size(); i++) {
				if (i > 0) {
					publications_value += ",";
				}
				publications_value += pub_results[i].get().Cast<IdentifierParseResult>().identifier;
			}
			result->info->named_parameters["publications"] = make_uniq<ConstantExpression>(Value(publications_value));
		}
	}

	return std::move(result);
}

} // namespace duckdb
