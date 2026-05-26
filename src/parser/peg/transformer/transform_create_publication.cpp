#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

// CreatePublicationStatement <- 'CREATE' 'PUBLICATION' Identifier PublicationFor?
// PublicationFor           <- 'FOR' (PublicationForAllTables / PublicationForTable)
// PublicationForAllTables  <- 'ALL' 'TABLES'
// PublicationForTable      <- 'TABLE' List(BaseTableName)
//
// Transforms to:  PRAGMA create_publication('pub_name')
//                 PRAGMA create_publication('pub_name', for_all_tables := true)
//                 PRAGMA create_publication('pub_name', tables := '[schema.]t1,[schema.]t2,...')
//
// The pragma handler itself is not yet implemented (catalog work is out of scope here).
// Parsing must succeed so that higher-level code can detect the statement type.
unique_ptr<SQLStatement> PEGTransformerFactory::TransformCreatePublicationStatement(PEGTransformer &transformer,
                                                                                    ParseResult &parse_result) {
	// Layout of the list children (literals are counted as positions):
	//  0: 'CREATE'
	//  1: 'PUBLICATION'
	//  2: Identifier  (publication name)
	//  3: PublicationFor?
	auto &list_pr = parse_result.Cast<ListParseResult>();

	auto pub_name = list_pr.Child<IdentifierParseResult>(2).identifier;

	auto result = make_uniq<PragmaStatement>();
	result->info->name = "create_publication";
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value(pub_name)));

	auto &for_opt = list_pr.Child<OptionalParseResult>(3);
	if (for_opt.HasResult()) {
		// PublicationFor <- 'FOR' (PublicationForAllTables / PublicationForTable)
		// List children:
		//  0: 'FOR'
		//  1: Choice between PublicationForAllTables and PublicationForTable
		auto &for_list = for_opt.GetResult().Cast<ListParseResult>();
		auto &for_choice = for_list.Child<ChoiceParseResult>(1).GetResult();

		if (for_choice.name == "PublicationForAllTables") {
			// FOR ALL TABLES
			result->info->named_parameters["for_all_tables"] = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
		} else {
			// FOR TABLE t1, t2, ...
			// PublicationForTable <- 'TABLE' List(BaseTableName)
			// List children:
			//  0: 'TABLE'
			//  1: the List(...) inline expansion — a ListParseResult of BaseTableName results
			auto table_results =
			    ExtractParseResultsFromList(for_choice.Cast<ListParseResult>().Child<ListParseResult>(1));
			string tables_value;
			for (idx_t i = 0; i < table_results.size(); i++) {
				auto base_ref = transformer.Transform<unique_ptr<BaseTableRef>>(table_results[i].get());
				if (i > 0) {
					tables_value += ",";
				}
				if (!base_ref->schema_name.empty() && base_ref->schema_name != INVALID_SCHEMA) {
					tables_value += base_ref->schema_name + "." + base_ref->table_name;
				} else {
					tables_value += base_ref->table_name;
				}
			}
			result->info->named_parameters["tables"] = make_uniq<ConstantExpression>(Value(tables_value));
		}
	}

	return std::move(result);
}

} // namespace duckdb
