#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/external_resource_options.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"
#include "duckdb/parser/statement/connect_statement.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

TEST_CASE("Parse ATTACH TO NEW TEMPORARY EXTERNAL RESOURCE + ToString roundtrip", "[parse_external_resource]") {
	Parser parser;
	parser.ParseQuery(
	    "ATTACH TO NEW TEMPORARY EXTERNAL RESOURCE 'quack@local' (INSTANCE 'r7i.16xlarge', REGION 'eu-west-1') "
	    "AS my_db (READ_ONLY)");
	REQUIRE(parser.statements.size() == 1);
	REQUIRE(parser.statements[0]->type == StatementType::ATTACH_STATEMENT);

	auto &attach = parser.statements[0]->Cast<AttachStatement>();
	REQUIRE(attach.info->external_resource != nullptr);
	auto &er = *attach.info->external_resource;
	// Create form: the type is separated from the create params; not a reference.
	REQUIRE(er.reference_name.empty());
	// The type is a string literal, so the parser resolves it directly — no bind step.
	REQUIRE(er.provider == "quack@local");
	REQUIRE(er.parsed_params.size() == 2);
	REQUIRE(er.parsed_params.find("INSTANCE") != er.parsed_params.end());
	REQUIRE(er.parsed_params.find("REGION") != er.parsed_params.end());
	// the attach alias is the database name.
	REQUIRE(attach.info->name.GetIdentifierName() == "my_db");

	// ToString renders back to the ATTACH TO NEW TEMPORARY EXTERNAL RESOURCE surface.
	auto str = attach.info->ToString();
	REQUIRE(StringUtil::Contains(str, "ATTACH TO NEW TEMPORARY EXTERNAL RESOURCE"));
	REQUIRE(StringUtil::Contains(str, "quack@local"));
	REQUIRE(StringUtil::Contains(str, "AS my_db"));

	// Roundtrip: re-parsing the rendered SQL yields the same statement + resource.
	Parser reparser;
	reparser.ParseQuery(str);
	REQUIRE(reparser.statements.size() == 1);
	REQUIRE(reparser.statements[0]->type == StatementType::ATTACH_STATEMENT);
	auto &re = reparser.statements[0]->Cast<AttachStatement>();
	REQUIRE(re.info->external_resource != nullptr);
	REQUIRE(re.info->external_resource->parsed_params.size() == 2);
	REQUIRE(re.info->name.GetIdentifierName() == "my_db");
	REQUIRE(StringUtil::Contains(re.info->ToString(), "ATTACH TO NEW TEMPORARY EXTERNAL RESOURCE"));
}

TEST_CASE("Parse ATTACH TO EXTERNAL RESOURCE (reference) + ToString roundtrip", "[parse_external_resource]") {
	Parser parser;
	parser.ParseQuery("ATTACH TO EXTERNAL RESOURCE beefy AS my_db");
	REQUIRE(parser.statements.size() == 1);
	REQUIRE(parser.statements[0]->type == StatementType::ATTACH_STATEMENT);

	auto &attach = parser.statements[0]->Cast<AttachStatement>();
	REQUIRE(attach.info->external_resource != nullptr);
	auto &er = *attach.info->external_resource;
	// Reference form: a bare identifier names a registered resource; no type/params.
	REQUIRE(er.reference_name == "beefy");
	REQUIRE(er.provider.empty());
	REQUIRE(er.parsed_params.empty());
	REQUIRE(attach.info->name.GetIdentifierName() == "my_db");

	auto str = attach.info->ToString();
	REQUIRE(StringUtil::Contains(str, "ATTACH TO EXTERNAL RESOURCE beefy"));
	REQUIRE(!StringUtil::Contains(str, "CREATE"));

	Parser reparser;
	reparser.ParseQuery(str);
	REQUIRE(reparser.statements.size() == 1);
	auto &re = reparser.statements[0]->Cast<AttachStatement>();
	REQUIRE(re.info->external_resource->reference_name == "beefy");
}

TEST_CASE("Parse CONNECT TO NEW TEMPORARY EXTERNAL RESOURCE + ToString roundtrip", "[parse_external_resource]") {
	Parser parser;
	parser.ParseQuery("CONNECT TO NEW TEMPORARY EXTERNAL RESOURCE 'quack@local' (region 'eu-west-1') (token 'abc')");
	REQUIRE(parser.statements.size() == 1);
	REQUIRE(parser.statements[0]->type == StatementType::CONNECT_STATEMENT);

	auto &connect = parser.statements[0]->Cast<ConnectStatement>();
	REQUIRE(connect.info->external_resource != nullptr);
	REQUIRE(connect.info->external_resource->parsed_params.size() == 1);

	auto str = connect.info->ToString();
	REQUIRE(StringUtil::Contains(str, "CONNECT TO NEW TEMPORARY EXTERNAL RESOURCE"));
	REQUIRE(StringUtil::Contains(str, "quack@local"));

	Parser reparser;
	reparser.ParseQuery(str);
	REQUIRE(reparser.statements.size() == 1);
	REQUIRE(reparser.statements[0]->type == StatementType::CONNECT_STATEMENT);
	REQUIRE(reparser.statements[0]->Cast<ConnectStatement>().info->external_resource != nullptr);
	REQUIRE(reparser.statements[0]->Cast<ConnectStatement>().info->ToString() == str);
}
