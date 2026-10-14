#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

struct NamedParameterMetadataFunction {
	static duckdb::unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &,
	                                             duckdb::vector<LogicalType> &return_types,
	                                             duckdb::vector<string> &names) {
		return_types = {LogicalType::BIGINT};
		names = {"x"};
		return make_uniq<TableFunctionData>();
	}

	static void Scan(ClientContext &, TableFunctionInput &, DataChunk &output) {
		output.SetCardinality(0);
	}

	//! A table function with named parameters of four different types, registered the way an extension does it:
	//! with a FunctionDescription naming every parameter, positional and named.
	static void Register(Connection &con, const string &name, bool describe_named_parameters) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function(name, {LogicalType::BIGINT}, Scan, Bind);
		function.named_parameters["greeting"] = LogicalType::VARCHAR;
		function.named_parameters["multiplier"] = LogicalType::BIGINT;
		function.named_parameters["scale"] = LogicalType::DOUBLE;
		function.named_parameters["enabled"] = LogicalType::BOOLEAN;

		FunctionDescription description;
		description.description = "named parameter metadata test";
		description.parameter_names = {"count"};
		description.parameter_types = {LogicalType::BIGINT};
		if (describe_named_parameters) {
			// Listed in declaration order, as an extension naturally writes them - not in the unordered map's
			// iteration order, which is what duckdb_functions() uses for the types.
			description.parameter_names.insert(description.parameter_names.end(),
			                                   {"greeting", "multiplier", "scale", "enabled"});
			description.parameter_types.insert(
			    description.parameter_types.end(),
			    {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::DOUBLE, LogicalType::BOOLEAN});
		}

		CreateTableFunctionInfo info(function);
		info.descriptions.push_back(std::move(description));
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

void RequireCorrectPairing(Connection &con, const string &function_name) {
	auto result = con.Query("SELECT name, type FROM (SELECT unnest(parameters) AS name, "
	                        "unnest(parameter_types) AS type FROM duckdb_functions() WHERE function_name = '" +
	                        function_name + "') ORDER BY name");
	REQUIRE(!result->HasError());
	REQUIRE(CHECK_COLUMN(result, 0, {"count", "enabled", "greeting", "multiplier", "scale"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"BIGINT", "BOOLEAN", "VARCHAR", "BIGINT", "DOUBLE"}));
}

} // namespace

TEST_CASE("duckdb_functions() pairs each named parameter with its own type", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("description names every parameter") {
		// Names used to come from the description and types from iterating named_parameters - an unordered map -
		// zipped by position. Unless the description happened to list the named parameters in hash order, names
		// landed on the wrong types.
		NamedParameterMetadataFunction::Register(con, "named_param_metadata_full", true);
		RequireCorrectPairing(con, "named_param_metadata_full");
	}

	SECTION("description names only the positional parameters") {
		// The named parameters used to be reported as col1..col4 in this case.
		NamedParameterMetadataFunction::Register(con, "named_param_metadata_positional", false);
		RequireCorrectPairing(con, "named_param_metadata_positional");
	}

	SECTION("named parameters are listed in a stable, sorted order") {
		NamedParameterMetadataFunction::Register(con, "named_param_metadata_order", true);
		auto result = con.Query("SELECT parameters, parameter_types FROM duckdb_functions() "
		                        "WHERE function_name = 'named_param_metadata_order'");
		REQUIRE(!result->HasError());
		REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({"count", "enabled", "greeting", "multiplier", "scale"})}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::LIST({"BIGINT", "BOOLEAN", "VARCHAR", "BIGINT", "DOUBLE"})}));
	}
}
