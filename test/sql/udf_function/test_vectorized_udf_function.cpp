#include "catch.hpp"
#include "test_helpers.hpp"
#include "udf_functions_to_test.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Vectorized UDF functions", "[udf_function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	string func_name, table_name, col_type;
	//The types supported by the templated CreateVectorizedFunction
	const vector<SQLType> sql_templated_types = {SQLType::BOOLEAN, SQLType::TINYINT, SQLType::SMALLINT,
												 SQLType::INTEGER, SQLType::BIGINT, SQLType::FLOAT,
												 SQLType::DOUBLE}; //, SQLType::VARCHAR

	//Creating the tables
	for(SQLType sql_type: sql_templated_types) {
		col_type = SQLTypeIdToString(sql_type.id);
		table_name = StringUtil::Lower(col_type);

		con.Query("CREATE TABLE " + table_name + " (a " + col_type +
											     ", b " + col_type +
												 ", c " + col_type + ")");
	}

	//Create the UDF functions into the catalog
	for(SQLType sql_type: sql_templated_types) {
		func_name = StringUtil::Lower(SQLTypeIdToString(sql_type.id));

		switch(sql_type.id) {
		case SQLTypeId::BOOLEAN:
		{
			con.CreateVectorizedFunction<bool, bool>(func_name + "_1", &udf_unary_function<bool>);
			con.CreateVectorizedFunction<bool, bool, bool>(func_name + "_2", &udf_binary_function<bool>);
			con.CreateVectorizedFunction<bool, bool, bool, bool>(func_name + "_3", &udf_ternary_function<bool>);
			break;
		}
		case SQLTypeId::TINYINT:
		{
			con.CreateVectorizedFunction<int8_t, int8_t>(func_name + "_1", &udf_unary_function<int8_t>);
			con.CreateVectorizedFunction<int8_t, int8_t, int8_t>(func_name + "_2", &udf_binary_function<int8_t>);
			con.CreateVectorizedFunction<int8_t, int8_t, int8_t, int8_t>(func_name + "_3", &udf_ternary_function<int8_t>);
			break;
		}
		case SQLTypeId::SMALLINT:
		{
			con.CreateVectorizedFunction<int16_t, int16_t>(func_name + "_1", &udf_unary_function<int16_t>);
			con.CreateVectorizedFunction<int16_t, int16_t, int16_t>(func_name + "_2", &udf_binary_function<int16_t>);
			con.CreateVectorizedFunction<int16_t, int16_t, int16_t, int16_t>(func_name + "_3", &udf_ternary_function<int16_t>);
			break;
		}
		case SQLTypeId::INTEGER:
		{
			con.CreateVectorizedFunction<int, int>(func_name + "_1", &udf_unary_function<int>);
			con.CreateVectorizedFunction<int, int, int>(func_name + "_2", &udf_binary_function<int>);
			con.CreateVectorizedFunction<int, int, int, int>(func_name + "_3", &udf_ternary_function<int>);
			break;
		}
		case SQLTypeId::BIGINT:
		{
			con.CreateVectorizedFunction<int64_t, int64_t>(func_name + "_1", &udf_unary_function<int64_t>);
			con.CreateVectorizedFunction<int64_t, int64_t, int64_t>(func_name + "_2", &udf_binary_function<int64_t>);
			con.CreateVectorizedFunction<int64_t, int64_t, int64_t, int64_t>(func_name + "_3", &udf_ternary_function<int64_t>);
			break;
		}
		case SQLTypeId::FLOAT:
		case SQLTypeId::DOUBLE:
		{
			con.CreateVectorizedFunction<double, double>(func_name + "_1", &udf_unary_function<double>);
			con.CreateVectorizedFunction<double, double, double>(func_name + "_2", &udf_binary_function<double>);
			con.CreateVectorizedFunction<double, double, double, double>(func_name + "_3", &udf_ternary_function<double>);
			break;
		}
		case SQLTypeId::VARCHAR:
		{
			con.CreateVectorizedFunction<string_t, string_t>(func_name + "_1", &udf_unary_function<char *>);
			con.CreateVectorizedFunction<string_t, string_t, string_t>(func_name + "_2", &udf_binary_function<char *>);
			con.CreateVectorizedFunction<string_t, string_t, string_t, string_t>(func_name + "_3", &udf_ternary_function<char *>);
			break;
		}
		default:
			break;
		}
	}

	SECTION("Testing Vectorized UDF functions") {
		//Inserting values
		for(SQLType sql_type: sql_templated_types) {
			table_name = StringUtil::Lower(SQLTypeIdToString(sql_type.id));

			string query = "INSERT INTO " + table_name + " VALUES";
			if(sql_type == SQLType::BOOLEAN) {
				con.Query(query + "(true, true, true), (true, true, false), (false, false, false);");
			} else if(sql_type.IsNumeric()) {
				con.Query(query + "(1, 10, 101),(2, 20, 102),(3, 30, 103);");
			} else if(sql_type == SQLType::VARCHAR) {
				con.Query(query + "('a', 'b', 'c'),('a', 'b', 'c'),('a', 'b', 'c');");
			}
		}
		
		//Running the UDF functions and checking the results
		for(SQLType sql_type: sql_templated_types) {
			table_name = StringUtil::Lower(SQLTypeIdToString(sql_type.id));
			func_name = table_name;
			if(sql_type.IsNumeric()) {
				result = con.Query("SELECT " + func_name + "_1(a) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
				
				result = con.Query("SELECT " + func_name + "_2(a, b) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {10, 20, 30}));
				
				result = con.Query("SELECT " + func_name + "_3(a, b, c) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {101, 102, 103}));
				
			} else if(sql_type == SQLType::BOOLEAN) {
				result = con.Query("SELECT " + func_name + "_1(a) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {true, true, false}));
				
				result = con.Query("SELECT " + func_name + "_2(a, b) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {true, true, false}));
				
				result = con.Query("SELECT " + func_name + "_3(a, b, c) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {true, false, false}));
				
			} else if(sql_type == SQLType::VARCHAR) {
				result = con.Query("SELECT " + func_name + "_1(a) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {"a", "a", "a"}));
				
				result = con.Query("SELECT " + func_name + "_2(a, b) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {"b", "b", "b"}));
				
				result = con.Query("SELECT " + func_name + "_3(a, b, c) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {"c", "c", "c"}));
			}
		}
	}
	SECTION("Cheking if the Vectorized UDF functions are temporary") {
		Connection con_test(db);
		con_test.EnableQueryVerification();
		for(SQLType sql_type: sql_templated_types) {
			table_name = StringUtil::Lower(SQLTypeIdToString(sql_type.id));
			func_name = table_name;

			REQUIRE_FAIL(con_test.Query("SELECT " + func_name + "_1(a) FROM " + table_name));

			REQUIRE_FAIL(con_test.Query("SELECT " + func_name + "_2(a, b) FROM " + table_name));

			REQUIRE_FAIL(con_test.Query("SELECT " + func_name + "_3(a, b, c) FROM " + table_name));
		}
	}

	SECTION("Cheking NULLs with Vectorized UDF functions") {
		for(SQLType sql_type: sql_templated_types) {
			table_name = StringUtil::Lower(SQLTypeIdToString(sql_type.id));
			func_name = table_name;

			//Deleting old values
			REQUIRE_NO_FAIL(con.Query("DELETE FROM " + table_name));

			//Inserting NULLs
			string query = "INSERT INTO " + table_name + " VALUES";
			con.Query(query + "(NULL, NULL, NULL), (NULL, NULL, NULL), (NULL, NULL, NULL);");

			//Testing NULLs
			result = con.Query("SELECT " + func_name + "_1(a) FROM " + table_name);
			REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr)}));

			result = con.Query("SELECT " + func_name + "_2(a, b) FROM " + table_name);
			REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr)}));

			result = con.Query("SELECT " + func_name + "_3(a, b, c) FROM " + table_name);
			REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), Value(nullptr), Value(nullptr)}));
		}
	}
}
