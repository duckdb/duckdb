#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

//UDF Functions to test
bool BOOL(bool a) {return a;}
bool BOOL(bool a, bool b) {return a & b;}
bool BOOL(bool a, bool b, bool c) {return a & b & c;}

int8_t INT8(int8_t a) {return a;}
int8_t INT8(int8_t a, int8_t b) {return a * b;}
int8_t INT8(int8_t a, int8_t b, int8_t c) {return a + b + c;}

int16_t INT16(int16_t a) {return a;}
int16_t INT16(int16_t a, int16_t b) {return a * b;}
int16_t INT16(int16_t a, int16_t b, int16_t c) {return a + b + c;}

int INT(int a) {return a;}
int INT(int a, int b) {return a * b;}
int INT(int a, int b, int c) {return a + b + c;}

int64_t INT64(int64_t a) {return a;}
int64_t INT64(int64_t a, int64_t b) {return a * b;}
int64_t INT64(int64_t a, int64_t b, int64_t c) {return a + b + c;}

float FLOAT(float a) {return a;}
float FLOAT(float a, float b) {return a * b;}
float FLOAT(float a, float b, float c) {return a + b + c;}

double DOUBLE(double a) {return a;}
double DOUBLE(double a, double b) {return a * b;}
double DOUBLE(double a, double b, double c) {return a + b + c;}

string_t VARCHAR(string_t a) {return a;}
string_t VARCHAR(string_t a, string_t b) {return b;}
string_t VARCHAR(string_t a, string_t b, string_t c) {return c;}

TEST_CASE("UDF functions with template", "[udf_function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	string func_name, table_name, col_type;
	//The types supported by the templated CreateFunction 
	const vector<SQLType> sql_templated_types = {SQLType::BOOLEAN, SQLType::TINYINT, SQLType::SMALLINT,
												 SQLType::INTEGER, SQLType::BIGINT, SQLType::FLOAT,
												 SQLType::DOUBLE, SQLType::VARCHAR};

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
			con.CreateFunction<bool, bool>(func_name + "_1", &BOOL);
			con.CreateFunction<bool, bool, bool>(func_name + "_2", &BOOL);
			con.CreateFunction<bool, bool, bool, bool>(func_name + "_3", &BOOL);
			break;
		}
		case SQLTypeId::TINYINT:
		{
			con.CreateFunction<int8_t, int8_t>(func_name + "_1", &INT8);
			con.CreateFunction<int8_t, int8_t, int8_t>(func_name + "_2", &INT8);
			con.CreateFunction<int8_t, int8_t, int8_t, int8_t>(func_name + "_3", &INT8);
			break;
		}
		case SQLTypeId::SMALLINT:
		{
			con.CreateFunction<int16_t, int16_t>(func_name + "_1", &INT16);
			con.CreateFunction<int16_t, int16_t, int16_t>(func_name + "_2", &INT16);
			con.CreateFunction<int16_t, int16_t, int16_t, int16_t>(func_name + "_3", &INT16);
			break;
		}
		case SQLTypeId::INTEGER:
		{
			con.CreateFunction<int32_t, int32_t>(func_name + "_1", &INT);
			con.CreateFunction<int32_t, int32_t, int32_t>(func_name + "_2", &INT);
			con.CreateFunction<int32_t, int32_t, int32_t, int32_t>(func_name + "_3", &INT);
			break;
		}
		case SQLTypeId::BIGINT:
		{
			con.CreateFunction<int64_t, int64_t>(func_name + "_1", &INT64);
			con.CreateFunction<int64_t, int64_t, int64_t>(func_name + "_2", &INT64);
			con.CreateFunction<int64_t, int64_t, int64_t, int64_t>(func_name + "_3", &INT64);
			break;
		}
		case SQLTypeId::FLOAT:
		//FIXME: there is an implicit cast to DOUBLE before calling the function: float_1(CAST[DOUBLE](a)),
		//because of that we cannot invoke such a function: float udf(float a);
//		{
//			con.CreateFunction<float, float>(func_name + "_1", &FLOAT);
//			con.CreateFunction<float, float, float>(func_name + "_2", &FLOAT);
//			con.CreateFunction<float, float, float, float>(func_name + "_3", &FLOAT);
//			break;
//		}
		case SQLTypeId::DOUBLE:
		{
			con.CreateFunction<double, double>(func_name + "_1", &DOUBLE);
			con.CreateFunction<double, double, double>(func_name + "_2", &DOUBLE);
			con.CreateFunction<double, double, double, double>(func_name + "_3", &DOUBLE);
			break;
		}
		case SQLTypeId::VARCHAR:
		{
			con.CreateFunction<string_t, string_t>(func_name + "_1", &VARCHAR);
			con.CreateFunction<string_t, string_t, string_t>(func_name + "_2", &VARCHAR);
			con.CreateFunction<string_t, string_t, string_t, string_t>(func_name + "_3", &VARCHAR);
			break;
		}
		default:
			break;
		}
	}

	SECTION("Testing UDF functions") {
		//Inserting values
		for(SQLType sql_type: sql_templated_types) {
			table_name = StringUtil::Lower(SQLTypeIdToString(sql_type.id));

			string query = "INSERT INTO " + table_name + " VALUES";
			if(sql_type == SQLType::BOOLEAN) {
				con.Query(query + "(true, true, true), (true, true, false), (false, false, false);");
			} else if(sql_type.IsNumeric()) {
				con.Query(query + "(1, 10, 100),(2, 10, 100),(3, 10, 100);");
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
				REQUIRE(CHECK_COLUMN(result, 0, {111, 112, 113}));
				
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
	SECTION("Cheking if the UDF functions are temporary") {
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

	SECTION("Cheking NULLs with UDF functions") {
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
