#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace duckdb;
using namespace std;

//UDF Functions to test
bool BOOL_1(bool a) {return a;}
bool BOOL_2(bool a, bool b) {return a & b;}
bool BOOL_3(bool a, bool b, bool c) {return a & b & c;}

int8_t INT8_1(int8_t a) {return a;}
int8_t INT8_2(int8_t a, int8_t b) {return a * b;}
int8_t INT8_3(int8_t a, int8_t b, int8_t c) {return a + b + c;}

int16_t INT16_1(int16_t a) {return a;}
int16_t INT16_2(int16_t a, int16_t b) {return a * b;}
int16_t INT16_3(int16_t a, int16_t b, int16_t c) {return a + b + c;}

int DATE_1(int a) {return a;}
int DATE_2(int a, int b) {return b;}
int DATE_3(int a, int b, int c) {return c;}

int TIME_1(int a) {return a;}
int TIME_2(int a, int b) {return b;}
int TIME_3(int a, int b, int c) {return c;}

int INT_1(int a) {return a;}
int INT_2(int a, int b) {return a * b;}
int INT_3(int a, int b, int c) {return a + b + c;}

int64_t INT64_1(int64_t a) {return a;}
int64_t INT64_2(int64_t a, int64_t b) {return a * b;}
int64_t INT64_3(int64_t a, int64_t b, int64_t c) {return a + b + c;}

int64_t TIMESTAMP_1(int64_t a) {return a;}
int64_t TIMESTAMP_2(int64_t a, int64_t b) {return b;}
int64_t TIMESTAMP_3(int64_t a, int64_t b, int64_t c) {return c;}

float FLOAT_1(float a) {return a;}
float FLOAT_2(float a, float b) {return a * b;}
float FLOAT_3(float a, float b, float c) {return a + b + c;}

double DOUBLE_1(double a) {return a;}
double DOUBLE_2(double a, double b) {return a * b;}
double DOUBLE_3(double a, double b, double c) {return a + b + c;}

double DECIMAL_1(double a) {return a;}
double DECIMAL_2(double a, double b) {return a * b;}
double DECIMAL_3(double a, double b, double c) {return a + b + c;}

string_t VARCHAR_1(string_t a) {return a;}
string_t VARCHAR_2(string_t a, string_t b) {return b;}
string_t VARCHAR_3(string_t a, string_t b, string_t c) {return c;}

TEST_CASE("UDF functions with arguments", "[udf_function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	string func_name, table_name, col_type;
	//The types supported by the argumented CreateFunction
	const vector<SQLTypeId> all_sql_types = {SQLTypeId::BOOLEAN, SQLTypeId::TINYINT, SQLTypeId::SMALLINT,
											 SQLTypeId::DATE, SQLTypeId::TIME, SQLTypeId::INTEGER,
											 SQLTypeId::BIGINT, SQLTypeId::TIMESTAMP, SQLTypeId::FLOAT,
											 SQLTypeId::DOUBLE, SQLTypeId::DECIMAL, SQLTypeId::VARCHAR,
											 SQLTypeId::BLOB};

	//Creating the tables
	for(SQLType sql_type: all_sql_types) {
		col_type = SQLTypeIdToString(sql_type.id);
		table_name = StringUtil::Lower(col_type);

		con.Query("CREATE TABLE " + table_name + " (a " + col_type +
											     ", b " + col_type +
												 ", c " + col_type + ")");
	}

	//Creating the UDF functions into the catalog
	for(SQLType sql_type: all_sql_types) {
		func_name = StringUtil::Lower(SQLTypeIdToString(sql_type.id));

		switch(sql_type.id) {
		case SQLTypeId::BOOLEAN:
		{
			con.CreateFunction(func_name + "_1", {SQLType::BOOLEAN}, SQLType::BOOLEAN, (void *)&BOOL_1);
			con.CreateFunction(func_name + "_2", {SQLType::BOOLEAN, SQLType::BOOLEAN},
												  SQLType::BOOLEAN, (void *)&BOOL_2);
			con.CreateFunction(func_name + "_3", {SQLType::BOOLEAN, SQLType::BOOLEAN, SQLType::BOOLEAN},
					  	  	  	  	  	  	  	  SQLType::BOOLEAN, (void *)&BOOL_3);
			break;
		}
		case SQLTypeId::TINYINT:
		{
			con.CreateFunction(func_name + "_1", {SQLType::TINYINT}, SQLType::TINYINT, (void *)&INT8_1);
			con.CreateFunction(func_name + "_2", {SQLType::TINYINT, SQLType::TINYINT},
												  SQLType::TINYINT, (void *)&INT8_2);
			con.CreateFunction(func_name + "_3", {SQLType::TINYINT, SQLType::TINYINT, SQLType::TINYINT},
					  	  	  	  	  	  	  	  SQLType::TINYINT, (void *)&INT8_3);
			break;
		}
		case SQLTypeId::SMALLINT:
		{
			con.CreateFunction(func_name + "_1", {SQLType::SMALLINT}, SQLType::SMALLINT, (void *)&INT16_1);
			con.CreateFunction(func_name + "_2", {SQLType::SMALLINT, SQLType::SMALLINT},
												  SQLType::SMALLINT, (void *)&INT16_2);
			con.CreateFunction(func_name + "_3", {SQLType::SMALLINT, SQLType::SMALLINT, SQLType::SMALLINT},
					  	  	  	  	  	  	  	  SQLType::SMALLINT, (void *)&INT16_3);
			break;
		}
		case SQLTypeId::DATE:
		{
			con.CreateFunction(func_name + "_1", {SQLType::DATE}, SQLType::DATE, (void *)&DATE_1);
			con.CreateFunction(func_name + "_2", {SQLType::DATE, SQLType::DATE},
												  SQLType::DATE, (void *)&DATE_2);
			con.CreateFunction(func_name + "_3", {SQLType::DATE, SQLType::DATE, SQLType::DATE},
					  	  	  	  	  	  	  	  SQLType::DATE, (void *)&DATE_3);
			break;
		}
		case SQLTypeId::TIME:
		{
			con.CreateFunction(func_name + "_1", {SQLType::TIME}, SQLType::TIME, (void *)&TIME_1);
			con.CreateFunction(func_name + "_2", {SQLType::TIME, SQLType::TIME},
												  SQLType::TIME, (void *)&TIME_2);
			con.CreateFunction(func_name + "_3", {SQLType::TIME, SQLType::TIME, SQLType::TIME},
					  	  	  	  	  	  	  	  SQLType::TIME, (void *)&TIME_3);
			break;
		}
		case SQLTypeId::INTEGER:
		{
			con.CreateFunction(func_name + "_1", {SQLType::INTEGER}, SQLType::INTEGER, (void *)&INT_1);
			con.CreateFunction(func_name + "_2", {SQLType::INTEGER, SQLType::INTEGER},
												  SQLType::INTEGER, (void *)&INT_2);
			con.CreateFunction(func_name + "_3", {SQLType::INTEGER, SQLType::INTEGER, SQLType::INTEGER},
					  	  	  	  	  	  	  	  SQLType::INTEGER, (void *)&INT_3);
			break;
		}
		case SQLTypeId::BIGINT:
		{
			con.CreateFunction(func_name + "_1", {SQLType::BIGINT}, SQLType::BIGINT, (void *)&INT64_1);
			con.CreateFunction(func_name + "_2", {SQLType::BIGINT, SQLType::BIGINT},
												  SQLType::BIGINT, (void *)&INT64_2);
			con.CreateFunction(func_name + "_3", {SQLType::BIGINT, SQLType::BIGINT, SQLType::BIGINT},
					  	  	  	  	  	  	  	  SQLType::BIGINT, (void *)&INT64_3);
			break;
		}
		case SQLTypeId::TIMESTAMP:
		{
			con.CreateFunction(func_name + "_1", {SQLType::TIMESTAMP}, SQLType::TIMESTAMP, (void *)&TIMESTAMP_1);
			con.CreateFunction(func_name + "_2", {SQLType::TIMESTAMP, SQLType::TIMESTAMP},
												  SQLType::TIMESTAMP, (void *)&TIMESTAMP_2);
			con.CreateFunction(func_name + "_3", {SQLType::TIMESTAMP, SQLType::TIMESTAMP, SQLType::TIMESTAMP},
					  	  	  	  	  	  	  	  SQLType::TIMESTAMP, (void *)&TIMESTAMP_3);
			break;
		}
		case SQLTypeId::FLOAT:
		//FIXME: there is an implicit cast to double before calling the function
		// float_1(CAST[DOUBLE](a))
//		{
//			con.CreateFunction<float, float>(func_name + "_1", &FLOAT);
//			con.CreateFunction<float, float, float>(func_name + "_2", &FLOAT);
//			con.CreateFunction<float, float, float, float>(func_name + "_3", &FLOAT);
//			break;
//		}
		case SQLTypeId::DOUBLE:
		{
			con.CreateFunction(func_name + "_1", {SQLType::DOUBLE}, SQLType::DOUBLE, (void *)&DOUBLE_1);
			con.CreateFunction(func_name + "_2", {SQLType::DOUBLE, SQLType::DOUBLE},
												  SQLType::DOUBLE, (void *)&DOUBLE_2);
			con.CreateFunction(func_name + "_3", {SQLType::DOUBLE, SQLType::DOUBLE, SQLType::DOUBLE},
					  	  	  	  	  	  	  	  SQLType::DOUBLE, (void *)&DOUBLE_3);
			break;
		}
		case SQLTypeId::DECIMAL:
		{
			con.CreateFunction(func_name + "_1", {SQLType::DOUBLE}, SQLType::DOUBLE, (void *)&DECIMAL_1);
			con.CreateFunction(func_name + "_2", {SQLType::DOUBLE, SQLType::DOUBLE},
												  SQLType::DOUBLE, (void *)&DECIMAL_2);
			con.CreateFunction(func_name + "_3", {SQLType::DOUBLE, SQLType::DOUBLE, SQLType::DOUBLE},
					  	  	  	  	  	  	  	  SQLType::DOUBLE, (void *)&DECIMAL_3);
			break;
		}

		case SQLTypeId::VARCHAR:
		{
			con.CreateFunction(func_name + "_1", {SQLType::VARCHAR}, SQLType::VARCHAR, (void *)&VARCHAR_1);
			con.CreateFunction(func_name + "_2", {SQLType::VARCHAR, SQLType::VARCHAR},
												  SQLType::VARCHAR, (void *)&VARCHAR_2);
			con.CreateFunction(func_name + "_3", {SQLType::VARCHAR, SQLType::VARCHAR, SQLType::VARCHAR},
					  	  	  	  	  	  	  	  SQLType::VARCHAR, (void *)&VARCHAR_3);
			break;
		}
		case SQLTypeId::BLOB:
		{
			con.CreateFunction(func_name + "_1", {SQLType::BLOB}, SQLType::BLOB, (void *)&VARCHAR_1);
			con.CreateFunction(func_name + "_2", {SQLType::BLOB, SQLType::BLOB},
												  SQLType::BLOB, (void *)&VARCHAR_2);
			con.CreateFunction(func_name + "_3", {SQLType::BLOB, SQLType::BLOB, SQLType::BLOB},
					  	  	  	  	  	  	  	  SQLType::BLOB, (void *)&VARCHAR_3);
			break;
		}
//		case SQLTypeId::VARBINARY:
		default:
			break;
		}
	}

	SECTION("Testing UDF functions") {
		//Inserting values
		for(SQLType sql_type: all_sql_types) {
			table_name = StringUtil::Lower(SQLTypeIdToString(sql_type.id));

			string query = "INSERT INTO " + table_name + " VALUES";
			if(sql_type == SQLType::BOOLEAN) {
				con.Query(query + "(true, true, true), (true, true, false), (false, false, false);");
			} else if(sql_type.IsNumeric()) {
				con.Query(query + "(1, 10, 100),(2, 10, 100),(3, 10, 100);");
			} else if(sql_type == SQLType::VARCHAR |sql_type == SQLType::BLOB) {
				con.Query(query + "('a', 'b', 'c'),('a', 'b', 'c'),('a', 'b', 'c');");
			} else if(sql_type == SQLType::DATE) {
				con.Query(query + "('2008-01-01', '2009-01-01', '2010-01-01')," +
								  "('2008-01-01', '2009-01-01', '2010-01-01')," +
								  "('2008-01-01', '2009-01-01', '2010-01-01')");
			} else if(sql_type == SQLType::TIME) {
				con.Query(query + "('01:00:00', '02:00:00', '03:00:00')," +
								  "('04:00:00', '05:00:00', '06:00:00')," +
								  "('07:00:00', '08:00:00', '09:00:00')");
			} else if(sql_type == SQLType::TIMESTAMP) {
				con.Query(query + "('2008-01-01 00:00:00', '2009-01-01 00:00:00', '2010-01-01 00:00:00')," +
								  "('2008-01-01 00:00:00', '2009-01-01 00:00:00', '2010-01-01 00:00:00')," +
								  "('2008-01-01 00:00:00', '2009-01-01 00:00:00', '2010-01-01 00:00:00')");
			}
		}
		
		//Running the UDF functions and checking the results
		for(SQLType sql_type: all_sql_types) {
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
				
			} else if(sql_type == SQLType::VARCHAR || sql_type == SQLType::BLOB) {
				result = con.Query("SELECT " + func_name + "_1(a) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {"a", "a", "a"}));

				result = con.Query("SELECT " + func_name + "_2(a, b) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {"b", "b", "b"}));

				result = con.Query("SELECT " + func_name + "_3(a, b, c) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {"c", "c", "c"}));
			} else if(sql_type == SQLType::DATE) {
				result = con.Query("SELECT " + func_name + "_1(a) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {Date::FromString("2008-01-01"), Date::FromString("2008-01-01"), Date::FromString("2008-01-01")}));

				result = con.Query("SELECT " + func_name + "_2(a, b) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {Date::FromString("2009-01-01"), Date::FromString("2009-01-01"), Date::FromString("2009-01-01")}));

				result = con.Query("SELECT " + func_name + "_3(a, b, c) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {Date::FromString("2010-01-01"), Date::FromString("2010-01-01"), Date::FromString("2010-01-01")}));
			} else if(sql_type == SQLType::TIME) {
				result = con.Query("SELECT " + func_name + "_1(a) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {Time::FromString("01:00:00"), Time::FromString("04:00:00"), Time::FromString("07:00:00")}));

				result = con.Query("SELECT " + func_name + "_2(a, b) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {Time::FromString("02:00:00"), Time::FromString("05:00:00"), Time::FromString("08:00:00")}));

				result = con.Query("SELECT " + func_name + "_3(a, b, c) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {Time::FromString("03:00:00"), Time::FromString("06:00:00"), Time::FromString("09:00:00")}));
			} else if(sql_type == SQLType::TIMESTAMP) {
				result = con.Query("SELECT " + func_name + "_1(a) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {Timestamp::FromString("2008-01-01 00:00:00"), Timestamp::FromString("2008-01-01 00:00:00"), Timestamp::FromString("2008-01-01 00:00:00")}));

				result = con.Query("SELECT " + func_name + "_2(a, b) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {Timestamp::FromString("2009-01-01 00:00:00"), Timestamp::FromString("2009-01-01 00:00:00"), Timestamp::FromString("2009-01-01 00:00:00")}));

				result = con.Query("SELECT " + func_name + "_3(a, b, c) FROM " + table_name);
				REQUIRE(CHECK_COLUMN(result, 0, {Timestamp::FromString("2010-01-01 00:00:00"), Timestamp::FromString("2010-01-01 00:00:00"), Timestamp::FromString("2010-01-01 00:00:00")}));
			}
		}
	}
	SECTION("Cheking if the UDF functions are temporary") {
		Connection con_NEW(db);
		con_NEW.EnableQueryVerification();
		for(SQLType sql_type: all_sql_types) {
			table_name = StringUtil::Lower(SQLTypeIdToString(sql_type.id));
			func_name = table_name;

			REQUIRE_FAIL(con_NEW.Query("SELECT " + func_name + "_1(a) FROM " + table_name));

			REQUIRE_FAIL(con_NEW.Query("SELECT " + func_name + "_2(a, b) FROM " + table_name));

			REQUIRE_FAIL(con_NEW.Query("SELECT " + func_name + "_3(a, b, c) FROM " + table_name));
		}
	}

	SECTION("Cheking NULLs with UDF functions") {
		for(SQLType sql_type: all_sql_types) {
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
