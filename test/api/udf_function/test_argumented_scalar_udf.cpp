#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "udf_functions_to_test.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("UDF functions with arguments", "[udf_function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	string func_name, table_name, col_type;
	//The types supported by the argumented CreateScalarFunction
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
			con.CreateScalarFunction<bool, bool>(func_name + "_1",
										   {SQLType::BOOLEAN},
										   SQLType::BOOLEAN,
										   &udf_bool);

			con.CreateScalarFunction<bool, bool, bool>(func_name + "_2",
												 {SQLType::BOOLEAN, SQLType::BOOLEAN},
												 SQLType::BOOLEAN,
												 &udf_bool);

			con.CreateScalarFunction<bool, bool, bool, bool>(func_name + "_3",
													   {SQLType::BOOLEAN, SQLType::BOOLEAN, SQLType::BOOLEAN},
													   SQLType::BOOLEAN,
													   &udf_bool);
			break;
		}
		case SQLTypeId::TINYINT:
		{
			con.CreateScalarFunction<int8_t, int8_t>(func_name + "_1",
											   {SQLType::TINYINT},
											   SQLType::TINYINT,
											   &udf_int8);

			con.CreateScalarFunction<int8_t, int8_t, int8_t>(func_name + "_2",
													   {SQLType::TINYINT, SQLType::TINYINT},
												  	   SQLType::TINYINT,
													   &udf_int8);

			con.CreateScalarFunction<int8_t, int8_t, int8_t, int8_t>(func_name + "_3",
															   {SQLType::TINYINT, SQLType::TINYINT, SQLType::TINYINT},
					  	  	  	  	  	  	  	  	  	  	   SQLType::TINYINT,
															   &udf_int8);
			break;
		}
		case SQLTypeId::SMALLINT:
		{
			con.CreateScalarFunction<int16_t, int16_t>(func_name + "_1",
												 {SQLType::SMALLINT},
												 SQLType::SMALLINT,
												 &udf_int16);

			con.CreateScalarFunction<int16_t, int16_t, int16_t>(func_name + "_2",
														  {SQLType::SMALLINT, SQLType::SMALLINT},
														  SQLType::SMALLINT,
														  &udf_int16);

			con.CreateScalarFunction<int16_t, int16_t, int16_t, int16_t>(func_name + "_3",
																   {SQLType::SMALLINT, SQLType::SMALLINT, SQLType::SMALLINT},
																   SQLType::SMALLINT,
																   &udf_int16);
			break;
		}
		case SQLTypeId::DATE:
		{
			con.CreateScalarFunction<date_t, date_t>(func_name + "_1",
												 {SQLType::DATE},
												 SQLType::DATE,
												 &udf_date);

			con.CreateScalarFunction<date_t, date_t, date_t>(func_name + "_2",
													  {SQLType::DATE, SQLType::DATE},
													  SQLType::DATE,
													  &udf_date);

			con.CreateScalarFunction<date_t, date_t, date_t, date_t>(func_name + "_3",
																   {SQLType::DATE, SQLType::DATE, SQLType::DATE},
																   SQLType::DATE,
																   &udf_date);
			break;
		}
		case SQLTypeId::TIME:
		{
			con.CreateScalarFunction<dtime_t, dtime_t>(func_name + "_1",
												 {SQLType::TIME},
												 SQLType::TIME,
												 &udf_time);

			con.CreateScalarFunction<dtime_t, dtime_t, dtime_t>(func_name + "_2",
														  {SQLType::TIME, SQLType::TIME},
														  SQLType::TIME,
														  &udf_time);

			con.CreateScalarFunction<dtime_t, dtime_t, dtime_t, dtime_t>(func_name + "_3",
																   {SQLType::TIME, SQLType::TIME, SQLType::TIME},
																   SQLType::TIME,
																   &udf_time);
			break;
		}
		case SQLTypeId::INTEGER:
		{
			con.CreateScalarFunction<int32_t, int32_t>(func_name + "_1",
												{SQLType::INTEGER},
												SQLType::INTEGER,
												&udf_int);

			con.CreateScalarFunction<int32_t, int32_t, int32_t>(func_name + "_2",
														  {SQLType::INTEGER, SQLType::INTEGER},
														  SQLType::INTEGER,
														  &udf_int);

			con.CreateScalarFunction<int32_t, int32_t, int32_t, int32_t>(func_name + "_3",
																   {SQLType::INTEGER, SQLType::INTEGER, SQLType::INTEGER},
																   SQLType::INTEGER,
																   &udf_int);
			break;
		}
		case SQLTypeId::BIGINT:
		{
			con.CreateScalarFunction<int64_t, int64_t>(func_name + "_1",
												 {SQLType::BIGINT},
												 SQLType::BIGINT,
												 &udf_int64);

			con.CreateScalarFunction<int64_t, int64_t, int64_t>(func_name + "_2",
														  {SQLType::BIGINT, SQLType::BIGINT},
														  SQLType::BIGINT,
														  &udf_int64);

			con.CreateScalarFunction<int64_t, int64_t, int64_t, int64_t>(func_name + "_3",
																   {SQLType::BIGINT, SQLType::BIGINT, SQLType::BIGINT},
																   SQLType::BIGINT,
																   &udf_int64);
			break;
		}
		case SQLTypeId::TIMESTAMP:
		{
			con.CreateScalarFunction<timestamp_t, timestamp_t>(func_name + "_1",
												 {SQLType::TIMESTAMP},
												 SQLType::TIMESTAMP,
												 &udf_timestamp);

			con.CreateScalarFunction<timestamp_t, timestamp_t, timestamp_t>(func_name + "_2",
														  {SQLType::TIMESTAMP, SQLType::TIMESTAMP},
														  SQLType::TIMESTAMP,
														  &udf_timestamp);

			con.CreateScalarFunction<timestamp_t, timestamp_t, timestamp_t, timestamp_t>(func_name + "_3",
																   {SQLType::TIMESTAMP, SQLType::TIMESTAMP, SQLType::TIMESTAMP},
																   SQLType::TIMESTAMP,
																   &udf_timestamp);
			break;
		}
		case SQLTypeId::FLOAT:
		//FIXME: there is an implicit cast to double before calling the function
		// float_1(CAST[DOUBLE](a))
//		{
//			con.CreateScalarFunction<float, float>(func_name + "_1", &FLOAT);
//			con.CreateScalarFunction<float, float, float>(func_name + "_2", &FLOAT);
//			con.CreateScalarFunction<float, float, float, float>(func_name + "_3", &FLOAT);
//			break;
//		}
		case SQLTypeId::DOUBLE:
		{
			con.CreateScalarFunction<double, double>(func_name + "_1",
											   {SQLType::DOUBLE},
											   SQLType::DOUBLE,
											   &udf_double);

			con.CreateScalarFunction<double, double, double>(func_name + "_2",
													   {SQLType::DOUBLE, SQLType::DOUBLE},
													   SQLType::DOUBLE,
													   &udf_double);

			con.CreateScalarFunction<double, double, double, double>(func_name + "_3",
															   {SQLType::DOUBLE, SQLType::DOUBLE, SQLType::DOUBLE},
															   SQLType::DOUBLE,
															   &udf_double);
			break;
		}
		case SQLTypeId::VARCHAR:
		{
			con.CreateScalarFunction<string_t, string_t>(func_name + "_1",
												   {SQLType::VARCHAR},
												   SQLType::VARCHAR,
												   &udf_varchar);

			con.CreateScalarFunction<string_t, string_t, string_t>(func_name + "_2",
															 {SQLType::VARCHAR, SQLType::VARCHAR},
															 SQLType::VARCHAR,
															 &udf_varchar);

			con.CreateScalarFunction<string_t, string_t, string_t, string_t>(func_name + "_3",
																	   {SQLType::VARCHAR, SQLType::VARCHAR, SQLType::VARCHAR},
																	   SQLType::VARCHAR,
																	   &udf_varchar);
			break;
		}
		case SQLTypeId::BLOB:
		{
			con.CreateScalarFunction<string_t, string_t>(func_name + "_1",
												   {SQLType::BLOB},
												   SQLType::BLOB,
												   &udf_varchar);

			con.CreateScalarFunction<string_t, string_t, string_t>(func_name + "_2",
															 {SQLType::BLOB, SQLType::BLOB},
															 SQLType::BLOB,
															 &udf_varchar);

			con.CreateScalarFunction<string_t, string_t, string_t, string_t>(func_name + "_3",
																	   {SQLType::BLOB, SQLType::BLOB, SQLType::BLOB},
																	   SQLType::BLOB,
																	   &udf_varchar);
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
