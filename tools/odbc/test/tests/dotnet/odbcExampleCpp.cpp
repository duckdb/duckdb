#include "pch.h"
#include <sstream>
#include <iostream>
#include <string>
#include <exception>
#include <cstdarg>
#include <vector>

#define CATCH_CONFIG_MAIN
#include "catch.hpp"

using namespace System;
using namespace System::Data;
using namespace System::Data::Odbc;

//*************************************************************
// convert .NET System::String to std::string
static std::string toss(System::String ^ s) {
	using Runtime::InteropServices::Marshal;
	System::IntPtr pointer = Marshal::StringToHGlobalAnsi(s);
	char *charPointer = reinterpret_cast<char *>(pointer.ToPointer());
	std::string returnString(charPointer, s->Length);
	Marshal::FreeHGlobal(pointer);
	return returnString;
}

TEST_CASE("System.Data.ODBC", "test .NET OdbcDataAdapter functionality") {

	OdbcConnection ^ Conn = nullptr;
	try {

		System::String ^ connStr = "Driver=DuckDB Driver;Database=test.duckdb;";
		Conn = gcnew OdbcConnection(connStr);
		Conn->Open();

		OdbcCommand ^ DbCmd = Conn->CreateCommand();

		DbCmd->CommandText = "drop table if exists weather;";
		DbCmd->ExecuteNonQuery();

		DbCmd->CommandText =
		    "CREATE TABLE weather(city VARCHAR, temp_lo INTEGER, temp_hi INTEGER, prcp FLOAT, date DATE);";
		DbCmd->ExecuteNonQuery();

		DbCmd->CommandText = "INSERT INTO weather VALUES ('San Francisco', 46, 50, 0.25, '1994-11-27');";
		int i = DbCmd->ExecuteNonQuery();
		REQUIRE(i == 1);

		DbCmd->CommandText = "select count(1) from weather;";
		Int64 i64 = static_cast<Int64>(DbCmd->ExecuteScalar());
		REQUIRE(i64 == 1);

		DataTable ^ dt = gcnew DataTable();
		OdbcDataAdapter ^ adapter =
		    gcnew OdbcDataAdapter("select city, temp_lo, prcp, date from weather limit 1;", Conn);

		// FillSchema() .NET code outputs some cruft so set up to capture it to a string and ignore
		std::stringstream ss;
		auto old_buf = std::cout.rdbuf(ss.rdbuf());

		adapter->FillSchema(dt, SchemaType::Source);

		std::cout.rdbuf(old_buf);

		REQUIRE(dt->Rows->Count == 0);
		REQUIRE(dt->Columns->Count == 4);
		REQUIRE(toss(dt->Columns[0]->ColumnName) == "city");
		REQUIRE(toss(dt->Columns[0]->DataType->ToString()) == "System.String");
		REQUIRE(toss(dt->Columns[1]->ColumnName) == "temp_lo");
		REQUIRE(toss(dt->Columns[1]->DataType->ToString()) == "System.Int32");
		REQUIRE(toss(dt->Columns[2]->ColumnName) == "prcp");
		REQUIRE(toss(dt->Columns[2]->DataType->ToString()) == "System.Double");
		REQUIRE(toss(dt->Columns[3]->ColumnName) == "date");
		REQUIRE(toss(dt->Columns[3]->DataType->ToString()) == "System.DateTime");

		dt->Clear();
		adapter->Fill(dt);

		REQUIRE(dt->Rows->Count == 1);
		REQUIRE(dt->Columns->Count == 4);
		REQUIRE(toss(dt->Columns[0]->ColumnName) == "city");
		REQUIRE(toss(dt->Columns[0]->DataType->ToString()) == "System.String");
		REQUIRE(toss(dt->Columns[1]->ColumnName) == "temp_lo");
		REQUIRE(toss(dt->Columns[1]->DataType->ToString()) == "System.Int32");
		REQUIRE(toss(dt->Columns[2]->ColumnName) == "prcp");
		REQUIRE(toss(dt->Columns[2]->DataType->ToString()) == "System.Double");
		REQUIRE(toss(dt->Columns[3]->ColumnName) == "date");
		REQUIRE(toss(dt->Columns[3]->DataType->ToString()) == "System.DateTime");
		REQUIRE(toss(dt->Rows[0]->ItemArray[0]->ToString()) == "San Francisco");
		REQUIRE(toss(dt->Rows[0]->ItemArray[1]->ToString()) == "46");
		REQUIRE(toss(dt->Rows[0]->ItemArray[2]->ToString()) == "0.25");
		REQUIRE(toss(dt->Rows[0]->ItemArray[3]->ToString()) == "11/27/1994 12:00:00 AM");

	} catch (OdbcException ^ ex) {

		FAIL("OdbcException: {" << toss(ex->Message) << "}");

	} finally {

		if (Conn != nullptr) {
			delete Conn;
		}
	}
}
