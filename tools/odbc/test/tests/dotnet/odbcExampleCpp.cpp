#include "pch.h"
#include <iostream>
#include <string>
#include <exception>
#include <cstdarg>
#include <vector>

using namespace System;
using namespace System::Data;
using namespace System::Data::Odbc;

//*************************************************************
static int32_t local_assert(const char *what, bool passed, const char *assert, const char *file, long line) {
	if (passed == false) {
		std::cout << "FAIL " << what << " " << assert << " in " << file << " at " << line << "\n";
		return EXIT_FAILURE;
	} else {
		std::cout << "PASS " << what << " " << assert << "\n";
		return EXIT_SUCCESS;
	}
}

#define assert(w, x) local_assert(w, x, #x, __FILE__, __LINE__)

//*************************************************************
static std::string toss(System::String ^ s) {
	using namespace Runtime::InteropServices;
	// convert .NET System::String to std::string
	const char *cstr = (const char *)(Marshal::StringToHGlobalAnsi(s)).ToPointer();
	std::string sstr = cstr;
	Marshal::FreeHGlobal(System::IntPtr((void *)cstr));
	return sstr;
}

//*************************************************************
// requires at least C++11
const std::string vformat(const char *const zcFormat, ...) {

	// initialize use of the variable argument array
	va_list vaArgs;
	va_start(vaArgs, zcFormat);

	// reliably acquire the size
	// from a copy of the variable argument array
	// and a functionally reliable call to mock the formatting
	va_list vaArgsCopy;
	va_copy(vaArgsCopy, vaArgs);
	const int iLen = std::vsnprintf(NULL, 0, zcFormat, vaArgsCopy);
	va_end(vaArgsCopy);

	// return a formatted string without risking memory mismanagement
	// and without assuming any compiler or platform specific behavior
	std::vector<char> zc(iLen + 1);
	std::vsnprintf(zc.data(), zc.size(), zcFormat, vaArgs);
	va_end(vaArgs);
	return std::string(zc.data(), iLen);
}

//*************************************************************
std::string doReader(OdbcConnection ^ DbConnection, System::String ^ query) {
	std::string s("");
	std::string t("");
	s += vformat("\ndoReader(%d)> %s\n", DbConnection->State, query);

	OdbcCommand ^ DbCommand = DbConnection->CreateCommand();
	DbCommand->CommandText = query;
	try {
		OdbcDataReader ^ DbReader = DbCommand->ExecuteReader();
		do {
			int fCount = DbReader->FieldCount;
			if (fCount > 0) {
				for (int i = 0; i < fCount; i++) {
					s += toss(DbReader->GetName(i));
					s += ":";
				}
				s += "\n";
				while (DbReader->Read()) {
					for (int i = 0; i < fCount; i++) {
						s += toss(DbReader->GetString(i));
						s += ":";
					}
					s += "\n";
				}
			} else {
				int row_count = DbReader->RecordsAffected;
				s += vformat("Query affected %d row(s)\n", row_count);
			}
		} while (DbReader->NextResult());
		DbReader->Close();
	} catch (OdbcException ^ ex) {
		std::string e("");
		e += toss(ex->Message);
		return e;
	}

	delete DbCommand;

	return s;
}

//*************************************************************
int main(array<System::String ^> ^ args) {
	int32_t returnVal = EXIT_SUCCESS;
	int32_t failCount = 0;
	std::string s("");
	OdbcConnection ^ DbConnection = nullptr;
	try {

		System::String ^ connStr = "Driver=DuckDB Driver;Database=test.duckdb;";
		DbConnection = gcnew OdbcConnection(connStr);

		DbConnection->Open();

		std::cout << "\nBuild DB" << std::endl;

		s = doReader(DbConnection, "PRAGMA version;");
		std::cout << s;

		s = doReader(DbConnection, "select * from sqlite_master;");
		std::cout << s;

		s = doReader(DbConnection, "drop table if exists weather;");
		std::cout << s;

		s = doReader(DbConnection, "select * from sqlite_master;");
		std::cout << s;

		s = doReader(DbConnection,
		             "CREATE TABLE weather(city VARCHAR, temp_lo INTEGER, temp_hi INTEGER, prcp FLOAT, date DATE);");
		std::cout << s;

		s = doReader(DbConnection, "INSERT INTO weather VALUES ('San Francisco', 46, 50, 0.25, '1994-11-27');");
		std::cout << s;

		s = doReader(DbConnection, "select temp_lo from weather limit 1;");
		std::cout << s;

		std::cout << std::endl;

		DataTable ^ dt = gcnew DataTable();
		OdbcDataAdapter ^ adapter =
		    gcnew OdbcDataAdapter("select city, temp_lo, prcp, date from weather limit 1;", DbConnection);

		adapter->FillSchema(dt, SchemaType::Source);

		failCount += assert("FillSchema()", dt->Rows->Count == 0);
		failCount += assert("FillSchema()", dt->Columns->Count == 4);
		failCount += assert("FillSchema()", toss(dt->Columns[0]->ColumnName) == "city");
		failCount += assert("FillSchema()", toss(dt->Columns[0]->DataType->ToString()) == "System.String");
		failCount += assert("FillSchema()", toss(dt->Columns[1]->ColumnName) == "temp_lo");
		failCount += assert("FillSchema()", toss(dt->Columns[1]->DataType->ToString()) == "System.Int32");
		failCount += assert("FillSchema()", toss(dt->Columns[2]->ColumnName) == "prcp");
		failCount += assert("FillSchema()", toss(dt->Columns[2]->DataType->ToString()) == "System.Double");
		failCount += assert("FillSchema()", toss(dt->Columns[3]->ColumnName) == "date");
		failCount += assert("FillSchema()", toss(dt->Columns[3]->DataType->ToString()) == "System.DateTime");

		dt->Clear();
		adapter->Fill(dt);

		failCount += assert("Fill()", dt->Rows->Count == 1);
		failCount += assert("Fill()", dt->Columns->Count == 4);
		failCount += assert("Fill()", toss(dt->Columns[0]->ColumnName) == "city");
		failCount += assert("Fill()", toss(dt->Columns[0]->DataType->ToString()) == "System.String");
		failCount += assert("Fill()", toss(dt->Columns[1]->ColumnName) == "temp_lo");
		failCount += assert("Fill()", toss(dt->Columns[1]->DataType->ToString()) == "System.Int32");
		failCount += assert("Fill()", toss(dt->Columns[2]->ColumnName) == "prcp");
		failCount += assert("Fill()", toss(dt->Columns[2]->DataType->ToString()) == "System.Double");
		failCount += assert("Fill()", toss(dt->Columns[3]->ColumnName) == "date");
		failCount += assert("Fill()", toss(dt->Columns[3]->DataType->ToString()) == "System.DateTime");

		failCount += assert("Fill()", toss(dt->Rows[0]->ItemArray[0]->ToString()) == "San Francisco");
		failCount += assert("Fill()", toss(dt->Rows[0]->ItemArray[1]->ToString()) == "46");
		failCount += assert("Fill()", toss(dt->Rows[0]->ItemArray[2]->ToString()) == "0.25");
		failCount += assert("Fill()", toss(dt->Rows[0]->ItemArray[3]->ToString()) == "11/27/1994 12:00:00 AM");

		if (failCount > 0) {
			std::cout << "failCount " << failCount << std::endl;
			returnVal = EXIT_FAILURE;
		}
	} catch (OdbcException ^ ex) {
		std::cout << "OdbcException: {" << toss(ex->Message) << "}\n";
		returnVal = EXIT_FAILURE;
	} catch (Exception ^ ex) {
		std::cout << "Exception: {" << toss(ex->Message) << "}\n";
		returnVal = EXIT_FAILURE;
	} finally {
		if (DbConnection != nullptr) {
			delete DbConnection;
		}
	}

	std::cout << std::endl;

	// std::cout << "press any key to continue..." << std::endl;
	// Console::ReadLine();

	return returnVal;
}
