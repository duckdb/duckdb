#include "duckdb.hpp"
#include <iostream>

using namespace duckdb;

string TestDirectoryPath() {
	string TESTING_DIRECTORY_NAME = "sub";
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	if (!fs->DirectoryExists(TESTING_DIRECTORY_NAME)) {
		fs->CreateDirectory(TESTING_DIRECTORY_NAME);
	}
	return TESTING_DIRECTORY_NAME;
}

string TestCreatePath(string suffix) {
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	return fs->JoinPath(TestDirectoryPath(), suffix);
}

void TestDeleteFile(string path) {
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	if (fs->FileExists(path)) {
		fs->RemoveFile(path);
	}
}

void DeleteDatabase(string path) {
	TestDeleteFile(path);
	TestDeleteFile(path + ".wal");
}

int main() {
	// DuckDB db(nullptr);

	// Connection con(db);

	// SELECT 'a'::VARCHAR::BYTEA
	// std::cout << std::endl << std::endl << std::endl << "BLOB CAST ==========================" << std::endl;
	// auto blob_rv = con.Query("SELECT 'a'::VARCHAR::BYTEA");
	// std::cout << "Main ============================ " << std::endl;
	// blob_rv->Print();

	// SELECT 'a';
	// std::cout << std::endl << std::endl << std::endl << "String SELECT ==========================" << std::endl;
	// auto str_rv = con.Query("SELECT box_out('a'), box_in('c')");
	// std::cout << "Main ============================ " << std::endl;
	// str_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "CREATE TYPE ==========================" << std::endl;
	// auto createtype_rv = con.Query("CREATE TYPE box (input := 'box_in', output := 'box_out')");
	// std::cout << "Main ============================ " << std::endl;
	// createtype_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Create table custom type ================ " << std::endl;
	// auto t_custom_type_rv = con.Query("CREATE TABLE person_string (name text, current_mood box)");
	// t_custom_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Insert Table ================ " << std::endl;
	// auto ic_type_rv = con.Query("INSERT INTO person_string VALUES ('Tuyen', 'sadd'), ('Nga basdfsadfsd', 'happy')");
	// // auto ic_type_rv = con.Query("INSERT INTO person_string VALUES ('Tuyen', 2), ('Nga basdfsadfsd', 3)");
	// ic_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Select Table ================ " << std::endl;
	// // auto sc_type_rv = con.Query("SELECT * FROM person_string ORDER BY name;");
	// // auto sc_type_rv = con.Query("SELECT * FROM person_string WHERE name='Tuyen';");
	// auto sc_type_rv = con.Query("SELECT name, box_add_end(current_mood) FROM person_string;");
	// // auto sc_type_rv = con.Query("SELECT * FROM person_string WHERE current_mood=='sadd'");
	// // auto sc_type_rv = con.Query("SELECT current_mood, count(*) FROM person_string GROUP BY current_mood");
	// std::cout << "Select Table Result ================================== " << std::endl;
	// sc_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Drop function ==========================" << std::endl;
	// auto drop_f_rv = con.Query("DROP FUNCTION box_in;");
	// drop_f_rv->Print();

	// std::cout << "Create function =======================" << std::endl;
	// auto createfunc_rv = con.Query("create function apply(x, y) as x + y;");
	// createfunc_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "FUnction call ================ " << std::endl;
	// auto f_call_rv = con.Query(
	// 						   "select DATESUB('second', '2004-01-31 12:00:05'::TIMESTAMP, '2004-02-01
	// 12:00:00'::TIMESTAMP);"); f_call_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Create function call table ================ " << std::endl;
	// auto tf_type_rv = con.Query("CREATE TABLE function_table (id VARCHAR, abc VARCHAR)");
	// tf_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Insert function call Table ================ " << std::endl;
	// // auto if_type_rv = con.Query("INSERT INTO function_table VALUES (DATESUB('second', '2004-01-31
	// 12:00:05'::TIMESTAMP, '2004-02-01 12:00:00'::TIMESTAMP))");
	// // auto if_type_rv = con.Query("INSERT INTO function_table VALUES (box_in('abc'))");
	// // auto if_type_rv = con.Query("INSERT INTO function_table VALUES ('abc')");
	// auto if_type_rv = con.Query("INSERT INTO function_table VALUES (1, 'abc')");
	// if_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Select function call Table ================= " << std::endl;
	// auto sf_type_rv = con.Query("SELECT box_out(id), abc FROM function_table;");
	// sf_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Enum ================ " << std::endl;
	// auto e_type_rv = con.Query("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
	// e_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Create table ================ " << std::endl;
	// auto t_type_rv = con.Query("CREATE TABLE person_string (name text, current_mood mood)");
	// t_type_rv->Print();

	// // INSERT INTO enums VALUES ('sad'),('ok'),('sad'),('happy')
	// std::cout << std::endl << std::endl << std::endl << "Insert Table ================ " << std::endl;
	// auto i_type_rv = con.Query("INSERT INTO person_string VALUES ('Tuyen', 'sad'), ('Nga', 'happy')");
	// i_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Select Table ================ " << std::endl;
	// auto sc_type_rv = con.Query("SELECT * FROM person_string WHERE current_mood='happy';");
	// std::cout << "Select Table Result ================================== " << std::endl;
	// sc_type_rv->Print();

	// // DROP TYPE mood;
	// std::cout << std::endl << std::endl << std::endl << "Drop enum ================ " << std::endl;
	// auto d_type_rv = con.Query("DROP TYPE mood;");
	// d_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Enum ================ 2" << std::endl;
	// auto e_type_rv_2 = con.Query("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
	// e_type_rv_2->Print();

	// auto &catalog = Catalog::GetCatalog(context);
	// auto func = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, function.schema,
	// function.function_name, false, error_context);

	// std::cout << std::endl << std::endl << std::endl << "Create table custom type ================ " << std::endl;
	// auto t_custom_type_rv = con.Query("CREATE TABLE nameint (name text, id int)");
	// t_custom_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Insert Table ================ " << std::endl;
	// auto ic_type_rv = con.Query("INSERT INTO nameint VALUES ('Tuyen', 1), ('Nga basdfsadfsd', 2), ('Chi', 3)");
	// ic_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Select Table ================ " << std::endl;
	// auto sc_type_rv = con.Query("SELECT * FROM nameint WHERE name='Tuyen';");
	// // auto sc_type_rv = con.Query("SELECT * FROM nameint WHERE id=1;");
	// std::cout << "Select Table Result ================================== " << std::endl;
	// sc_type_rv->Print();

	// std::cout << std::endl << std::endl << std::endl << "Test Table ================ " << std::endl;
	// // auto sc1_type_rv = con.Query("CREATE TABLE t0(c0 INT);");
	// // auto sc2_type_rv = con.Query("INSERT INTO t0 VALUES (NULL), (0), (1);");
	// // auto sc3_type_rv = con.Query("SELECT COALESCE(CASE WHEN RANDOM()<100 THEN RANDOM() ELSE NULL END, NULL, 42)
	// FROM range(10)");
	// // sc3_type_rv->Print();
	// auto storage_database = TestCreatePath("storage_test");

	// std::cout << "Here ================================= 00000000" << std::endl;
	// DeleteDatabase(storage_database);
	// {
	// 	unique_ptr<QueryResult> result;

	// 	DuckDB db(storage_database);
	// 	Connection con(db);

	// 	con.Query("CREATE SEQUENCE seq;");
	// 	con.Query("CREATE SEQUENCE seq_cycle INCREMENT 1 MAXVALUE 3 START 2 CYCLE;");
	// 	result = con.Query("SELECT nextval('seq')");
	// 	result->Print();
	// 	result = con.Query("SELECT currval('seq')");
	// 	result->Print();
	// 	result = con.Query("SELECT currval('seq')");
	// 	result->Print();
	// 	result = con.Query("SELECT nextval('seq_cycle')");
	// 	result->Print();
	// }

	// // reload the database from disk twice
	// std::cout << "Here ================================= 00000000111111" << std::endl;
	// {
	// 	DuckDB db(storage_database);
	// 	Connection con(db);
	// }

	// std::cout << "Here ================================= 111111" << std::endl;
	// {
	// 	unique_ptr<QueryResult> result;

	// 	DuckDB db(storage_database);
	// 	Connection con(db);

	// 	result = con.Query("SELECT nextval('seq')");
	// 	result->Print();
	// 	result = con.Query("SELECT currval('seq')");
	// 	result->Print();
	// 	result = con.Query("SELECT nextval('seq_cycle')");
	// 	result->Print();
	// 	result = con.Query("SELECT currval('seq_cycle')");
	// 	result->Print();
	// }

	// std::cout << "Here ================================= 222222" << std::endl;
	// {
	// 	unique_ptr<QueryResult> result;

	// 	DuckDB db(storage_database);
	// 	Connection con(db);

	// 	result = con.Query("SELECT currval('seq'), currval('seq_cycle')");
	// 	result->Print();

	// 	result = con.Query("SELECT nextval('seq'), nextval('seq');");
	// 	result->Print();
	// 	result = con.Query("SELECT nextval('seq_cycle')");
	// 	result->Print();

	// 	result = con.Query("SELECT currval('seq'), currval('seq_cycle')");
	// 	result->Print();
	// }

	// DeleteDatabase(storage_database);

	// std::cout << std::endl << std::endl << std::endl << "Test Export Table ================ " << std::endl;
	// DuckDB db(nullptr);

	// Connection con(db);
	// auto sc0_type_rv = con.Query("BEGIN TRANSACTION");
	// std::cout << "Create Apply function ========================= 1" << std::endl;
	// auto f_rv = con.Query("create function apply(x, y) as x + y;");
	// std::cout << "Create Apply function ========================= 2" << std::endl;
	// auto sc1_type_rv = con.Query("CREATE TABLE integers(i INTEGER NOT NULL, j INTEGER)");
	// auto sc2_type_rv = con.Query("INSERT INTO integers SELECT i, i+1 FROM range(0, 1000) tbl(i)");
	// auto sc3_type_rv = con.Query("SELECT SUM(i), SUM(j) FROM integers");
	// std::cout << "Export Database ================================= 1" << std::endl;
	// auto sc4_type_rv = con.Query("EXPORT DATABASE 'sub/export_test' (FORMAT PARQUET)");
	// std::cout << "Export Database ================================= 2" << std::endl;
	// auto sc5_type_rv = con.Query("ROLLBACK");
	// sc5_type_rv->Print();
	// std::cout << "Before Import Database ============================= " << std::endl;
	// auto sc7_type_rv = con.Query("IMPORT DATABASE 'sub/export_test'");
	// sc7_type_rv->Print();

	std::cout << std::endl << std::endl << std::endl << "Test Create custom type ================ " << std::endl;
	DuckDB db(nullptr);

	Connection con(db);

	// std::cout << "Before Print Result ==================================== 0" << std::endl;
	// auto createtype_rv1 = con.Query("CREATE TYPE box (input := 'box_in', output := 'box_out');");
	// std::cout << "Before Print Result ==================================== 1" << std::endl;
	// auto t_custom_type_rv = con.Query("CREATE TABLE person_string (name text, current_mood box)");
	// std::cout << "Before Print Result ==================================== 2" << std::endl;
	// auto ic_type_rv = con.Query("INSERT INTO person_string VALUES ('Tuyen', 'sadd'), ('Nga basdfsadfsd', 'happy')");
	// // ic_type_rv->Print();
	// std::cout << "Before Print Result ==================================== 3" << std::endl;
	// auto sc_type_rv = con.Query("SELECT * FROM person_string ORDER BY name;");
	// // auto createtype_rv1 = con.Query("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
	// // auto createtype_rv2 = con.Query("select 'happy'::box;");
	// // auto createtype_rv2 = con.Query("select ['happy']::box[];");
	// // auto createtype_rv2 = con.Query("DROP TYPE mood;");
	// // auto createtype_rv2 = con.Query("select 'happy'::mood;");
	// std::cout << "Before Print Result ==================================== 4" << std::endl;
	// sc_type_rv->Print();
	// // createtype_rv2->Print();
	// // auto createtype_rv2 = con.Query("create function box_output(s) as concat(s, ' OUT');");
	// // std::cout << std::endl << std::endl << std::endl << "Test Create custom type ================ " << std::endl;
	// // auto createtype_rv3 = con.Query("CREATE TYPE box (input := 'box_input', output := 'box_output')");
	// // createtype_rv3->Print();
	// // auto createtype_rv4 = con.Query("Select box_input('a');");
	// // createtype_rv4->Print();

	std::cout << "Before Print Result ==================================== 0" << std::endl;
	auto createtype_rv1 = con.Query("CREATE TYPE box (input := 'box_in', output := 'box_out');");
	// auto createtype_rv1 = con.Query("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
	std::cout << "Before Print Result ==================================== 1" << std::endl;
	auto t_custom_type_rv = con.Query("CREATE TABLE person (name text, current_box box);");
	// auto t_custom_type_rv = con.Query("CREATE TABLE person (name text);");
	// auto t_custom_type_rv = con.Query("CREATE TABLE person (name text, current_mood mood);");
	t_custom_type_rv->Print();
	std::cout << "Before Print Result ==================================== 2" << std::endl;
	// auto t_custom_type_rv0 = con.Query("BEGIN TRANSACTION;");
	// auto t_custom_type_rv1 = con.Query("ALTER TABLE person ALTER current_box SET DATA TYPE VARCHAR;");
	// auto t_custom_type_rv1 = con.Query("ALTER TABLE person ADD COLUMN current_box box;");
	// auto t_custom_type_rv1 = con.Query("ALTER TABLE person ALTER current_mood SET DATA TYPE VARCHAR;");
	auto t_custom_type_rv1 =
	    con.Query("INSERT INTO person VALUES ('Pedro', 'ok'), ('Mark', 'sad'),('Moe', 'happy'), ('Diego', NULL);");
	std::cout << "Before Print Result ==================================== 2 ----- 1" << std::endl;
	// auto t_custom_type_rv2 = con.Query("ROLLBACK;");
	// t_custom_type_rv1->Print();
	std::cout << "Before Print Result ==================================== 3" << std::endl;
	// auto ic_type_rv = con.Query("drop type box;");
	// auto ic_type_rv = con.Query("drop type mood;");
	// ic_type_rv->Print();

	auto ic_type_rv = con.Query("select current_box::varchar from person;");
	std::cout << "Before Print Result ==================================== 4" << std::endl;
	ic_type_rv->Print();
}