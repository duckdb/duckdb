#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

using namespace duckdb;

static InsertStatement &ParseInsert(Parser &p, const string &sql) {
	p.ParseQuery(sql);
	REQUIRE(p.statements.size() == 1);
	REQUIRE(p.statements[0]->type == StatementType::INSERT_STATEMENT);
	return p.statements[0]->Cast<InsertStatement>();
}

static UpdateStatement &ParseUpdate(Parser &p, const string &sql) {
	p.ParseQuery(sql);
	REQUIRE(p.statements.size() == 1);
	REQUIRE(p.statements[0]->type == StatementType::UPDATE_STATEMENT);
	return p.statements[0]->Cast<UpdateStatement>();
}

static DeleteStatement &ParseDelete(Parser &p, const string &sql) {
	p.ParseQuery(sql);
	REQUIRE(p.statements.size() == 1);
	REQUIRE(p.statements[0]->type == StatementType::DELETE_STATEMENT);
	return p.statements[0]->Cast<DeleteStatement>();
}

TEST_CASE("InsertQueryNode Copy and Equals", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseInsert(p, "INSERT INTO t VALUES (1, 2)");

	InsertQueryNode node(stmt);
	REQUIRE(node.type == QueryNodeType::INSERT_QUERY_NODE);
	REQUIRE(node.table == "t");

	auto copy = node.Copy();
	REQUIRE(copy->type == QueryNodeType::INSERT_QUERY_NODE);
	REQUIRE(node.Equals(copy.get()));
}

TEST_CASE("InsertQueryNode ToString round-trip", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseInsert(p, "INSERT INTO t(a, b) VALUES (1, 2)");
	InsertQueryNode node(stmt);

	auto sql = node.ToString();
	REQUIRE(!sql.empty());

	// Parse the ToString output and verify it round-trips
	Parser p2;
	auto &stmt2 = ParseInsert(p2, sql);
	InsertQueryNode node2(stmt2);
	REQUIRE(node.Equals(&node2));
}

TEST_CASE("InsertQueryNode serialization round-trip", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseInsert(p, "INSERT INTO t VALUES (1, 2) RETURNING id");
	auto node = make_uniq<InsertQueryNode>(stmt);

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	BinarySerializer::Serialize(*node, stream, options);
	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);

	REQUIRE(deserialized->type == QueryNodeType::INSERT_QUERY_NODE);
	REQUIRE(node->Equals(deserialized.get()));
}

TEST_CASE("UpdateQueryNode Copy and Equals", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseUpdate(p, "UPDATE t SET a = 1 WHERE b = 2");

	UpdateQueryNode node(stmt);
	REQUIRE(node.type == QueryNodeType::UPDATE_QUERY_NODE);

	auto copy = node.Copy();
	REQUIRE(copy->type == QueryNodeType::UPDATE_QUERY_NODE);
	REQUIRE(node.Equals(copy.get()));
}

TEST_CASE("UpdateQueryNode ToString round-trip", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseUpdate(p, "UPDATE t SET a = 1 WHERE b = 2");
	UpdateQueryNode node(stmt);

	auto sql = node.ToString();
	REQUIRE(!sql.empty());

	Parser p2;
	auto &stmt2 = ParseUpdate(p2, sql);
	UpdateQueryNode node2(stmt2);
	REQUIRE(node.Equals(&node2));
}

TEST_CASE("UpdateQueryNode serialization round-trip", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseUpdate(p, "UPDATE t SET a = 1 WHERE b = 2");
	auto node = make_uniq<UpdateQueryNode>(stmt);

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	BinarySerializer::Serialize(*node, stream, options);
	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);

	REQUIRE(deserialized->type == QueryNodeType::UPDATE_QUERY_NODE);
	REQUIRE(node->Equals(deserialized.get()));
}

TEST_CASE("DeleteQueryNode Copy and Equals", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseDelete(p, "DELETE FROM t WHERE id = 1");

	DeleteQueryNode node(stmt);
	REQUIRE(node.type == QueryNodeType::DELETE_QUERY_NODE);

	auto copy = node.Copy();
	REQUIRE(copy->type == QueryNodeType::DELETE_QUERY_NODE);
	REQUIRE(node.Equals(copy.get()));
}

TEST_CASE("DeleteQueryNode ToString round-trip", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseDelete(p, "DELETE FROM t WHERE id = 1");
	DeleteQueryNode node(stmt);

	auto sql = node.ToString();
	REQUIRE(!sql.empty());

	Parser p2;
	auto &stmt2 = ParseDelete(p2, sql);
	DeleteQueryNode node2(stmt2);
	REQUIRE(node.Equals(&node2));
}

TEST_CASE("DeleteQueryNode serialization round-trip", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseDelete(p, "DELETE FROM t WHERE id = 1");
	auto node = make_uniq<DeleteQueryNode>(stmt);

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	BinarySerializer::Serialize(*node, stream, options);
	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);

	REQUIRE(deserialized->type == QueryNodeType::DELETE_QUERY_NODE);
	REQUIRE(node->Equals(deserialized.get()));
}

TEST_CASE("DML QueryNode inequality", "[dml_query_nodes]") {
	// Different table names
	Parser p;
	auto &ins_stmt = ParseInsert(p, "INSERT INTO t VALUES (1)");
	InsertQueryNode ins_node(ins_stmt);

	Parser p2;
	auto &ins_stmt2 = ParseInsert(p2, "INSERT INTO other_table VALUES (1)");
	InsertQueryNode ins_node2(ins_stmt2);
	REQUIRE(!ins_node.Equals(&ins_node2));

	// Different node types
	Parser p3;
	auto &del_stmt = ParseDelete(p3, "DELETE FROM t WHERE id = 1");
	DeleteQueryNode del_node(del_stmt);
	REQUIRE(!ins_node.Equals(&del_node));

	// INSERT: different returning_list — old Equals() would miss this
	Parser p4;
	auto &ins_ret1 = ParseInsert(p4, "INSERT INTO t VALUES (1) RETURNING id");
	InsertQueryNode ins_ret_node1(ins_ret1);

	Parser p5;
	auto &ins_ret2 = ParseInsert(p5, "INSERT INTO t VALUES (1)");
	InsertQueryNode ins_ret_node2(ins_ret2);
	REQUIRE(!ins_ret_node1.Equals(&ins_ret_node2));

	// UPDATE: different SET expressions — old Equals() would miss this
	Parser p6;
	auto &upd1 = ParseUpdate(p6, "UPDATE t SET a = 1 WHERE b = 2");
	UpdateQueryNode upd_node1(upd1);

	Parser p7;
	auto &upd2 = ParseUpdate(p7, "UPDATE t SET a = 99 WHERE b = 2");
	UpdateQueryNode upd_node2(upd2);
	REQUIRE(!upd_node1.Equals(&upd_node2));

	// DELETE: different USING clauses — old Equals() would miss this
	Parser p8;
	auto &del1 = ParseDelete(p8, "DELETE FROM t WHERE id = 1");
	DeleteQueryNode del_node1(del1);

	Parser p9;
	auto &del2 = ParseDelete(p9, "DELETE FROM t WHERE id = 2");
	DeleteQueryNode del_node2(del2);
	REQUIRE(!del_node1.Equals(&del_node2));
}
