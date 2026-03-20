#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
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

TEST_CASE("InsertQueryNode Copy and Equals", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseInsert(p, "INSERT INTO t VALUES (1, 2)");

	auto &node = *stmt.node;
	REQUIRE(node.type == QueryNodeType::INSERT_QUERY_NODE);
	REQUIRE(node.table->Cast<BaseTableRef>().table_name == "t");

	auto copy = node.Copy();
	REQUIRE(copy->type == QueryNodeType::INSERT_QUERY_NODE);
	REQUIRE(node.Equals(copy.get()));
}

TEST_CASE("InsertQueryNode ToString round-trip", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseInsert(p, "INSERT INTO t(a, b) VALUES (1, 2)");
	auto &node = *stmt.node;

	auto sql = node.ToString();
	REQUIRE(!sql.empty());

	// Parse the ToString output and verify it round-trips
	Parser p2;
	auto &stmt2 = ParseInsert(p2, sql);
	auto &node2 = *stmt2.node;
	REQUIRE(node.Equals(&node2));
}

TEST_CASE("InsertQueryNode serialization round-trip", "[dml_query_nodes]") {
	Parser p;
	auto &stmt = ParseInsert(p, "INSERT INTO t VALUES (1, 2) RETURNING id");
	auto node = unique_ptr_cast<QueryNode, InsertQueryNode>(stmt.node->Copy());

	Allocator allocator;
	MemoryStream stream(allocator);
	SerializationOptions options;
	BinarySerializer::Serialize(*node, stream, options);
	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);

	REQUIRE(deserialized->type == QueryNodeType::INSERT_QUERY_NODE);
	REQUIRE(node->Equals(deserialized.get()));
}

TEST_CASE("InsertQueryNode inequality", "[dml_query_nodes]") {
	// Different table names
	Parser p;
	auto &ins_stmt = ParseInsert(p, "INSERT INTO t VALUES (1)");
	auto &ins_node = *ins_stmt.node;

	Parser p2;
	auto &ins_stmt2 = ParseInsert(p2, "INSERT INTO other_table VALUES (1)");
	auto &ins_node2 = *ins_stmt2.node;
	REQUIRE(!ins_node.Equals(&ins_node2));

	// INSERT: different returning_list
	Parser p4;
	auto &ins_ret1 = ParseInsert(p4, "INSERT INTO t VALUES (1) RETURNING id");
	auto &ins_ret_node1 = *ins_ret1.node;

	Parser p5;
	auto &ins_ret2 = ParseInsert(p5, "INSERT INTO t VALUES (1)");
	auto &ins_ret_node2 = *ins_ret2.node;
	REQUIRE(!ins_ret_node1.Equals(&ins_ret_node2));
}
