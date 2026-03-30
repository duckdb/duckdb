#include "catch.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

using namespace duckdb;

static unique_ptr<InsertStatement> ParseInsert(const string &sql) {
	Parser p;
	p.ParseQuery(sql);
	REQUIRE(p.statements.size() == 1);
	REQUIRE(p.statements[0]->type == StatementType::INSERT_STATEMENT);
	return unique_ptr<InsertStatement>(static_cast<InsertStatement *>(p.statements[0].release()));
}

TEST_CASE("InsertQueryNode - InsertStatement is thin wrapper", "[insert_query_node]") {
	auto stmt = ParseInsert("INSERT INTO t VALUES (1, 2, 3)");
	REQUIRE(stmt->node != nullptr);
	REQUIRE(stmt->node->type == QueryNodeType::INSERT_QUERY_NODE);
	REQUIRE(stmt->node->table == "t");
}

TEST_CASE("InsertQueryNode - ToString round-trip", "[insert_query_node]") {
	const string sql = "INSERT INTO t VALUES (1, 2, 3)";
	auto stmt = ParseInsert(sql);
	auto str = stmt->ToString();

	auto stmt2 = ParseInsert(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("InsertQueryNode - ToString with columns", "[insert_query_node]") {
	const string sql = "INSERT INTO t (a, b) VALUES (1, 2)";
	auto stmt = ParseInsert(sql);
	REQUIRE(stmt->node->columns.size() == 2);
	auto str = stmt->ToString();

	auto stmt2 = ParseInsert(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("InsertQueryNode - ToString with RETURNING", "[insert_query_node]") {
	const string sql = "INSERT INTO t VALUES (1) RETURNING a, b";
	auto stmt = ParseInsert(sql);
	auto str = stmt->ToString();
	REQUIRE(str.find("RETURNING") != string::npos);

	auto stmt2 = ParseInsert(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("InsertQueryNode - Copy and Equals", "[insert_query_node]") {
	auto stmt = ParseInsert("INSERT INTO t (a, b) VALUES (1, 2)");
	auto &node = *stmt->node;

	auto copy = node.Copy();
	REQUIRE(copy != nullptr);
	REQUIRE(node.Equals(copy.get()));

	// Verify deep copy
	auto &copy_node = copy->Cast<InsertQueryNode>();
	copy_node.table = "other_table";
	REQUIRE(!node.Equals(copy.get()));
}

TEST_CASE("InsertQueryNode - Inequality on different tables", "[insert_query_node]") {
	auto stmt1 = ParseInsert("INSERT INTO t1 VALUES (1)");
	auto stmt2 = ParseInsert("INSERT INTO t2 VALUES (1)");
	REQUIRE(!stmt1->node->Equals(stmt2->node.get()));
}

TEST_CASE("InsertQueryNode - Serialization round-trip", "[insert_query_node]") {
	auto stmt = ParseInsert("INSERT INTO t (a, b) VALUES (1, 2) RETURNING a");
	InsertQueryNode &node = *stmt->node;

	MemoryStream stream;
	BinarySerializer::Serialize(static_cast<QueryNode &>(node), stream);

	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);
	REQUIRE(deserialized->type == QueryNodeType::INSERT_QUERY_NODE);
	REQUIRE(node.Equals(deserialized.get()));
}

TEST_CASE("InsertQueryNode - CTE in node", "[insert_query_node]") {
	auto stmt = ParseInsert("WITH cte AS (SELECT 1 AS x) INSERT INTO t SELECT * FROM cte");
	REQUIRE(!stmt->node->cte_map.map.empty());
	auto str = stmt->ToString();
	REQUIRE(str.find("WITH") != string::npos);

	auto stmt2 = ParseInsert(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("InsertQueryNode - DEFAULT VALUES", "[insert_query_node]") {
	auto stmt = ParseInsert("INSERT INTO t DEFAULT VALUES");
	REQUIRE(stmt->node->default_values == true);
	REQUIRE(stmt->node->select_statement == nullptr);

	auto str = stmt->ToString();
	REQUIRE(str.find("DEFAULT VALUES") != string::npos);

	auto stmt2 = ParseInsert(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("InsertQueryNode - OR REPLACE", "[insert_query_node]") {
	auto stmt = ParseInsert("INSERT OR REPLACE INTO t VALUES (1, 2)");
	REQUIRE(stmt->node->on_conflict_info != nullptr);
	REQUIRE(stmt->node->on_conflict_info->action_type == OnConflictAction::REPLACE);

	auto str = stmt->ToString();
	REQUIRE(str.find("OR REPLACE") != string::npos);

	auto stmt2 = ParseInsert(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("InsertQueryNode - INSERT BY NAME", "[insert_query_node]") {
	auto stmt = ParseInsert("INSERT INTO t BY NAME SELECT 1 AS a, 2 AS b");
	REQUIRE(stmt->node->column_order == InsertColumnOrder::INSERT_BY_NAME);

	auto str = stmt->ToString();
	REQUIRE(str.find("BY NAME") != string::npos);

	auto stmt2 = ParseInsert(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("InsertQueryNode - ON CONFLICT DO NOTHING", "[insert_query_node]") {
	auto stmt = ParseInsert("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING");
	REQUIRE(stmt->node->on_conflict_info != nullptr);
	REQUIRE(stmt->node->on_conflict_info->action_type == OnConflictAction::NOTHING);

	auto str = stmt->ToString();
	REQUIRE(str.find("ON CONFLICT") != string::npos);
	REQUIRE(str.find("DO NOTHING") != string::npos);

	auto stmt2 = ParseInsert(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("InsertQueryNode - Serialization with ON CONFLICT", "[insert_query_node]") {
	auto stmt = ParseInsert("INSERT OR REPLACE INTO t VALUES (1, 2)");
	InsertQueryNode &node = *stmt->node;

	MemoryStream stream;
	BinarySerializer::Serialize(static_cast<QueryNode &>(node), stream);

	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);
	REQUIRE(deserialized->type == QueryNodeType::INSERT_QUERY_NODE);
	REQUIRE(node.Equals(deserialized.get()));
}
