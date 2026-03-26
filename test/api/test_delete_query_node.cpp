#include "catch.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

using namespace duckdb;

static unique_ptr<DeleteStatement> ParseDelete(const string &sql) {
	Parser p;
	p.ParseQuery(sql);
	REQUIRE(p.statements.size() == 1);
	REQUIRE(p.statements[0]->type == StatementType::DELETE_STATEMENT);
	return unique_ptr<DeleteStatement>(static_cast<DeleteStatement *>(p.statements[0].release()));
}

TEST_CASE("DeleteQueryNode - DeleteStatement is thin wrapper", "[delete_query_node]") {
	auto stmt = ParseDelete("DELETE FROM t WHERE a > 0");
	REQUIRE(stmt->node != nullptr);
	REQUIRE(stmt->node->type == QueryNodeType::DELETE_QUERY_NODE);
	REQUIRE(stmt->node->table != nullptr);
	REQUIRE(stmt->node->condition != nullptr);
}

TEST_CASE("DeleteQueryNode - ToString round-trip", "[delete_query_node]") {
	const string sql = "DELETE FROM t WHERE a > 0";
	auto stmt = ParseDelete(sql);
	auto str = stmt->ToString();

	auto stmt2 = ParseDelete(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("DeleteQueryNode - ToString with RETURNING", "[delete_query_node]") {
	const string sql = "DELETE FROM t WHERE a > 0 RETURNING a, b";
	auto stmt = ParseDelete(sql);
	auto str = stmt->ToString();
	REQUIRE(str.find("RETURNING") != string::npos);

	auto stmt2 = ParseDelete(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("DeleteQueryNode - ToString with USING", "[delete_query_node]") {
	const string sql = "DELETE FROM t USING src WHERE t.id = src.id";
	auto stmt = ParseDelete(sql);
	REQUIRE(!stmt->node->using_clauses.empty());
	auto str = stmt->ToString();
	REQUIRE(str.find("USING") != string::npos);

	auto stmt2 = ParseDelete(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("DeleteQueryNode - Copy and Equals", "[delete_query_node]") {
	auto stmt = ParseDelete("DELETE FROM t WHERE a > 0");
	auto &node = *stmt->node;

	auto copy = node.Copy();
	REQUIRE(copy != nullptr);
	REQUIRE(node.Equals(copy.get()));

	// Verify deep copy
	auto &copy_node = copy->Cast<DeleteQueryNode>();
	copy_node.condition = nullptr;
	REQUIRE(!node.Equals(copy.get()));
}

TEST_CASE("DeleteQueryNode - Inequality on different tables", "[delete_query_node]") {
	auto stmt1 = ParseDelete("DELETE FROM t1");
	auto stmt2 = ParseDelete("DELETE FROM t2");
	REQUIRE(!stmt1->node->Equals(stmt2->node.get()));
}

TEST_CASE("DeleteQueryNode - Serialization round-trip", "[delete_query_node]") {
	auto stmt = ParseDelete("DELETE FROM t WHERE a > 0 RETURNING a");
	DeleteQueryNode &node = *stmt->node;

	MemoryStream stream;
	BinarySerializer::Serialize(static_cast<QueryNode &>(node), stream);

	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);
	REQUIRE(deserialized->type == QueryNodeType::DELETE_QUERY_NODE);
	REQUIRE(node.Equals(deserialized.get()));
}

TEST_CASE("DeleteQueryNode - CTE in node", "[delete_query_node]") {
	auto stmt = ParseDelete("WITH cte AS (SELECT 1 AS x) DELETE FROM t WHERE a IN (SELECT x FROM cte)");
	REQUIRE(!stmt->node->cte_map.map.empty());
	auto str = stmt->ToString();
	REQUIRE(str.find("WITH") != string::npos);

	auto stmt2 = ParseDelete(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}
