#include "catch.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

using namespace duckdb;

static unique_ptr<MergeIntoStatement> ParseMerge(const string &sql) {
	Parser p;
	p.ParseQuery(sql);
	REQUIRE(p.statements.size() == 1);
	REQUIRE(p.statements[0]->type == StatementType::MERGE_INTO_STATEMENT);
	return unique_ptr<MergeIntoStatement>(static_cast<MergeIntoStatement *>(p.statements[0].release()));
}

static const char *BASE_MERGE =
    "MERGE INTO t USING s ON t.i = s.i WHEN MATCHED THEN UPDATE SET j = s.j WHEN NOT MATCHED THEN INSERT VALUES (s.i, s.j)";

TEST_CASE("MergeQueryNode - MergeIntoStatement is thin wrapper", "[merge_query_node]") {
	auto stmt = ParseMerge(BASE_MERGE);
	REQUIRE(stmt->node != nullptr);
	REQUIRE(stmt->node->type == QueryNodeType::MERGE_QUERY_NODE);
	REQUIRE(stmt->node->target != nullptr);
	REQUIRE(stmt->node->source != nullptr);
	REQUIRE(stmt->node->join_condition != nullptr);
}

TEST_CASE("MergeQueryNode - ToString round-trip", "[merge_query_node]") {
	auto stmt = ParseMerge(BASE_MERGE);
	auto str = stmt->ToString();

	auto stmt2 = ParseMerge(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("MergeQueryNode - ToString with RETURNING", "[merge_query_node]") {
	auto stmt = ParseMerge(string(BASE_MERGE) + " RETURNING *");
	auto str = stmt->ToString();
	REQUIRE(str.find("RETURNING") != string::npos);

	auto stmt2 = ParseMerge(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("MergeQueryNode - Copy and Equals", "[merge_query_node]") {
	auto stmt = ParseMerge(BASE_MERGE);
	auto &node = *stmt->node;

	auto copy = node.Copy();
	REQUIRE(copy != nullptr);
	REQUIRE(node.Equals(copy.get()));

	// Verify deep copy - mutating the copy breaks equality
	auto &copy_node = copy->Cast<MergeQueryNode>();
	copy_node.using_columns.push_back("extra");
	REQUIRE(!node.Equals(copy.get()));
}

TEST_CASE("MergeQueryNode - Inequality on different actions", "[merge_query_node]") {
	auto stmt1 = ParseMerge("MERGE INTO t USING s ON t.i = s.i WHEN MATCHED THEN DELETE");
	auto stmt2 = ParseMerge("MERGE INTO t USING s ON t.i = s.i WHEN MATCHED THEN UPDATE SET j = s.j");
	REQUIRE(!stmt1->node->Equals(stmt2->node.get()));
}

TEST_CASE("MergeQueryNode - Serialization round-trip", "[merge_query_node]") {
	auto stmt = ParseMerge(BASE_MERGE);
	MergeQueryNode &node = *stmt->node;

	MemoryStream stream;
	BinarySerializer::Serialize(static_cast<QueryNode &>(node), stream);

	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);
	REQUIRE(deserialized->type == QueryNodeType::MERGE_QUERY_NODE);
	REQUIRE(node.Equals(deserialized.get()));
}

TEST_CASE("MergeQueryNode - Serialization with RETURNING and DELETE", "[merge_query_node]") {
	auto stmt = ParseMerge("MERGE INTO t USING s ON t.i = s.i WHEN MATCHED THEN DELETE "
	                       "WHEN NOT MATCHED BY SOURCE THEN ERROR RETURNING merge_action");
	MergeQueryNode &node = *stmt->node;

	MemoryStream stream;
	BinarySerializer::Serialize(static_cast<QueryNode &>(node), stream);

	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);
	REQUIRE(deserialized->type == QueryNodeType::MERGE_QUERY_NODE);
	REQUIRE(node.Equals(deserialized.get()));
}

TEST_CASE("MergeQueryNode - USING column list", "[merge_query_node]") {
	auto stmt = ParseMerge("MERGE INTO t USING s USING (i) WHEN MATCHED THEN DELETE");
	REQUIRE(stmt->node->join_condition == nullptr);
	REQUIRE(stmt->node->using_columns.size() == 1);

	auto str = stmt->ToString();
	auto stmt2 = ParseMerge(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("MergeQueryNode - CTE in node", "[merge_query_node]") {
	auto stmt = ParseMerge("WITH src AS (SELECT 1 AS i, 2 AS j) "
	                       "MERGE INTO t USING src ON t.i = src.i WHEN MATCHED THEN DELETE");
	REQUIRE(!stmt->node->cte_map.map.empty());
	auto str = stmt->ToString();
	REQUIRE(str.find("WITH") != string::npos);

	auto stmt2 = ParseMerge(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}
