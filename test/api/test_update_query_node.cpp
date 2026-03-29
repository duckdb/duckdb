#include "catch.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

using namespace duckdb;

static unique_ptr<UpdateStatement> ParseUpdate(const string &sql) {
	Parser p;
	p.ParseQuery(sql);
	REQUIRE(p.statements.size() == 1);
	REQUIRE(p.statements[0]->type == StatementType::UPDATE_STATEMENT);
	return unique_ptr<UpdateStatement>(static_cast<UpdateStatement *>(p.statements[0].release()));
}

TEST_CASE("UpdateQueryNode - UpdateStatement is thin wrapper", "[update_query_node]") {
	auto stmt = ParseUpdate("UPDATE t SET a = 1 WHERE a > 0");
	REQUIRE(stmt->node != nullptr);
	REQUIRE(stmt->node->type == QueryNodeType::UPDATE_QUERY_NODE);
	REQUIRE(stmt->node->table != nullptr);
	REQUIRE(stmt->node->set_info != nullptr);
	REQUIRE(stmt->node->set_info->columns.size() == 1);
	REQUIRE(stmt->node->set_info->columns[0] == "a");
	REQUIRE(stmt->node->set_info->condition != nullptr);
}

TEST_CASE("UpdateQueryNode - ToString round-trip", "[update_query_node]") {
	const string sql = "UPDATE t SET a = 1 WHERE a > 0";
	auto stmt = ParseUpdate(sql);
	auto str = stmt->ToString();

	// Parse the round-tripped string
	auto stmt2 = ParseUpdate(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("UpdateQueryNode - ToString with RETURNING", "[update_query_node]") {
	const string sql = "UPDATE t SET a = a + 1 RETURNING a, b";
	auto stmt = ParseUpdate(sql);
	auto str = stmt->ToString();
	REQUIRE(str.find("RETURNING") != string::npos);

	auto stmt2 = ParseUpdate(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("UpdateQueryNode - ToString with FROM", "[update_query_node]") {
	const string sql = "UPDATE t SET a = s.x FROM src AS s WHERE t.id = s.id";
	auto stmt = ParseUpdate(sql);
	REQUIRE(stmt->node->from_table != nullptr);
	auto str = stmt->ToString();
	REQUIRE(str.find("FROM") != string::npos);

	auto stmt2 = ParseUpdate(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("UpdateQueryNode - Copy and Equals", "[update_query_node]") {
	auto stmt = ParseUpdate("UPDATE t SET a = 1, b = 'x' WHERE a > 0");
	auto &node = *stmt->node;

	auto copy = node.Copy();
	REQUIRE(copy != nullptr);
	REQUIRE(node.Equals(copy.get()));

	// Verify deep copy — mutating copy doesn't affect original
	auto &copy_node = copy->Cast<UpdateQueryNode>();
	copy_node.set_info->columns[0] = "changed";
	REQUIRE(!node.Equals(copy.get()));
}

TEST_CASE("UpdateQueryNode - Inequality on different tables", "[update_query_node]") {
	auto stmt1 = ParseUpdate("UPDATE t1 SET a = 1");
	auto stmt2 = ParseUpdate("UPDATE t2 SET a = 1");
	REQUIRE(!stmt1->node->Equals(stmt2->node.get()));
}

TEST_CASE("UpdateQueryNode - Inequality on different SET", "[update_query_node]") {
	auto stmt1 = ParseUpdate("UPDATE t SET a = 1");
	auto stmt2 = ParseUpdate("UPDATE t SET a = 2");
	REQUIRE(!stmt1->node->Equals(stmt2->node.get()));
}

TEST_CASE("UpdateQueryNode - Serialization round-trip", "[update_query_node]") {
	auto stmt = ParseUpdate("UPDATE t SET a = 1, b = 'x' WHERE a > 0 RETURNING a");
	UpdateQueryNode &node = *stmt->node;

	// Serialize via the base class (polymorphic)
	MemoryStream stream;
	BinarySerializer::Serialize(static_cast<QueryNode &>(node), stream);

	// Deserialize via QueryNode (polymorphic dispatch)
	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);
	REQUIRE(deserialized->type == QueryNodeType::UPDATE_QUERY_NODE);
	REQUIRE(node.Equals(deserialized.get()));
}

TEST_CASE("UpdateQueryNode - CTE in node", "[update_query_node]") {
	auto stmt = ParseUpdate("WITH cte AS (SELECT 1 AS x) UPDATE t SET a = cte.x FROM cte");
	REQUIRE(!stmt->node->cte_map.map.empty());
	auto str = stmt->ToString();
	REQUIRE(str.find("WITH") != string::npos);

	auto stmt2 = ParseUpdate(str);
	REQUIRE(stmt->node->Equals(stmt2->node.get()));
}

TEST_CASE("UpdateQueryNode - prioritize_table_when_binding survives copy", "[update_query_node]") {
	auto stmt = ParseUpdate("UPDATE t SET a = 1");
	stmt->node->prioritize_table_when_binding = true;

	auto copy = stmt->node->Copy();
	auto &copy_node = copy->Cast<UpdateQueryNode>();
	REQUIRE(copy_node.prioritize_table_when_binding == true);

	// Nodes with different flag values are not equal
	stmt->node->prioritize_table_when_binding = false;
	REQUIRE(!stmt->node->Equals(copy.get()));
}

TEST_CASE("UpdateQueryNode - prioritize_table_when_binding survives serialization", "[update_query_node]") {
	auto stmt = ParseUpdate("UPDATE t SET a = 1");
	stmt->node->prioritize_table_when_binding = true;
	UpdateQueryNode &node = *stmt->node;

	MemoryStream stream;
	BinarySerializer::Serialize(static_cast<QueryNode &>(node), stream);

	stream.Rewind();
	auto deserialized = BinaryDeserializer::Deserialize<QueryNode>(stream);
	auto &deserialized_node = deserialized->Cast<UpdateQueryNode>();
	REQUIRE(deserialized_node.prioritize_table_when_binding == true);
	REQUIRE(node.Equals(deserialized.get()));
}
