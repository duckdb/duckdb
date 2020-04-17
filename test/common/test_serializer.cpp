#include "catch.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "expression_helper.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Basic serializer test", "[serializer]") {
	BufferedSerializer serializer(4);
	serializer.Write<int32_t>(33);
	serializer.Write<uint64_t>(42);
	auto data = serializer.GetData();

	Deserializer source(data.data.get(), data.size);
	REQUIRE_NOTHROW(source.Read<int32_t>() == 33);
	REQUIRE_NOTHROW(source.Read<uint64_t>() == 42);
}

TEST_CASE("Data Chunk serialization", "[serializer]") {
	Value a = Value::INTEGER(33);
	Value b = Value::INTEGER(42);
	Value c = Value("hello");
	Value d = Value("world");
	// test serializing of DataChunk
	DataChunk chunk;
	vector<TypeId> types = {TypeId::INT32, TypeId::VARCHAR};
	chunk.Initialize(types);
	chunk.SetCardinality(2);
	chunk.SetValue(0, 0, a);
	chunk.SetValue(0, 1, b);
	chunk.SetValue(1, 0, c);
	chunk.SetValue(1, 1, d);

	BufferedSerializer serializer;
	chunk.Serialize(serializer);

	auto data = serializer.GetData();
	BufferedDeserializer source(data.data.get(), data.size);

	DataChunk other_chunk;
	REQUIRE_NOTHROW(other_chunk.Deserialize(source));
	REQUIRE(other_chunk.size() == 2);
	REQUIRE(other_chunk.column_count() == 2);
	REQUIRE(other_chunk.data[0].count == 2);
	REQUIRE(ValueOperations::Equals(other_chunk.GetValue(0, 0), a));
	REQUIRE(ValueOperations::Equals(other_chunk.GetValue(0, 1), b));
	REQUIRE(other_chunk.data[1].count == 2);
	REQUIRE(ValueOperations::Equals(other_chunk.GetValue(1, 0), c));
	REQUIRE(ValueOperations::Equals(other_chunk.GetValue(1, 1), d));
}

TEST_CASE("Value serialization", "[serializer]") {
	Value a = Value::BOOLEAN(12);
	Value b = Value::TINYINT(12);
	Value c = Value::SMALLINT(12);
	Value d = Value::INTEGER(12);
	Value e = Value::BIGINT(12);
	Value f = Value::POINTER(12);
	Value g = Value::DATE(12);
	Value h = Value();
	Value i = Value("hello world");

	BufferedSerializer serializer(4);
	a.Serialize(serializer);
	b.Serialize(serializer);
	c.Serialize(serializer);
	d.Serialize(serializer);
	e.Serialize(serializer);
	f.Serialize(serializer);
	g.Serialize(serializer);
	h.Serialize(serializer);
	i.Serialize(serializer);

	auto data = serializer.GetData();
	BufferedDeserializer source(data.data.get(), data.size);

	Value a1, b1, c1, d1, e1, f1, g1, h1, i1, j1;

	REQUIRE_NOTHROW(a1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(a, a1));
	REQUIRE_NOTHROW(b1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(b, b1));
	REQUIRE_NOTHROW(c1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(c, c1));
	REQUIRE_NOTHROW(d1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(d, d1));
	REQUIRE_NOTHROW(e1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(e, e1));
	REQUIRE_NOTHROW(f1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(f, f1));
	REQUIRE_NOTHROW(g1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(g, g1));
	REQUIRE_NOTHROW(h1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(h, h1));
	REQUIRE_NOTHROW(i1 = Value::Deserialize(source));
	REQUIRE(ValueOperations::Equals(i, i1));
	// try to deserialize too much, should throw a serialization exception
	REQUIRE_THROWS(j1 = Value::Deserialize(source));
}

TEST_CASE("Expression serializer", "[serializer]") {
	DuckDB db(nullptr);
	Connection con(db);

	ExpressionHelper helper(con.context);
	{
		// operator, columnref, constant
		auto expression = helper.ParseExpression("a + 2");
		REQUIRE(expression.get());

		BufferedSerializer serializer;
		expression->Serialize(serializer);

		auto data = serializer.GetData();
		BufferedDeserializer source(data.data.get(), data.size);
		unique_ptr<Expression> deserialized_expression;
		REQUIRE_NOTHROW(deserialized_expression = Expression::Deserialize(source));
		REQUIRE(deserialized_expression.get());
		REQUIRE(expression->Equals(deserialized_expression.get()));
	}

	{
		// case, columnref, comparison, cast, conjunction
		auto expression = helper.ParseExpression("cast(a >= 2 as integer) OR CASE WHEN a >= b THEN "
		                                         "a >= 5 ELSE a >= 7 END");
		REQUIRE(expression.get());

		BufferedSerializer serializer;
		expression->Serialize(serializer);

		auto data = serializer.GetData();
		BufferedDeserializer source(data.data.get(), data.size);
		unique_ptr<Expression> deserialized_expression;
		REQUIRE_NOTHROW(deserialized_expression = Expression::Deserialize(source));
		REQUIRE(deserialized_expression.get());
		REQUIRE(expression->Equals(deserialized_expression.get()));
		REQUIRE(expression->Hash() == deserialized_expression->Hash());
	}
	{
		// subquery, function, aggregate, case, negation
		auto expression = helper.ParseExpression("(SELECT 42) - COUNT(*) + + 33 - (CASE "
		                                         "WHEN NOT 0 THEN 33 ELSE 22 END)");
		REQUIRE(expression.get());

		BufferedSerializer serializer;
		expression->Serialize(serializer);

		auto data = serializer.GetData();
		BufferedDeserializer source(data.data.get(), data.size);
		unique_ptr<Expression> deserialized_expression;
		REQUIRE_NOTHROW(deserialized_expression = Expression::Deserialize(source));
		REQUIRE(deserialized_expression.get());
		REQUIRE(expression->Equals(deserialized_expression.get()));
		REQUIRE(expression->Hash() == deserialized_expression->Hash());
	}
	{
		// subtle differences should result in different results
		auto expression = helper.ParseExpression("(SELECT 42) - COUNT(*) + + 33 - (CASE "
		                                         "WHEN NOT 0 THEN 33 ELSE 22 END)");
		auto expression2 = helper.ParseExpression("(SELECT 43) - COUNT(*) + + 33 - (CASE "
		                                          "WHEN NOT 0 THEN 33 ELSE 22 END)");
		REQUIRE(expression.get());
		REQUIRE(expression2.get());

		BufferedSerializer serializer;
		expression->Serialize(serializer);
		expression2->Serialize(serializer);

		auto data = serializer.GetData();
		BufferedDeserializer source(data.data.get(), data.size);
		unique_ptr<Expression> deserialized, deserialized2;
		REQUIRE_NOTHROW(deserialized = Expression::Deserialize(source));
		REQUIRE_NOTHROW(deserialized2 = Expression::Deserialize(source));
		REQUIRE(deserialized.get());
		REQUIRE(expression->Equals(deserialized.get()));
		REQUIRE(expression->Hash() == deserialized->Hash());
		REQUIRE(expression2->Equals(deserialized2.get()));
		REQUIRE(expression2->Hash() == deserialized2->Hash());
		REQUIRE(!deserialized->Equals(deserialized2.get()));
	}
	{
		// conjunctions are commutative
		auto expression = helper.ParseExpression("(A = 42) AND (B = 43)");
		auto expression2 = helper.ParseExpression("(B = 43) AND (A = 42)");

		REQUIRE(expression.get());
		REQUIRE(expression2.get());

		REQUIRE(expression->Equals(expression2.get()));
		REQUIRE(expression->Hash() == expression2->Hash());
	}
}
