#include "catch.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"

using namespace duckdb;
using namespace std;

//! Simulates an operator that used to work based on an integer but was was enhanced to work with floating point
//! at some point
class NewOperator : public LogicalOperator {

public:
	explicit NewOperator(double_t value)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_DUMMY_SCAN, vector<duckdb::unique_ptr<Expression>>()),
	      value(value) {
	}

	double_t value;

	// older versions used to use an integer
	static const uint64_t MINIMUM_VERSION_FOR_FLOAT = 2;

public:
	void Serialize(FieldWriter &writer) const override {
		if (writer.GetSerializer().GetVersion() >= MINIMUM_VERSION_FOR_FLOAT) {
			writer.WriteField(value);
		} else {
			// This is not a trivial choice and depends on the implementation. It may be
			// that just a truncate to an integer is the right thing. It may be something completely different.
			// Having the flexibility to use code in defining it is one of the key value-add of this approach
			writer.WriteField(static_cast<int32_t>(value));
		}
	}
	static duckdb::unique_ptr<LogicalOperator> Deserialize(ClientContext &context, LogicalOperatorType type,
	                                                       FieldReader &reader) {
		double_t value;
		if (reader.GetSource().GetVersion() >= MINIMUM_VERSION_FOR_FLOAT) {
			value = reader.ReadField<double_t>(-1.2);
		} else {
			value = reader.ReadField<int32_t>(-1);
		}
		return make_uniq_base<LogicalOperator, NewOperator>(value);
	}

private:
	void ResolveTypes() override {
	}
};

class OldOperator : public LogicalOperator {

public:
	explicit OldOperator(int32_t value) : LogicalOperator(LogicalOperatorType::LOGICAL_DUMMY_SCAN), value(value) {
	}

	int32_t value;

public:
	void Serialize(FieldWriter &writer) const override {
		writer.WriteField(value);
	}
	static duckdb::unique_ptr<LogicalOperator> Deserialize(ClientContext &context, LogicalOperatorType type,
	                                                       FieldReader &reader) {
		int32_t value = reader.ReadField(0);
		return make_uniq_base<LogicalOperator, OldOperator>(value);
	}

private:
	void ResolveTypes() override {
	}
};

template <typename S, typename T, typename V>
static void test_helper(const V version_compatible_value, const uint64_t sourceVersion, S *source_factory(V),
                        const uint64_t targetVersion,
                        duckdb::unique_ptr<LogicalOperator> target_deserialize(ClientContext &, LogicalOperatorType,
                                                                               FieldReader &)) {
	DuckDB db;
	Connection con(db);
	BufferedSerializer serializer;
	serializer.SetVersion(min(sourceVersion, targetVersion));
	INFO("source version: " << serializer.GetVersion());
	serializer.Write(serializer.GetVersion());
	duckdb::unique_ptr<S> write_op(source_factory(version_compatible_value));
	FieldWriter writer(serializer);
	write_op->Serialize(writer);
	writer.Finalize();

	auto data = serializer.GetData();
	auto deserializer = BufferedDeserializer(data.data.get(), data.size);
	deserializer.SetVersion(deserializer.Read<uint64_t>());
	INFO("target version: " << deserializer.GetVersion());
	FieldReader reader(deserializer);
	auto read_op = target_deserialize(*con.context, LogicalOperatorType::LOGICAL_DUMMY_SCAN, reader);
	reader.Finalize();

	REQUIRE(version_compatible_value == Approx(((T *)read_op.get())->value));
}

TEST_CASE("Test serializing / deserializing on the same version", "[serialization]") {
	const uint64_t NEW_VERSION = 3;
	test_helper<NewOperator, NewOperator, double_t>(
	    1.1, NEW_VERSION, [](double_t d) { return new NewOperator(d); }, NEW_VERSION, NewOperator::Deserialize);
}

TEST_CASE("Test serializing / deserializing from new to old", "[serialization]") {
	const uint64_t NEW_VERSION = 3;
	const uint64_t OLD_VERSION = 1;
	test_helper<NewOperator, OldOperator, double_t>(
	    1.0, NEW_VERSION, [](double_t d) { return new NewOperator(d); }, OLD_VERSION, OldOperator::Deserialize);
}

TEST_CASE("Test serializing / deserializing from old to new", "[serialization]") {
	const uint64_t NEW_VERSION = 3;
	const uint64_t OLD_VERSION = 1;
	test_helper<OldOperator, NewOperator, int32_t>(
	    1, OLD_VERSION, [](int32_t d) { return new OldOperator(d); }, NEW_VERSION, NewOperator::Deserialize);
}
