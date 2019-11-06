#include "duckdb/parser/constraints/unique_constraint.hpp"

#include "duckdb/common/serializer.hpp"

using namespace std;
using namespace duckdb;

string UniqueConstraint::ToString() const {
	return is_primary_key ? "PRIMARY KEY constraint" : "UNIQUE Constraint";
}

unique_ptr<Constraint> UniqueConstraint::Copy() {
	if (index == INVALID_INDEX) {
		return make_unique<UniqueConstraint>(columns, is_primary_key);
	} else {
		assert(columns.size() == 0);
		return make_unique<UniqueConstraint>(index, is_primary_key);
	}
}

void UniqueConstraint::Serialize(Serializer &serializer) {
	Constraint::Serialize(serializer);
	serializer.Write<bool>(is_primary_key);
	serializer.Write<uint64_t>(index);
	assert(columns.size() <= numeric_limits<uint32_t>::max());
	serializer.Write<uint32_t>((uint32_t)columns.size());
	for (auto &column : columns) {
		serializer.WriteString(column);
	}
}

unique_ptr<Constraint> UniqueConstraint::Deserialize(Deserializer &source) {
	auto is_primary_key = source.Read<bool>();
	auto index = source.Read<uint64_t>();
	auto column_count = source.Read<uint32_t>();

	if (index != INVALID_INDEX) {
		// single column parsed constraint
		return make_unique<UniqueConstraint>(index, is_primary_key);
	} else {
		// column list parsed constraint
		vector<string> columns;
		for (uint32_t i = 0; i < column_count; i++) {
			auto column_name = source.Read<string>();
			columns.push_back(column_name);
		}
		return make_unique<UniqueConstraint>(columns, is_primary_key);
	}
}
