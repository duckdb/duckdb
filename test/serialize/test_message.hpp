#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

//! Test-only message: a list whose count is unknown while it is written, and two values that are
//! only known afterwards.
struct TestMessageItem {
	int32_t value = 0;
	string label;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<TestMessageItem> Deserialize(Deserializer &deserializer);
};

struct TestMessage {
	string name;
	vector<unique_ptr<TestMessageItem>> items;
	idx_t total_items = 0;
	optional_idx next_index;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<TestMessage> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
