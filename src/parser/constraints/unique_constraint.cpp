#include "duckdb/parser/constraints/unique_constraint.hpp"

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

string UniqueConstraint::ToString() const {
	string base = is_primary_key ? "PRIMARY KEY(" : "UNIQUE(";
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			base += ", ";
		}
		base += KeywordHelper::WriteOptionallyQuoted(columns[i]);
	}
	return base + ")";
}

unique_ptr<Constraint> UniqueConstraint::Copy() {
	if (index == DConstants::INVALID_INDEX) {
		return make_unique<UniqueConstraint>(columns, is_primary_key);
	} else {
		auto result = make_unique<UniqueConstraint>(index, is_primary_key);
		result->columns = columns;
		return move(result);
	}
}

void UniqueConstraint::Serialize(Serializer &serializer) {
	Constraint::Serialize(serializer);
	serializer.Write<bool>(is_primary_key);
	serializer.Write<uint64_t>(index);
	D_ASSERT(columns.size() <= NumericLimits<uint32_t>::Maximum());
	serializer.Write<uint32_t>((uint32_t)columns.size());
	for (auto &column : columns) {
		serializer.WriteString(column);
	}
}

unique_ptr<Constraint> UniqueConstraint::Deserialize(Deserializer &source) {
	auto is_primary_key = source.Read<bool>();
	auto index = source.Read<uint64_t>();
	auto column_count = source.Read<uint32_t>();
	vector<string> columns;
	for (uint32_t i = 0; i < column_count; i++) {
		auto column_name = source.Read<string>();
		columns.push_back(column_name);
	}

	if (index != DConstants::INVALID_INDEX) {
		// single column parsed constraint
		auto result = make_unique<UniqueConstraint>(index, is_primary_key);
		result->columns = move(columns);
		return move(result);
	} else {
		// column list parsed constraint
		return make_unique<UniqueConstraint>(move(columns), is_primary_key);
	}
}

} // namespace duckdb
