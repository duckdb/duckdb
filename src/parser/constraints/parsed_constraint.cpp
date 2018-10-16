
#include "common/serializer.hpp"

#include "parser/constraints/parsed_constraint.hpp"

using namespace std;
using namespace duckdb;

void ParsedConstraint::Serialize(Serializer &serializer) {
	Constraint::Serialize(serializer);
	serializer.Write<int>((int)ctype);
	serializer.Write<size_t>(index);
	serializer.Write<uint32_t>(columns.size());
	for (auto &column : columns) {
		serializer.WriteString(column);
	}
}

unique_ptr<Constraint> ParsedConstraint::Deserialize(Deserializer &source) {
	auto type = (ConstraintType)source.Read<int>();
	auto index = source.Read<size_t>();
	auto column_count = source.Read<uint32_t>();

	if (index != (size_t)-1) {
		// single column parsed constraint
		return make_unique_base<Constraint, ParsedConstraint>(type, index);
	} else {
		// column list parsed constraint
		vector<string> columns;
		for (size_t i = 0; i < column_count; i++) {
			auto column_name = source.Read<string>();
			columns.push_back(column_name);
		}
		return make_unique_base<Constraint, ParsedConstraint>(type, columns);
	}
}
