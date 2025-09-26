#pragma once

#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/variant.hpp"

namespace duckdb {

struct ShreddingType {
public:
	ShreddingType();
	explicit ShreddingType(const LogicalType &shredding_type);

public:
	void Serialize(Serializer &serializer) const;
	static ShreddingType Deserialize(Deserializer &source);

public:
	static ShreddingType GetShreddingTypes(const Value &val);

public:
	bool set = false;
	LogicalType shredding_type;
	case_insensitive_map_t<ShreddingType> children;
};

} // namespace duckdb
