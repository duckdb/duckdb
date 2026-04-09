#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class Serializer;
class Deserializer;
class BaseStatistics;

// Forward-declare StatisticsType (defined in base_statistics.hpp)
enum class StatisticsType : uint8_t;

struct LogicalTypeModifier {
public:
	explicit LogicalTypeModifier(Value value_p) : value(std::move(value_p)) {
	}
	string ToString() const {
		return label.empty() ? value.ToString() : label;
	}

public:
	Value value;
	string label;

	void Serialize(Serializer &serializer) const;
	static LogicalTypeModifier Deserialize(Deserializer &source);
};

//! Callback type for custom statistics collection on extension type values.
//! Called during checkpoint to update column statistics from each value.
typedef void (*extension_stats_update_t)(BaseStatistics &stats, const string_t &value);

struct ExtensionTypeInfo {
	vector<LogicalTypeModifier> modifiers;
	unordered_map<string, Value> properties;

	//! Override the statistics type used for columns of this extension type.
	//! When set to a valid StatisticsType (e.g., GEOMETRY_STATS), the column uses that
	//! statistics format instead of the default for its physical type.
	//! Default: unset (255) — use the base type's default statistics.
	static constexpr uint8_t STATS_TYPE_DEFAULT = 255;
	uint8_t stats_type = STATS_TYPE_DEFAULT;

	//! Custom statistics update function for extension types.
	//! If non-null, called instead of the default statistics Update function when
	//! collecting column statistics during checkpoint. This allows extension types
	//! to parse their custom binary format and produce correct statistics.
	//! Not serialized — restored when the extension is loaded.
	extension_stats_update_t stats_update = nullptr;

public:
	void Serialize(Serializer &serializer) const;
	static unique_ptr<ExtensionTypeInfo> Deserialize(Deserializer &source);
	static bool Equals(optional_ptr<ExtensionTypeInfo> rhs, optional_ptr<ExtensionTypeInfo> lhs);
};

} // namespace duckdb
