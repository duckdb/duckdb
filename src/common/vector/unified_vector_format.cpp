#include "duckdb/common/vector/unified_vector_format.hpp"

namespace duckdb {

UnifiedVectorFormat::UnifiedVectorFormat() : sel(nullptr), data(nullptr), physical_type(PhysicalType::INVALID) {
}

UnifiedVectorFormat::UnifiedVectorFormat(UnifiedVectorFormat &&other) noexcept
    : sel(nullptr), data(nullptr), physical_type(PhysicalType::INVALID) {
	bool refers_to_self = other.sel == &other.owned_sel;
	std::swap(sel, other.sel);
	std::swap(data, other.data);
	std::swap(validity, other.validity);
	std::swap(owned_sel, other.owned_sel);
	std::swap(physical_type, other.physical_type);
	if (refers_to_self) {
		sel = &owned_sel;
	}
}

UnifiedVectorFormat &UnifiedVectorFormat::operator=(UnifiedVectorFormat &&other) noexcept {
	bool refers_to_self = other.sel == &other.owned_sel;
	std::swap(sel, other.sel);
	std::swap(data, other.data);
	std::swap(validity, other.validity);
	std::swap(owned_sel, other.owned_sel);
	std::swap(physical_type, other.physical_type);
	if (refers_to_self) {
		sel = &owned_sel;
	}
	return *this;
}

} // namespace duckdb
