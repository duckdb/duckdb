#include "duckdb/common/types/geometry_type.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

// TODO: Verify depth and z/m consistency
void geometry_t::Verify() const {
#ifdef DEBUG

	// Always aligned
	D_ASSERT(reinterpret_cast<uintptr_t>(array) % alignof(geometry_t) == 0);

	const auto vert_type = GetVertType();
	switch (vert_type) {
	case VertexType::XY:
	case VertexType::XYZ:
	case VertexType::XYM:
	case VertexType::XYZM:
		break;
	default:
		throw InternalException("Invalid vertex type in geometry_t::Verify");
	}

	// Valid types
	switch (part_type) {
	case GeometryType::INVALID:
		break;
	case GeometryType::POINT:
		D_ASSERT(count == 0 || count == 1);
		break;
	case GeometryType::LINESTRING:
		break;
	case GeometryType::POLYGON: {
		const auto ring_array = GetParts();
		for (uint32_t ring_idx = 0; ring_idx < count; ring_idx++) {
			const auto &ring = ring_array[ring_idx];
			D_ASSERT(ring.GetType() == GeometryType::LINESTRING);
			ring.Verify();
		}
	} break;
	case GeometryType::MULTIPOINT: {
		const auto part_array = GetParts();
		for (uint32_t part_idx = 0; part_idx < count; part_idx++) {
			const auto &part = part_array[part_idx];
			D_ASSERT(part.GetType() == GeometryType::POINT);
			part.Verify();
		}
	} break;
	case GeometryType::MULTILINESTRING: {
		const auto part_array = GetParts();
		for (uint32_t part_idx = 0; part_idx < count; part_idx++) {
			const auto &part = part_array[part_idx];
			D_ASSERT(part.GetType() == GeometryType::LINESTRING);
			part.Verify();
		}
	} break;
	case GeometryType::MULTIPOLYGON: {
		const auto part_array = GetParts();
		for (uint32_t part_idx = 0; part_idx < count; part_idx++) {
			const auto &part = part_array[part_idx];
			D_ASSERT(part.GetType() == GeometryType::POLYGON);
			part.Verify();
		}
	} break;
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_array = GetParts();
		for (uint32_t part_idx = 0; part_idx < count; part_idx++) {
			const auto &part = part_array[part_idx];
			part.Verify();
		}
	} break;
	default:
		throw InternalException("Invalid geometry part type in geometry_t::Verify");
	}
#endif
}

bool geometry_t::operator==(const geometry_t &other) const {
	if (part_type != other.part_type) {
		return false;
	}
	if (vert_type != other.vert_type) {
		return false;
	}
	if (count != other.count) {
		return false;
	}
	switch (part_type) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto width = GetWidth();
		const auto total_size = count * width;
		return memcmp(array, other.array, total_size) == 0;
	}
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_array = GetParts();
		const auto other_part_array = other.GetParts();

		for (uint32_t part_idx = 0; part_idx < count; part_idx++) {
			if (part_array[part_idx] != other_part_array[part_idx]) {
				return false;
			}
		}
		return true;
	}
	default:
		return true;
	}
}

bool geometry_t::operator<(const geometry_t &other) const {
	if (part_type != other.part_type) {
		return part_type < other.part_type;
	}
	if (vert_type != other.vert_type) {
		return vert_type < other.vert_type;
	}
	if (count != other.count) {
		return count < other.count;
	}
	switch (part_type) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto width = GetWidth();
		const auto total_size = count * width;
		return memcmp(array, other.array, total_size) < 0;
	}
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_array = GetParts();
		const auto other_part_array = other.GetParts();

		for (uint32_t part_idx = 0; part_idx < count; part_idx++) {
			if (part_array[part_idx] < other_part_array[part_idx]) {
				return true;
			}
			if (other_part_array[part_idx] < part_array[part_idx]) {
				return false;
			}
		}
		return false;
	}
	default:
		return false;
	}
}


} // namespace duckdb
