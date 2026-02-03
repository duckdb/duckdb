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

/*
void geometry_t::Serialize(Serializer &serializer) const {
    serializer.WritePropertyWithDefault(100, "type", part_type, GeometryType::INVALID);
    serializer.WritePropertyWithDefault(101, "vertex_type", vert_type, VertexType::XY);
    serializer.WritePropertyWithDefault(102, "count", count, 0u);

    switch (part_type) {
    case GeometryType::POINT:
    case GeometryType::LINESTRING: {
        const auto width = GetWidth();
        const auto verts = GetVerts<VertexXY>();
        serializer.WriteList(103, "vertices", count, [&](Serializer::List &list, idx_t idx) {
            list.WriteElement(verts[idx]);
        });
    } break;
    case GeometryType::POLYGON:
    case GeometryType::MULTIPOINT:
    case GeometryType::MULTILINESTRING:
    case GeometryType::MULTIPOLYGON:
    case GeometryType::GEOMETRYCOLLECTION: {
        const auto parts = GetParts();
        serializer.WriteList(103, "parts", count, [&](Serializer::List &list, idx_t idx) {
            list.WriteElement(parts[idx]);
        });
    } break;
    default:
        break;
    }
}

geometry_t geometry_t::Deserialize(Deserializer &deserializer) {

    auto &vec = deserializer.GetSerializationData().Get<Vector>();
    auto &arena = GeometryVector::GetArena(vec);

    geometry_t geom;
    geom.part_type = deserializer.ReadPropertyWithExplicitDefault<GeometryType>(100, "type", GeometryType::INVALID);
    geom.vert_type = deserializer.ReadPropertyWithExplicitDefault<VertexType>(101, "vertex_type", VertexType::XY);
    geom.count = deserializer.ReadPropertyWithExplicitDefault<uint32_t>(102, "count", 0u);
    switch (geom.part_type) {
    case GeometryType::POINT:
    case GeometryType::LINESTRING: {
        auto buffer = arena.AllocateAligned(geom.count * geom.GetWidth());
        auto verts = reinterpret_cast<VertexXY *>(buffer);
        geom.SetVerts(reinterpret_cast<double *>(verts), geom.count);

        deserializer.ReadList(103, "vertices", [&](Deserializer::List &list, idx_t idx) {
            verts[idx] = list.ReadElement<VertexXY>();
        });

        return geom;
    }
    case GeometryType::POLYGON:
    case GeometryType::MULTIPOINT:
    case GeometryType::MULTILINESTRING:
    case GeometryType::MULTIPOLYGON:
    case GeometryType::GEOMETRYCOLLECTION: {
        auto buffer = arena.AllocateAligned(geom.count * sizeof(geometry_t));
        auto parts = reinterpret_cast<geometry_t *>(buffer);
        geom.SetParts(parts, geom.count);

        deserializer.ReadList(103, "parts", [&](Deserializer::List &list, idx_t idx) {
            parts[idx] = list.ReadElement<geometry_t>();
        });

        return geom;
    }
    default:
        return geom;
    }
}
*/

} // namespace duckdb
