//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/geometry_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/hash.hpp"

namespace duckdb {

enum class GeometryType : uint8_t {
	INVALID = 0,
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7,
};

enum class VertexType : uint8_t {
	XY = 0,
	XYZ = 1,
	XYM = 2,
	XYZM = 3,
};

class GeometryExtent;
class Allocator;

struct geometry_t {
public:
	geometry_t() : part_type(GeometryType::INVALID), vert_type(VertexType::XY), count(0), array(nullptr) {
	}
	geometry_t(GeometryType type, char *data, uint32_t count)
	    : part_type(type), vert_type(VertexType::XY), count(count), array(data) {
		D_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(geometry_t) == 0);
	}

	geometry_t(GeometryType gtype, VertexType vtype, char *data, uint32_t count)
	    : part_type(gtype), vert_type(vtype), count(count), array(data) {
		D_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(geometry_t) == 0);
	}
	geometry_t(GeometryType type, bool has_z, bool has_m, char *data, uint32_t count)
	    : part_type(type),
	      vert_type(has_z ? (has_m ? VertexType::XYZM : VertexType::XYZ) : (has_m ? VertexType::XYM : VertexType::XY)),
	      count(count), array(data) {
		D_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(geometry_t) == 0);
	}

	geometry_t(GeometryType type, bool has_z, bool has_m, geometry_t *data, uint32_t count)
	    : part_type(type),
	      vert_type(has_z ? (has_m ? VertexType::XYZM : VertexType::XYZ) : (has_m ? VertexType::XYM : VertexType::XY)),
	      count(count), array(data) {
		D_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(geometry_t) == 0);
	}

	geometry_t(GeometryType type, bool has_z, bool has_m, double *data, uint32_t count)
	    : part_type(type),
	      vert_type(has_z ? (has_m ? VertexType::XYZM : VertexType::XYZ) : (has_m ? VertexType::XYM : VertexType::XY)),
	      count(count), array(data) {
		D_ASSERT(reinterpret_cast<uintptr_t>(data) % alignof(geometry_t) == 0);
	}

	geometry_t(GeometryType type, bool has_z, bool has_m)
	    : part_type(type),
	      vert_type(has_z ? (has_m ? VertexType::XYZM : VertexType::XYZ) : (has_m ? VertexType::XYM : VertexType::XY)),
	      count(0), array(nullptr) {
	}

	GeometryType GetType() const {
		return part_type;
	}
	uint32_t GetCount() const {
		return count;
	}
	bool IsEmpty() const {
		return count == 0;
	}

	template <class T = double>
	const T *GetVerts() const {
		return static_cast<const T *>(array);
	}
	const geometry_t *GetParts() const {
		return static_cast<const geometry_t *>(array);
	}

	template <class T = double>
	T *GetVerts() {
		return static_cast<T *>(array);
	}
	geometry_t *GetParts() {
		return static_cast<geometry_t *>(array);
	}

	void SetParts(geometry_t *parts, uint32_t part_count) {
		count = part_count;
		array = parts;
	}

	void SetVerts(double *verts, uint32_t vert_count) {
		count = vert_count;
		array = verts;
	}

	data_ptr_t GetDataPointer() const {
		return static_cast<data_ptr_t>(array);
	}

	size_t GetWidth() const {
		switch (vert_type) {
		case VertexType::XY:
			return 2 * sizeof(double);
		case VertexType::XYZ:
		case VertexType::XYM:
			return 3 * sizeof(double);
		case VertexType::XYZM:
			return 4 * sizeof(double);
		default:
			throw InternalException("Invalid vertex type in geometry_t::GetWidth");
		}
	}
	size_t GetDims() const {
		switch (vert_type) {
		case VertexType::XY:
			return 2;
		case VertexType::XYZ:
		case VertexType::XYM:
			return 3;
		case VertexType::XYZM:
			return 4;
		default:
			throw InternalException("Invalid vertex type in geometry_t::GetDims");
		}
	}

	GeometryType GetPartType() const {
		return part_type;
	}
	VertexType GetVertType() const {
		return vert_type;
	}

	bool HasZ() const {
		return vert_type == VertexType::XYZ || vert_type == VertexType::XYZM;
	}
	bool HasM() const {
		return vert_type == VertexType::XYM || vert_type == VertexType::XYZM;
	}

	bool operator==(const geometry_t &other) const;
	bool operator!=(const geometry_t &other) const {
		return !(*this == other);
	}
	bool operator<(const geometry_t &other) const;
	bool operator<=(const geometry_t &other) const {
		return *this < other || *this == other;
	}
	bool operator>(const geometry_t &other) const {
		return !(*this <= other);
	}
	bool operator>=(const geometry_t &other) const {
		return !(*this < other);
	}

	void Verify() const;

	static string Serialize(const geometry_t &geom);
	static geometry_t Deserialize(const string &data, Allocator &allocator);

	size_t GetTotalByteSize() const;
	void Serialize(char *ptr, uint32_t len) const;
	static geometry_t Deserialize(Vector &vector, const char *data, uint32_t len);

	static size_t GetHeapSize(const geometry_t &geom);
	static geometry_t CopyToHeap(const geometry_t &geom, data_ptr_t heap_data, size_t heap_size);
	static void RecomputeHeapPointers(geometry_t &geom, data_ptr_t old_heap_ptr, data_ptr_t new_heap_ptr);

	// Heap swizzling
	// static size_t GetHeapSize(const geometry_t &geom);
	// static void SerializeToHeap(const geometry_t &geom, data_ptr_t heap_data, size_t heap_size);
	// static void DeserializeFromHeap(geometry_t &root, data_ptr_t heap_data, bool allow_zero_copy, Allocator
	// &allocator);

private:
	GeometryType part_type;
	VertexType vert_type;
	uint16_t unused;
	uint32_t count;
	void *array;
};

} // namespace duckdb

namespace std {
template <>
struct hash<duckdb::geometry_t> {
	size_t operator()(const duckdb::geometry_t &val) const {
		return duckdb::Hash(val);
	}
};
} // namespace std
