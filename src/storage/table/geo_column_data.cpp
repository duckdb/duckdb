#include "duckdb/storage/table/geo_column_data.hpp"

#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

static void UnshredPoints(Vector &blob_vec, Vector &geom_vec, Vector &result, idx_t count) {
	UnifiedVectorFormat blob_uvu;
	UnifiedVectorFormat geom_uvu;

	blob_vec.ToUnifiedFormat(count, blob_uvu);
	geom_vec.ToUnifiedFormat(count, geom_uvu);

	const auto &parts = StructVector::GetEntries(geom_vec);
	const auto &x_vec = *parts[0];
	const auto &y_vec = *parts[1];

	const auto result_data = FlatVector::GetData<string_t>(result);
	const auto blob_data = UnifiedVectorFormat::GetData<string_t>(blob_uvu);
	const auto x_data = FlatVector::GetData<double>(x_vec);
	const auto y_data = FlatVector::GetData<double>(y_vec);

	for (idx_t res_idx = 0; res_idx < count; res_idx++) {
		const auto blob_idx = blob_uvu.sel->get_index(res_idx);
		const auto geom_idx = geom_uvu.sel->get_index(res_idx);
		const auto blob_valid = blob_uvu.validity.RowIsValid(blob_idx);
		const auto geom_valid = geom_uvu.validity.RowIsValid(geom_idx);

		// Both null
		if (!blob_valid && !geom_valid) {
			FlatVector::SetNull(result, res_idx, true);
			continue;
		}

		if (blob_valid && !geom_valid) {
			result_data[res_idx] = StringVector::AddStringOrBlob(result, blob_data[blob_idx]);
			continue;
		}

		if (!blob_valid && geom_valid) {
			char buffer[1 + 4 + 8 + 8];
			memcpy(buffer, "\x01\x01\x00\x00\x00", 5); // POINT type
			memcpy(buffer + 5, &x_data[geom_idx], 8);
			memcpy(buffer + 13, &y_data[geom_idx], 8);
			result_data[res_idx] = StringVector::AddStringOrBlob(result, string_t(buffer, sizeof(buffer)));
			continue;
		}

		// Both cant be valid
		D_ASSERT(false);
	}
}

static void ShredPoints(Vector &scan_vector, Vector &blob_vector, Vector &geom_vector, idx_t count) {
	auto &parts = StructVector::GetEntries(geom_vector);
	auto &x_vec = *parts[0];
	auto &y_vec = *parts[1];
	const auto x_data = FlatVector::GetData<double>(x_vec);
	const auto y_data = FlatVector::GetData<double>(y_vec);
	const auto blob_data = FlatVector::GetData<string_t>(blob_vector);

	// TODO: This is cheating and should be generalized:
	UnifiedVectorFormat scan_uvu;
	scan_vector.ToUnifiedFormat(count, scan_uvu);
	const auto scan_data = UnifiedVectorFormat::GetData<string_t>(scan_uvu);

	for (idx_t res_idx = 0; res_idx < count; res_idx++) {
		const auto row_idx = scan_uvu.sel->get_index(res_idx);

		if (!scan_uvu.validity.RowIsValid(row_idx)) {
			FlatVector::SetNull(blob_vector, res_idx, true);
			FlatVector::SetNull(geom_vector, res_idx, true);
			continue;
		}

		const auto &blob = scan_data[row_idx];
		const auto type = Geometry::GetType(blob);
		if (type.first == GeometryType::POINT && type.second == VertexType::XY) {
			// Shred!
			const auto data = blob.GetData();
			// X/Y is at (1 + 4) offset
			memcpy(&x_data[res_idx], data + sizeof(uint8_t) + sizeof(uint32_t), sizeof(double));
			memcpy(&y_data[res_idx], data + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(double), sizeof(double));
			FlatVector::SetNull(blob_vector, res_idx, true);
		} else {
			// Keep in BLOB
			blob_data[res_idx] = StringVector::AddStringOrBlob(blob_vector, blob);
			FlatVector::SetNull(geom_vector, res_idx, true);
		}
	}
}

static void UnshredPolygons(Vector &blob_vec, Vector &geom_vec, Vector &result, idx_t count) {
	// First pass, handle unshredded types

	const auto result_data = FlatVector::GetData<string_t>(result);

	UnifiedVectorFormat blob_uvu;
	blob_vec.ToUnifiedFormat(count, blob_uvu);
	for (idx_t res_idx = 0; res_idx < count; res_idx++) {
		const auto row_idx = blob_uvu.sel->get_index(res_idx);
		const auto blob_valid = blob_uvu.validity.RowIsValid(row_idx);
		if (blob_valid) {
			const auto &blob_data = FlatVector::GetData<string_t>(blob_vec);
			result_data[res_idx] = StringVector::AddStringOrBlob(result, blob_data[row_idx]);
		}
	}

	// Second pass, handle shredded types
	geom_vec.Flatten(count);

	const auto &poly_data = ListVector::GetData(geom_vec);
	const auto &ring_vec = ListVector::GetEntry(geom_vec);
	const auto &ring_data = ListVector::GetData(ring_vec);
	const auto &vert_vec = ListVector::GetEntry(ring_vec);
	const auto &vert_data = StructVector::GetEntries(vert_vec);
	const auto &x_vec = *vert_data[0];
	const auto &y_vec = *vert_data[1];

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto &poly_entry = poly_data[row_idx];

		// First pass, determine required size
		size_t required_size = 1 + 4 + 4; // Header + type + num_rings
		for (idx_t ring_idx = 0; ring_idx < poly_entry.length; ring_idx++) {
			const auto &ring_entry = ring_data[poly_entry.offset + ring_idx];
			required_size += 4;
			required_size += ring_entry.length * (sizeof(double) + sizeof(double)); // Vertices
		}

		auto target_blob = StringVector::EmptyString(result, required_size);

		// Second pass, serialize
		auto ptr = target_blob.GetDataWriteable();

		// Header
		memcpy(ptr, "\x01\x03\x00\x00\x00", 5);
		ptr += 5;

		// Num rings
		const auto num_rings = static_cast<uint32_t>(poly_entry.length);
		memcpy(ptr, &num_rings, sizeof(uint32_t));
		ptr += sizeof(uint32_t);

		for (idx_t ring_idx = 0; ring_idx < poly_entry.length; ring_idx++) {
			const auto &ring_entry = ring_data[poly_entry.offset + ring_idx];

			// Num vertices
			const auto num_verts = static_cast<uint32_t>(ring_entry.length);
			memcpy(ptr, &num_verts, sizeof(uint32_t));
			ptr += sizeof(uint32_t);

			for (idx_t vert_idx = 0; vert_idx < ring_entry.length; vert_idx++) {
				// Reconstruct polygon from x/y
				auto x = FlatVector::GetData<double>(x_vec)[ring_entry.offset + vert_idx];
				auto y = FlatVector::GetData<double>(y_vec)[ring_entry.offset + vert_idx];

				memcpy(ptr, &x, sizeof(double));
				ptr += sizeof(double);
				memcpy(ptr, &y, sizeof(double));
				ptr += sizeof(double);
			}
		}

		result_data[row_idx] = target_blob;

		D_ASSERT(ptr - target_blob.GetDataWriteable() == static_cast<ptrdiff_t>(required_size));
	}
}

static void ShredPolygons(Vector &scan_vector, Vector &blob_vector, Vector &poly_vec, idx_t count) {
	// TODO: Handle mixed types

	// First pass, compute lengths
	scan_vector.Flatten(count);

	// Make blob vector all null
	for (idx_t i = 0; i < count; i++) {
		FlatVector::SetNull(blob_vector, i, true);
	}

	const auto &scan_data = FlatVector::GetData<string_t>(scan_vector);

	idx_t total_rings = 0;
	idx_t total_verts = 0;

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		// Get the blob

		const auto &blob = scan_data[row_idx];
		const auto type = Geometry::GetType(blob);

		// TODO: Handle!
		if (type.first != GeometryType::POLYGON || type.second != VertexType::XY) {
			throw NotImplementedException("Shredding only implemented for POLYGON XY types");
		}

		auto ptr = blob.GetData();
		D_ASSERT(ptr[0] == 1); // Endianness
		ptr += sizeof(uint8_t);

		uint32_t header;
		memcpy(&header, ptr, sizeof(uint32_t));
		ptr += sizeof(uint32_t);

		D_ASSERT(header == 3); // POLYGON
		uint32_t num_rings;
		memcpy(&num_rings, ptr, sizeof(uint32_t));
		ptr += sizeof(uint32_t);

		total_rings += num_rings;

		for (idx_t ring_idx = 0; ring_idx < num_rings; ring_idx++) {
			uint32_t num_verts;
			memcpy(&num_verts, ptr, sizeof(uint32_t));
			ptr += sizeof(uint32_t);

			total_verts += num_verts;

			// Skip vertices
			ptr += num_verts * (sizeof(double) + sizeof(double));
		}
	}

	auto &ring_vec = ListVector::GetEntry(poly_vec);
	auto &vert_vec = ListVector::GetEntry(ring_vec);

	// Allocate entries
	ListVector::Reserve(poly_vec, total_rings);
	ListVector::SetListSize(poly_vec, total_rings);

	ListVector::Reserve(ring_vec, total_verts);
	ListVector::SetListSize(ring_vec, total_verts);

	auto poly_data = ListVector::GetData(poly_vec);
	auto ring_data = ListVector::GetData(ring_vec);

	auto &vert_data = StructVector::GetEntries(vert_vec);
	auto &x_vec = *vert_data[0];
	auto &y_vec = *vert_data[1];
	const auto x_data = FlatVector::GetData<double>(x_vec);
	const auto y_data = FlatVector::GetData<double>(y_vec);

	// Second pass, fill in data
	idx_t ring_offset = 0;
	idx_t vert_offset = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		// Get the blob

		const auto &blob = scan_data[row_idx];

		auto ptr = blob.GetData();
		D_ASSERT(ptr[0] == 1); // Endianness
		ptr += sizeof(uint8_t);

		uint32_t header;
		memcpy(&header, ptr, sizeof(uint32_t));
		ptr += sizeof(uint32_t);

		D_ASSERT(header == 3); // POLYGON

		uint32_t num_rings;
		memcpy(&num_rings, ptr, sizeof(uint32_t));
		ptr += sizeof(uint32_t);

		poly_data[row_idx] = list_entry_t(ring_offset, num_rings);
		for (idx_t ring_idx = 0; ring_idx < num_rings; ring_idx++) {
			uint32_t num_verts;
			memcpy(&num_verts, ptr, sizeof(uint32_t));
			ptr += sizeof(uint32_t);

			ring_data[ring_offset + ring_idx] = list_entry_t(vert_offset, num_verts);

			for (idx_t vert_idx = 0; vert_idx < num_verts; vert_idx++) {
				// Reconstruct polygon from x/y
				double x;
				double y;
				memcpy(&x, ptr, sizeof(double));
				ptr += sizeof(double);
				memcpy(&y, ptr, sizeof(double));
				ptr += sizeof(double);

				x_data[vert_offset + vert_idx] = x;
				y_data[vert_offset + vert_idx] = y;
			}

			vert_offset += num_verts;
		}
		ring_offset += num_rings;
	}

	D_ASSERT(ring_offset == total_rings);
	D_ASSERT(vert_offset == total_verts);
}

void GeoColumnData::Reassemble(DataChunk &split, Vector &base) {
	auto &source_type = split.data[1].GetType();

	if (source_type.id() == LogicalTypeId::STRUCT) {
		UnshredPoints(split.data[0], split.data[1], base, split.size());
		return;
	}

	if (source_type.id() == LogicalTypeId::LIST) {
		UnshredPolygons(split.data[0], split.data[1], base, split.size());
		return;
	}

	throw InvalidInputException("GeoColumnData::Reassemble: Unsupported split type %s",
	                            split.data[1].GetType().ToString());
}

void GeoColumnData::Split(Vector &base, DataChunk &split) {
	auto &target_type = split.data[1].GetType();
	if (target_type.id() == LogicalTypeId::STRUCT) {
		ShredPoints(base, split.data[0], split.data[1], split.size());
		return;
	}
	if (target_type.id() == LogicalTypeId::LIST) {
		ShredPolygons(base, split.data[0], split.data[1], split.size());
		return;
	}

	throw InvalidInputException("GeoColumnData::Split: Unsupported split type %s", target_type.ToString());
}

LogicalType GeoColumnData::GetBaseType() {
	return LogicalType::GEOMETRY();
}

void GeoColumnData::GetSplitTypes(vector<LogicalType> &result) {
	UnifiedVectorFormat v_format;
	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::GEOMETRY()}, STANDARD_VECTOR_SIZE);
	auto &scan_vector = scan_chunk.data[0];

	ColumnScanState scan_state(nullptr);
	InitializeScan(scan_state);

	auto scanned_stats = GeometryStats::CreateEmpty(type);

	idx_t total_count = count.load();
	idx_t vector_index = 0;
	for (idx_t scanned = 0; scanned < total_count; scanned += STANDARD_VECTOR_SIZE) {
		scan_chunk.Reset();
		auto to_scan = MinValue(total_count - scanned, static_cast<idx_t>(STANDARD_VECTOR_SIZE));
		ScanCommitted(vector_index++, scan_state, scan_vector, false, to_scan);

		// Now update the stats
		scan_vector.ToUnifiedFormat(to_scan, v_format);
		const auto v_data = UnifiedVectorFormat::GetData<string_t>(v_format);
		for (idx_t r_idx = 0; r_idx < to_scan; r_idx++) {
			const auto v_idx = v_format.sel->get_index(r_idx);
			if (!v_format.validity.RowIsValid(v_idx)) {
				continue;
			}
			GeometryStats::Update(scanned_stats, v_data[v_idx]);
		}
	}

	// If the stats only contain POINT XY, we can shred
	auto &types = GeometryStats::GetTypes(scanned_stats);
	if (types.HasOnly(GeometryType::POINT, VertexType::XY)) {
		// Push POINT_XY type
		result.push_back(LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}}));
		return;
	}

	if (types.HasOnly(GeometryType::POLYGON, VertexType::XY)) {
		// Push POLYGON_XY type
		result.push_back(LogicalType::LIST(
		    LogicalType::LIST(LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}}))));
	}
}

void GeoColumnData::CombineStats(const vector<unique_ptr<BaseStatistics>> &source_stats, BaseStatistics &target_stats) const {
	// Combine the stats from the split columns into the target stats
	// TODO:
}

GeoColumnData::GeoColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, LogicalType type,
                             ColumnDataType data_type, optional_ptr<ColumnData> parent)
    : SplitColumnData(block_manager, info, column_index, std::move(type), data_type, parent) {
}

shared_ptr<SplitColumnData> GeoColumnData::Create(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                                  LogicalType type, ColumnDataType data_type,
                                                  optional_ptr<ColumnData> parent) const {
	return make_shared_ptr<GeoColumnData>(block_manager, info, column_index, type, data_type, parent);
}

} // namespace duckdb
