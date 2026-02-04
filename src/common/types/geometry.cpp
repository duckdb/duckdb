#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "fast_float/fast_float.h"
#include "fmt/format.h"

//----------------------------------------------------------------------------------------------------------------------
// Internals
//----------------------------------------------------------------------------------------------------------------------
namespace duckdb {

namespace {

class BlobWriter {
public:
	template <class T>
	void Write(const T &value) {
		auto le_value = BSwapIfBE(value);
		auto ptr = reinterpret_cast<const char *>(&le_value);
		buffer.insert(buffer.end(), ptr, ptr + sizeof(T));
	}

	template <class T>
	struct Reserved {
		size_t offset;
		T value;
	};

	template <class T>
	Reserved<T> Reserve() {
		auto offset = buffer.size();
		buffer.resize(buffer.size() + sizeof(T));
		return {offset, T()};
	}

	template <class T>
	void Write(const Reserved<T> &reserved) {
		if (reserved.offset + sizeof(T) > buffer.size()) {
			throw InternalException("Write out of bounds in BinaryWriter");
		}
		auto le_value = BSwapIfBE(reserved.value);
		auto ptr = reinterpret_cast<const char *>(&le_value);
		// We've reserved 0 bytes, so we can safely memcpy
		memcpy(buffer.data() + reserved.offset, ptr, sizeof(T));
	}

	const vector<char> &GetBuffer() const {
		return buffer;
	}

	void Clear() {
		buffer.clear();
	}

private:
	vector<char> buffer;
};

class FixedSizeBlobWriter {
public:
	FixedSizeBlobWriter(char *data, uint32_t size) : beg(data), pos(data), end(data + size) {
	}

	template <class T>
	void Write(const T &value) {
		if (pos + sizeof(T) > end) {
			throw InvalidInputException("Writing beyond end of binary data at position %zu", pos - beg);
		}
		auto le_value = BSwapIfBE(value);
		memcpy(pos, &le_value, sizeof(T));
		pos += sizeof(T);
	}

	size_t GetPosition() const {
		return static_cast<idx_t>(pos - beg);
	}

	char *GetPointer() {
		return pos;
	}

	void Skip(size_t size) {
		pos += size;
	}

	bool IsAtEnd() const {
		return pos >= end;
	}

private:
	const char *beg;
	char *pos;
	const char *end;
};

class BlobReader {
public:
	BlobReader(const char *data, uint32_t size) : beg(data), pos(data), end(data + size) {
	}

	template <class T>
	T Read(const bool le) {
		if (le) {
			return Read<T, true>();
		} else {
			return Read<T, false>();
		}
	}

	template <class T, bool LE = true>
	T Read() {
		if (pos + sizeof(T) > end) {
			throw InvalidInputException("Unexpected end of binary data at position %zu", pos - beg);
		}
		T value;
		memcpy(&value, pos, sizeof(T));
		value = LE ? BSwapIfBE(value) : BSwapIfLE(value);
		pos += sizeof(T);
		return value;
	}

	void Skip(size_t size) {
		if (pos + size > end) {
			throw InvalidInputException("Skipping beyond end of binary data at position %zu", pos - beg);
		}
		pos += size;
	}

	const char *Reserve(size_t size) {
		if (pos + size > end) {
			throw InvalidInputException("Reserving beyond end of binary data at position %zu", pos - beg);
		}
		auto current_pos = pos;
		pos += size;
		return current_pos;
	}

	size_t GetPosition() const {
		return static_cast<idx_t>(pos - beg);
	}

	const char *GetDataPtr() const {
		return pos;
	}

	bool IsAtEnd() const {
		return pos >= end;
	}

	void Reset() {
		pos = beg;
	}

private:
	const char *beg;
	const char *pos;
	const char *end;
};

class TextWriter {
public:
	void Write(const char *str) {
		buffer.insert(buffer.end(), str, str + strlen(str));
	}
	void Write(char c) {
		buffer.push_back(c);
	}
	void Write(double value) {
		duckdb_fmt::format_to(std::back_inserter(buffer), "{}", value);
		// Remove trailing zero
		if (buffer.back() == '0') {
			buffer.pop_back();
			if (buffer.back() == '.') {
				buffer.pop_back();
			}
		}
	}
	const vector<char> &GetBuffer() const {
		return buffer;
	}

private:
	vector<char> buffer;
};

class TextReader {
public:
	TextReader(const char *text, const uint32_t size) : beg(text), pos(text), end(text + size) {
	}

	bool TryMatch(const char *str) {
		auto ptr = pos;
		while (*str && pos < end && tolower(*pos) == tolower(*str)) {
			pos++;
			str++;
		}
		if (*str == '\0') {
			SkipWhitespace(); // remove trailing whitespace
			return true;      // matched
		}
		pos = ptr;    // reset position
		return false; // not matched
	}

	bool TryMatch(char c) {
		if (pos < end && tolower(*pos) == tolower(c)) {
			pos++;
			SkipWhitespace(); // remove trailing whitespace
			return true;      // matched
		}
		return false; // not matched
	}

	void Match(const char *str) {
		if (!TryMatch(str)) {
			throw InvalidInputException("Expected '%s' but got '%c' at position %zu", str, *pos, pos - beg);
		}
	}

	void Match(char c) {
		if (!TryMatch(c)) {
			throw InvalidInputException("Expected '%c' but got '%c' at position %zu", c, *pos, pos - beg);
		}
	}

	double MatchNumber() {
		// Now use fast_float to parse the number
		double num;
		const auto res = duckdb_fast_float::from_chars(pos, end, num);
		if (res.ec != std::errc()) {
			throw InvalidInputException("Expected number at position %zu", pos - beg);
		}

		pos = res.ptr; // update position to the end of the parsed number

		SkipWhitespace(); // remove trailing whitespace
		return num;       // return the parsed number
	}

	idx_t GetPosition() const {
		return static_cast<idx_t>(pos - beg);
	}

	void Reset() {
		pos = beg;
	}

private:
	void SkipWhitespace() {
		while (pos < end && isspace(*pos)) {
			pos++;
		}
	}

	const char *beg;
	const char *pos;
	const char *end;
};

void FromStringRecursive(TextReader &reader, BlobWriter &writer, uint32_t depth, bool parent_has_z, bool parent_has_m) {
	if (depth == Geometry::MAX_RECURSION_DEPTH) {
		throw InvalidInputException("Geometry string exceeds maximum recursion depth of %d",
		                            Geometry::MAX_RECURSION_DEPTH);
	}

	GeometryType type;

	if (reader.TryMatch("point")) {
		type = GeometryType::POINT;
	} else if (reader.TryMatch("linestring")) {
		type = GeometryType::LINESTRING;
	} else if (reader.TryMatch("polygon")) {
		type = GeometryType::POLYGON;
	} else if (reader.TryMatch("multipoint")) {
		type = GeometryType::MULTIPOINT;
	} else if (reader.TryMatch("multilinestring")) {
		type = GeometryType::MULTILINESTRING;
	} else if (reader.TryMatch("multipolygon")) {
		type = GeometryType::MULTIPOLYGON;
	} else if (reader.TryMatch("geometrycollection")) {
		type = GeometryType::GEOMETRYCOLLECTION;
	} else {
		throw InvalidInputException("Unknown geometry type at position %zu", reader.GetPosition());
	}

	const auto has_z = reader.TryMatch("z");
	const auto has_m = reader.TryMatch("m");

	const auto is_empty = reader.TryMatch("empty");

	if ((depth != 0) && ((parent_has_z != has_z) || (parent_has_m != has_m))) {
		throw InvalidInputException("Geometry has inconsistent Z/M dimensions, starting at position %zu",
		                            reader.GetPosition());
	}

	// How many dimensions does this geometry have?
	const uint32_t dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);

	// WKB type
	const auto meta = static_cast<uint32_t>(type) + (has_z ? 1000 : 0) + (has_m ? 2000 : 0);
	// Write the geometry type and vertex type
	writer.Write<uint8_t>(1); // LE Byte Order
	writer.Write<uint32_t>(meta);

	switch (type) {
	case GeometryType::POINT: {
		if (is_empty) {
			for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
				// Write NaN for each dimension, if point is empty
				writer.Write<double>(std::numeric_limits<double>::quiet_NaN());
			}
		} else {
			reader.Match('(');
			for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
				auto value = reader.MatchNumber();
				writer.Write<double>(value);
			}
			reader.Match(')');
		}
	} break;
	case GeometryType::LINESTRING: {
		if (is_empty) {
			writer.Write<uint32_t>(0); // No vertices in empty linestring
			break;
		}
		auto vert_count = writer.Reserve<uint32_t>();
		reader.Match('(');
		do {
			for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
				auto value = reader.MatchNumber();
				writer.Write<double>(value);
			}
			vert_count.value++;
		} while (reader.TryMatch(','));
		reader.Match(')');
		writer.Write(vert_count);
	} break;
	case GeometryType::POLYGON: {
		if (is_empty) {
			writer.Write<uint32_t>(0);
			break; // No rings in empty polygon
		}
		auto ring_count = writer.Reserve<uint32_t>();
		reader.Match('(');
		do {
			auto vert_count = writer.Reserve<uint32_t>();
			reader.Match('(');
			do {
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					auto value = reader.MatchNumber();
					writer.Write<double>(value);
				}
				vert_count.value++;
			} while (reader.TryMatch(','));
			reader.Match(')');
			writer.Write(vert_count);
			ring_count.value++;
		} while (reader.TryMatch(','));
		reader.Match(')');
		writer.Write(ring_count);
	} break;
	case GeometryType::MULTIPOINT: {
		if (is_empty) {
			writer.Write<uint32_t>(0); // No points in empty multipoint
			break;
		}
		auto part_count = writer.Reserve<uint32_t>();
		reader.Match('(');
		do {
			bool has_paren = reader.TryMatch('(');

			const auto part_meta = static_cast<uint32_t>(GeometryType::POINT) + (has_z ? 1000 : 0) + (has_m ? 2000 : 0);
			writer.Write<uint8_t>(1);
			writer.Write<uint32_t>(part_meta);

			if (reader.TryMatch("EMPTY")) {
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					// Write NaN for each dimension, if point is empty
					writer.Write<double>(std::numeric_limits<double>::quiet_NaN());
				}
			} else {
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					auto value = reader.MatchNumber();
					writer.Write<double>(value);
				}
			}
			if (has_paren) {
				reader.Match(')'); // Match the closing parenthesis if it was opened
			}
			part_count.value++;
		} while (reader.TryMatch(','));
		writer.Write(part_count);
	} break;
	case GeometryType::MULTILINESTRING: {
		if (is_empty) {
			writer.Write<uint32_t>(0);
			return; // No linestrings in empty multilinestring
		}
		auto part_count = writer.Reserve<uint32_t>();
		reader.Match('(');
		do {
			const auto part_meta =
			    static_cast<uint32_t>(GeometryType::LINESTRING) + (has_z ? 1000 : 0) + (has_m ? 2000 : 0);
			writer.Write<uint8_t>(1);
			writer.Write<uint32_t>(part_meta);

			auto vert_count = writer.Reserve<uint32_t>();
			reader.Match('(');
			do {
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					auto value = reader.MatchNumber();
					writer.Write<double>(value);
				}
				vert_count.value++;
			} while (reader.TryMatch(','));
			reader.Match(')');
			writer.Write(vert_count);
			part_count.value++;
		} while (reader.TryMatch(','));
		reader.Match(')');
		writer.Write(part_count);
	} break;
	case GeometryType::MULTIPOLYGON: {
		if (is_empty) {
			writer.Write<uint32_t>(0); // No polygons in empty multipolygon
			break;
		}
		auto part_count = writer.Reserve<uint32_t>();
		reader.Match('(');
		do {
			const auto part_meta =
			    static_cast<uint32_t>(GeometryType::POLYGON) + (has_z ? 1000 : 0) + (has_m ? 2000 : 0);
			writer.Write<uint8_t>(1);
			writer.Write<uint32_t>(part_meta);

			auto ring_count = writer.Reserve<uint32_t>();
			reader.Match('(');
			do {
				auto vert_count = writer.Reserve<uint32_t>();
				reader.Match('(');
				do {
					for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
						auto value = reader.MatchNumber();
						writer.Write<double>(value);
					}
					vert_count.value++;
				} while (reader.TryMatch(','));
				reader.Match(')');
				writer.Write(vert_count);
				ring_count.value++;
			} while (reader.TryMatch(','));
			reader.Match(')');
			writer.Write(ring_count);
			part_count.value++;
		} while (reader.TryMatch(','));
		reader.Match(')');
		writer.Write(part_count);
	} break;
	case GeometryType::GEOMETRYCOLLECTION: {
		if (is_empty) {
			writer.Write<uint32_t>(0); // No geometries in empty geometry collection
			break;
		}
		auto part_count = writer.Reserve<uint32_t>();
		reader.Match('(');
		do {
			// Recursively parse the geometry inside the collection
			FromStringRecursive(reader, writer, depth + 1, has_z, has_m);
			part_count.value++;
		} while (reader.TryMatch(','));
		reader.Match(')');
		writer.Write(part_count);
	} break;
	default:
		throw InvalidInputException("Unknown geometry type %d at position %zu", static_cast<int>(type),
		                            reader.GetPosition());
	}
}

void ToStringRecursive(BlobReader &reader, TextWriter &writer, idx_t depth, bool parent_has_z, bool parent_has_m) {
	if (depth == Geometry::MAX_RECURSION_DEPTH) {
		throw InvalidInputException("Geometry exceeds maximum recursion depth of %d", Geometry::MAX_RECURSION_DEPTH);
	}

	// Read the byte order (should always be 1 for little-endian)
	auto byte_order = reader.Read<uint8_t>();
	if (byte_order != 1) {
		throw InvalidInputException("Unsupported byte order %d in WKB", byte_order);
	}

	const auto meta = reader.Read<uint32_t>();
	const auto type = static_cast<GeometryType>((meta & 0x0000FFFF) % 1000);
	const auto flag = (meta & 0x0000FFFF) / 1000;
	const auto has_z = (flag & 0x01) != 0;
	const auto has_m = (flag & 0x02) != 0;

	if ((depth != 0) && ((parent_has_z != has_z) || (parent_has_m != has_m))) {
		throw InvalidInputException("Geometry has inconsistent Z/M dimensions, starting at position %zu",
		                            reader.GetPosition());
	}

	const uint32_t dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);
	const auto flag_str = has_z ? (has_m ? " ZM " : " Z ") : (has_m ? " M " : " ");

	switch (type) {
	case GeometryType::POINT: {
		writer.Write("POINT");
		writer.Write(flag_str);

		double vert[4] = {0, 0, 0, 0};
		auto all_nan = true;
		for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
			vert[d_idx] = reader.Read<double>();
			all_nan &= std::isnan(vert[d_idx]);
		}
		if (all_nan) {
			writer.Write("EMPTY");
			return;
		}
		writer.Write('(');
		for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
			if (d_idx > 0) {
				writer.Write(' ');
			}
			writer.Write(vert[d_idx]);
		}
		writer.Write(')');
	} break;
	case GeometryType::LINESTRING: {
		writer.Write("LINESTRING");
		;
		writer.Write(flag_str);
		const auto vert_count = reader.Read<uint32_t>();
		if (vert_count == 0) {
			writer.Write("EMPTY");
			return;
		}
		writer.Write('(');
		for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
			if (vert_idx > 0) {
				writer.Write(", ");
			}
			for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
				if (d_idx > 0) {
					writer.Write(' ');
				}
				auto value = reader.Read<double>();
				writer.Write(value);
			}
		}
		writer.Write(')');
	} break;
	case GeometryType::POLYGON: {
		writer.Write("POLYGON");
		writer.Write(flag_str);
		const auto ring_count = reader.Read<uint32_t>();
		if (ring_count == 0) {
			writer.Write("EMPTY");
			return;
		}
		writer.Write('(');
		for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
			if (ring_idx > 0) {
				writer.Write(", ");
			}
			const auto vert_count = reader.Read<uint32_t>();
			if (vert_count == 0) {
				writer.Write("EMPTY");
				continue;
			}
			writer.Write('(');
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				if (vert_idx > 0) {
					writer.Write(", ");
				}
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					if (d_idx > 0) {
						writer.Write(' ');
					}
					auto value = reader.Read<double>();
					writer.Write(value);
				}
			}
			writer.Write(')');
		}
		writer.Write(')');
	} break;
	case GeometryType::MULTIPOINT: {
		writer.Write("MULTIPOINT");
		writer.Write(flag_str);
		const auto part_count = reader.Read<uint32_t>();
		if (part_count == 0) {
			writer.Write("EMPTY");
			return;
		}
		writer.Write('(');
		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			const auto part_byte_order = reader.Read<uint8_t>();
			if (part_byte_order != 1) {
				throw InvalidInputException("Unsupported byte order %d in WKB", part_byte_order);
			}
			const auto part_meta = reader.Read<uint32_t>();
			const auto part_type = static_cast<GeometryType>((part_meta & 0x0000FFFF) % 1000);
			const auto part_flag = (part_meta & 0x0000FFFF) / 1000;
			const auto part_has_z = (part_flag & 0x01) != 0;
			const auto part_has_m = (part_flag & 0x02) != 0;

			if (part_type != GeometryType::POINT) {
				throw InvalidInputException("Expected POINT in MULTIPOINT but got %d", static_cast<int>(part_type));
			}

			if ((has_z != part_has_z) || (has_m != part_has_m)) {
				throw InvalidInputException(
				    "Geometry has inconsistent Z/M dimensions in MULTIPOINT, starting at position %zu",
				    reader.GetPosition());
			}
			if (part_idx > 0) {
				writer.Write(", ");
			}
			double vert[4] = {0, 0, 0, 0};
			auto all_nan = true;
			for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
				vert[d_idx] = reader.Read<double>();
				all_nan &= std::isnan(vert[d_idx]);
			}
			if (all_nan) {
				writer.Write("EMPTY");
				continue;
			}
			// writer.Write('(');
			for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
				if (d_idx > 0) {
					writer.Write(' ');
				}
				writer.Write(vert[d_idx]);
			}
			// writer.Write(')');
		}
		writer.Write(')');

	} break;
	case GeometryType::MULTILINESTRING: {
		writer.Write("MULTILINESTRING");
		writer.Write(flag_str);
		const auto part_count = reader.Read<uint32_t>();
		if (part_count == 0) {
			writer.Write("EMPTY");
			return;
		}
		writer.Write('(');
		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			const auto part_byte_order = reader.Read<uint8_t>();
			if (part_byte_order != 1) {
				throw InvalidInputException("Unsupported byte order %d in WKB", part_byte_order);
			}
			const auto part_meta = reader.Read<uint32_t>();
			const auto part_type = static_cast<GeometryType>((part_meta & 0x0000FFFF) % 1000);
			const auto part_flag = (part_meta & 0x0000FFFF) / 1000;
			const auto part_has_z = (part_flag & 0x01) != 0;
			const auto part_has_m = (part_flag & 0x02) != 0;

			if (part_type != GeometryType::LINESTRING) {
				throw InvalidInputException("Expected LINESTRING in MULTILINESTRING but got %d",
				                            static_cast<int>(part_type));
			}
			if ((has_z != part_has_z) || (has_m != part_has_m)) {
				throw InvalidInputException(
				    "Geometry has inconsistent Z/M dimensions in MULTILINESTRING, starting at position %zu",
				    reader.GetPosition());
			}
			if (part_idx > 0) {
				writer.Write(", ");
			}
			const auto vert_count = reader.Read<uint32_t>();
			if (vert_count == 0) {
				writer.Write("EMPTY");
				continue;
			}
			writer.Write('(');
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				if (vert_idx > 0) {
					writer.Write(", ");
				}
				for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
					if (d_idx > 0) {
						writer.Write(' ');
					}
					auto value = reader.Read<double>();
					writer.Write(value);
				}
			}
			writer.Write(')');
		}
		writer.Write(')');
	} break;
	case GeometryType::MULTIPOLYGON: {
		writer.Write("MULTIPOLYGON");
		writer.Write(flag_str);
		const auto part_count = reader.Read<uint32_t>();
		if (part_count == 0) {
			writer.Write("EMPTY");
			return;
		}
		writer.Write('(');
		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			if (part_idx > 0) {
				writer.Write(", ");
			}

			const auto part_byte_order = reader.Read<uint8_t>();
			if (part_byte_order != 1) {
				throw InvalidInputException("Unsupported byte order %d in WKB", part_byte_order);
			}
			const auto part_meta = reader.Read<uint32_t>();
			const auto part_type = static_cast<GeometryType>((part_meta & 0x0000FFFF) % 1000);
			const auto part_flag = (part_meta & 0x0000FFFF) / 1000;
			const auto part_has_z = (part_flag & 0x01) != 0;
			const auto part_has_m = (part_flag & 0x02) != 0;
			if (part_type != GeometryType::POLYGON) {
				throw InvalidInputException("Expected POLYGON in MULTIPOLYGON but got %d", static_cast<int>(part_type));
			}
			if ((has_z != part_has_z) || (has_m != part_has_m)) {
				throw InvalidInputException(
				    "Geometry has inconsistent Z/M dimensions in MULTIPOLYGON, starting at position %zu",
				    reader.GetPosition());
			}

			const auto ring_count = reader.Read<uint32_t>();
			if (ring_count == 0) {
				writer.Write("EMPTY");
				continue;
			}
			writer.Write('(');
			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				if (ring_idx > 0) {
					writer.Write(", ");
				}
				const auto vert_count = reader.Read<uint32_t>();
				if (vert_count == 0) {
					writer.Write("EMPTY");
					continue;
				}
				writer.Write('(');
				for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
					if (vert_idx > 0) {
						writer.Write(", ");
					}
					for (uint32_t d_idx = 0; d_idx < dims; d_idx++) {
						if (d_idx > 0) {
							writer.Write(' ');
						}
						auto value = reader.Read<double>();
						writer.Write(value);
					}
				}
				writer.Write(')');
			}
			writer.Write(')');
		}
		writer.Write(')');
	} break;
	case GeometryType::GEOMETRYCOLLECTION: {
		writer.Write("GEOMETRYCOLLECTION");
		writer.Write(flag_str);
		const auto part_count = reader.Read<uint32_t>();
		if (part_count == 0) {
			writer.Write("EMPTY");
			return;
		}
		writer.Write('(');
		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			if (part_idx > 0) {
				writer.Write(", ");
			}
			// Recursively parse the geometry inside the collection
			ToStringRecursive(reader, writer, depth + 1, has_z, has_m);
		}
		writer.Write(')');
	} break;
	default:
		throw InvalidInputException("Unsupported geometry type %d in WKB", static_cast<int>(type));
	}
}

} // namespace

} // namespace duckdb

//----------------------------------------------------------------------------------------------------------------------
// Public interface
//----------------------------------------------------------------------------------------------------------------------
namespace duckdb {
constexpr const idx_t Geometry::MAX_RECURSION_DEPTH;

static geometry_t ReadWKB(BlobReader &reader, Allocator &allocator, idx_t depth = 0) {
	if (depth > Geometry::MAX_RECURSION_DEPTH) {
		throw InvalidInputException("Geometry exceeds maximum recursion depth of %d", Geometry::MAX_RECURSION_DEPTH);
	}

	const auto le = reader.Read<uint8_t>() == 1;
	const auto meta = reader.Read<uint32_t>(le);
	const auto type_id = (meta & 0x0000FFFF) % 1000;
	const auto flag_id = (meta & 0x0000FFFF) / 1000;

	// Extended WKB detection
	const auto has_extz = (meta & 0x80000000) != 0;
	const auto has_extm = (meta & 0x40000000) != 0;
	const auto has_srid = (meta & 0x20000000) != 0;

	const auto has_z = ((flag_id & 0x01) != 0) || has_extz;
	const auto has_m = ((flag_id & 0x02) != 0) || has_extm;

	if (has_srid) {
		reader.Skip(sizeof(uint32_t)); // Skip SRID
	}

	if (!le) {
		// TODO: Support big-endian WKB
		throw InvalidInputException("Big-endian WKB is not supported");
	}

	if (type_id < 1 || type_id > 7) {
		throw InvalidInputException("Unsupported geometry type %d in WKB", type_id);
	}

	const auto type = static_cast<GeometryType>(type_id);

	switch (type) {
	case GeometryType::POINT: {
		const auto vert_width = (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0)) * sizeof(double);
		const auto vert_array = reinterpret_cast<double *>(allocator.AllocateData(vert_width));
		const auto data_array = reader.Reserve(vert_width);
		memcpy(vert_array, data_array, vert_width);

		// Check if all dimensions are NaN (empty point)
		bool all_nan = true;
		const uint32_t dims_count = (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0));
		for (uint32_t dim_idx = 0; dim_idx < dims_count; dim_idx++) {
			if (!std::isnan(vert_array[dim_idx])) {
				all_nan = false;
				break;
			}
		}

		if (all_nan) {
			return geometry_t(type, has_z, has_m);
		}

		return geometry_t(type, has_z, has_m, vert_array, 1);
	}
	case GeometryType::LINESTRING: {
		const auto vert_count = reader.Read<uint32_t>();
		if (vert_count == 0) {
			return geometry_t(type, has_z, has_m);
		}

		const auto vert_width = (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0)) * sizeof(double);
		const auto vert_array = reinterpret_cast<double *>(allocator.AllocateData(vert_count * vert_width));
		const auto data_array = reader.Reserve(vert_count * vert_width);
		memcpy(vert_array, data_array, vert_count * vert_width);

		return geometry_t(type, has_z, has_m, vert_array, vert_count);
	}
	case GeometryType::POLYGON: {
		const auto ring_count = reader.Read<uint32_t>();
		if (ring_count == 0) {
			return geometry_t(type, has_z, has_m);
		}

		const auto ring_array = reinterpret_cast<geometry_t *>(allocator.AllocateData(ring_count * sizeof(geometry_t)));

		for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
			const auto vert_count = reader.Read<uint32_t>();

			if (vert_count == 0) {
				ring_array[ring_idx] = geometry_t(GeometryType::LINESTRING, has_z, has_m);
				continue;
			}

			const auto vert_width = (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0)) * sizeof(double);
			const auto vert_array = reinterpret_cast<double *>(allocator.AllocateData(vert_count * vert_width));
			const auto data_array = reader.Reserve(vert_count * vert_width);
			memcpy(vert_array, data_array, vert_count * vert_width);

			ring_array[ring_idx] = geometry_t(GeometryType::LINESTRING, has_z, has_m, vert_array, vert_count);
		}

		return geometry_t(type, has_z, has_m, ring_array, ring_count);
	}
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_count = reader.Read<uint32_t>();

		if (part_count == 0) {
			return geometry_t(type, has_z, has_m);
		}

		const auto part_array = reinterpret_cast<geometry_t *>(allocator.AllocateData(part_count * sizeof(geometry_t)));

		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			part_array[part_idx] = ReadWKB(reader, allocator, depth + 1);
		}

		return geometry_t(type, has_z, has_m, part_array, part_count);
	}
	default:
		throw InvalidInputException("Unsupported geometry type %d in WKB", type_id);
	}
}

geometry_t Geometry::FromBinary(const string_t &wkb, Vector &result_vector) {
	BlobReader reader(wkb.GetData(), static_cast<uint32_t>(wkb.GetSize()));
	auto result = ReadWKB(reader, GeometryVector::GetArena(result_vector).GetAllocator());
	result.Verify();
	return result;
}

bool Geometry::FromBinary(Vector &source, Vector &result, idx_t count, bool strict) {
	if (strict) {
		UnaryExecutor::Execute<string_t, geometry_t>(source, result, count,
		                                             [&](const string_t &wkb) { return FromBinary(wkb, result); });
		return true;
	}

	auto all_ok = true;
	UnaryExecutor::ExecuteWithNulls<string_t, geometry_t>(source, result, count,
	                                                      [&](const string_t &wkb, ValidityMask &mask, idx_t idx) {
		                                                      try {
			                                                      return FromBinary(wkb, result);
		                                                      } catch (...) {
			                                                      all_ok = false;
			                                                      mask.SetInvalid(idx);
		                                                      	 return geometry_t();
		                                                      }
	                                                      });

	return all_ok;
}

static size_t RequiredSizeWKB(const geometry_t &geom) {
	size_t size = sizeof(uint8_t) + sizeof(uint32_t); // Byte order + type/meta
	switch (geom.GetType()) {
	case GeometryType::POINT: {
		const auto vert_width = geom.GetWidth();
		size += vert_width;
	} break;
	case GeometryType::LINESTRING: {
		const auto vert_count = geom.GetCount();
		const auto vert_width = geom.GetWidth();
		size += sizeof(uint32_t) + (vert_count * vert_width);
	} break;
	case GeometryType::POLYGON: {
		const auto ring_count = geom.GetCount();
		const auto ring_array = geom.GetParts();

		size += sizeof(uint32_t);
		for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
			const auto &ring = ring_array[ring_idx];
			const auto vert_count = ring.GetCount();
			const auto vert_width = ring.GetWidth();
			size += sizeof(uint32_t) + (vert_count * vert_width);
		}
	} break;
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_count = geom.GetCount();
		const auto part_array = geom.GetParts();
		size += sizeof(uint32_t);
		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			const auto &part = part_array[part_idx];
			size += RequiredSizeWKB(part);
		}
	} break;
	default:
		throw InvalidInputException("Unsupported geometry type %d in WKB", static_cast<int>(geom.GetType()));
	}
	return size;
}

static void WriteWKB(FixedSizeBlobWriter &writer, const geometry_t &geom) {
	const auto geom_type = geom.GetType();
	const auto vert_type = geom.GetVertType();

	const auto has_z = vert_type == VertexType::XYZ || vert_type == VertexType::XYZM;
	const auto has_m = vert_type == VertexType::XYM || vert_type == VertexType::XYZM;

	// Always write LE & type header
	writer.Write<uint8_t>(1);
	writer.Write<uint32_t>(static_cast<uint32_t>(geom_type) + (1000 * has_z) + (2000 * has_m));

	switch (geom_type) {
	case GeometryType::POINT: {
		const auto vert_array = geom.GetVerts();
		const auto dims_count = geom.GetDims();
		for (uint32_t dim_idx = 0; dim_idx < dims_count; dim_idx++) {
			writer.Write<double>(vert_array[dim_idx]);
		}
	} break;
	case GeometryType::LINESTRING: {
		const auto vert_count = geom.GetCount();
		const auto vert_array = geom.GetVerts();
		writer.Write<uint32_t>(vert_count);
		const auto dims_count = geom.GetDims();
		for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
			for (uint32_t dim_idx = 0; dim_idx < dims_count; dim_idx++) {
				writer.Write<double>(vert_array[vert_idx * dims_count + dim_idx]);
			}
		}
	} break;
	case GeometryType::POLYGON: {
		const auto ring_count = geom.GetCount();
		const auto ring_array = geom.GetParts();
		writer.Write<uint32_t>(ring_count);
		for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
			const auto &ring = ring_array[ring_idx];
			const auto vert_count = ring.GetCount();
			const auto vert_array = ring.GetVerts();
			writer.Write<uint32_t>(vert_count);
			const auto dims_count = ring.GetDims();
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				for (uint32_t dim_idx = 0; dim_idx < dims_count; dim_idx++) {
					writer.Write<double>(vert_array[vert_idx * dims_count + dim_idx]);
				}
			}
		}
	} break;
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_count = geom.GetCount();
		const auto part_array = geom.GetParts();
		writer.Write<uint32_t>(part_count);
		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			const auto &part = part_array[part_idx];
			WriteWKB(writer, part);
		}
	} break;
	default:
		throw InvalidInputException("Unsupported geometry type %d in WKB", static_cast<int>(geom_type));
	}
}

idx_t Geometry::ToBinarySize(const geometry_t &geom) {
	return RequiredSizeWKB(geom);
}

string Geometry::ToBinary(const geometry_t &geom) {
	const auto required_size = RequiredSizeWKB(geom);
	if (required_size > NumericLimits<uint32_t>::Maximum()) {
		throw InvalidInputException("Geometry is too large to convert to WKB");
	}

	const auto size = static_cast<uint32_t>(required_size);
	string result;
	result.resize(size);

	// Get mutable pointer to string data
	auto ptr = &result[0];

	FixedSizeBlobWriter writer(ptr, size);
	WriteWKB(writer, geom);

	// We should be exactly at the end of the blob now
	D_ASSERT(writer.IsAtEnd());

	return result;
}

void Geometry::ToBinary(Vector &source, Vector &result, idx_t count) {
	UnaryExecutor::Execute<geometry_t, string_t>(source, result, count, [&](const geometry_t &geom) {
		const auto required_size = RequiredSizeWKB(geom);
		if (required_size > NumericLimits<uint32_t>::Maximum()) {
			throw InvalidInputException("Geometry is too large to convert to WKB");
		}

		const auto size = static_cast<uint32_t>(required_size);
		const auto blob = StringVector::EmptyString(result, size);

		FixedSizeBlobWriter writer(blob.GetDataWriteable(), size);
		WriteWKB(writer, geom);

		// We should be exactly at the end of the blob now
		D_ASSERT(writer.IsAtEnd());

		return blob;
	});
}

static geometry_t ParseInternal(TextReader &reader, ArenaAllocator &allocator) {
	if (reader.TryMatch("POINT")) {
		const auto has_z = reader.TryMatch("Z");
		const auto has_m = reader.TryMatch("M");

		geometry_t geom(GeometryType::POINT, has_z, has_m);

		if (reader.TryMatch("EMPTY")) {
			return geom;
		}

		reader.Match('(');

		const auto vertex_width = geom.GetWidth();
		const auto vertex_dims = geom.GetDims();
		const auto vertex_array = reinterpret_cast<double *>(allocator.AllocateAligned(vertex_width));

		for (uint32_t d_idx = 0; d_idx < vertex_dims; d_idx++) {
			vertex_array[d_idx] = reader.MatchNumber();
		}

		reader.Match(')');
		geom.SetVerts(vertex_array, 1);
		return geom;
	}

	if (reader.TryMatch("LINESTRING")) {
		const auto has_z = reader.TryMatch("Z");
		const auto has_m = reader.TryMatch("M");

		auto geom = geometry_t(GeometryType::LINESTRING, has_z, has_m);

		if (reader.TryMatch("EMPTY")) {
			return geom;
		}

		reader.Match('(');
		const auto vertex_dims = geom.GetDims();
		vector<double> vertices;
		do {
			for (uint32_t d_idx = 0; d_idx < vertex_dims; d_idx++) {
				auto value = reader.MatchNumber();
				vertices.push_back(value);
			}
		} while (reader.TryMatch(','));
		reader.Match(')');
		const auto vertex_array =
		    reinterpret_cast<double *>(allocator.AllocateAligned(vertices.size() * sizeof(double)));
		memcpy(vertex_array, vertices.data(), vertices.size() * sizeof(double));
		geom.SetVerts(vertex_array, static_cast<uint32_t>(vertices.size() / vertex_dims));
		return geom;
	}

	throw InvalidInputException("Unsupported geometry type at position %zu", reader.GetPosition());
}

bool Geometry::FromString(const string_t &wkt_text, geometry_t &result, Vector &result_vector, bool strict) {
	TextReader reader(wkt_text.GetData(), static_cast<uint32_t>(wkt_text.GetSize()));

	result = ParseInternal(reader, GeometryVector::GetArena(result_vector));

	// const auto &buffer = writer.GetBuffer();
	// result = StringVector::AddStringOrBlob(result_vector, buffer.data(), buffer.size());
	return true;
}

void WriteString(TextWriter &writer, const geometry_t &geom) {
	switch (geom.GetType()) {
	case GeometryType::POINT: {
		const auto vert_count = geom.GetCount();
		if (vert_count == 0) {
			writer.Write("POINT EMPTY");
			;
			return;
		}
		writer.Write("POINT (");
		const auto vertex_dims = geom.GetDims();
		const auto vertex_array = geom.GetVerts();
		for (uint32_t d_idx = 0; d_idx < vertex_dims; d_idx++) {
			if (d_idx > 0) {
				writer.Write(' ');
			}
			writer.Write(vertex_array[d_idx]);
		}
		writer.Write(')');
	} break;
	case GeometryType::LINESTRING: {
		const auto vert_count = geom.GetCount();
		if (vert_count == 0) {
			writer.Write("LINESTRING EMPTY");
			return;
		}
		writer.Write("LINESTRING (");
		const auto vertex_dims = geom.GetDims();
		const auto vertex_array = geom.GetVerts();
		for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
			if (vert_idx > 0) {
				writer.Write(", ");
			}
			for (uint32_t d_idx = 0; d_idx < vertex_dims; d_idx++) {
				if (d_idx > 0) {
					writer.Write(' ');
				}
				writer.Write(vertex_array[vert_idx * vertex_dims + d_idx]);
			}
		}
		writer.Write(')');
	} break;
	case GeometryType::POLYGON: {
		const auto ring_count = geom.GetCount();
		if (ring_count == 0) {
			writer.Write("POLYGON EMPTY");
			return;
		}
		writer.Write("POLYGON (");
		const auto ring_array = geom.GetParts();
		for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
			if (ring_idx > 0) {
				writer.Write(", ");
			}
			const auto &ring = ring_array[ring_idx];
			const auto vert_count = ring.GetCount();
			if (vert_count == 0) {
				writer.Write("EMPTY");
				continue;
			}
			writer.Write('(');
			const auto vert_array = ring.GetVerts();
			const auto dims_count = ring.GetDims();
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				if (vert_idx > 0) {
					writer.Write(", ");
				}
				for (uint32_t d_idx = 0; d_idx < dims_count; d_idx++) {
					if (d_idx > 0) {
						writer.Write(' ');
					}
					writer.Write(vert_array[vert_idx * dims_count + d_idx]);
				}
			}
			writer.Write(')');
		}
		writer.Write(')');
	} break;
	default:
		writer.Write("UNKNOWN GEOMETRY TYPE");
		// throw InvalidInputException("Unsupported geometry type %d in WKB", static_cast<int>(geom.GetType()));
	}
}

string_t Geometry::ToString(Vector &result, const geometry_t &geom) {
	TextWriter writer;

	// ToStringRecursive(reader, writer, 0, false, false);
	WriteString(writer, geom);

	// Convert the buffer to string_t
	const auto &buffer = writer.GetBuffer();
	return StringVector::AddString(result, buffer.data(), buffer.size());
}

template <class VERTEX_TYPE = VertexXY>
static uint32_t ParseVerticesInternal(BlobReader &reader, GeometryExtent &extent, uint32_t vert_count, bool check_nan) {
	uint32_t count = 0;

	// Issue a single .Reserve() for all vertices, to minimize bounds checking overhead
	const auto ptr = const_data_ptr_cast(reader.Reserve(vert_count * sizeof(VERTEX_TYPE)));
#if DUCKDB_IS_BIG_ENDIAN
	double be_buffer[sizeof(VERTEX_TYPE)];
	auto be_ptr = reinterpret_cast<const_data_ptr_t>(be_buffer);
#endif
	for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
#if DUCKDB_IS_BIG_ENDIAN
		auto vert_ofs = vert_idx * sizeof(VERTEX_TYPE);
		for (idx_t i = 0; i < sizeof(VERTEX_TYPE) / sizeof(double); ++i) {
			be_buffer[i] = LoadLE<double>(ptr + vert_ofs + i * sizeof(double));
		}
		VERTEX_TYPE vertex = Load<VERTEX_TYPE>(be_ptr);
#else
		VERTEX_TYPE vertex = Load<VERTEX_TYPE>(ptr + vert_idx * sizeof(VERTEX_TYPE));
#endif
		if (check_nan && vertex.AllNan()) {
			continue;
		}

		extent.Extend(vertex);
		count++;
	}
	return count;
}

static uint32_t ParseVertices(BlobReader &reader, GeometryExtent &extent, uint32_t vert_count, VertexType type,
                              bool check_nan) {
	switch (type) {
	case VertexType::XY:
		return ParseVerticesInternal<VertexXY>(reader, extent, vert_count, check_nan);
	case VertexType::XYZ:
		return ParseVerticesInternal<VertexXYZ>(reader, extent, vert_count, check_nan);
	case VertexType::XYM:
		return ParseVerticesInternal<VertexXYM>(reader, extent, vert_count, check_nan);
	case VertexType::XYZM:
		return ParseVerticesInternal<VertexXYZM>(reader, extent, vert_count, check_nan);
	default:
		throw InvalidInputException("Unsupported vertex type %d in WKB", static_cast<int>(type));
	}
}

uint32_t Geometry::GetExtent(const geometry_t &geometry, GeometryExtent &extent) {
	bool has_any_empty = false;
	return GetExtent(geometry, extent, has_any_empty);
}

uint32_t Geometry::GetExtent(const geometry_t &geometry, GeometryExtent &extent, bool &has_any_empty) {
	if (geometry.IsEmpty()) {
		has_any_empty = true;
		return 0;
	}

	switch (geometry.GetPartType()) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto vert_vtype = geometry.GetVertType();
		const auto vert_count = geometry.GetCount();

		switch (vert_vtype) {
		case VertexType::XY: {
			const auto vert_array = geometry.GetVerts<VertexXY>();
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				const auto &vertex = vert_array[vert_idx];
				extent.Extend(vertex);
			}
		} break;
		case VertexType::XYZ: {
			const auto vert_array = geometry.GetVerts<VertexXYZ>();
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				const auto &vertex = vert_array[vert_idx];
				extent.Extend(vertex);
			}
		} break;
		case VertexType::XYM: {
			const auto vert_array = geometry.GetVerts<VertexXYM>();
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				const auto &vertex = vert_array[vert_idx];
				extent.Extend(vertex);
			}
		} break;
		case VertexType::XYZM: {
			const auto vert_array = geometry.GetVerts<VertexXYZM>();
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				const auto &vertex = vert_array[vert_idx];
				extent.Extend(vertex);
			}
		} break;
		default:
			break;
		}

		return vert_count;
	}
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_array = geometry.GetParts();
		const auto part_count = geometry.GetCount();

		uint32_t vert_count = 0;

		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			const auto &part_geom = part_array[part_idx];
			vert_count += GetExtent(part_geom, extent, has_any_empty);
		}

		return vert_count;
	}
	default:
		return 0;
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Shredding
//----------------------------------------------------------------------------------------------------------------------

template <class V = VertexXY>
static void ToPoints(Vector &source_vec, Vector &target_vec, idx_t row_count) {
	// Flatten the source vector to extract all vertices
	source_vec.Flatten(row_count);

	const auto geom_data = FlatVector::GetData<geometry_t>(source_vec);
	const auto &vert_parts = StructVector::GetEntries(target_vec);
	double *vert_data[V::WIDTH];

	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, row_idx, true);
			continue;
		}

		const auto &geom = geom_data[row_idx];
		const auto vert_count = geom.GetCount();
		const auto vert_array = geom.GetVerts();

		if (vert_count == 0) {
			// Empty point, set all dimensions to NaN
			for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
				vert_data[dim_idx][row_idx] = std::numeric_limits<double>::quiet_NaN();
			}
			continue;
		}

		for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
			vert_data[dim_idx][row_idx] = vert_array[dim_idx];
		}
	}
}

template <class V = VertexXY>
static void FromPoints(Vector &source_vec, Vector &target_vec, idx_t row_count, idx_t result_offset) {
	// Flatten the source vector to extract all vertices
	source_vec.Flatten(row_count);

	const auto &vert_parts = StructVector::GetEntries(source_vec);
	const auto geom_data = FlatVector::GetData<geometry_t>(target_vec);
	double *vert_data[V::WIDTH];

	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	auto &arena = GeometryVector::GetArena(target_vec);

	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		const auto out_idx = result_offset + row_idx;

		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, out_idx, true);
			continue;
		}

		const auto vert_array = reinterpret_cast<double *>(arena.AllocateAligned(sizeof(V)));
		for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
			vert_array[dim_idx] = vert_data[dim_idx][row_idx];
		}
		geom_data[out_idx] = geometry_t(GeometryType::POINT, V::HAS_Z, V::HAS_M, vert_array, 1);
	}
}

template <class V = VertexXY>
static void ToLineStrings(Vector &source_vec, Vector &target_vec, idx_t row_count) {
	// Flatten the source vector to extract all vertices
	source_vec.Flatten(row_count);

	idx_t vert_total = 0;
	idx_t vert_start = 0;

	// First pass, figure out how many vertices are in this linestring
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, row_idx, true);
			continue;
		}

		const auto &geom = FlatVector::GetData<geometry_t>(source_vec)[row_idx];
		vert_total += geom.GetCount();
	}

	ListVector::Reserve(target_vec, vert_total);
	ListVector::SetListSize(target_vec, vert_total);

	auto list_data = ListVector::GetData(target_vec);
	auto &vert_parts = StructVector::GetEntries(ListVector::GetEntry(target_vec));
	double *vert_data[V::WIDTH];
	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	// Second pass, write out the linestrings
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			continue;
		}

		const auto &geom = FlatVector::GetData<geometry_t>(source_vec)[row_idx];
		const auto vert_count = geom.GetCount();
		const auto vert_array = geom.GetVerts();

		// Set list entry
		auto &list_entry = list_data[row_idx];
		list_entry.offset = vert_start;
		list_entry.length = vert_count;

		// Read vertices
		for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
			for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
				vert_data[dim_idx][vert_start + vert_idx] = vert_array[vert_idx * V::WIDTH + dim_idx];
			}
		}

		vert_start += vert_count;
	}

	D_ASSERT(vert_start == vert_total);
}

template <class V = VertexXY>
static void FromLineStrings(Vector &source_vec, Vector &target_vec, idx_t row_count, idx_t result_offset) {
	auto &arena = GeometryVector::GetArena(target_vec);

	// Flatten the source vector to extract all vertices
	source_vec.Flatten(row_count);

	const auto line_data = ListVector::GetData(source_vec);
	const auto &vert_parts = StructVector::GetEntries(ListVector::GetEntry(source_vec));
	double *vert_data[V::WIDTH];
	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	auto result_data = FlatVector::GetData<geometry_t>(target_vec);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		const auto out_idx = result_offset + row_idx;
		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, out_idx, true);
			continue;
		}

		const auto &line_entry = line_data[row_idx];

		const auto vert_count = line_entry.length;
		const auto vert_array = reinterpret_cast<double *>(arena.AllocateAligned(vert_count * sizeof(V)));

		// Write vertex data
		for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
			for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
				vert_array[vert_idx * V::WIDTH + dim_idx] = vert_data[dim_idx][line_entry.offset + vert_idx];
			}
		}

		result_data[out_idx] = geometry_t(GeometryType::LINESTRING, V::HAS_Z, V::HAS_M, vert_array, vert_count);
	}
}

template <class V = VertexXY>
static void ToPolygons(Vector &source_vec, Vector &target_vec, idx_t row_count) {
	source_vec.Flatten(row_count);

	idx_t vert_total = 0;
	idx_t ring_total = 0;
	idx_t vert_start = 0;
	idx_t ring_start = 0;

	// First pass, figure out how many vertices and rings are in this polygon
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, row_idx, true);
			continue;
		}

		const auto &geom = FlatVector::GetData<geometry_t>(source_vec)[row_idx];

		const auto ring_count = geom.GetCount();
		const auto ring_array = geom.GetParts();

		for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
			const auto &ring = ring_array[ring_idx];
			vert_total += ring.GetCount();
		}

		ring_total += ring_count;
	}

	// Reserve space in the target vector
	ListVector::Reserve(target_vec, ring_total);
	ListVector::SetListSize(target_vec, ring_total);

	auto &ring_vec = ListVector::GetEntry(target_vec);
	ListVector::Reserve(ring_vec, vert_total);
	ListVector::SetListSize(ring_vec, vert_total);

	const auto poly_data = ListVector::GetData(target_vec);
	const auto ring_data = ListVector::GetData(ring_vec);
	auto &vert_parts = StructVector::GetEntries(ListVector::GetEntry(ring_vec));
	double *vert_data[V::WIDTH];

	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			continue;
		}

		const auto &geom = FlatVector::GetData<geometry_t>(source_vec)[row_idx];
		const auto ring_count = geom.GetCount();
		const auto ring_array = geom.GetParts();


		// Set polygon entry
		auto &poly_entry = poly_data[row_idx];
		poly_entry.offset = ring_start;
		poly_entry.length = ring_count;

		for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
			const auto &ring = ring_array[ring_idx];

			const auto vert_count = ring.GetCount();
			const auto vert_array = ring.GetVerts();

			// Set ring entry
			auto &ring_entry = ring_data[ring_start + ring_idx];
			ring_entry.offset = vert_start;
			ring_entry.length = vert_count;

			// Read vertices
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
					vert_data[dim_idx][vert_start + vert_idx] = vert_array[vert_idx * V::WIDTH + dim_idx];
				}
			}

			vert_start += vert_count;
		}

		ring_start += ring_count;
	}

	D_ASSERT(vert_start == vert_total);
	D_ASSERT(ring_start == ring_total);
}

template <class V = VertexXY>
static void FromPolygons(Vector &source_vec, Vector &target_vec, idx_t row_count, idx_t result_offset) {
	auto &arena = GeometryVector::GetArena(target_vec);

	source_vec.Flatten(row_count);

	const auto poly_data = ListVector::GetData(source_vec);
	const auto &ring_vec = ListVector::GetEntry(source_vec);
	const auto ring_data = ListVector::GetData(ring_vec);
	const auto &vert_parts = StructVector::GetEntries(ListVector::GetEntry(ring_vec));

	double *vert_data[V::WIDTH];
	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	auto result_data = FlatVector::GetData<geometry_t>(target_vec);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		const auto out_idx = result_offset + row_idx;

		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, out_idx, true);
			continue;
		}

		const auto &poly_entry = poly_data[row_idx];
		const auto ring_count = poly_entry.length;

		const auto ring_array = reinterpret_cast<geometry_t *>(arena.AllocateAligned(ring_count * sizeof(geometry_t)));

		for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
			const auto &ring_entry = ring_data[poly_entry.offset + ring_idx];
			const auto vert_count = ring_entry.length;

			const auto vert_array = reinterpret_cast<double *>(arena.AllocateAligned(vert_count * sizeof(V)));

			// Write vertex data
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
					vert_array[vert_idx * V::WIDTH + dim_idx] = vert_data[dim_idx][ring_entry.offset + vert_idx];
				}
			}

			ring_array[ring_idx] =
			    geometry_t(GeometryType::LINESTRING, V::HAS_Z, V::HAS_M, vert_array, vert_count);
		}

		result_data[out_idx] = geometry_t(GeometryType::POLYGON, V::HAS_Z, V::HAS_M, ring_array, ring_count);
	}
}

template <class V = VertexXY>
static void ToMultiPoints(Vector &source_vec, Vector &target_vec, idx_t row_count) {
	source_vec.Flatten(row_count);

	const auto geom_data = FlatVector::GetData<geometry_t>(source_vec);

	idx_t vert_total = 0;
	idx_t vert_start = 0;

	// First pass, figure out how many vertices are in this multipoint
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, row_idx, true);
			continue;
		}
		const auto &geom = geom_data[row_idx];

		const auto part_count = geom.GetCount();
		vert_total += part_count;
	}

	// Reserve space in the target vector
	ListVector::Reserve(target_vec, vert_total);
	ListVector::SetListSize(target_vec, vert_total);

	auto mult_data = ListVector::GetData(target_vec);
	auto &vert_parts = StructVector::GetEntries(ListVector::GetEntry(target_vec));
	double *vert_data[V::WIDTH];
	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	// Second pass, write out the multipoints
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			continue;
		}

		const auto &geom = geom_data[row_idx];
		const auto part_count = geom.GetCount();
		const auto part_array = geom.GetParts();

		// Set multipoint entry
		auto &mult_entry = mult_data[row_idx];
		mult_entry.offset = vert_start;
		mult_entry.length = part_count;

		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {

			const auto &part = part_array[part_idx];
			const auto vert_array = part.GetVerts();

			D_ASSERT(part.GetCount() > 0);

			// Skip byte order and type/meta of the point
			for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
				vert_data[dim_idx][vert_start + part_idx] = vert_array[dim_idx];
			}
		}

		vert_start += part_count;
	}

	D_ASSERT(vert_start == vert_total);
}

template <class V = VertexXY>
static void FromMultiPoints(Vector &source_vec, Vector &target_vec, idx_t row_count, idx_t result_offset) {
	auto &arena = GeometryVector::GetArena(target_vec);

	// Flatten the source vector to extract all vertices
	source_vec.Flatten(row_count);

	const auto mult_data = ListVector::GetData(source_vec);
	const auto &vert_parts = StructVector::GetEntries(ListVector::GetEntry(source_vec));

	double *vert_data[V::WIDTH];
	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	auto result_data = FlatVector::GetData<geometry_t>(target_vec);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		const auto out_idx = result_offset + row_idx;
		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, out_idx, true);
			continue;
		}

		const auto &mult_entry = mult_data[row_idx];
		const auto part_count = mult_entry.length;
		const auto part_array = reinterpret_cast<geometry_t *>(arena.AllocateAligned(part_count * sizeof(geometry_t)));

		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			const auto vert_array = reinterpret_cast<double *>(arena.AllocateAligned(sizeof(V)));

			// Write vertex data
			for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
				vert_array[dim_idx] = vert_data[dim_idx][mult_entry.offset + part_idx];
			}

			part_array[part_idx] = geometry_t(GeometryType::POINT, V::HAS_Z, V::HAS_M, vert_array, 1);
		}

		result_data[out_idx] = geometry_t(GeometryType::MULTIPOINT, V::HAS_Z, V::HAS_M, part_array, part_count);
	}
}

template <class V = VertexXY>
static void ToMultiLineStrings(Vector &source_vec, Vector &target_vec, idx_t row_count) {
	// Flatten the source vector to extract all vertices
	source_vec.Flatten(row_count);

	// This is basically the same as Polygons

	idx_t vert_total = 0;
	idx_t line_total = 0;
	idx_t vert_start = 0;
	idx_t line_start = 0;

	// First pass, figure out how many vertices and lines are in this multilinestring
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, row_idx, true);
			continue;
		}

		const auto &geom = FlatVector::GetData<geometry_t>(source_vec)[row_idx];
		const auto part_count = geom.GetCount();
		const auto part_array = geom.GetParts();

		// Line count
		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			const auto &part = part_array[part_idx];
			const auto vert_count = part.GetCount();
			vert_total += vert_count;
		}
		line_total += part_count;
	}

	// Reserve space in the target vector
	ListVector::Reserve(target_vec, line_total);
	ListVector::SetListSize(target_vec, line_total);

	auto &line_vec = ListVector::GetEntry(target_vec);
	ListVector::Reserve(line_vec, vert_total);
	ListVector::SetListSize(line_vec, vert_total);

	const auto mult_data = ListVector::GetData(target_vec);
	const auto line_data = ListVector::GetData(line_vec);
	auto &vert_parts = StructVector::GetEntries(ListVector::GetEntry(line_vec));
	double *vert_data[V::WIDTH];
	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	// Second pass, write out the multilinestrings
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			continue;
		}

		const auto &geom = FlatVector::GetData<geometry_t>(source_vec)[row_idx];
		const auto line_count = geom.GetCount();
		const auto line_array = geom.GetParts();

		// Set multilinestring entry
		auto &mult_entry = mult_data[row_idx];
		mult_entry.offset = line_start;
		mult_entry.length = line_count;

		for (uint32_t line_idx = 0; line_idx < line_count; line_idx++) {
			const auto &line = line_array[line_idx];

			const auto vert_count = line.GetCount();
			const auto vert_array = line.GetVerts();

			// Set line entry
			auto &line_entry = line_data[line_start + line_idx];
			line_entry.offset = vert_start;
			line_entry.length = vert_count;

			// Read vertices
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
					vert_data[dim_idx][vert_start + vert_idx] = vert_array[vert_idx * V::WIDTH + dim_idx];
				}
			}

			vert_start += vert_count;
		}
		line_start += line_count;
	}

	D_ASSERT(vert_start == vert_total);
	D_ASSERT(line_start == line_total);
}

template <class V = VertexXY>
static void FromMultiLineStrings(Vector &source_vec, Vector &target_vec, idx_t row_count, idx_t result_offset) {
	auto &arena = GeometryVector::GetArena(target_vec);
	// Flatten the source vector to extract all vertices

	source_vec.Flatten(row_count);

	const auto mult_data = ListVector::GetData(source_vec);
	const auto line_vec = ListVector::GetEntry(source_vec);
	const auto line_data = ListVector::GetData(line_vec);
	const auto &vert_parts = StructVector::GetEntries(ListVector::GetEntry(line_vec));
	double *vert_data[V::WIDTH];
	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	const auto result_data = FlatVector::GetData<geometry_t>(target_vec);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		const auto out_idx = result_offset + row_idx;
		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, out_idx, true);
			continue;
		}

		const auto &mult_entry = mult_data[row_idx];

		const auto line_count = mult_entry.length;
		const auto line_array = reinterpret_cast<geometry_t *>(arena.AllocateAligned(line_count * sizeof(geometry_t)));

		for (uint32_t line_idx = 0; line_idx < line_count; line_idx++) {
			const auto &line_entry = line_data[mult_entry.offset + line_idx];
			const auto vert_count = line_entry.length;

			const auto vert_array = reinterpret_cast<double *>(arena.AllocateAligned(vert_count * sizeof(V)));

			// Write vertex data
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
					vert_array[vert_idx * V::WIDTH + dim_idx] = vert_data[dim_idx][line_entry.offset + vert_idx];
				}
			}

			line_array[line_idx] = geometry_t(GeometryType::LINESTRING, V::HAS_Z, V::HAS_M, vert_array, vert_count);
		}
		result_data[out_idx] =
		    geometry_t(GeometryType::MULTILINESTRING, V::HAS_Z, V::HAS_M, line_array, line_count);
	}
}

template <class V = VertexXY>
static void ToMultiPolygons(Vector &source_vec, Vector &target_vec, idx_t row_count) {
	// Flatten the source vector to extract all vertices
	source_vec.Flatten(row_count);

	idx_t vert_total = 0;
	idx_t ring_total = 0;
	idx_t poly_total = 0;
	idx_t vert_start = 0;
	idx_t ring_start = 0;
	idx_t poly_start = 0;

	// First pass, figure out how many vertices, rings and polygons are in this multipolygon
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, row_idx, true);
			continue;
		}

		const auto &geom = FlatVector::GetData<geometry_t>(source_vec)[row_idx];

		const auto poly_count = geom.GetCount();
		const auto poly_array = geom.GetParts();

		for (uint32_t poly_idx = 0; poly_idx < poly_count; poly_idx++) {

			const auto poly = poly_array[poly_idx];

			const auto ring_count = poly.GetCount();
			const auto ring_array = poly.GetParts();

			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				const auto ring = ring_array[ring_idx];

				const auto vert_count = ring.GetCount();

				vert_total += vert_count;
			}
			ring_total += ring_count;
		}
		poly_total += poly_count;
	}

	// Reserve space in the target vector
	ListVector::Reserve(target_vec, poly_total);
	ListVector::SetListSize(target_vec, poly_total);
	auto &poly_vec = ListVector::GetEntry(target_vec);
	ListVector::Reserve(poly_vec, ring_total);
	ListVector::SetListSize(poly_vec, ring_total);
	auto &ring_vec = ListVector::GetEntry(poly_vec);
	ListVector::Reserve(ring_vec, vert_total);
	ListVector::SetListSize(ring_vec, vert_total);

	const auto mult_data = ListVector::GetData(target_vec);
	const auto poly_data = ListVector::GetData(poly_vec);
	const auto ring_data = ListVector::GetData(ring_vec);
	auto &vert_parts = StructVector::GetEntries(ListVector::GetEntry(ring_vec));
	double *vert_data[V::WIDTH];
	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	// Second pass, write out the multipolygons
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(source_vec, row_idx)) {
			continue;
		}

		const auto &geom = FlatVector::GetData<geometry_t>(source_vec)[row_idx];
		const auto poly_count = geom.GetCount();
		const auto poly_array = geom.GetParts();

		// Set multipolygon entry
		auto &mult_entry = mult_data[row_idx];
		mult_entry.offset = poly_start;
		mult_entry.length = poly_count;

		// Read polygons
		for (uint32_t poly_idx = 0; poly_idx < poly_count; poly_idx++) {
			const auto &poly = poly_array[poly_idx];

			// Read ring count
			const auto ring_count = poly.GetCount();
			const auto ring_array = poly.GetParts();

			// Set polygon entry
			auto &poly_entry = poly_data[poly_start + poly_idx];
			poly_entry.offset = ring_start;
			poly_entry.length = ring_count;

			// Read rings
			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				const auto &ring = ring_array[ring_idx];

				// Read vertex count
				const auto vert_count = ring.GetCount();
				const auto vert_array = ring.GetVerts();

				// Set ring entry
				auto &ring_entry = ring_data[ring_start + ring_idx];
				ring_entry.offset = vert_start;
				ring_entry.length = vert_count;

				// Read vertices
				for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
					for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
						vert_data[dim_idx][vert_start + vert_idx] = vert_array[vert_idx * V::WIDTH + dim_idx];
					}
				}
				vert_start += vert_count;
			}
			ring_start += ring_count;
		}
		poly_start += poly_count;
	}
}

template <class V = VertexXY>
static void FromMultiPolygons(Vector &source_vec, Vector &target_vec, idx_t row_count, idx_t result_offset) {
	auto &arena = GeometryVector::GetArena(target_vec);

	// Flatten the source vector to extract all vertices
	source_vec.Flatten(row_count);

	const auto mult_data = ListVector::GetData(source_vec);
	const auto &poly_vec = ListVector::GetEntry(source_vec);
	const auto poly_data = ListVector::GetData(poly_vec);
	const auto &ring_vec = ListVector::GetEntry(poly_vec);
	const auto ring_data = ListVector::GetData(ring_vec);
	const auto &vert_parts = StructVector::GetEntries(ListVector::GetEntry(ring_vec));

	double *vert_data[V::WIDTH];
	for (idx_t i = 0; i < V::WIDTH; i++) {
		vert_data[i] = FlatVector::GetData<double>(*vert_parts[i]);
	}

	auto result_data = FlatVector::GetData<geometry_t>(target_vec);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		const auto out_idx = result_offset + row_idx;
		if (FlatVector::IsNull(source_vec, row_idx)) {
			FlatVector::SetNull(target_vec, out_idx, true);
			continue;
		}

		const auto &mult_entry = mult_data[row_idx];

		const auto poly_count = mult_entry.length;
		const auto poly_array =	 reinterpret_cast<geometry_t *>(arena.AllocateAligned(poly_count * sizeof(geometry_t)));

		for (uint32_t poly_idx = 0; poly_idx < poly_count; poly_idx++) {

			const auto &poly_entry = poly_data[mult_entry.offset + poly_idx];
			const auto ring_count = poly_entry.length;
			const auto ring_array = reinterpret_cast<geometry_t *>(arena.AllocateAligned(ring_count * sizeof(geometry_t)));

			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				const auto &ring_entry = ring_data[poly_entry.offset + ring_idx];
				const auto vert_count = ring_entry.length;
				const auto vert_array = reinterpret_cast<double *>(arena.AllocateAligned(vert_count * sizeof(V)));

				// Write vertex data
				for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
					for (uint32_t dim_idx = 0; dim_idx < V::WIDTH; dim_idx++) {
						vert_array[vert_idx * V::WIDTH + dim_idx] = vert_data[dim_idx][ring_entry.offset + vert_idx];
					}
				}

				ring_array[ring_idx] =
				    geometry_t(GeometryType::LINESTRING, V::HAS_Z, V::HAS_M, vert_array, vert_count);
			}

			poly_array[poly_idx] = geometry_t(GeometryType::POLYGON, V::HAS_Z, V::HAS_M, ring_array, ring_count);
		}

		result_data[out_idx] =
		    geometry_t(GeometryType::MULTIPOLYGON, V::HAS_Z, V::HAS_M, poly_array, poly_count);
	}
}

template <class V = VertexXY>
static void ToVectorizedFormatInternal(Vector &source, Vector &target, idx_t count, GeometryType geom_type) {
	switch (geom_type) {
	case GeometryType::POINT:
		ToPoints<V>(source, target, count);
		break;
	case GeometryType::LINESTRING:
		ToLineStrings<V>(source, target, count);
		break;
	case GeometryType::POLYGON:
		ToPolygons<V>(source, target, count);
		break;
	case GeometryType::MULTIPOINT:
		ToMultiPoints<V>(source, target, count);
		break;
	case GeometryType::MULTILINESTRING:
		ToMultiLineStrings<V>(source, target, count);
		break;
	case GeometryType::MULTIPOLYGON:
		ToMultiPolygons<V>(source, target, count);
		break;
	default:
		throw NotImplementedException("Unsupported geometry type %d", static_cast<int>(geom_type));
	}
}

void Geometry::ToVectorizedFormat(Vector &source, Vector &target, idx_t count, GeometryType geom_type,
                                  VertexType vert_type) {
	switch (vert_type) {
	case VertexType::XY:
		ToVectorizedFormatInternal<VertexXY>(source, target, count, geom_type);
		break;
	case VertexType::XYZ:
		ToVectorizedFormatInternal<VertexXYZ>(source, target, count, geom_type);
		break;
	case VertexType::XYM:
		ToVectorizedFormatInternal<VertexXYM>(source, target, count, geom_type);
		break;
	case VertexType::XYZM:
		ToVectorizedFormatInternal<VertexXYZM>(source, target, count, geom_type);
		break;
	default:
		throw InvalidInputException("Unsupported vertex type %d", static_cast<int>(vert_type));
	}
}

template <class V = VertexXY>
static void FromVectorizedFormatInternal(Vector &source, Vector &target, idx_t count, GeometryType geom_type,
                                         idx_t result_offset) {
	switch (geom_type) {
	case GeometryType::POINT:
		FromPoints<V>(source, target, count, result_offset);
		break;
	case GeometryType::LINESTRING:
		FromLineStrings<V>(source, target, count, result_offset);
		break;
	case GeometryType::POLYGON:
		FromPolygons<V>(source, target, count, result_offset);
		break;
	case GeometryType::MULTIPOINT:
		FromMultiPoints<V>(source, target, count, result_offset);
		break;
	case GeometryType::MULTILINESTRING:
		FromMultiLineStrings<V>(source, target, count, result_offset);
		break;
	case GeometryType::MULTIPOLYGON:
		FromMultiPolygons<V>(source, target, count, result_offset);
		break;
	default:
		throw NotImplementedException("Unsupported geometry type %d", static_cast<int>(geom_type));
	}
}

void Geometry::FromVectorizedFormat(Vector &source, Vector &target, idx_t count, GeometryType geom_type,
                                    VertexType vert_type, idx_t result_offset) {
	switch (vert_type) {
	case VertexType::XY:
		FromVectorizedFormatInternal<VertexXY>(source, target, count, geom_type, result_offset);
		break;
	case VertexType::XYZ:
		FromVectorizedFormatInternal<VertexXYZ>(source, target, count, geom_type, result_offset);
		break;
	case VertexType::XYM:
		FromVectorizedFormatInternal<VertexXYM>(source, target, count, geom_type, result_offset);
		break;
	case VertexType::XYZM:
		FromVectorizedFormatInternal<VertexXYZM>(source, target, count, geom_type, result_offset);
		break;
	default:
		throw InvalidInputException("Unsupported vertex type %d", static_cast<int>(vert_type));
	}
}

static LogicalType GetVectorizedTypeInternal(GeometryType geom_type, LogicalType vertex_type) {
	switch (geom_type) {
	case GeometryType::POINT:
		return vertex_type;
	case GeometryType::LINESTRING:
		return LogicalType::LIST(vertex_type);
	case GeometryType::POLYGON:
		return LogicalType::LIST(LogicalType::LIST(vertex_type));
	case GeometryType::MULTIPOINT:
		return LogicalType::LIST(vertex_type);
	case GeometryType::MULTILINESTRING:
		return LogicalType::LIST(LogicalType::LIST(vertex_type));
	case GeometryType::MULTIPOLYGON:
		return LogicalType::LIST(LogicalType::LIST(LogicalType::LIST(vertex_type)));
	case GeometryType::GEOMETRYCOLLECTION:
		throw NotImplementedException("GEOMETRYCOLLECTION vectorized type not implemented");
	default:
		throw InvalidInputException("Unsupported geometry type %d", static_cast<int>(geom_type));
	}
}

LogicalType Geometry::GetVectorizedType(GeometryType geom_type, VertexType vert_type) {
	switch (vert_type) {
	case VertexType::XY: {
		auto vert = LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});
		return GetVectorizedTypeInternal(geom_type, std::move(vert));
	}
	case VertexType::XYZ: {
		auto vert =
		    LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}, {"z", LogicalType::DOUBLE}});
		return GetVectorizedTypeInternal(geom_type, std::move(vert));
	}
	case VertexType::XYM: {
		auto vert =
		    LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}, {"m", LogicalType::DOUBLE}});
		return GetVectorizedTypeInternal(geom_type, std::move(vert));
	}
	case VertexType::XYZM: {
		auto vert = LogicalType::STRUCT({{"x", LogicalType::DOUBLE},
		                                 {"y", LogicalType::DOUBLE},
		                                 {"z", LogicalType::DOUBLE},
		                                 {"m", LogicalType::DOUBLE}});
		return GetVectorizedTypeInternal(geom_type, std::move(vert));
	}
	default:
		throw InvalidInputException("Unsupported vertex type %d", static_cast<int>(vert_type));
	}
}

/*
uint32_t GetRequiredSize(const geometry_t &other) {
    const auto part_type = other.GetPartType();
    const auto item_size = other.GetCount();

    switch (part_type) {
    case GeometryType::POINT:
    case GeometryType::LINESTRING: {
        return sizeof(geometry_t) + (item_size * other.GetWidth());
    }
    case GeometryType::POLYGON:
    case GeometryType::MULTIPOINT:
    case GeometryType::MULTILINESTRING:
    case GeometryType::MULTIPOLYGON:
    case GeometryType::GEOMETRYCOLLECTION: {
        uint32_t size = sizeof(geometry_t);
        const auto part_array = other.GetParts();
        for (uint32_t i = 0; i < item_size; i++) {
            size += GetRequiredSize(part_array[i]);
        }
    }
    default:
        throw InvalidInputException("Unsupported geometry type %d", static_cast<int>(part_type));
    }
}*/
static void CopyInto(const geometry_t &source, geometry_t &target, ArenaAllocator &allocator) {
	target = source;

	if (target.IsEmpty()) {
		// No need to copy the body
		return;
	}

	const auto part_type = source.GetPartType();

	switch (part_type) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto source_vertex_width = source.GetWidth();
		const auto source_vertex_count = source.GetCount();
		const auto source_vertex_array = source.GetVerts();

		const auto target_buffer = allocator.AllocateAligned(source_vertex_width * source_vertex_count);
		const auto target_vertex_array = reinterpret_cast<double *>(target_buffer);

		memcpy(target_vertex_array, source_vertex_array, source_vertex_width * source_vertex_count);
		target.SetVerts(target_vertex_array, source_vertex_count);
	} break;
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto source_part_count = source.GetCount();
		const auto source_part_array = source.GetParts();

		const auto target_buffer = allocator.AllocateAligned(source_part_count * sizeof(geometry_t));
		const auto target_part_array = reinterpret_cast<geometry_t *>(target_buffer);

		for (uint32_t i = 0; i < source_part_count; i++) {
			CopyInto(source_part_array[i], target_part_array[i], allocator);
		}

		target.SetParts(target_part_array, source_part_count);
	} break;
	default:
		throw InvalidInputException("Unsupported geometry type %d", static_cast<int>(part_type));
	}
}

geometry_t Geometry::Copy(const geometry_t &geom, ArenaAllocator &allocator) {
	geometry_t result;
	CopyInto(geom, result, allocator);
	result.Verify();
	return result;
}

//----------------------------------------------------------------------------------------------------------------------
// Serialization
//----------------------------------------------------------------------------------------------------------------------
// Flatten the geometry by turning the pointers into relative offsets

static size_t GetBinarySize(const geometry_t &geom) {
	switch (geom.GetPartType()) {
	case GeometryType::INVALID: {
		return sizeof(geometry_t);
	}
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto vertex_count = geom.GetCount();
		const auto vertex_dims = geom.GetDims();
		return sizeof(geometry_t) + (vertex_count * vertex_dims * sizeof(double));
	}
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		size_t size = sizeof(geometry_t);
		const auto part_count = geom.GetCount();
		const auto part_array = geom.GetParts();
		for (uint32_t i = 0; i < part_count; i++) {
			size += GetBinarySize(part_array[i]);
		}
		return size;
	}
	default:
		throw InvalidInputException("Unsupported geometry type %d", static_cast<int>(geom.GetPartType()));
	}
}

static void Write(const geometry_t &geom, FixedSizeBlobWriter &head, FixedSizeBlobWriter &data) {
	const auto part_type = geom.GetPartType();
	const auto vert_type = geom.GetVertType();
	const auto item_count = geom.GetCount();

	head.Write<uint8_t>(static_cast<uint8_t>(part_type));
	head.Write<uint8_t>(static_cast<uint8_t>(vert_type));
	head.Write<uint8_t>(0); // Padding
	head.Write<uint8_t>(0); // Padding

	head.Write<uint32_t>(item_count);

	// Write offset
	const auto data_offset = data.GetPosition() - head.GetPosition() - sizeof(uint64_t);
	head.Write<uint64_t>(data_offset);

	switch (part_type) {
	case GeometryType::INVALID:
		break;
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto vertex_count = item_count;
		const auto vertex_dims = geom.GetDims();
		const auto vertex_array = geom.GetVerts();

		for (uint32_t i = 0; i < vertex_count * vertex_dims; i++) {
			data.Write<double>(vertex_array[i]);
		}
	} break;
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		auto part = data;

		const auto part_array = geom.GetParts();
		const auto part_count = item_count;

		data.Skip(part_count * sizeof(geometry_t));

		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			Write(part_array[part_idx], part, data);
		}
	} break;
	default:
		throw InvalidInputException("Unsupported geometry type %d", static_cast<int>(geom.GetPartType()));
	}
}

string geometry_t::Serialize(const geometry_t &geom) {
	const auto required_size = GetBinarySize(geom);
	string result;
	result.resize(required_size);

	char *ptr = &result[0];

	FixedSizeBlobWriter head_writer(ptr, static_cast<uint32_t>(required_size));
	FixedSizeBlobWriter data_writer(ptr, static_cast<uint32_t>(required_size));
	data_writer.Skip(sizeof(geometry_t));

	Write(geom, head_writer, data_writer);

	return result;
}

// TODO: Maybe we want to make offsets signed (probably)
static geometry_t Read(BlobReader &reader, Allocator &allocator) {
	const auto part_type = static_cast<GeometryType>(reader.Read<uint8_t>());
	const auto vert_type = static_cast<VertexType>(reader.Read<uint8_t>());

	reader.Skip(1); // Padding
	reader.Skip(1); // Padding

	const auto item_count = reader.Read<uint32_t>();
	const auto data_offset = reader.Read<uint64_t>();

	geometry_t geom(part_type, vert_type, nullptr, item_count);

	auto data_reader = reader;
	data_reader.Skip(data_offset);

	switch (part_type) {
	case GeometryType::INVALID:
		break;
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto vertex_count = item_count;

		if (vertex_count == 0) {
			geom.SetVerts(nullptr, 0);
			return geom;
		}

		const auto vertex_dims = geom.GetDims();

		const auto vertex_buffer = allocator.AllocateData(vertex_count * vertex_dims * sizeof(double));
		const auto vertex_array = reinterpret_cast<double *>(vertex_buffer);

		for (uint32_t i = 0; i < vertex_count * vertex_dims; i++) {
			vertex_array[i] = data_reader.Read<double>();
		}

		geom.SetVerts(vertex_array, vertex_count);
	} break;
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_count = item_count;

		if (part_count == 0) {
			geom.SetParts(nullptr, 0);
			return geom;
		}

		const auto part_buffer = allocator.AllocateData(part_count * sizeof(geometry_t));
		const auto part_array = reinterpret_cast<geometry_t *>(part_buffer);

		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			part_array[part_idx] = Read(data_reader, allocator);
		}

		geom.SetParts(part_array, part_count);
	} break;
	default:
		throw InvalidInputException("Unsupported geometry type %d", static_cast<int>(part_type));
	}

	return geom;
}

geometry_t geometry_t::Deserialize(const string &data, Allocator &allocator) {
	BlobReader reader(data.c_str(), static_cast<uint32_t>(data.size()));
	const auto result = Read(reader, allocator);
	result.Verify();
	return result;
}

size_t geometry_t::GetTotalByteSize() const {
	return GetBinarySize(*this);
}

void geometry_t::Serialize(char *ptr, uint32_t len) const {
	FixedSizeBlobWriter head_writer(ptr, len);
	FixedSizeBlobWriter data_writer(ptr, len);
	data_writer.Skip(sizeof(geometry_t));

	Write(*this, head_writer, data_writer);
}

geometry_t geometry_t::Deserialize(Vector &vector, const char *data, uint32_t len) {
	BlobReader reader(data, len);
	auto &allocator = GeometryVector::GetArena(vector).GetAllocator();
	const auto result = Read(reader, allocator);
	result.Verify();
	return result;
}

//----------------------------------------------------------------------------------------------------------------------
// Swizzling
//----------------------------------------------------------------------------------------------------------------------
size_t geometry_t::GetHeapSize(const geometry_t &geom) {
	if (geom.IsEmpty()) {
		return 0;
	}

	switch (geom.GetPartType()) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto vert_count = geom.GetCount();
		const auto vert_width = geom.GetWidth();
		return vert_count * vert_width;
	} break;
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_count = geom.GetCount();
		const auto part_array = geom.GetParts();

		size_t size = part_count * sizeof(geometry_t);
		for (uint32_t i = 0; i < part_count; i++) {
			size += GetHeapSize(part_array[i]);
		}
		return size;
	}
	default:
		throw InvalidInputException("Unsupported geometry type %d", static_cast<int>(geom.GetPartType()));
	}
}

static geometry_t CopyToHeapInternal(const geometry_t &geom, FixedSizeBlobWriter &writer) {
	geometry_t result = geom;

	switch (geom.GetType()) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto vert_count = geom.GetCount();
		const auto vert_array = geom.GetVerts();
		const auto dims_count = geom.GetDims();

		const auto data_array = reinterpret_cast<double *>(writer.GetPointer());
		D_ASSERT(reinterpret_cast<uintptr_t>(data_array) % alignof(double) == 0);

		for (uint32_t i = 0; i < vert_count * dims_count; i++) {
			writer.Write<double>(vert_array[i]);
		}

		result.SetVerts(data_array, vert_count);
		return result;
	}
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_count = geom.GetCount();
		const auto part_array = geom.GetParts();

		const auto data_array = reinterpret_cast<geometry_t *>(writer.GetPointer());
		D_ASSERT(reinterpret_cast<uintptr_t>(data_array) % alignof(geometry_t) == 0);

		writer.Skip(part_count * sizeof(geometry_t));
		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			data_array[part_idx] = CopyToHeapInternal(part_array[part_idx], writer);
		}

		result.SetParts(data_array, part_count);
		return result;
	}
	default:
		throw InvalidInputException("Unsupported geometry type %d", static_cast<int>(geom.GetPartType()));
	}
}
geometry_t geometry_t::CopyToHeap(const geometry_t &geom, data_ptr_t heap_data, size_t heap_size) {
	FixedSizeBlobWriter writer(char_ptr_cast(heap_data), static_cast<uint32_t>(heap_size));
	return CopyToHeapInternal(geom, writer);
}

void geometry_t::RecomputeHeapPointers(geometry_t &geom, data_ptr_t old_heap_ptr, data_ptr_t new_heap_ptr) {
	switch (geom.GetType()) {
	case GeometryType::POINT:
	case GeometryType::LINESTRING: {
		const auto vert_count = geom.GetCount();

		const auto old_data = reinterpret_cast<ptrdiff_t>(geom.GetVerts());
		const auto old_heap = reinterpret_cast<ptrdiff_t>(old_heap_ptr);
		const auto new_heap = reinterpret_cast<ptrdiff_t>(new_heap_ptr);

		const auto offset = old_data - old_heap;
		D_ASSERT(offset >= 0);

		const auto new_data = new_heap + offset;
		D_ASSERT(new_data % alignof(double) == 0);

		geom.SetVerts(reinterpret_cast<double *>(new_data), vert_count);

	} break;
	case GeometryType::POLYGON:
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto part_count = geom.GetCount();

		const auto old_data = reinterpret_cast<ptrdiff_t>(geom.GetParts());
		const auto old_heap = reinterpret_cast<ptrdiff_t>(old_heap_ptr);
		const auto new_heap = reinterpret_cast<ptrdiff_t>(new_heap_ptr);

		const auto offset = old_data - old_heap;
		D_ASSERT(offset >= 0);

		const auto new_data = new_heap + offset;
		D_ASSERT(new_data % alignof(geometry_t) == 0);

		geom.SetParts(reinterpret_cast<geometry_t *>(new_data), part_count);

		// Also recompute pointers in parts
		const auto part_array = geom.GetParts();
		for (uint32_t part_idx = 0; part_idx < part_count; part_idx++) {
			RecomputeHeapPointers(part_array[part_idx], old_heap_ptr, new_heap_ptr);
		}

	} break;
	default:
		throw InvalidInputException("Unsupported geometry type %d", static_cast<int>(geom.GetPartType()));
	}
}

} // namespace duckdb
