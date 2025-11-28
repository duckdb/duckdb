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
		auto ptr = reinterpret_cast<const char *>(&value);
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
		auto ptr = reinterpret_cast<const char *>(&reserved.value);
		// We've reserved 0 bytes, so we can safely memcpy
		memcpy(buffer.data() + reserved.offset, ptr, sizeof(T));
	}

	void Write(const char *data, size_t size) {
		D_ASSERT(data != nullptr);
		buffer.insert(buffer.end(), data, data + size);
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
		memcpy(pos, &value, sizeof(T));
		pos += sizeof(T);
	}

	void Write(const char *data, size_t size) {
		if (pos + size > end) {
			throw InvalidInputException("Writing beyond end of binary data at position %zu", pos - beg);
		}
		memcpy(pos, data, size);
		pos += size;
	}

	size_t GetPosition() const {
		return static_cast<idx_t>(pos - beg);
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
		if (LE) {
			memcpy(&value, pos, sizeof(T));
			pos += sizeof(T);
		} else {
			char temp[sizeof(T)];
			for (size_t i = 0; i < sizeof(T); ++i) {
				temp[i] = pos[sizeof(T) - 1 - i];
			}
			memcpy(&value, temp, sizeof(T));
			pos += sizeof(T);
		}
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

struct WKBAnalysis {
	uint32_t size = 0;
	bool any_be = false;
	bool any_z = false;
	bool any_m = false;
	bool any_unknown = false;
	bool any_ewkb = false;
};

WKBAnalysis AnalyzeWKB(BlobReader &reader) {
	WKBAnalysis result;

	while (!reader.IsAtEnd()) {
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
			result.any_ewkb = true;
			reader.Skip(sizeof(uint32_t)); // Skip SRID
			                               // Do not include SRID in the size
		}

		if (has_extz || has_extm || has_srid) {
			// EWKB flags are set
			result.any_ewkb = true;
		}

		const auto v_size = (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0)) * sizeof(double);

		result.any_z |= has_z;
		result.any_m |= has_m;
		result.any_be |= !le;

		result.size += sizeof(uint8_t) + sizeof(uint32_t); // Byte order + type/meta

		switch (type_id) {
		case 1: { // POINT
			reader.Skip(v_size);
			result.size += v_size;
		} break;
		case 2: { // LINESTRING
			const auto vert_count = reader.Read<uint32_t>(le);
			reader.Skip(vert_count * v_size);
			result.size += sizeof(uint32_t) + vert_count * v_size;
		} break;
		case 3: { // POLYGON
			const auto ring_count = reader.Read<uint32_t>(le);
			result.size += sizeof(uint32_t);
			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				const auto vert_count = reader.Read<uint32_t>(le);
				reader.Skip(vert_count * v_size);
				result.size += sizeof(uint32_t) + vert_count * v_size;
			}
		} break;
		case 4:   // MULTIPOINT
		case 5:   // MULTILINESTRING
		case 6:   // MULTIPOLYGON
		case 7: { // GEOMETRYCOLLECTION
			reader.Skip(sizeof(uint32_t));
			result.size += sizeof(uint32_t); // part count
		} break;
		default: {
			result.any_unknown = true;
			return result;
		}
		}
	}
	return result;
}

void ConvertWKB(BlobReader &reader, FixedSizeBlobWriter &writer) {
	while (!reader.IsAtEnd()) {
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

		const auto v_width = static_cast<uint32_t>((2 + (has_z ? 1 : 0) + (has_m ? 1 : 0)));

		writer.Write<uint8_t>(1);                                          // Always write LE
		writer.Write<uint32_t>(type_id + (1000 * has_z) + (2000 * has_m)); // Write meta

		switch (type_id) {
		case 1: { // POINT
			for (uint32_t d_idx = 0; d_idx < v_width; d_idx++) {
				auto value = reader.Read<double>(le);
				writer.Write<double>(value);
			}
		} break;
		case 2: { // LINESTRING
			const auto vert_count = reader.Read<uint32_t>(le);
			writer.Write<uint32_t>(vert_count);
			for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
				for (uint32_t d_idx = 0; d_idx < v_width; d_idx++) {
					auto value = reader.Read<double>(le);
					writer.Write<double>(value);
				}
			}
		} break;
		case 3: { // POLYGON
			const auto ring_count = reader.Read<uint32_t>(le);
			writer.Write<uint32_t>(ring_count);
			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				const auto vert_count = reader.Read<uint32_t>(le);
				writer.Write<uint32_t>(vert_count);
				for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
					for (uint32_t d_idx = 0; d_idx < v_width; d_idx++) {
						auto value = reader.Read<double>(le);
						writer.Write<double>(value);
					}
				}
			}
		} break;
		case 4:   // MULTIPOINT
		case 5:   // MULTILINESTRING
		case 6:   // MULTIPOLYGON
		case 7: { // GEOMETRYCOLLECTION
			const auto part_count = reader.Read<uint32_t>(le);
			writer.Write<uint32_t>(part_count);
		} break;
		default:
			D_ASSERT(false);
			break;
		}
	}
}

} // namespace

} // namespace duckdb

//----------------------------------------------------------------------------------------------------------------------
// Public interface
//----------------------------------------------------------------------------------------------------------------------
namespace duckdb {

constexpr const idx_t Geometry::MAX_RECURSION_DEPTH;

bool Geometry::FromBinary(const string_t &wkb, string_t &result, Vector &result_vector, bool strict) {
	BlobReader reader(wkb.GetData(), static_cast<uint32_t>(wkb.GetSize()));

	const auto analysis = AnalyzeWKB(reader);
	if (analysis.any_unknown) {
		if (strict) {
			throw InvalidInputException("Unsupported geometry type in WKB");
		}
		return false;
	}

	if (analysis.any_be || analysis.any_ewkb) {
		reader.Reset();
		// Make a new WKB with all LE
		auto blob = StringVector::EmptyString(result_vector, analysis.size);
		FixedSizeBlobWriter writer(blob.GetDataWriteable(), static_cast<uint32_t>(blob.GetSize()));
		ConvertWKB(reader, writer);
		blob.Finalize();
		result = blob;
		return true;
	}

	// Copy the WKB as-is
	result = StringVector::AddStringOrBlob(result_vector, wkb.GetData(), wkb.GetSize());
	return true;
}

bool Geometry::FromBinary(Vector &source, Vector &result, idx_t count, bool strict) {
	if (strict) {
		UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](const string_t &wkb) {
			string_t geom;
			FromBinary(wkb, geom, result, true);
			return geom;
		});
		return true;
	}

	auto all_ok = true;
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(source, result, count,
	                                                    [&](const string_t &wkb, ValidityMask &mask, idx_t idx) {
		                                                    string_t geom;
		                                                    if (!FromBinary(wkb, geom, result, false)) {
			                                                    all_ok = false;
			                                                    mask.SetInvalid(idx);
			                                                    return string_t();
		                                                    }
		                                                    return geom;
	                                                    });
	return all_ok;
}

void Geometry::ToBinary(Vector &source, Vector &result, idx_t count) {
	// We are currently using WKB internally, so just copy as-is!
	result.Reinterpret(source);
}

bool Geometry::FromString(const string_t &wkt_text, string_t &result, Vector &result_vector, bool strict) {
	TextReader reader(wkt_text.GetData(), static_cast<uint32_t>(wkt_text.GetSize()));
	BlobWriter writer;

	FromStringRecursive(reader, writer, 0, false, false);

	const auto &buffer = writer.GetBuffer();
	result = StringVector::AddStringOrBlob(result_vector, buffer.data(), buffer.size());
	return true;
}

string_t Geometry::ToString(Vector &result, const string_t &geom) {
	BlobReader reader(geom.GetData(), static_cast<uint32_t>(geom.GetSize()));
	TextWriter writer;

	ToStringRecursive(reader, writer, 0, false, false);

	// Convert the buffer to string_t
	const auto &buffer = writer.GetBuffer();
	return StringVector::AddString(result, buffer.data(), buffer.size());
}

pair<GeometryType, VertexType> Geometry::GetType(const string_t &wkb) {
	BlobReader reader(wkb.GetData(), static_cast<uint32_t>(wkb.GetSize()));

	// Read the byte order (should always be 1 for little-endian)
	const auto byte_order = reader.Read<uint8_t>();
	if (byte_order != 1) {
		throw InvalidInputException("Unsupported byte order %d in WKB", byte_order);
	}

	const auto meta = reader.Read<uint32_t>();
	const auto type_id = (meta & 0x0000FFFF) % 1000;
	const auto flag_id = (meta & 0x0000FFFF) / 1000;

	if (type_id < 1 || type_id > 7) {
		throw InvalidInputException("Unsupported geometry type %d in WKB", type_id);
	}
	if (flag_id > 3) {
		throw InvalidInputException("Unsupported geometry flag %d in WKB", flag_id);
	}

	const auto geom_type = static_cast<GeometryType>(type_id);
	const auto vert_type = static_cast<VertexType>(flag_id);

	return {geom_type, vert_type};
}

template <class VERTEX_TYPE = VertexXY>
static uint32_t ParseVerticesInternal(BlobReader &reader, GeometryExtent &extent, uint32_t vert_count, bool check_nan) {
	uint32_t count = 0;

	// Issue a single .Reserve() for all vertices, to minimize bounds checking overhead
	const auto ptr = const_data_ptr_cast(reader.Reserve(vert_count * sizeof(VERTEX_TYPE)));

	for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
		VERTEX_TYPE vertex = Load<VERTEX_TYPE>(ptr + vert_idx * sizeof(VERTEX_TYPE));
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

uint32_t Geometry::GetExtent(const string_t &wkb, GeometryExtent &extent) {
	BlobReader reader(wkb.GetData(), static_cast<uint32_t>(wkb.GetSize()));

	uint32_t vertex_count = 0;

	while (!reader.IsAtEnd()) {
		const auto byte_order = reader.Read<uint8_t>();
		if (byte_order != 1) {
			throw InvalidInputException("Unsupported byte order %d in WKB", byte_order);
		}
		const auto meta = reader.Read<uint32_t>();
		const auto type_id = (meta & 0x0000FFFF) % 1000;
		const auto flag_id = (meta & 0x0000FFFF) / 1000;
		if (type_id < 1 || type_id > 7) {
			throw InvalidInputException("Unsupported geometry type %d in WKB", type_id);
		}
		if (flag_id > 3) {
			throw InvalidInputException("Unsupported geometry flag %d in WKB", flag_id);
		}
		const auto geom_type = static_cast<GeometryType>(type_id);
		const auto vert_type = static_cast<VertexType>(flag_id);

		switch (geom_type) {
		case GeometryType::POINT: {
			vertex_count += ParseVertices(reader, extent, 1, vert_type, true);
		} break;
		case GeometryType::LINESTRING: {
			const auto vert_count = reader.Read<uint32_t>();
			vertex_count += ParseVertices(reader, extent, vert_count, vert_type, false);
		} break;
		case GeometryType::POLYGON: {
			const auto ring_count = reader.Read<uint32_t>();
			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				const auto vert_count = reader.Read<uint32_t>();
				vertex_count += ParseVertices(reader, extent, vert_count, vert_type, false);
			}
		} break;
		case GeometryType::MULTIPOINT:
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON:
		case GeometryType::GEOMETRYCOLLECTION: {
			// Skip count. We don't need it for extent calculation.
			reader.Skip(sizeof(uint32_t));
		} break;
		default:
			throw InvalidInputException("Unsupported geometry type %d in WKB", static_cast<int>(geom_type));
		}
	}
	return vertex_count;
}

} // namespace duckdb
