#include "catch.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/enums/wal_type.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

//===--------------------------------------------------------------------===//
// Corrupting the index storage information of a WAL entry
//===--------------------------------------------------------------------===//
static duckdb::vector<data_t> ReadFile(LocalFileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto size = handle->GetFileSize();
	duckdb::vector<data_t> result(size);
	if (size > 0) {
		handle->Read(QueryContext(), result.data(), size, 0);
	}
	return result;
}

static void WriteFile(LocalFileSystem &fs, const string &path, const duckdb::vector<data_t> &contents) {
	BufferedFileWriter writer(fs, path, FileFlags::FILE_FLAGS_WRITE);
	if (!contents.empty()) {
		writer.WriteData(contents.data(), contents.size());
	}
	writer.Sync();
}

//! Checksum requires aligned input, so copy the range into an aligned buffer first.
static uint64_t ChecksumAligned(const duckdb::vector<data_t> &contents, const idx_t offset, const idx_t size) {
	duckdb::vector<data_t> aligned(size);
	memcpy(aligned.data(), contents.data() + offset, size);
	return Checksum(aligned.data(), size);
}

//! Deserialize a LEB128 varint at the given position. Fails if it runs past 'end'.
static bool ReadVarint(const duckdb::vector<data_t> &buffer, const idx_t pos, const idx_t end, idx_t &value,
                       idx_t &width) {
	value = 0;
	width = 0;
	idx_t shift = 0;
	while (pos + width < end) {
		auto byte = buffer[pos + width];
		if (shift >= 64) {
			// A varint of more than 10 bytes can not hold an idx_t.
			return false;
		}
		value |= idx_t(byte & 0x7F) << shift;
		width++;
		if (!(byte & 0x80)) {
			return true;
		}
		shift += 7;
	}
	return false;
}

static duckdb::vector<data_t> EncodeVarint(idx_t value) {
	duckdb::vector<data_t> result;
	do {
		auto byte = uint8_t(value & 0x7F);
		value >>= 7;
		if (value != 0) {
			byte |= 0x80;
		}
		result.push_back(data_t(byte));
	} while (value != 0);
	return result;
}

static void AppendU64(duckdb::vector<data_t> &buffer, const uint64_t value) {
	for (idx_t i = 0; i < sizeof(uint64_t); i++) {
		buffer.push_back(data_t((value >> (8 * i)) & 0xFF));
	}
}

struct IndexBufferLocation {
	//! The position and width of the allocation size of the first index buffer (field 104 of the allocator info).
	idx_t value_pos;
	idx_t value_width;
	//! The position, the width of the length prefix, and the length of the matching blob (field 103).
	idx_t blob_pos;
	idx_t blob_width;
	idx_t blob_length;
};

//! Locate the first index buffer in a CREATE_INDEX WAL entry.
static bool FindIndexBuffer(const duckdb::vector<data_t> &contents, const idx_t payload_off, const idx_t payload_end,
                            IndexBufferLocation &location) {
	if (payload_end - payload_off < 3 || contents[payload_off] != 100 || contents[payload_off + 1] != 0) {
		return false;
	}
	// The entry starts with field 100 (the WAL type), which must be CREATE_INDEX.
	idx_t wal_type;
	idx_t width;
	if (!ReadVarint(contents, payload_off + 2, payload_end, wal_type, width) ||
	    wal_type != idx_t(WALType::CREATE_INDEX)) {
		return false;
	}

	// The entry ends with a terminator.
	const idx_t list_end = payload_end - 2;
	// The index buffers are written after the property tree, so they are the trailing blob list of the entry. The
	// writer declares the number of fixed-size allocators, but writes one element per buffer, so we parse greedily.
	for (idx_t field = payload_off + 3; field + 2 <= list_end; field++) {
		if (contents[field] != 103 || contents[field + 1] != 0) {
			continue;
		}
		idx_t count;
		idx_t count_width;
		if (!ReadVarint(contents, field + 2, list_end, count, count_width) || count == 0 || count > 64) {
			continue;
		}
		idx_t pos = field + 2 + count_width;
		duckdb::vector<idx_t> prefixes;
		duckdb::vector<idx_t> lengths;
		bool valid = true;
		while (pos < list_end) {
			idx_t length;
			idx_t length_width;
			if (!ReadVarint(contents, pos, list_end, length, length_width) || length == 0 ||
			    pos + length_width + length > list_end) {
				valid = false;
				break;
			}
			prefixes.push_back(pos);
			lengths.push_back(length);
			pos += length_width + length;
		}
		if (!valid || pos != list_end || lengths.empty()) {
			continue;
		}

		// Find the matching allocation sizes (field 104 of the allocator info): one allocation size per buffer.
		for (idx_t alloc = payload_off + 3; alloc < field; alloc++) {
			if (contents[alloc] != 104 || contents[alloc + 1] != 0) {
				continue;
			}
			idx_t alloc_count;
			idx_t alloc_width;
			if (!ReadVarint(contents, alloc + 2, field, alloc_count, alloc_width) || alloc_count == 0) {
				continue;
			}
			idx_t value_pos = alloc + 2 + alloc_width;
			idx_t value;
			idx_t value_width;
			if (!ReadVarint(contents, value_pos, field, value, value_width) || value != lengths[0]) {
				continue;
			}
			location.value_pos = value_pos;
			location.value_width = value_width;
			location.blob_pos = prefixes[0];
			location.blob_length = lengths[0];
			if (!ReadVarint(contents, prefixes[0], list_end, location.blob_length, location.blob_width)) {
				continue;
			}
			return true;
		}
	}
	return false;
}

//! Inflate the allocation size of the first index buffer of the CREATE_INDEX entry, together with its blob.
static void InflateIndexBufferSize(LocalFileSystem &fs, const string &wal_path, const idx_t inflated_size) {
	auto contents = ReadFile(fs, wal_path);

	// The WAL starts with a header that ends with a 0xFFFF terminator, followed by the entries.
	idx_t frame_start = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i + 2 <= contents.size() && i < 64; i++) {
		if (contents[i] == 0xFF && contents[i + 1] == 0xFF) {
			frame_start = i + 2;
			break;
		}
	}
	REQUIRE(frame_start != DConstants::INVALID_INDEX);

	// The entries are framed as [u64 size][u64 checksum][payload].
	for (idx_t offset = frame_start; offset + 2 * sizeof(uint64_t) <= contents.size();) {
		uint64_t entry_size;
		uint64_t entry_checksum;
		memcpy(&entry_size, contents.data() + offset, sizeof(uint64_t));
		memcpy(&entry_checksum, contents.data() + offset + sizeof(uint64_t), sizeof(uint64_t));

		const idx_t payload_off = offset + 2 * sizeof(uint64_t);
		REQUIRE(payload_off + entry_size <= contents.size());
		REQUIRE(ChecksumAligned(contents, payload_off, entry_size) == entry_checksum);

		IndexBufferLocation location;
		if (!FindIndexBuffer(contents, payload_off, payload_off + entry_size, location)) {
			offset = payload_off + entry_size;
			continue;
		}

		// Rewrite the allocation size and the length prefix of the blob with the inflated size, and pad the blob.
		auto new_value = EncodeVarint(inflated_size);
		REQUIRE(location.blob_length < inflated_size);

		duckdb::vector<data_t> payload;
		payload.insert(payload.end(), contents.begin() + payload_off, contents.begin() + location.value_pos);
		payload.insert(payload.end(), new_value.begin(), new_value.end());
		payload.insert(payload.end(), contents.begin() + location.value_pos + location.value_width,
		               contents.begin() + location.blob_pos);
		payload.insert(payload.end(), new_value.begin(), new_value.end());
		payload.insert(payload.end(), contents.begin() + location.blob_pos + location.blob_width,
		               contents.begin() + location.blob_pos + location.blob_width + location.blob_length);
		payload.resize(payload.size() + (inflated_size - location.blob_length), data_t(0));
		payload.insert(payload.end(), contents.begin() + location.blob_pos + location.blob_width + location.blob_length,
		               contents.begin() + payload_off + entry_size);

		// Rebuild the WAL with the resized entry and its new checksum.
		duckdb::vector<data_t> new_contents;
		new_contents.insert(new_contents.end(), contents.begin(), contents.begin() + offset);
		uint64_t new_entry_size = payload.size();
		uint64_t new_entry_checksum = Checksum(payload.data(), payload.size());
		AppendU64(new_contents, new_entry_size);
		AppendU64(new_contents, new_entry_checksum);
		new_contents.insert(new_contents.end(), payload.begin(), payload.end());
		new_contents.insert(new_contents.end(), contents.begin() + payload_off + entry_size, contents.end());
		WriteFile(fs, wal_path, new_contents);
		return;
	}
	FAIL("no CREATE_INDEX entry with index buffers found in the WAL");
}

TEST_CASE("Reject an index buffer size that exceeds the block size during WAL replay", "[storage][wal]") {
	auto config = GetTestConfig();
	config->options.checkpoint_wal_size = idx_t(-1);
	config->options.checkpoint_on_shutdown = false;

	auto database_path = TestCreatePath("wal_corrupt_index_storage");
	auto wal_path = database_path + ".wal";
	LocalFileSystem fs;
	DeleteDatabase(database_path);

	{
		DuckDB db(database_path, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT range FROM range(1000)"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX idx_i ON t(i)"));
	}
	REQUIRE(fs.FileExists(wal_path));

	// The index buffer claims to be larger than a block, so it can not be read into a single block during replay.
	InflateIndexBufferSize(fs, wal_path, 1000000);

	// Failing to replay the WAL is fatal here, so opening the database throws.
	{
		bool threw = false;
		try {
			DuckDB db(database_path, config.get());
		} catch (std::exception &ex) {
			threw = true;
			REQUIRE(StringUtil::Contains(ex.what(), "Corrupt WAL: index buffer size 1000000 exceeds the block size"));
		}
		REQUIRE(threw);
		REQUIRE(fs.FileExists(wal_path));
	}

	// By default, WAL replay tolerates entries that it can not replay: the corrupted index is dropped instead of
	// overflowing the index buffer.
	config->options.abort_on_wal_failure = false;
	{
		DuckDB db(database_path, config.get());
		Connection con(db);

		// The remaining WAL entries replay fine.
		auto result = con.Query("SELECT count(*) FROM t");
		REQUIRE(CHECK_COLUMN(result, 0, {1000}));

		// The corrupted entry is rejected instead of overflowing the index buffer.
		result = con.Query("SELECT count(*) FROM duckdb_indexes()");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}

	DeleteDatabase(database_path);
}
