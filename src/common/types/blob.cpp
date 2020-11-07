#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

constexpr const char *Blob::HEX_TABLE;
const int Blob::HEX_MAP[256] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
};

idx_t Blob::GetStringSize(string_t blob) {
	auto data = blob.GetDataUnsafe();
	auto len = blob.GetSize();
	idx_t str_len = 0;
	for(idx_t i = 0; i < len; i++) {
		if (data[i] >= 32 && data [i] <= 127 && data[i] != '\\') {
			// ascii characters are rendered as-is
			str_len++;
		} else {
			// non-ascii characters are rendered as hexadecimal (e.g. \x00)
			str_len += 4;
		}
	}
	return str_len;
}

void Blob::ToString(string_t blob, char *output) {
	auto data = (data_ptr_t) blob.GetDataUnsafe();
	auto len = blob.GetSize();
	idx_t str_idx = 0;
	for(idx_t i = 0; i < len; i++) {
		if (data[i] >= 32 && data [i] <= 127 && data[i] != '\\') {
			// ascii characters are rendered as-is
			output[str_idx++] = data[i];
		} else {
			auto byte_a = data[i] >> 4;
			auto byte_b = data[i] & 0x0F;
			D_ASSERT(byte_a >= 0 && byte_a < 16);
			D_ASSERT(byte_b >= 0 && byte_b < 16);
			// non-ascii characters are rendered as hexadecimal (e.g. \x00)
			output[str_idx++] = '\\';
			output[str_idx++] = 'x';
			output[str_idx++] = Blob::HEX_TABLE[byte_a];
			output[str_idx++] = Blob::HEX_TABLE[byte_b];
		}
	}
	D_ASSERT(str_idx == GetStringSize(blob));
}

string Blob::ToString(string_t blob) {
	auto str_len = GetStringSize(blob);
	auto buffer = std::unique_ptr<char[]>(new char[str_len]);
	Blob::ToString(blob, buffer.get());
	return string(buffer.get(), str_len);
}

idx_t Blob::GetBlobSize(string_t str) {
	auto data = str.GetDataUnsafe();
	auto len = str.GetSize();
	idx_t str_len = 0;
	for(idx_t i = 0; i < len; i++) {
		if (data[i] == '\\') {
			if (i + 3 >= len) {
				throw ConversionException("Invalid hex escape code encountered in string -> blob conversion: unterminated escape code at end of blob");
			}
			if (data[i + 1] != 'x' || Blob::HEX_MAP[unsigned(data[i + 2])] < 0 || Blob::HEX_MAP[unsigned(data[i + 3])] < 0) {
				throw ConversionException("Invalid hex escape code encountered in string -> blob conversion: %s", string(data + i, 4));
			}
			str_len++;
			i += 3;
		} else if (data[i] >= 32 || data[i] <= 127) {
			str_len++;
		} else {
			throw ConversionException("Invalid byte encountered in STRING -> BLOB conversion. All non-ascii characters must be escaped with hex codes (e.g. \\xAA)");
		}
	}
	return str_len;
}

void Blob::ToBlob(string_t str, data_ptr_t output) {
	auto data = str.GetDataUnsafe();
	auto len = str.GetSize();
	idx_t blob_idx = 0;
	for(idx_t i = 0; i < len; i++) {
		if (data[i] == '\\') {
			int byte_a = Blob::HEX_MAP[unsigned(data[i + 2])];
			int byte_b = Blob::HEX_MAP[unsigned(data[i + 3])];
			D_ASSERT(i + 3 < len);
			D_ASSERT(byte_a >= 0 && byte_b >= 0);
			D_ASSERT(data[i + 1] == 'x');
			output[blob_idx++] = (Blob::HEX_MAP[unsigned(data[i + 2])] << 4) +Blob::HEX_MAP[unsigned(data[i + 3])];
			i += 3;
		} else if (data[i] >= 32 || data[i] <= 127) {
			output[blob_idx++] = data_t(data[i]);
		} else {
			throw ConversionException("Invalid byte encountered in STRING -> BLOB conversion. All non-ascii characters must be escaped with hex codes (e.g. \\xAA)");
		}
	}
	D_ASSERT(blob_idx == GetBlobSize(str));
}

string Blob::ToBlob(string_t str) {
	auto blob_len = GetBlobSize(str);
	auto buffer = std::unique_ptr<char[]>(new char[blob_len]);
	Blob::ToBlob(str, (data_ptr_t) buffer.get());
	return string(buffer.get(), blob_len);
}

}