#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/string_uncompressed.hpp"

namespace duckdb {

typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t index_buffer_offset;
	uint32_t index_buffer_count;
	uint32_t bitpacking_width;
} dictionary_compression_header_t;

struct DictionaryCompression {
public:
	static constexpr float MINIMUM_COMPRESSION_RATIO = 1.2F;
	//! Dictionary header size at the beginning of the string segment (offset + length)
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dictionary_compression_header_t);

public:
	static bool HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width, const idx_t block_size);
	static idx_t RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);

	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);
	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);

	template <class T>
	static bool UpdateState(T &state, const Vector &scan_vector) {
		state.Verify();

		for (auto entry : scan_vector.Values<string_t>()) {
			idx_t string_size = 0;
			bool new_string = false;
			auto row_is_valid = entry.IsValid();

			if (row_is_valid) {
				auto &str = entry.GetValue();
				string_size = str.GetSize();
				if (string_size >= StringUncompressed::GetStringBlockLimit(state.info.GetBlockSize())) {
					// Big strings not implemented for dictionary compression
					return false;
				}
				new_string = !state.LookupString(str);
			}

			bool fits = state.CalculateSpaceRequirements(new_string, string_size);
			if (!fits) {
				state.Flush();
				new_string = true;

				fits = state.CalculateSpaceRequirements(new_string, string_size);
				if (!fits) {
					throw InternalException("Dictionary compression could not write to new segment");
				}
			}

			if (!row_is_valid) {
				state.AddNull();
			} else if (new_string) {
				state.AddNewString(entry.GetValue());
			} else {
				state.AddLastLookup();
			}

			state.Verify();
		}

		return true;
	}
};

} // namespace duckdb
