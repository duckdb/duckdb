#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/serializer/read_stream.hpp"
#include "duckdb/common/types/selection_vector.hpp"

namespace duckdb {

ValidityData::ValidityData(idx_t count) : TemplatedValidityData(count) {
}
ValidityData::ValidityData(const ValidityMask &original, idx_t count)
    : TemplatedValidityData(original.GetData(), count) {
}

void ValidityMask::Combine(const ValidityMask &other, idx_t count) {
	if (other.AllValid()) {
		// X & 1 = X
		return;
	}
	if (AllValid()) {
		// 1 & Y = Y
		Initialize(other);
		return;
	}
	if (validity_mask == other.validity_mask) {
		// X & X == X
		return;
	}
	// have to merge
	// create a new validity mask that contains the combined mask
	auto owned_data = std::move(validity_data);
	auto data = GetData();
	auto other_data = other.GetData();

	Initialize(count);
	auto result_data = GetData();

	auto entry_count = ValidityData::EntryCount(count);
	for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
		result_data[entry_idx] = data[entry_idx] & other_data[entry_idx];
	}
}

// LCOV_EXCL_START
string ValidityMask::ToString(idx_t count) const {
	string result = "Validity Mask (" + to_string(count) + ") [";
	for (idx_t i = 0; i < count; i++) {
		result += RowIsValid(i) ? "." : "X";
	}
	result += "]";
	return result;
}
// LCOV_EXCL_STOP

void ValidityMask::Resize(idx_t old_size, idx_t new_size) {
	D_ASSERT(new_size >= old_size);
	target_count = new_size;
	if (validity_mask) {
		auto new_size_count = EntryCount(new_size);
		auto old_size_count = EntryCount(old_size);
		auto new_validity_data = make_buffer<ValidityBuffer>(new_size);
		auto new_owned_data = new_validity_data->owned_data.get();
		for (idx_t entry_idx = 0; entry_idx < old_size_count; entry_idx++) {
			new_owned_data[entry_idx] = validity_mask[entry_idx];
		}
		for (idx_t entry_idx = old_size_count; entry_idx < new_size_count; entry_idx++) {
			new_owned_data[entry_idx] = ValidityData::MAX_ENTRY;
		}
		validity_data = std::move(new_validity_data);
		validity_mask = validity_data->owned_data.get();
	}
}

idx_t ValidityMask::TargetCount() const {
	return target_count;
}

void ValidityMask::Slice(const ValidityMask &other, idx_t source_offset, idx_t count) {
	if (other.AllValid()) {
		validity_mask = nullptr;
		validity_data.reset();
		return;
	}
	if (source_offset == 0) {
		Initialize(other);
		return;
	}
	ValidityMask new_mask(count);
	new_mask.SliceInPlace(other, 0, source_offset, count);
	Initialize(new_mask);
}

bool ValidityMask::IsAligned(idx_t count) {
	return count % BITS_PER_VALUE == 0;
}

void ValidityMask::CopySel(const ValidityMask &other, const SelectionVector &sel, idx_t source_offset,
                           idx_t target_offset, idx_t copy_count) {
	if (!other.IsMaskSet() && !IsMaskSet()) {
		// no need to copy anything if neither has any null values
		return;
	}

	if (!sel.IsSet() && IsAligned(source_offset) && IsAligned(target_offset)) {
		// common case where we are shifting into an aligned mask using a flat vector
		SliceInPlace(other, target_offset, source_offset, copy_count);
		return;
	}
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel.get_index(source_offset + i);
		Set(target_offset + i, other.RowIsValid(source_idx));
	}
}

void ValidityMask::SliceInPlace(const ValidityMask &other, idx_t target_offset, idx_t source_offset, idx_t count) {
	EnsureWritable();
	const idx_t ragged = count % BITS_PER_VALUE;
	const idx_t entire_units = count / BITS_PER_VALUE;
	if (IsAligned(source_offset) && IsAligned(target_offset)) {
		auto target_validity = GetData();
		auto source_validity = other.GetData();
		auto source_offset_entries = EntryCount(source_offset);
		auto target_offset_entries = EntryCount(target_offset);
		if (!source_validity) {
			// if source has no validity mask - set all bytes to 1
			memset(target_validity + target_offset_entries, 0xFF, sizeof(validity_t) * entire_units);
		} else {
			memcpy(target_validity + target_offset_entries, source_validity + source_offset_entries,
			       sizeof(validity_t) * entire_units);
		}
		if (ragged) {
			auto src_entry =
			    source_validity ? source_validity[source_offset_entries + entire_units] : ValidityBuffer::MAX_ENTRY;
			src_entry &= (ValidityBuffer::MAX_ENTRY >> (BITS_PER_VALUE - ragged));

			target_validity += target_offset_entries + entire_units;
			auto tgt_entry = *target_validity;
			tgt_entry &= (ValidityBuffer::MAX_ENTRY << ragged);

			*target_validity = tgt_entry | src_entry;
		}
		return;
	} else if (IsAligned(target_offset)) {
		//	Simple common case where we are shifting into an aligned mask (e.g., 0 in Slice above)
		const idx_t tail = source_offset % BITS_PER_VALUE;
		const idx_t head = BITS_PER_VALUE - tail;
		auto source_validity = other.GetData() + (source_offset / BITS_PER_VALUE);
		auto target_validity = this->GetData() + (target_offset / BITS_PER_VALUE);
		auto src_entry = *source_validity++;
		for (idx_t i = 0; i < entire_units; ++i) {
			//	Start with head of previous src
			validity_t tgt_entry = src_entry >> tail;
			src_entry = *source_validity++;
			// 	Add in tail of current src
			tgt_entry |= (src_entry << head);
			*target_validity++ = tgt_entry;
		}
		//	Finish last ragged entry
		if (ragged) {
			//	Start with head of previous src
			validity_t tgt_entry = (src_entry >> tail);
			//  Add in the tail of the next src, if head was too small
			if (head < ragged) {
				src_entry = *source_validity++;
				tgt_entry |= (src_entry << head);
			}
			//  Mask off the bits that go past the ragged end
			tgt_entry &= (ValidityBuffer::MAX_ENTRY >> (BITS_PER_VALUE - ragged));
			//	Restore the ragged end of the target
			tgt_entry |= *target_validity & (ValidityBuffer::MAX_ENTRY << ragged);
			*target_validity++ = tgt_entry;
		}
		return;
	}

	// FIXME: use bitwise operations here
#if 1
	for (idx_t i = 0; i < count; i++) {
		Set(target_offset + i, other.RowIsValid(source_offset + i));
	}
#else
	// first shift the "whole" units
	idx_t entire_units = offset / BITS_PER_VALUE;
	idx_t sub_units = offset - entire_units * BITS_PER_VALUE;
	if (entire_units > 0) {
		idx_t validity_idx;
		for (validity_idx = 0; validity_idx + entire_units < STANDARD_ENTRY_COUNT; validity_idx++) {
			new_mask.validity_mask[validity_idx] = other.validity_mask[validity_idx + entire_units];
		}
	}
	// now we shift the remaining sub units
	// this gets a bit more complicated because we have to shift over the borders of the entries
	// e.g. suppose we have 2 entries of length 4 and we left-shift by two
	// 0101|1010
	// a regular left-shift of both gets us:
	// 0100|1000
	// we then OR the overflow (right-shifted by BITS_PER_VALUE - offset) together to get the correct result
	// 0100|1000 ->
	// 0110|1000
	if (sub_units > 0) {
		idx_t validity_idx;
		for (validity_idx = 0; validity_idx + 1 < STANDARD_ENTRY_COUNT; validity_idx++) {
			new_mask.validity_mask[validity_idx] =
			    (other.validity_mask[validity_idx] >> sub_units) |
			    (other.validity_mask[validity_idx + 1] << (BITS_PER_VALUE - sub_units));
		}
		new_mask.validity_mask[validity_idx] >>= sub_units;
	}
#ifdef DEBUG
	for (idx_t i = offset; i < STANDARD_VECTOR_SIZE; i++) {
		D_ASSERT(new_mask.RowIsValid(i - offset) == other.RowIsValid(i));
	}
	Initialize(new_mask);
#endif
#endif
}

enum class ValiditySerialization : uint8_t { BITMASK = 0, VALID_VALUES = 1, INVALID_VALUES = 2 };

void ValidityMask::Write(WriteStream &writer, idx_t count) {
	auto valid_values = CountValid(count);
	auto invalid_values = count - valid_values;
	auto bitmask_bytes = ValidityMask::ValidityMaskSize(count);
	auto need_u32 = count >= NumericLimits<uint16_t>::Maximum();
	auto bytes_per_value = need_u32 ? sizeof(uint32_t) : sizeof(uint16_t);
	auto valid_value_size = bytes_per_value * valid_values + sizeof(uint32_t);
	auto invalid_value_size = bytes_per_value * invalid_values + sizeof(uint32_t);
	if (valid_value_size < bitmask_bytes || invalid_value_size < bitmask_bytes) {
		auto serialize_valid = valid_value_size < invalid_value_size;
		// serialize (in)valid value indexes as [COUNT][V0][V1][...][VN]
		auto flag = serialize_valid ? ValiditySerialization::VALID_VALUES : ValiditySerialization::INVALID_VALUES;
		writer.Write(flag);
		writer.Write<uint32_t>(NumericCast<uint32_t>(MinValue(valid_values, invalid_values)));
		for (idx_t i = 0; i < count; i++) {
			if (RowIsValid(i) == serialize_valid) {
				if (need_u32) {
					writer.Write<uint32_t>(UnsafeNumericCast<uint32_t>(i));
				} else {
					writer.Write<uint16_t>(UnsafeNumericCast<uint16_t>(i));
				}
			}
		}
	} else {
		// serialize the entire bitmask
		writer.Write(ValiditySerialization::BITMASK);
		writer.WriteData(const_data_ptr_cast(GetData()), bitmask_bytes);
	}
}

void ValidityMask::Read(ReadStream &reader, idx_t count) {
	Initialize(count);
	// deserialize the storage type
	auto flag = reader.Read<ValiditySerialization>();
	if (flag == ValiditySerialization::BITMASK) {
		// deserialize the bitmask
		reader.ReadData(data_ptr_cast(GetData()), ValidityMask::ValidityMaskSize(count));
		return;
	}
	auto is_u32 = count >= NumericLimits<uint16_t>::Maximum();
	auto is_valid = flag == ValiditySerialization::VALID_VALUES;
	auto serialize_count = reader.Read<uint32_t>();
	if (is_valid) {
		SetAllInvalid(count);
	}
	for (idx_t i = 0; i < serialize_count; i++) {
		idx_t index = is_u32 ? reader.Read<uint32_t>() : reader.Read<uint16_t>();
		Set(index, is_valid);
	}
}

} // namespace duckdb
