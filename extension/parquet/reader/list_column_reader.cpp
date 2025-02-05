#include "list_column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// List Column Reader
//===--------------------------------------------------------------------===//
idx_t ListColumnReader::Read(uint64_t num_values, parquet_filter_t &filter, data_ptr_t define_out,
                             data_ptr_t repeat_out, Vector &result_out) {
	idx_t result_offset = 0;
	auto result_ptr = FlatVector::GetData<list_entry_t>(result_out);
	auto &result_mask = FlatVector::Validity(result_out);

	if (pending_skips > 0) {
		ApplyPendingSkips(pending_skips);
	}

	D_ASSERT(ListVector::GetListSize(result_out) == 0);
	// if an individual list is longer than STANDARD_VECTOR_SIZE we actually have to loop the child read to fill it
	bool finished = false;
	while (!finished) {
		idx_t child_actual_num_values = 0;

		// check if we have any overflow from a previous read
		if (overflow_child_count == 0) {
			// we don't: read elements from the child reader
			child_defines.zero();
			child_repeats.zero();
			// we don't know in advance how many values to read because of the beautiful repetition/definition setup
			// we just read (up to) a vector from the child column, and see if we have read enough
			// if we have not read enough, we read another vector
			// if we have read enough, we leave any unhandled elements in the overflow vector for a subsequent read
			auto child_req_num_values =
			    MinValue<idx_t>(STANDARD_VECTOR_SIZE, child_column_reader->GroupRowsAvailable());
			read_vector.ResetFromCache(read_cache);
			child_actual_num_values = child_column_reader->Read(child_req_num_values, child_filter, child_defines_ptr,
			                                                    child_repeats_ptr, read_vector);
		} else {
			// we do: use the overflow values
			child_actual_num_values = overflow_child_count;
			overflow_child_count = 0;
		}

		if (child_actual_num_values == 0) {
			// no more elements available: we are done
			break;
		}
		read_vector.Verify(child_actual_num_values);
		idx_t current_chunk_offset = ListVector::GetListSize(result_out);

		// hard-won piece of code this, modify at your own risk
		// the intuition is that we have to only collapse values into lists that are repeated *on this level*
		// the rest is pretty much handed up as-is as a single-valued list or NULL
		idx_t child_idx;
		for (child_idx = 0; child_idx < child_actual_num_values; child_idx++) {
			if (child_repeats_ptr[child_idx] == max_repeat) {
				// value repeats on this level, append
				D_ASSERT(result_offset > 0);
				result_ptr[result_offset - 1].length++;
				continue;
			}

			if (result_offset >= num_values) {
				// we ran out of output space
				finished = true;
				break;
			}
			if (child_defines_ptr[child_idx] >= max_define) {
				// value has been defined down the stack, hence its NOT NULL
				result_ptr[result_offset].offset = child_idx + current_chunk_offset;
				result_ptr[result_offset].length = 1;
			} else if (child_defines_ptr[child_idx] == max_define - 1) {
				// empty list
				result_ptr[result_offset].offset = child_idx + current_chunk_offset;
				result_ptr[result_offset].length = 0;
			} else {
				// value is NULL somewhere up the stack
				result_mask.SetInvalid(result_offset);
				result_ptr[result_offset].offset = 0;
				result_ptr[result_offset].length = 0;
			}

			repeat_out[result_offset] = child_repeats_ptr[child_idx];
			define_out[result_offset] = child_defines_ptr[child_idx];

			result_offset++;
		}
		// actually append the required elements to the child list
		ListVector::Append(result_out, read_vector, child_idx);

		// we have read more values from the child reader than we can fit into the result for this read
		// we have to pass everything from child_idx to child_actual_num_values into the next call
		if (child_idx < child_actual_num_values && result_offset == num_values) {
			read_vector.Slice(read_vector, child_idx, child_actual_num_values);
			overflow_child_count = child_actual_num_values - child_idx;
			read_vector.Verify(overflow_child_count);

			// move values in the child repeats and defines *backward* by child_idx
			for (idx_t repdef_idx = 0; repdef_idx < overflow_child_count; repdef_idx++) {
				child_defines_ptr[repdef_idx] = child_defines_ptr[child_idx + repdef_idx];
				child_repeats_ptr[repdef_idx] = child_repeats_ptr[child_idx + repdef_idx];
			}
		}
	}
	result_out.Verify(result_offset);
	return result_offset;
}

ListColumnReader::ListColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p,
                                   idx_t schema_idx_p, idx_t max_define_p, idx_t max_repeat_p,
                                   unique_ptr<ColumnReader> child_column_reader_p)
    : ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p),
      child_column_reader(std::move(child_column_reader_p)),
      read_cache(reader.allocator, ListType::GetChildType(Type())), read_vector(read_cache), overflow_child_count(0) {

	child_defines.resize(reader.allocator, STANDARD_VECTOR_SIZE);
	child_repeats.resize(reader.allocator, STANDARD_VECTOR_SIZE);
	child_defines_ptr = (uint8_t *)child_defines.ptr;
	child_repeats_ptr = (uint8_t *)child_repeats.ptr;

	child_filter.set();
}

void ListColumnReader::ApplyPendingSkips(idx_t num_values) {
	pending_skips -= num_values;

	auto define_out = unique_ptr<uint8_t[]>(new uint8_t[num_values]);
	auto repeat_out = unique_ptr<uint8_t[]>(new uint8_t[num_values]);

	idx_t remaining = num_values;
	idx_t read = 0;

	while (remaining) {
		Vector result_out(Type());
		parquet_filter_t filter;
		idx_t to_read = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
		read += Read(to_read, filter, define_out.get(), repeat_out.get(), result_out);
		remaining -= to_read;
	}

	if (read != num_values) {
		throw InternalException("Not all skips done!");
	}
}

}
