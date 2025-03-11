#include "writer/primitive_column_writer.hpp"
#include "parquet_rle_bp_decoder.hpp"
#include "parquet_rle_bp_encoder.hpp"
#include "parquet_writer.hpp"

namespace duckdb {
using duckdb_parquet::Encoding;
using duckdb_parquet::PageType;

PrimitiveColumnWriter::PrimitiveColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema,
                                             vector<string> schema_path, bool can_have_nulls)
    : ColumnWriter(writer, column_schema, std::move(schema_path), can_have_nulls) {
}

unique_ptr<ColumnWriterState> PrimitiveColumnWriter::InitializeWriteState(duckdb_parquet::RowGroup &row_group) {
	auto result = make_uniq<PrimitiveColumnWriterState>(writer, row_group, row_group.columns.size());
	RegisterToRowGroup(row_group);
	return std::move(result);
}

void PrimitiveColumnWriter::RegisterToRowGroup(duckdb_parquet::RowGroup &row_group) {
	duckdb_parquet::ColumnChunk column_chunk;
	column_chunk.__isset.meta_data = true;
	column_chunk.meta_data.codec = writer.GetCodec();
	column_chunk.meta_data.path_in_schema = schema_path;
	column_chunk.meta_data.num_values = 0;
	column_chunk.meta_data.type = writer.GetType(SchemaIndex());
	row_group.columns.push_back(std::move(column_chunk));
}

unique_ptr<ColumnWriterPageState> PrimitiveColumnWriter::InitializePageState(PrimitiveColumnWriterState &state,
                                                                             idx_t page_idx) {
	return nullptr;
}

void PrimitiveColumnWriter::FlushPageState(WriteStream &temp_writer, ColumnWriterPageState *state) {
}

void PrimitiveColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector,
                                    idx_t count) {
	auto &state = state_p.Cast<PrimitiveColumnWriterState>();
	auto &col_chunk = state.row_group.columns[state.col_idx];

	idx_t vcount = parent ? parent->definition_levels.size() - state.definition_levels.size() : count;
	idx_t parent_index = state.definition_levels.size();
	auto &validity = FlatVector::Validity(vector);
	HandleRepeatLevels(state, parent, count, MaxRepeat());
	HandleDefineLevels(state, parent, validity, count, MaxDefine(), MaxDefine() - 1);

	idx_t vector_index = 0;
	reference<PageInformation> page_info_ref = state.page_info.back();
	col_chunk.meta_data.num_values += NumericCast<int64_t>(vcount);

	const bool check_parent_empty = parent && !parent->is_empty.empty();
	if (!check_parent_empty && validity.AllValid() && TypeIsConstantSize(vector.GetType().InternalType()) &&
	    page_info_ref.get().estimated_page_size + GetRowSize(vector, vector_index, state) * vcount <
	        MAX_UNCOMPRESSED_PAGE_SIZE) {
		// Fast path: fixed-size type, all valid, and it fits on the current page
		auto &page_info = page_info_ref.get();
		page_info.row_count += vcount;
		page_info.estimated_page_size += GetRowSize(vector, vector_index, state) * vcount;
	} else {
		for (idx_t i = 0; i < vcount; i++) {
			auto &page_info = page_info_ref.get();
			page_info.row_count++;
			if (check_parent_empty && parent->is_empty[parent_index + i]) {
				page_info.empty_count++;
				continue;
			}
			if (validity.RowIsValid(vector_index)) {
				page_info.estimated_page_size += GetRowSize(vector, vector_index, state);
				if (page_info.estimated_page_size >= MAX_UNCOMPRESSED_PAGE_SIZE) {
					PageInformation new_info;
					new_info.offset = page_info.offset + page_info.row_count;
					state.page_info.push_back(new_info);
					page_info_ref = state.page_info.back();
				}
			} else {
				page_info.null_count++;
			}
			vector_index++;
		}
	}
}

duckdb_parquet::Encoding::type PrimitiveColumnWriter::GetEncoding(PrimitiveColumnWriterState &state) {
	return Encoding::PLAIN;
}

void PrimitiveColumnWriter::BeginWrite(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<PrimitiveColumnWriterState>();

	// set up the page write info
	state.stats_state = InitializeStatsState();
	for (idx_t page_idx = 0; page_idx < state.page_info.size(); page_idx++) {
		auto &page_info = state.page_info[page_idx];
		if (page_info.row_count == 0) {
			D_ASSERT(page_idx + 1 == state.page_info.size());
			state.page_info.erase_at(page_idx);
			break;
		}
		PageWriteInformation write_info;
		// set up the header
		auto &hdr = write_info.page_header;
		hdr.compressed_page_size = 0;
		hdr.uncompressed_page_size = 0;
		hdr.type = PageType::DATA_PAGE;
		hdr.__isset.data_page_header = true;

		hdr.data_page_header.num_values = UnsafeNumericCast<int32_t>(page_info.row_count);
		hdr.data_page_header.encoding = GetEncoding(state);
		hdr.data_page_header.definition_level_encoding = Encoding::RLE;
		hdr.data_page_header.repetition_level_encoding = Encoding::RLE;

		write_info.temp_writer = make_uniq<MemoryStream>(
		    Allocator::Get(writer.GetContext()),
		    MaxValue<idx_t>(NextPowerOfTwo(page_info.estimated_page_size), MemoryStream::DEFAULT_INITIAL_CAPACITY));
		write_info.write_count = page_info.empty_count;
		write_info.max_write_count = page_info.row_count;
		write_info.page_state = InitializePageState(state, page_idx);

		write_info.compressed_size = 0;
		write_info.compressed_data = nullptr;

		state.write_info.push_back(std::move(write_info));
	}

	// start writing the first page
	NextPage(state);
}

void PrimitiveColumnWriter::WriteLevels(WriteStream &temp_writer, const unsafe_vector<uint16_t> &levels,
                                        idx_t max_value, idx_t offset, idx_t count, optional_idx null_count) {
	if (levels.empty() || count == 0) {
		return;
	}

	// write the levels using the RLE-BP encoding
	const auto bit_width = RleBpDecoder::ComputeBitWidth((max_value));
	RleBpEncoder rle_encoder(bit_width);

	// have to write to an intermediate stream first because we need to know the size
	MemoryStream intermediate_stream(Allocator::DefaultAllocator());

	rle_encoder.BeginWrite();
	if (null_count.IsValid() && null_count.GetIndex() == 0) {
		// Fast path: no nulls
		rle_encoder.WriteMany(intermediate_stream, levels[0], count);
	} else {
		for (idx_t i = offset; i < offset + count; i++) {
			rle_encoder.WriteValue(intermediate_stream, levels[i]);
		}
	}
	rle_encoder.FinishWrite(intermediate_stream);

	// start off by writing the byte count as a uint32_t
	temp_writer.Write(NumericCast<uint32_t>(intermediate_stream.GetPosition()));
	// copy over the written data
	temp_writer.WriteData(intermediate_stream.GetData(), intermediate_stream.GetPosition());
}

void PrimitiveColumnWriter::NextPage(PrimitiveColumnWriterState &state) {
	if (state.current_page > 0) {
		// need to flush the current page
		FlushPage(state);
	}
	if (state.current_page >= state.write_info.size()) {
		state.current_page = state.write_info.size() + 1;
		return;
	}
	auto &page_info = state.page_info[state.current_page];
	auto &write_info = state.write_info[state.current_page];
	state.current_page++;

	auto &temp_writer = *write_info.temp_writer;

	// write the repetition levels
	WriteLevels(temp_writer, state.repetition_levels, MaxRepeat(), page_info.offset, page_info.row_count);

	// write the definition levels
	WriteLevels(temp_writer, state.definition_levels, MaxDefine(), page_info.offset, page_info.row_count,
	            state.null_count + state.parent_null_count);
}

void PrimitiveColumnWriter::FlushPage(PrimitiveColumnWriterState &state) {
	D_ASSERT(state.current_page > 0);
	if (state.current_page > state.write_info.size()) {
		return;
	}

	// compress the page info
	auto &write_info = state.write_info[state.current_page - 1];
	auto &temp_writer = *write_info.temp_writer;
	auto &hdr = write_info.page_header;

	FlushPageState(temp_writer, write_info.page_state.get());

	// now that we have finished writing the data we know the uncompressed size
	if (temp_writer.GetPosition() > idx_t(NumericLimits<int32_t>::Maximum())) {
		throw InternalException("Parquet writer: %d uncompressed page size out of range for type integer",
		                        temp_writer.GetPosition());
	}
	hdr.uncompressed_page_size = UnsafeNumericCast<int32_t>(temp_writer.GetPosition());

	// compress the data
	CompressPage(temp_writer, write_info.compressed_size, write_info.compressed_data, write_info.compressed_buf);
	hdr.compressed_page_size = UnsafeNumericCast<int32_t>(write_info.compressed_size);
	D_ASSERT(hdr.uncompressed_page_size > 0);
	D_ASSERT(hdr.compressed_page_size > 0);

	if (write_info.compressed_buf) {
		// if the data has been compressed, we no longer need the uncompressed data
		D_ASSERT(write_info.compressed_buf.get() == write_info.compressed_data);
		write_info.temp_writer.reset();
	}
}

unique_ptr<ColumnWriterStatistics> PrimitiveColumnWriter::InitializeStatsState() {
	return make_uniq<ColumnWriterStatistics>();
}

idx_t PrimitiveColumnWriter::GetRowSize(const Vector &vector, const idx_t index,
                                        const PrimitiveColumnWriterState &state) const {
	throw InternalException("GetRowSize unsupported for struct/list column writers");
}

void PrimitiveColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<PrimitiveColumnWriterState>();

	idx_t remaining = count;
	idx_t offset = 0;
	while (remaining > 0) {
		auto &write_info = state.write_info[state.current_page - 1];
		if (!write_info.temp_writer) {
			throw InternalException("Writes are not correctly aligned!?");
		}
		auto &temp_writer = *write_info.temp_writer;
		idx_t write_count = MinValue<idx_t>(remaining, write_info.max_write_count - write_info.write_count);
		D_ASSERT(write_count > 0);

		WriteVector(temp_writer, state.stats_state.get(), write_info.page_state.get(), vector, offset,
		            offset + write_count);

		write_info.write_count += write_count;
		if (write_info.write_count == write_info.max_write_count) {
			NextPage(state);
		}
		offset += write_count;
		remaining -= write_count;
	}
}

void PrimitiveColumnWriter::SetParquetStatistics(PrimitiveColumnWriterState &state,
                                                 duckdb_parquet::ColumnChunk &column_chunk) {
	if (!state.stats_state) {
		return;
	}
	if (MaxRepeat() == 0) {
		column_chunk.meta_data.statistics.null_count = NumericCast<int64_t>(state.null_count);
		column_chunk.meta_data.statistics.__isset.null_count = true;
		column_chunk.meta_data.__isset.statistics = true;
	}
	// set min/max/min_value/max_value
	// this code is not going to win any beauty contests, but well
	auto min = state.stats_state->GetMin();
	if (!min.empty()) {
		column_chunk.meta_data.statistics.min = std::move(min);
		column_chunk.meta_data.statistics.__isset.min = true;
		column_chunk.meta_data.__isset.statistics = true;
	}
	auto max = state.stats_state->GetMax();
	if (!max.empty()) {
		column_chunk.meta_data.statistics.max = std::move(max);
		column_chunk.meta_data.statistics.__isset.max = true;
		column_chunk.meta_data.__isset.statistics = true;
	}
	if (state.stats_state->HasStats()) {
		column_chunk.meta_data.statistics.min_value = state.stats_state->GetMinValue();
		column_chunk.meta_data.statistics.__isset.min_value = true;
		column_chunk.meta_data.__isset.statistics = true;

		column_chunk.meta_data.statistics.max_value = state.stats_state->GetMaxValue();
		column_chunk.meta_data.statistics.__isset.max_value = true;
		column_chunk.meta_data.__isset.statistics = true;
	}
	if (HasDictionary(state)) {
		column_chunk.meta_data.statistics.distinct_count = UnsafeNumericCast<int64_t>(DictionarySize(state));
		column_chunk.meta_data.statistics.__isset.distinct_count = true;
		column_chunk.meta_data.__isset.statistics = true;
	}
	for (const auto &write_info : state.write_info) {
		// only care about data page encodings, data_page_header.encoding is meaningless for dict
		if (write_info.page_header.type != PageType::DATA_PAGE &&
		    write_info.page_header.type != PageType::DATA_PAGE_V2) {
			continue;
		}
		column_chunk.meta_data.encodings.push_back(write_info.page_header.data_page_header.encoding);
	}
}

void PrimitiveColumnWriter::FinalizeWrite(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<PrimitiveColumnWriterState>();
	auto &column_chunk = state.row_group.columns[state.col_idx];

	// flush the last page (if any remains)
	FlushPage(state);

	auto &column_writer = writer.GetWriter();
	auto start_offset = column_writer.GetTotalWritten();
	// flush the dictionary
	if (HasDictionary(state)) {
		column_chunk.meta_data.statistics.distinct_count = UnsafeNumericCast<int64_t>(DictionarySize(state));
		column_chunk.meta_data.statistics.__isset.distinct_count = true;
		column_chunk.meta_data.dictionary_page_offset = UnsafeNumericCast<int64_t>(column_writer.GetTotalWritten());
		column_chunk.meta_data.__isset.dictionary_page_offset = true;
		FlushDictionary(state, state.stats_state.get());
	}

	// record the start position of the pages for this column
	column_chunk.meta_data.data_page_offset = 0;
	SetParquetStatistics(state, column_chunk);

	// write the individual pages to disk
	idx_t total_uncompressed_size = 0;
	for (auto &write_info : state.write_info) {
		// set the data page offset whenever we see the *first* data page
		if (column_chunk.meta_data.data_page_offset == 0 && (write_info.page_header.type == PageType::DATA_PAGE ||
		                                                     write_info.page_header.type == PageType::DATA_PAGE_V2)) {
			column_chunk.meta_data.data_page_offset = UnsafeNumericCast<int64_t>(column_writer.GetTotalWritten());
			;
		}
		D_ASSERT(write_info.page_header.uncompressed_page_size > 0);
		auto header_start_offset = column_writer.GetTotalWritten();
		writer.Write(write_info.page_header);
		// total uncompressed size in the column chunk includes the header size (!)
		total_uncompressed_size += column_writer.GetTotalWritten() - header_start_offset;
		total_uncompressed_size += write_info.page_header.uncompressed_page_size;
		writer.WriteData(write_info.compressed_data, write_info.compressed_size);
	}
	column_chunk.meta_data.total_compressed_size =
	    UnsafeNumericCast<int64_t>(column_writer.GetTotalWritten() - start_offset);
	column_chunk.meta_data.total_uncompressed_size = UnsafeNumericCast<int64_t>(total_uncompressed_size);
	state.row_group.total_byte_size += column_chunk.meta_data.total_uncompressed_size;

	if (state.bloom_filter) {
		writer.BufferBloomFilter(state.col_idx, std::move(state.bloom_filter));
	}
	// which row group is this?
}

void PrimitiveColumnWriter::FlushDictionary(PrimitiveColumnWriterState &state, ColumnWriterStatistics *stats) {
	throw InternalException("This page does not have a dictionary");
}

idx_t PrimitiveColumnWriter::DictionarySize(PrimitiveColumnWriterState &state) {
	throw InternalException("This page does not have a dictionary");
}

void PrimitiveColumnWriter::WriteDictionary(PrimitiveColumnWriterState &state, unique_ptr<MemoryStream> temp_writer,
                                            idx_t row_count) {
	D_ASSERT(temp_writer);
	D_ASSERT(temp_writer->GetPosition() > 0);

	// write the dictionary page header
	PageWriteInformation write_info;
	// set up the header
	auto &hdr = write_info.page_header;
	hdr.uncompressed_page_size = UnsafeNumericCast<int32_t>(temp_writer->GetPosition());
	hdr.type = PageType::DICTIONARY_PAGE;
	hdr.__isset.dictionary_page_header = true;

	hdr.dictionary_page_header.encoding = Encoding::PLAIN;
	hdr.dictionary_page_header.is_sorted = false;
	hdr.dictionary_page_header.num_values = UnsafeNumericCast<int32_t>(row_count);

	write_info.temp_writer = std::move(temp_writer);
	write_info.write_count = 0;
	write_info.max_write_count = 0;

	// compress the contents of the dictionary page
	CompressPage(*write_info.temp_writer, write_info.compressed_size, write_info.compressed_data,
	             write_info.compressed_buf);
	hdr.compressed_page_size = UnsafeNumericCast<int32_t>(write_info.compressed_size);

	// insert the dictionary page as the first page to write for this column
	state.write_info.insert(state.write_info.begin(), std::move(write_info));
}

} // namespace duckdb
