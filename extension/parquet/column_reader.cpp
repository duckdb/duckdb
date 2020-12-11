#include "column_reader.hpp"
#include <iostream>

namespace duckdb {
ColumnReader::~ColumnReader() {
}

void ColumnReader::Read(uint64_t num_values, Vector &result) {
	auto trans = (DuckdbFileTransport *)protocol.getTransport().get();

	idx_t to_read = num_values;
	idx_t result_offset = 0;

	while (to_read > 0) {

		while (rows_available == 0) {
			col_data_buf = nullptr;
			dict_decoder.reset();

			// TODO make this a function, read page header, buffers and unpack if req.
			parquet::format::PageHeader page_hdr;
			page_hdr.read(&protocol);


			read_buffer.resize(page_hdr.compressed_page_size);
			trans->read((uint8_t *)read_buffer.ptr, page_hdr.compressed_page_size);

			//			page_hdr.printTo(std::cout);
			//			std::cout << '\n';

			switch (chunk.meta_data.codec) {
			case CompressionCodec::UNCOMPRESSED:
				col_data_buf = &read_buffer;
				break;
			case CompressionCodec::GZIP: {
				MiniZStream s;

				unpack_buffer.resize(page_hdr.uncompressed_page_size);
				s.Decompress(read_buffer.ptr, page_hdr.compressed_page_size, unpack_buffer.ptr,
				             page_hdr.uncompressed_page_size);
				col_data_buf = &unpack_buffer;

				break;
			}
			default:
				D_ASSERT(0);
				break;
			}

			switch (page_hdr.type) {
			case PageType::DATA_PAGE: {
				rows_available = page_hdr.data_page_header.num_values;

				// TODO skip this if no nulls
				// TODO fix len, its no longer accurate!

				uint32_t def_length = col_data_buf->read<uint32_t>();
				col_data_buf->available(def_length);
				defined_decoder = make_unique<RleBpDecoder>((const uint8_t *)col_data_buf->ptr, def_length, 1);
				col_data_buf->inc(def_length);

				switch (page_hdr.data_page_header.encoding) {
				case Encoding::RLE_DICTIONARY:
				case Encoding::PLAIN_DICTIONARY: {
					auto enc_length = col_data_buf->read<uint8_t>();
					// TODO fix len, its no longer accurate!
					dict_decoder =
					    make_unique<RleBpDecoder>((const uint8_t *)col_data_buf->ptr, col_data_buf->len, enc_length);

					break;
				}
				case Encoding::PLAIN:
					// nothing here, see below
					break;

				default:
					D_ASSERT(0);
					break;
				}

				break;
			}
			case PageType::DICTIONARY_PAGE:
				Dictionary(col_data_buf, page_hdr.dictionary_page_header.num_values);
				col_data_buf = nullptr; // make sure nobody below reads this
				break;
			default:
				D_ASSERT(0);
				break;
			}

			// TODO abort when running out of column
		}

		D_ASSERT(col_data_buf);

		auto read_now = MinValue<idx_t>(to_read, rows_available);
		if (dict_decoder) {
			offset_buffer.resize(sizeof(uint32_t) * read_now);
			dict_decoder->GetBatch<uint32_t>(offset_buffer.ptr, read_now);
			Offsets((uint32_t *)offset_buffer.ptr, read_now, result_offset, result);
		} else {
			Plain(col_data_buf, read_now, result_offset, result);
		}
		result_offset += read_now;
		rows_available -= read_now;
		to_read -= read_now;
	}
}
} // namespace duckdb
