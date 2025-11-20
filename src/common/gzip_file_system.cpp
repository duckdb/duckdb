#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include "miniz.hpp"
#include "miniz_wrapper.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

/*

  0      2 bytes  magic header  0x1f, 0x8b (\037 \213)
  2      1 byte   compression method
                     0: store (copied)
                     1: compress
                     2: pack
                     3: lzh
                     4..7: reserved
                     8: deflate
  3      1 byte   flags
                     bit 0 set: file probably ascii text
                     bit 1 set: continuation of multi-part gzip file, part number present
                     bit 2 set: extra field present
                     bit 3 set: original file name present
                     bit 4 set: file comment present
                     bit 5 set: file is encrypted, encryption header present
                     bit 6,7:   reserved
  4      4 bytes  file modification time in Unix format
  8      1 byte   extra flags (depend on compression method)
  9      1 byte   OS type
[
         2 bytes  optional part number (second part=1)
]?
[
         2 bytes  optional extra field length (e)
        (e)bytes  optional extra field
]?
[
           bytes  optional original file name, zero terminated
]?
[
           bytes  optional file comment, zero terminated
]?
[
        12 bytes  optional encryption header
]?
           bytes  compressed data
         4 bytes  crc32
         4 bytes  uncompressed input size modulo 2^32

 */

static idx_t GZipConsumeString(QueryContext context, FileHandle &input) {
	idx_t size = 1; // terminator
	char buffer[1];
	while (input.Read(context, buffer, 1) == 1) {
		if (buffer[0] == '\0') {
			break;
		}
		size++;
	}
	return size;
}

struct MiniZStreamWrapper : public StreamWrapper {
	~MiniZStreamWrapper() override;

	CompressedFile *file = nullptr;
	unique_ptr<duckdb_miniz::mz_stream> mz_stream_ptr;
	bool writing = false;
	duckdb_miniz::mz_ulong crc;
	idx_t total_size;

public:
	void Initialize(QueryContext context, CompressedFile &file, bool write) override;

	bool Read(StreamData &stream_data) override;
	void Write(CompressedFile &file, StreamData &stream_data, data_ptr_t buffer, int64_t nr_bytes) override;

	void Close() override;

	void FlushStream() const;
};

MiniZStreamWrapper::~MiniZStreamWrapper() {
	// avoid closing if destroyed during stack unwinding
	if (Exception::UncaughtException()) {
		return;
	}
	try {
		MiniZStreamWrapper::Close();
	} catch (std::exception &ex) {
		if (file && file->child_handle) {
			// FIXME: Make any log context available here.
			ErrorData data(ex);
			try {
				const auto logger = file->child_handle->logger;
				if (logger) {
					DUCKDB_LOG_ERROR(logger, "MiniZStreamWrapper::~MiniZStreamWrapper()\t\t" + data.Message())
				}
			} catch (...) { // NOLINT
			}
		}
	} catch (...) { // NOLINT
	}
}

void MiniZStreamWrapper::Initialize(QueryContext context, CompressedFile &file, bool write) {
	Close();
	this->file = &file;
	mz_stream_ptr = make_uniq<duckdb_miniz::mz_stream>();
	memset(mz_stream_ptr.get(), 0, sizeof(duckdb_miniz::mz_stream));
	this->writing = write;

	// TODO use custom alloc/free methods in miniz to throw exceptions on OOM
	uint8_t gzip_hdr[GZIP_HEADER_MINSIZE];
	if (write) {
		crc = MZ_CRC32_INIT;
		total_size = 0;

		MiniZStream::InitializeGZIPHeader(gzip_hdr);
		file.child_handle->Write(gzip_hdr, GZIP_HEADER_MINSIZE);

		auto ret = mz_deflateInit2(mz_stream_ptr.get(), duckdb_miniz::MZ_DEFAULT_LEVEL, MZ_DEFLATED,
		                           -MZ_DEFAULT_WINDOW_BITS, 1, 0);
		if (ret != duckdb_miniz::MZ_OK) {
			throw InternalException("Failed to initialize miniz");
		}
	} else {
		idx_t data_start = GZIP_HEADER_MINSIZE;
		auto read_count = file.child_handle->Read(context, gzip_hdr, GZIP_HEADER_MINSIZE);
		GZipFileSystem::VerifyGZIPHeader(gzip_hdr, NumericCast<idx_t>(read_count), &file);
		// Skip over the extra field if necessary
		if (gzip_hdr[3] & GZIP_FLAG_EXTRA) {
			uint8_t gzip_xlen[2];
			file.child_handle->Seek(data_start);
			file.child_handle->Read(context, gzip_xlen, 2);
			auto xlen = NumericCast<idx_t>((uint8_t)gzip_xlen[0] | (uint8_t)gzip_xlen[1] << 8);
			data_start += xlen + 2;
		}
		// Skip over the file name if necessary
		if (gzip_hdr[3] & GZIP_FLAG_NAME) {
			file.child_handle->Seek(data_start);
			data_start += GZipConsumeString(context, *file.child_handle);
		}
		file.child_handle->Seek(data_start);
		// stream is now set to beginning of payload data
		auto ret = duckdb_miniz::mz_inflateInit2(mz_stream_ptr.get(), -MZ_DEFAULT_WINDOW_BITS);
		if (ret != duckdb_miniz::MZ_OK) {
			throw InternalException("Failed to initialize miniz");
		}
	}
}

bool MiniZStreamWrapper::Read(StreamData &sd) {
	// Handling for the concatenated files
	if (sd.refresh) {
		auto available = static_cast<uint32_t>(sd.in_buff_end - sd.in_buff_start);
		if (available <= GZIP_FOOTER_SIZE) {
			// Only footer is available so we just close and return finished
			Close();
			return true;
		}

		sd.refresh = false;
		auto body_ptr = sd.in_buff_start + GZIP_FOOTER_SIZE;
		uint8_t gzip_hdr[GZIP_HEADER_MINSIZE];
		memcpy(gzip_hdr, body_ptr, GZIP_HEADER_MINSIZE);
		GZipFileSystem::VerifyGZIPHeader(gzip_hdr, GZIP_HEADER_MINSIZE, nullptr);
		body_ptr += GZIP_HEADER_MINSIZE;
		if (gzip_hdr[3] & GZIP_FLAG_EXTRA) {
			auto xlen = NumericCast<idx_t>((uint8_t)*body_ptr | (uint8_t) * (body_ptr + 1) << 8);
			body_ptr += xlen + 2;
			if (GZIP_FOOTER_SIZE + GZIP_HEADER_MINSIZE + 2 + xlen >= GZIP_HEADER_MAXSIZE) {
				throw InternalException("Extra field resulting in GZIP header larger than defined maximum (%d)",
				                        GZIP_HEADER_MAXSIZE);
			}
		}
		if (gzip_hdr[3] & GZIP_FLAG_NAME) {
			char c;
			do {
				c = UnsafeNumericCast<char>(*body_ptr);
				body_ptr++;
			} while (c != '\0' && body_ptr < sd.in_buff_end);
			if (static_cast<idx_t>(body_ptr - sd.in_buff_start) >= GZIP_HEADER_MAXSIZE) {
				throw InternalException("Filename resulting in GZIP header larger than defined maximum (%d)",
				                        GZIP_HEADER_MAXSIZE);
			}
		}
		sd.in_buff_start = body_ptr;
		if (sd.in_buff_end - sd.in_buff_start < 1) {
			Close();
			return true;
		}
		duckdb_miniz::mz_inflateEnd(mz_stream_ptr.get());
		auto sta = duckdb_miniz::mz_inflateInit2(mz_stream_ptr.get(), -MZ_DEFAULT_WINDOW_BITS);
		if (sta != duckdb_miniz::MZ_OK) {
			throw InternalException("Failed to initialize miniz");
		}
	}

	// actually decompress
	mz_stream_ptr->next_in = sd.in_buff_start;
	D_ASSERT(sd.in_buff_end - sd.in_buff_start < NumericLimits<int32_t>::Maximum());
	mz_stream_ptr->avail_in = static_cast<uint32_t>(sd.in_buff_end - sd.in_buff_start);
	mz_stream_ptr->next_out = data_ptr_cast(sd.out_buff_end);
	mz_stream_ptr->avail_out = static_cast<uint32_t>((sd.out_buff.get() + sd.out_buf_size) - sd.out_buff_end);
	auto ret = duckdb_miniz::mz_inflate(mz_stream_ptr.get(), duckdb_miniz::MZ_NO_FLUSH);
	if (ret != duckdb_miniz::MZ_OK && ret != duckdb_miniz::MZ_STREAM_END) {
		throw IOException("Failed to decode gzip stream: %s", duckdb_miniz::mz_error(ret));
	}
	// update pointers following inflate()
	sd.in_buff_start = (data_ptr_t)mz_stream_ptr->next_in; // NOLINT
	sd.in_buff_end = sd.in_buff_start + mz_stream_ptr->avail_in;
	sd.out_buff_end = data_ptr_cast(mz_stream_ptr->next_out);
	D_ASSERT(sd.out_buff_end + mz_stream_ptr->avail_out == sd.out_buff.get() + sd.out_buf_size);

	// if stream ended, deallocate inflator
	if (ret == duckdb_miniz::MZ_STREAM_END) {
		// Concatenated GZIP potentially coming up - refresh input buffer
		sd.refresh = true;
	}
	return false;
}

void MiniZStreamWrapper::Write(CompressedFile &file, StreamData &sd, data_ptr_t uncompressed_data,
                               int64_t uncompressed_size) {
	// update the src and the total size
	crc = duckdb_miniz::mz_crc32(crc, reinterpret_cast<const unsigned char *>(uncompressed_data),
	                             UnsafeNumericCast<size_t>(uncompressed_size));
	total_size += UnsafeNumericCast<idx_t>(uncompressed_size);

	auto remaining = uncompressed_size;
	while (remaining > 0) {
		auto output_remaining = UnsafeNumericCast<idx_t>((sd.out_buff.get() + sd.out_buf_size) - sd.out_buff_start);

		mz_stream_ptr->next_in = reinterpret_cast<const unsigned char *>(uncompressed_data);
		mz_stream_ptr->avail_in = NumericCast<unsigned int>(remaining);
		mz_stream_ptr->next_out = sd.out_buff_start;
		mz_stream_ptr->avail_out = NumericCast<unsigned int>(output_remaining);

		auto res = mz_deflate(mz_stream_ptr.get(), duckdb_miniz::MZ_NO_FLUSH);
		if (res != duckdb_miniz::MZ_OK) {
			D_ASSERT(res != duckdb_miniz::MZ_STREAM_END);
			throw InternalException("Failed to compress GZIP block");
		}
		sd.out_buff_start += output_remaining - mz_stream_ptr->avail_out;
		if (mz_stream_ptr->avail_out == 0) {
			// no more output buffer available: flush
			file.child_handle->Write(sd.out_buff.get(),
			                         UnsafeNumericCast<idx_t>(sd.out_buff_start - sd.out_buff.get()));
			sd.out_buff_start = sd.out_buff.get();
		}
		auto written = UnsafeNumericCast<idx_t>(remaining - mz_stream_ptr->avail_in);
		uncompressed_data += written;
		remaining = mz_stream_ptr->avail_in;
	}
}

void MiniZStreamWrapper::FlushStream() const {
	auto &sd = file->stream_data;
	mz_stream_ptr->next_in = nullptr;
	mz_stream_ptr->avail_in = 0;
	while (true) {
		auto output_remaining = (sd.out_buff.get() + sd.out_buf_size) - sd.out_buff_start;
		mz_stream_ptr->next_out = sd.out_buff_start;
		mz_stream_ptr->avail_out = NumericCast<unsigned int>(output_remaining);

		auto res = mz_deflate(mz_stream_ptr.get(), duckdb_miniz::MZ_FINISH);
		sd.out_buff_start += (output_remaining - mz_stream_ptr->avail_out);
		if (sd.out_buff_start > sd.out_buff.get()) {
			file->child_handle->Write(sd.out_buff.get(),
			                          UnsafeNumericCast<idx_t>(sd.out_buff_start - sd.out_buff.get()));
			sd.out_buff_start = sd.out_buff.get();
		}
		if (res == duckdb_miniz::MZ_STREAM_END) {
			break;
		}
		if (res != duckdb_miniz::MZ_OK) {
			throw InternalException("Failed to compress GZIP block");
		}
	}
}

void MiniZStreamWrapper::Close() {
	if (!mz_stream_ptr) {
		return;
	}
	if (writing) {
		// flush anything remaining in the stream
		FlushStream();

		// write the footer
		unsigned char gzip_footer[MiniZStream::GZIP_FOOTER_SIZE];
		MiniZStream::InitializeGZIPFooter(gzip_footer, crc, total_size);
		file->child_handle->Write(gzip_footer, MiniZStream::GZIP_FOOTER_SIZE);

		duckdb_miniz::mz_deflateEnd(mz_stream_ptr.get());
	} else {
		duckdb_miniz::mz_inflateEnd(mz_stream_ptr.get());
	}
	mz_stream_ptr = nullptr;
	file = nullptr;
}

class GZipFile : public CompressedFile {
public:
	GZipFile(QueryContext context, unique_ptr<FileHandle> child_handle_p, const string &path, bool write)
	    : CompressedFile(gzip_fs, std::move(child_handle_p), path) {
		Initialize(context, write);
	}
	FileCompressionType GetFileCompressionType() override {
		return FileCompressionType::GZIP;
	}
	GZipFileSystem gzip_fs;
};

void GZipFileSystem::VerifyGZIPHeader(uint8_t gzip_hdr[], idx_t read_count, optional_ptr<CompressedFile> source_file) {
	// include the filename in the error message if known
	string file_info = source_file ? ": " + source_file->path : "";

	// check for incorrectly formatted files
	if (read_count != GZIP_HEADER_MINSIZE) {
		throw IOException("Input is not a GZIP stream" + file_info);
	}
	if (gzip_hdr[0] != 0x1F || gzip_hdr[1] != 0x8B) { // magic header
		throw IOException("Input is not a GZIP stream" + file_info);
	}
	if (gzip_hdr[2] != GZIP_COMPRESSION_DEFLATE) { // compression method
		throw IOException("Unsupported GZIP compression method" + file_info);
	}
	if (gzip_hdr[3] & GZIP_FLAG_UNSUPPORTED) {
		throw IOException("Unsupported GZIP archive" + file_info);
	}
}

bool GZipFileSystem::CheckIsZip(const char *data, duckdb::idx_t size) {
	if (size < GZIP_HEADER_MINSIZE) {
		return false;
	}

	auto data_ptr = reinterpret_cast<const uint8_t *>(data);
	if (data_ptr[0] != 0x1F || data_ptr[1] != 0x8B) {
		return false;
	}

	if (data_ptr[2] != GZIP_COMPRESSION_DEFLATE) {
		return false;
	}

	return true;
}

string GZipFileSystem::UncompressGZIPString(const string &in) {
	return UncompressGZIPString(in.data(), in.size());
}

string GZipFileSystem::UncompressGZIPString(const char *data, idx_t size) {
	// decompress file
	auto body_ptr = data;

	auto mz_stream_ptr = make_uniq<duckdb_miniz::mz_stream>();
	memset(mz_stream_ptr.get(), 0, sizeof(duckdb_miniz::mz_stream));

	uint8_t gzip_hdr[GZIP_HEADER_MINSIZE];

	// check for incorrectly formatted files

	// TODO this is mostly the same as gzip_file_system.cpp
	if (size < GZIP_HEADER_MINSIZE) {
		throw IOException("Input is not a GZIP stream");
	}
	memcpy(gzip_hdr, body_ptr, GZIP_HEADER_MINSIZE);
	body_ptr += GZIP_HEADER_MINSIZE;
	GZipFileSystem::VerifyGZIPHeader(gzip_hdr, GZIP_HEADER_MINSIZE, nullptr);

	if (gzip_hdr[3] & GZIP_FLAG_EXTRA) {
		throw IOException("Extra field in a GZIP stream unsupported");
	}

	if (gzip_hdr[3] & GZIP_FLAG_NAME) {
		char c;
		do {
			c = *body_ptr;
			body_ptr++;
		} while (c != '\0' && static_cast<idx_t>(body_ptr - data) < size);
	}

	// stream is now set to beginning of payload data
	auto status = duckdb_miniz::mz_inflateInit2(mz_stream_ptr.get(), -MZ_DEFAULT_WINDOW_BITS);
	if (status != duckdb_miniz::MZ_OK) {
		throw InternalException("Failed to initialize miniz");
	}

	auto bytes_remaining = size - NumericCast<idx_t>(body_ptr - data);
	mz_stream_ptr->next_in = const_uchar_ptr_cast(body_ptr);
	mz_stream_ptr->avail_in = NumericCast<unsigned int>(bytes_remaining);

	string decompressed;

	while (status == duckdb_miniz::MZ_OK) {
		unsigned char decompress_buffer[BUFSIZ];
		mz_stream_ptr->next_out = decompress_buffer;
		mz_stream_ptr->avail_out = sizeof(decompress_buffer);
		status = mz_inflate(mz_stream_ptr.get(), duckdb_miniz::MZ_NO_FLUSH);
		if (status != duckdb_miniz::MZ_STREAM_END && status != duckdb_miniz::MZ_OK) {
			throw IOException("Failed to uncompress");
		}
		decompressed.append(char_ptr_cast(decompress_buffer), mz_stream_ptr->total_out - decompressed.size());
	}
	duckdb_miniz::mz_inflateEnd(mz_stream_ptr.get());

	if (decompressed.empty()) {
		throw IOException("Failed to uncompress");
	}
	return decompressed;
}

unique_ptr<FileHandle> GZipFileSystem::OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle,
                                                          bool write) {
	auto path = handle->path;
	return make_uniq<GZipFile>(context, std::move(handle), path, write);
}

unique_ptr<StreamWrapper> GZipFileSystem::CreateStream() {
	return make_uniq<MiniZStreamWrapper>();
}

idx_t GZipFileSystem::InBufferSize() {
	return BUFFER_SIZE;
}

idx_t GZipFileSystem::OutBufferSize() {
	return BUFFER_SIZE;
}

} // namespace duckdb
