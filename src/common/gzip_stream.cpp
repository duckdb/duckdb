#include "common/gzip_stream.hpp"
#include "common/exception.hpp"
#include "common/fstream_util.hpp"

#include <cstdio>

#define MINIZ_NO_ARCHIVE_APIS
#define MINIZ_NO_STDIO
#define MINIZ_NO_ZLIB_COMPATIBLE_NAMES

#include "miniz.h"

using namespace std;
using namespace duckdb;


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

static size_t consume_string(fstream &input) {
	size_t size = 1; // terminator
	while (input.get() != '\0') {
		size++;
	}
	return size;
}

static const uint8_t GZIP_COMPRESSION_DEFLATE = 0x08;

static const uint8_t GZIP_FLAG_ASCII = 0x1;
static const uint8_t GZIP_FLAG_MULTIPART = 0x2;
static const uint8_t GZIP_FLAG_EXTRA = 0x4;
static const uint8_t GZIP_FLAG_NAME = 0x8;
static const uint8_t GZIP_FLAG_COMMENT = 0x10;
static const uint8_t GZIP_FLAG_ENCRYPT = 0x20;

static const unsigned char GZIP_FLAG_UNSUPPORTED = GZIP_FLAG_ASCII | GZIP_FLAG_MULTIPART | GZIP_FLAG_EXTRA | GZIP_FLAG_COMMENT | GZIP_FLAG_ENCRYPT;

// read and check GZIP stream header
GzipStreamBuf::GzipStreamBuf(string filename) {
  	uint8_t gzip_hdr[10];
  	data_start = 10;

  	in_buff = new char [BUFSIZ];
	in_buff_start = in_buff;
	in_buff_end = in_buff;
	out_buff = new char [BUFSIZ];

	FstreamUtil::OpenFile(filename, input);

  	input.read((char*) gzip_hdr, 10);
  	if (!input) {
  		throw Exception("Input is not a GZIP stream");
  	}
  	if (gzip_hdr[0] != 0x1F || gzip_hdr[1] != 0x8B) { // magic header
  		throw Exception("Input is not a GZIP stream");
  	}
  	if (gzip_hdr[2] != GZIP_COMPRESSION_DEFLATE) { // compression method
  		throw Exception("Unsupported GZIP compression method");
  	}
	if (gzip_hdr[3] & GZIP_FLAG_UNSUPPORTED) {
  		throw Exception("Unsupported GZIP archive");
	}

	if (gzip_hdr[3] & GZIP_FLAG_NAME) {
		input.seekg(data_start, input.beg);
		data_start += consume_string(input);
	}
	input.seekg(data_start, input.beg);
	// stream is now set to beginning of payload data

	mz_stream_ptr = new mz_stream();
	// TODO use custom alloc/free methods in miniz to throw exceptions on OOM

    auto ret = mz_inflateInit2((mz_streamp) mz_stream_ptr, -MZ_DEFAULT_WINDOW_BITS);
    if (ret != MZ_OK) {
  		throw Exception("Failed to initialize miniz");
    }
}


streambuf::int_type GzipStreamBuf::underflow(){
	auto zstrm_p = (mz_streamp) mz_stream_ptr;
	if (!zstrm_p) return 0;
	 if (this->gptr() == this->egptr()) {
		// pointers for free region in output buffer
		char * out_buff_free_start = out_buff;
		do
		{
			assert(in_buff_start <= in_buff_end);
			assert(in_buff_end <= in_buff_start + BUFSIZ);

			// read more input if none available
			if (in_buff_start == in_buff_end) {
				// empty input buffer: refill from the start
				in_buff_start = in_buff;
				std::streamsize sz = input.rdbuf()->sgetn(in_buff, BUFSIZ);
				in_buff_end = in_buff + sz;
				if (in_buff_end == in_buff_start) break; // end of input
			}

			assert(zstrm_p);
			zstrm_p->next_in = reinterpret_cast< decltype(zstrm_p->next_in) >(in_buff_start);
			zstrm_p->avail_in = in_buff_end - in_buff_start;
			zstrm_p->next_out = reinterpret_cast< decltype(zstrm_p->next_out) >(out_buff_free_start);
			zstrm_p->avail_out = (out_buff + BUFSIZ) - out_buff_free_start;
			auto ret = mz_inflate(zstrm_p, MZ_NO_FLUSH);
			// process return code
			if (ret != MZ_OK && ret != MZ_STREAM_END) throw Exception(mz_error(ret));
			// update in&out pointers following inflate()
			in_buff_start = reinterpret_cast< decltype(in_buff_start) >((char*) zstrm_p->next_in);
			in_buff_end = in_buff_start + zstrm_p->avail_in;
			out_buff_free_start = reinterpret_cast< decltype(out_buff_free_start) >(zstrm_p->next_out);
			assert(out_buff_free_start + zstrm_p->avail_out == out_buff + BUFSIZ);
			// if stream ended, deallocate inflator
			if (ret == MZ_STREAM_END)
			{
				mz_stream_ptr = nullptr; // FIXME leak
			}

		} while (out_buff_free_start == out_buff);
		// 2 exit conditions:
		// - end of input: there might or might not be output available
		// - out_buff_free_start != out_buff: output available
		setg(out_buff, out_buff, out_buff_free_start);
	}
	return this->gptr() == this->egptr()
		? traits_type::eof()
		: traits_type::to_int_type(*this->gptr());


	return 0;
}

