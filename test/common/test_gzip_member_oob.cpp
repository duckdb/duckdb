#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "miniz_wrapper.hpp"

#include <cstring>
#include <string>

using namespace duckdb;

namespace {

// Builds one complete gzip member (header + deflate stream + footer) into an exactly sized buffer.
unsafe_unique_array<char> CompressToExactBuffer(const std::string &input, size_t &member_size) {
	auto scratch_size = MiniZStream::MaxCompressedLength(input.size());
	auto scratch = make_unsafe_uniq_array<char>(scratch_size);

	MiniZStream compressor;
	member_size = scratch_size;
	compressor.Compress(input.c_str(), input.size(), scratch.get(), &member_size);

	auto member = make_unsafe_uniq_array<char>(member_size);
	memcpy(member.get(), scratch.get(), member_size);
	return member;
}

// Copies the first `keep` bytes into an allocation of exactly that size, so a read past the end of
// the member lands outside a real allocation rather than in slack the allocator happened to give us.
unsafe_unique_array<char> TruncateToExactBuffer(const unsafe_unique_array<char> &member, size_t keep) {
	auto truncated = make_unsafe_uniq_array<char>(keep);
	memcpy(truncated.get(), member.get(), keep);
	return truncated;
}

} // namespace

TEST_CASE("gzip member missing its footer is rejected", "[miniz]") {
	const std::string payload(4096, 'q');

	size_t member_size;
	auto member = CompressToExactBuffer(payload, member_size);
	REQUIRE(member_size > MiniZStream::GZIP_HEADER_MINSIZE + MiniZStream::GZIP_FOOTER_SIZE);

	// The intact member round trips.
	{
		auto out = make_unsafe_uniq_array<char>(payload.size());
		MiniZStream decompressor;
		decompressor.Decompress(member.get(), member_size, out.get(), payload.size());
		REQUIRE(memcmp(out.get(), payload.c_str(), payload.size()) == 0);
	}

	// Dropping any part of the 8 byte footer used to underflow the remaining-size counter, so the
	// member loop ran once more and read a gzip header out of bounds. It has to be reported as a
	// truncated member instead. Checking the message matters: the out of bounds read usually landed
	// on bytes that failed the magic check, so "it throws" alone held before the fix as well.
	for (size_t missing = 1; missing <= MiniZStream::GZIP_FOOTER_SIZE; missing++) {
		auto keep = member_size - missing;
		auto truncated = TruncateToExactBuffer(member, keep);

		auto out = make_unsafe_uniq_array<char>(payload.size());
		MiniZStream decompressor;
		bool threw = false;
		try {
			decompressor.Decompress(truncated.get(), keep, out.get(), payload.size());
		} catch (const std::exception &ex) {
			threw = true;
			REQUIRE(std::string(ex.what()).find("incomplete gzip member footer") != std::string::npos);
		}
		REQUIRE(threw);
	}
}

TEST_CASE("concatenated gzip members still decompress", "[miniz]") {
	// The bounds check sits inside the member loop, so keep a multi member stream covered.
	const std::string first(1024, 'a');
	const std::string second(2048, 'b');

	size_t first_size;
	auto first_member = CompressToExactBuffer(first, first_size);
	size_t second_size;
	auto second_member = CompressToExactBuffer(second, second_size);

	auto combined_size = first_size + second_size;
	auto combined = make_unsafe_uniq_array<char>(combined_size);
	memcpy(combined.get(), first_member.get(), first_size);
	memcpy(combined.get() + first_size, second_member.get(), second_size);

	auto expected = first + second;
	auto out = make_unsafe_uniq_array<char>(expected.size());
	MiniZStream decompressor;
	decompressor.Decompress(combined.get(), combined_size, out.get(), expected.size());
	REQUIRE(memcmp(out.get(), expected.c_str(), expected.size()) == 0);
}
