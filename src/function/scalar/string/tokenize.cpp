#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "utf8proc.hpp"

namespace duckdb {

static bool whitespace_ascii(const char &c) {
	// TODO: implement
	return c == ' ';
}

static bool whitespace_utf8(const char &c) {
	// TODO: implement
	return c == ' ';
}

void tokenize_ascii(const char *input_data, size_t input_size, ChunkCollection &result) {
	auto append_chunk = make_unique<DataChunk>();
	vector<LogicalType> types = {LogicalType::VARCHAR};
	append_chunk->Initialize(types);

	for (idx_t offset = 0; offset < input_size; offset++) {
		while (offset < input_size && whitespace_ascii(input_data[offset])) {
			offset++;
		}
		if (offset == input_size)
			break;

		if (append_chunk->size() == STANDARD_VECTOR_SIZE) {
			result.count += append_chunk->size();
			result.chunks.push_back(move(append_chunk));
			append_chunk = make_unique<DataChunk>();
			append_chunk->Initialize(types);
		}

		idx_t token_end;
		for (token_end = offset + 1; token_end < input_size; token_end++) {
			if (whitespace_ascii(input_data[token_end])) {
				break;
			}
		}
		size_t length = token_end - offset;

		FlatVector::GetData<string_t>(append_chunk->data[0])[append_chunk->size()] =
		    StringVector::AddString(append_chunk->data[0], &input_data[offset], length);
		append_chunk->SetCardinality(append_chunk->size() + 1);
		offset = token_end;
	}
	if (append_chunk->size() > 0) {
		result.count += append_chunk->size();
		result.chunks.push_back(move(append_chunk));
	}
	result.Verify();
}

void tokenize_unicode(const char *input_data, size_t input_size, ChunkCollection &result) {
	auto append_chunk = make_unique<DataChunk>();
	vector<LogicalType> types = {LogicalType::VARCHAR};
	append_chunk->Initialize(types);

	for (idx_t offset = 0; offset + utf8proc_charwidth(input_data[offset]) < input_size;
	     offset = utf8proc_next_grapheme(input_data, input_size, offset)) {

		while (offset < input_size && whitespace_utf8(input_data[offset])) {
			offset++;
		}
		if (offset == input_size)
			break;

		if (append_chunk->size() == STANDARD_VECTOR_SIZE) {
			result.count += append_chunk->size();
			result.chunks.push_back(move(append_chunk));
			append_chunk = make_unique<DataChunk>();
			append_chunk->Initialize(types);
		}

		idx_t token_end;
		for (token_end = utf8proc_next_grapheme(input_data, input_size, offset);
		     token_end + utf8proc_charwidth(input_data[token_end]) < input_size;
		     token_end = utf8proc_next_grapheme(input_data, input_size, token_end)) {
			if (whitespace_utf8(input_data[token_end])) {
				break;
			}
		}
		size_t length = token_end - offset;

		FlatVector::GetData<string_t>(append_chunk->data[0])[append_chunk->size()] =
		    StringVector::AddString(append_chunk->data[0], &input_data[offset], length);
		append_chunk->SetCardinality(append_chunk->size() + 1);
		offset = token_end;
	}
	if (append_chunk->size() > 0) {
		result.count += append_chunk->size();
		result.chunks.push_back(move(append_chunk));
	}
	result.Verify();
}

unique_ptr<ChunkCollection> tokenize(string_t input) {
	const char *input_data = input.GetData();
	size_t input_size = input.GetSize();

	bool ascii_only = true;
	for (auto i = 0; i < (int)input_size; ++i) {
		if (input_data[i] & 0x80) {
			ascii_only = false;
			break;
		}
	}

	auto tokenized_string = make_unique<ChunkCollection>();
	vector<LogicalType> types = {LogicalType::VARCHAR};
	tokenized_string->types = types;

	if (ascii_only) {
		tokenize_ascii(input_data, input_size, *tokenized_string);
	} else {
		tokenize_unicode(input_data, input_size, *tokenized_string);
	}

	return tokenized_string;
}

static void tokenize_function(DataChunk &args, ExpressionState &state, Vector &result) {
	VectorData sdata;
	args.data[0].Orrify(args.size(), sdata);
	auto input = (string_t *)sdata.data;

	result.Initialize(LogicalType::LIST);
	auto list_struct_data = FlatVector::GetData<list_entry_t>(result);

	auto list_child = make_unique<ChunkCollection>();
    vector<LogicalType> types = {LogicalType::VARCHAR};
    list_child->types = types;

	size_t total_len = 0;
	for (idx_t i = 0; i < args.size(); i++) {
		if ((*sdata.nullmask)[sdata.sel->get_index(i)]) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		string_t input_string = input[sdata.sel->get_index(i)];
		auto tokenized_string = tokenize(input_string);
		list_struct_data[i].length = tokenized_string->count;
		list_struct_data[i].offset = total_len;
		total_len += tokenized_string->count;
		list_child->Append(*tokenized_string);
	}

	assert(list_child->count == total_len);
    result.vector_type = VectorType::CONSTANT_VECTOR;
	ListVector::SetEntry(result, move(list_child));
}

void TokenizeFun::RegisterFunction(BuiltinFunctions &set) {
    child_list_t<LogicalType> child_types;
    child_types.push_back(std::make_pair("token",LogicalType::VARCHAR));
    auto varchar_list_type = LogicalType(LogicalTypeId::LIST, child_types);
	set.AddFunction(
	    ScalarFunction("tokenize", {LogicalType::VARCHAR}, varchar_list_type, tokenize_function));
}

} // namespace duckdb
