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

vector<unique_ptr<DataChunk>> tokenize_ascii(const char *input_data, size_t input_size) {
    vector<unique_ptr<DataChunk>> result;

    auto append_chunk = make_unique<DataChunk>();
    vector<LogicalType> types = {LogicalType::VARCHAR};
    append_chunk->Initialize(types);

    for (idx_t offset = 0; offset < input_size; offset++) {

        if (append_chunk->size() == STANDARD_VECTOR_SIZE) {
            result.push_back(move(append_chunk));
            auto append_chunk = make_unique<DataChunk>();
            append_chunk->Initialize(types);
        }

        // skip leading whitespace
        while (offset < input_size && whitespace_ascii(input_data[offset])) {
            offset++;
        }

        // identify length of next token
        idx_t token_end;
        for (token_end = offset + 1; token_end < input_size; token_end++) {
            if (whitespace_ascii(input_data[token_end])) {
                break;
            }
        }
        size_t length = token_end - offset;

        StringVector::AddString(append_chunk->data[0], &input_data[offset], length);
        append_chunk->SetCardinality(append_chunk->size() + 1);

		offset = token_end;
    }
    return result;
}

vector<unique_ptr<DataChunk>> tokenize_unicode(const char *input_data, size_t input_size) {
    vector<unique_ptr<DataChunk>> result;

    auto append_chunk = make_unique<DataChunk>();
    vector<LogicalType> types = {LogicalType::VARCHAR};
    append_chunk->Initialize(types);

    // TODO: unsure whether offset + utf8proc_charwidth(input_data[offset]) is valid
	for (idx_t offset = 0;
         offset + utf8proc_charwidth(input_data[offset]) < input_size;
	     offset = utf8proc_next_grapheme(input_data, input_size, offset)) {
        
        if (append_chunk->size() == STANDARD_VECTOR_SIZE) {
            result.push_back(move(append_chunk));
            auto append_chunk = make_unique<DataChunk>();
            append_chunk->Initialize(types);
        }

		// skip leading whitespace
		while (offset + utf8proc_charwidth(input_data[offset]) < input_size && whitespace_utf8(input_data[offset])) {
			offset = utf8proc_next_grapheme(input_data, input_size, offset);
		}

        // identify length of next token
		idx_t token_end;
		for (token_end = utf8proc_next_grapheme(input_data, input_size, offset);
		     token_end + utf8proc_charwidth(input_data[token_end]) < input_size;
		     token_end = utf8proc_next_grapheme(input_data, input_size, token_end)) {
			if (whitespace_utf8(input_data[token_end])) {
                break;
			}
		}
        size_t length = token_end - offset;

        StringVector::AddString(append_chunk->data[0], &input_data[offset], length);
        append_chunk->SetCardinality(append_chunk->size() + 1);

		offset = token_end;
	}
    return result;
}

idx_t tokenize(Vector &result, string_t input) {
	const char *input_data = input.GetData();
	size_t input_size = input.GetSize();

	// check if there is any non-ascii
	bool ascii_only = true;
	for (auto i = 0; i < (int)input_size; ++i) {
		if (input_data[i] & 0x80) {
			ascii_only = false;
			break;
		}
	}

    vector<unique_ptr<DataChunk>> token_data;
	if (ascii_only) {
		token_data = tokenize_ascii(input_data, input_size);
	} else {
        token_data = tokenize_unicode(input_data, input_size);
    }

    // hand-roll a chunk collection to avoid copying strings
	auto output = make_unique<ChunkCollection>();
    vector<LogicalType> types = {LogicalType::VARCHAR};
    output->types = types;
    for (auto &td : token_data) {
        output->count += td->size();
        output->chunks.push_back(move(td));
    }

    idx_t count = output->count;
    ListVector::SetEntry(result, move(output));

    return count;
}

static void tokenize_function(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, idx_t, true>(args.data[0], result, args.size(), [&](string_t input) {
        return tokenize(result, input);
    });
}

void TokenizeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"tokenize"}, ScalarFunction({LogicalType::VARCHAR}, LogicalType(LogicalType::LIST), tokenize_function));
}

} // namespace duckdb
