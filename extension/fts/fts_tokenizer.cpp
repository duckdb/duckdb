#include "fts_tokenizer.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {

void FTSTokenizer::Tokenize() {
    // this method calls other methods
    // checks whether the columns to be tokenized are actually strings
    // makes sure data ends up in a table
}

void FTSTokenizer::Tokenize(const char *text, size_t text_size) {
    // this method should probably get a datachunk too
    // might need to count to make sure STANDARD_VECTOR_SIZE is obeyed

    // while ... create vectors
    Vector v = Vector(LogicalType::VARCHAR);

    for (idx_t i = 0; i < text_size; i++){
        // skip leading whitespace characters
        while (i < text_size && !IsToken(text[i])) {
            i++;
        }
        if (i == text_size) break;

        // identify size of term and add
        idx_t j = i + 1;
        while (j < text_size && IsToken(text[j])) {
            j++;
        }
        StringVector::AddString(v, &text[i], j - i);
        i = j + 1;
    }
    // put vectors in DataChunk
}

bool FTSTokenizer::IsToken(const char &c) {
    // TODO: implement
    return false;
}

} // namespace duckdb
