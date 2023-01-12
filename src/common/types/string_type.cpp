#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/algorithm.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

void string_t::Verify() const {
	auto dataptr = GetDataUnsafe();
	(void)dataptr;
	D_ASSERT(dataptr);

#ifdef DEBUG
	auto utf_type = Utf8Proc::Analyze(dataptr, GetSize());
	D_ASSERT(utf_type != UnicodeType::INVALID);
#endif

	// verify that the prefix contains the first four characters of the string
	for (idx_t i = 0; i < MinValue<uint32_t>(PREFIX_LENGTH, GetSize()); i++) {
		D_ASSERT(GetPrefix()[i] == dataptr[i]);
	}
	// verify that for strings with length < INLINE_LENGTH, the rest of the string is zero
	for (idx_t i = GetSize(); i < INLINE_LENGTH; i++) {
		D_ASSERT(GetDataUnsafe()[i] == '\0');
	}
}

string_t string_t::operator<<(const idx_t &shift) const {

    string_t result(this->GetSize());
    for(idx_t i = 0; i < result.GetSize() * 8; i++){
        if(i < ((this->GetSize() * 8) - shift)) {
            idx_t bit = Bit::GetBit(*this, shift + i);
            Bit::SetBit(result, i, bit);
        } else {
            Bit::SetBit(result, i, 0);
        }
    }

    return result;
}

string_t string_t::operator&(const string_t &rhs) const {
    if (this->GetSize() != rhs.GetSize()){
        throw InvalidInputException("Cannot AND bit strings of different sizes");
    }

    string_t result(this->GetSize());
    char *buf = result.GetDataWriteable();
    const char *r_buf = rhs.GetDataUnsafe();
    const char *l_buf = this->GetDataUnsafe();

    for(idx_t i = 0; i < this->GetSize(); i++){
        buf[i] = l_buf[i] & r_buf[i];
    }

    return result;
}

string_t string_t::operator|(const string_t &rhs) const {
    if (this->GetSize() != rhs.GetSize()){
        throw InvalidInputException("Cannot OR bit strings of different sizes");
    }

    string_t result(this->GetSize());
    char *buf = result.GetDataWriteable();
    const char *r_buf = rhs.GetDataUnsafe();
    const char *l_buf = this->GetDataUnsafe();

    for(idx_t i = 0; i < this->GetSize(); i++){
        buf[i] = l_buf[i] | r_buf[i];
    }

    return result;
}

string_t string_t::operator^(const string_t &rhs) const {
    if (this->GetSize() != rhs.GetSize()){
        throw InvalidInputException("Cannot XOR bit strings of different sizes");
    }

    string_t result(this->GetSize());
    char *buf = result.GetDataWriteable();
    const char *r_buf = rhs.GetDataUnsafe();
    const char *l_buf = this->GetDataUnsafe();

    for(idx_t i = 0; i < this->GetSize(); i++){
        buf[i] = l_buf[i] ^ r_buf[i];
    }

    return result;
}

string_t string_t::operator~() const {
    string_t result(this->GetSize());
    char *result_buf = result.GetDataWriteable();
    const char *buf = this->GetDataUnsafe();

    for(idx_t i = 0; i < this->GetSize(); i++){
        //result_buf[i] = buf[i] ? '0' : '1';
        result_buf[i] = ~buf[i];
    }

    return result;
}

} // namespace duckdb
