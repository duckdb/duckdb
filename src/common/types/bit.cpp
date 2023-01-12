#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/bit.hpp"

namespace duckdb {

    idx_t Bit::GetStringSize(string_t bits) {
        return bits.GetSize() * 8;
    }

    void Bit::ToString(string_t bits, char *output) {
        auto data = (const_data_ptr_t)bits.GetDataUnsafe();
        auto len = bits.GetSize();

        for (idx_t i = 0; i < len; i++) {
            for (idx_t j = 0; j < 8; j++) {
                char c = 1;
                c = c << (7 - j);
                output[(i * 8) + j] = data[i] & c ? '1' : '0';
            }
        }
    }

    bool Bit::TryGetBitSize(string_t str, idx_t &str_len, string *error_message) {
        auto data = (const_data_ptr_t)str.GetDataUnsafe();
        auto len = str.GetSize();
        str_len = 0;
        for (idx_t i = 0; i < len; i++) {
            if (data[i] == '0' || data[i] == '1' ) {
                str_len++;
            } else {
                string error =
                        StringUtil::Format("Invalid character encountered in string -> bit conversion: '%s'",
                                           string((char *)data + i, 1));
                HandleCastError::AssignError(error, error_message);
                return false;
            }
        }
        str_len = str_len % 8 ? (str_len / 8) + 1 : str_len / 8;
        return true;
    }


    void Bit::ToBit(string_t str, data_ptr_t output) {
        auto data = (const_data_ptr_t)str.GetDataUnsafe();
        auto len = str.GetSize();

        char c = 0;
        idx_t first_byte = len % 8;
        for (idx_t i = 0; i < first_byte; i++){
            c = c << 1;
            if (data[i] == '1') {
                c = c | 1;
            }
        }
        if(first_byte != 0){
            *(output++) = c;
        }
        for (idx_t i = first_byte; i < len; i+=8) {
            c = 0;
            for (idx_t j = 0; j < 8; j++){
                c = c << 1;
                if (data[i + j] == '1') {
                    c = c | 1;
                }
            }
            *(output++) = c;
        }
    }

    idx_t Bit::GetBit(string_t bit_string, idx_t n) {
        const char *buf = bit_string.GetDataUnsafe();
        char byte = buf[n / 8] >> (7 - (n % 8));

        idx_t ret = byte & 1 ? 1 : 0;
        return ret;
    }

    void Bit::SetBit(string_t &bit_string, idx_t n, idx_t new_value) {
        char *buf = bit_string.GetDataWriteable();

        char shift_byte = 1 << (7 - (n % 8));

        if(new_value == 0){
            shift_byte = ~shift_byte;
            buf[n / 8] = buf[n / 8] & shift_byte;
        } else {
            buf[n / 8] = buf[n / 8] | shift_byte;
        }
    }

} // namespace duckdb
