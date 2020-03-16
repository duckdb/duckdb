#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string.h>
#include <ctype.h>

using namespace std;

namespace duckdb {

static int32_t instr(string_t haystack, string_t needle);

struct InstrOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return instr(left, right);
	}
};

static int32_t instr(string_t haystack, string_t needle) {
    int32_t string_position=0;
    unsigned char firstChar;

    // Getting information about the needle and the haystack
    auto input_haystack = haystack.GetData();
    auto input_needle = needle.GetData();
    auto size_haystack = haystack.GetSize();
    auto size_needle = needle.GetSize();

    // Needle needs something to proceed
    // Haystack should be bigger than the needle
    if((size_needle>0) && (size_haystack>=size_needle)){
        firstChar=input_needle[0];
        
        // find the positions with the first letter
        while(size_haystack>0){
            char b = input_haystack[0];

            // Compare the first letter and with that compare Needle to the Haystack
            if((b==firstChar)&&((memcmp(input_haystack, input_needle, size_needle)==0))){
                string_position += (b & 0xC0) != 0x80;
                return string_position;
            }
            string_position += (b & 0xC0) != 0x80;
            size_haystack--;
            input_haystack++; 
        }

        // Did not find the needle
        string_position=0;
    }
    return string_position;
}

void InstrFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("instr", // name of the function
                                    {SQLType::VARCHAR,
                                    SQLType::VARCHAR}, // argument list
                                    SQLType::INTEGER, // return type
                                    ScalarFunction::BinaryFunction<string_t, string_t, int32_t, InstrOperator, true>));
}

} // namespace duckdb