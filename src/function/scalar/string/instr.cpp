#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string.h>
#include <ctype.h>

using namespace std;

namespace duckdb {


static int64_t instr(string_t haystack, string_t needle);

struct InstrOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return instr(left.GetData(), right.GetData());
	}
};

static int64_t instr(string_t haystack, string_t needle) {
    int64_t string_position=0;
    unsigned char firstChar;

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

            // Comapre the first letter and with that compare Needle to the Haystack
            if((input_haystack[0]==firstChar)&&((memcmp(input_haystack, input_needle, size_needle)==0))){
                return string_position++;
            }
            string_position++;
            size_haystack--;
            input_haystack++; 
        }
        // Did not find the needle
        string_position=0;
    }
    return string_position;
}

void InstrFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("instr", // name of the funciton
                                    {SQLType::VARCHAR,
                                    SQLType::VARCHAR}, // argument list
                                    SQLType::INTEGER, // return type
                                    ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrOperator, true>));
}

} // namespace duckdb


/*static idx_t instr(string_t haystack, string_t needle) {
//static int64_t instr(string_t haystack, string_t needle, char *output) {
	
    idx_t string_position=0;
    unsigned char firstChar;

    auto input_haystack = haystack.GetData();
    auto input_needle = needle.GetData();

    auto size_haystack = haystack.GetSize();
    auto size_needle = needle.GetSize();

    // Needle needs something to proceed
    // Haystack should be bigger than the needle
    if((size_needle>0) && (size_haystack>=size_needle)){
        firstChar=input_needle[0];
        
        // Search for the first char in the haystack
        while((input_haystack[0]!=firstChar) && (size_haystack>0)){
                string_position++;
                size_haystack--;
                input_haystack++;  

                // If we find a false positive, continue forward another position
                if((input_haystack[0]==firstChar)&&(memcmp(input_haystack, input_needle, size_needle)!=0)){
                    string_position++;
                    size_haystack--;
                    input_haystack++;  
                }
                // Check if we moved too far
                if(size_haystack<=0){
                    break;
                } 
        }

        // Compare the haystack to needle
        if(memcmp(input_haystack, input_needle, size_needle)==0){
            string_position++;    
        }else
        {
            string_position=0;
        }
    }
    // return que esta errado pq retorna somente UM valor e não uma lista de valores
    return string_position;
}

static void instr_chunk_function(DataChunk &args, ExpressionState &state, Vector &result) {
	
	assert(args.data[0].type == TypeId::VARCHAR);
	assert(args.data[1].type == TypeId::VARCHAR);

    //unique_ptr<char[]> output;

    // problema esta no retorno da inner class. Ver como simplificar isso como length.cpp
	BinaryExecutor::Execute<string_t, string_t, string_t, true>(args.data[0],args.data[1], result, [&](string_t haystack, string_t needle) {
        //auto input_length = haystack.GetSize();
		//auto target = result.EmptyString(input_length);
        return instr(haystack, needle);
		//return instr(haystack, needle, target.GetData());
	});
}*/