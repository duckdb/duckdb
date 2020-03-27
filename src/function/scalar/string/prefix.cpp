#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "duckdb/common/exception.hpp"
#include <omp.h>

using namespace std;

namespace duckdb {

static bool prefix(const string_t &str, const string_t &prefix);

static bool prefix2(const string_t &str, const string_t &pattern);

static bool prefix3(const string_t &str, const string_t &pattern);

static bool prefix4(const string_t &str, const string_t &pattern);

static bool prefix5(const string_t &str, const string_t &prefix);

static bool prefix6(const string_t &str, const string_t &pattern);

static bool prefix7(const string_t &str, const string_t &pattern);

struct PrefixOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return prefix(left, right);
	}
};

struct Prefix2Operator {
    template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
        return prefix2(left, right);
    }
};

struct Prefix3Operator {
    template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
        return prefix3(left, right);
    }
};

struct Prefix4Operator {
    template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
        return prefix4(left, right);
    }
};

struct Prefix5Operator {
    template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
        return prefix5(left, right);
    }
};

struct Prefix6Operator {
    template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
        return prefix6(left, right);
    }
};

struct Prefix7Operator {
    template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
        return prefix7(left, right);
    }
};

static bool prefix(const string_t &str, const string_t &prefix) {
    uint32_t prefix_len = prefix.GetSize();
    if(prefix_len == 0)
        return true;

    uint32_t str_len = str.GetSize();
    if(prefix_len > str_len)
        return false;

    bool equal;
    uint32_t num_char_equals = 0;
    const char *str_data = str.GetData();
    const char *prefix_data = prefix.GetData();

    for(idx_t i = 0; i < prefix_len; ++i) {
        equal = (str_data[i] == prefix_data[i]); // removed branch
        num_char_equals += equal;
    }

    return (num_char_equals == prefix_len);
}

static bool prefix2(const string_t &str, const string_t &pattern) {
    uint32_t pattern_len = pattern.GetSize();
    if(pattern_len == 0)
        return true;

    uint32_t str_len = str.GetSize();
    if(pattern_len > str_len)
        return false;

    // variables to remove branches through boolean calculation
    bool equal;
    uint32_t num_char_equals = 0;

    // prefix early out
    const char *str_pref  = str.GetPrefix();
    const char *patt_pref = pattern.GetPrefix();
    bool patt_has_prefix_lesser = (pattern_len < string_t::PREFIX_LENGTH);
    uint32_t prefix_size = patt_has_prefix_lesser ? pattern_len : string_t::PREFIX_LENGTH;
    for(idx_t i=0; i < prefix_size; ++i) {
        equal = (str_pref[i] == patt_pref[i]); // removed branch
        num_char_equals += equal;
    }
    if(patt_has_prefix_lesser) // prefix lesser than string_t::PREFIX_LENGTH
        return (num_char_equals == pattern_len);
    if(num_char_equals < prefix_size) // early out
        return false;

    // check the remaining chars after prefix early out
    const char *str_data = str.GetData();
    const char *patt_data = pattern.GetData();
    for(idx_t i = string_t::PREFIX_LENGTH; i < pattern_len; ++i) {
        equal = (str_data[i] == patt_data[i]); // removed branch
        num_char_equals += equal;
    }
    return (num_char_equals == pattern_len);
}

static bool prefix3(const string_t &str, const string_t &pattern) {
    uint32_t pattern_len = pattern.GetSize();
    if(pattern_len == 0)
        return true;

    uint32_t str_len = str.GetSize();
    if(pattern_len > str_len)
        return false;

    // prefix early out
    const char *str_pref  = str.GetPrefix();
    const char *patt_pref = pattern.GetPrefix();
    bool patt_has_prefix_lesser = (pattern_len < string_t::PREFIX_LENGTH);
    uint32_t prefix_size = patt_has_prefix_lesser ? pattern_len : string_t::PREFIX_LENGTH;
    for(idx_t i=0; i < prefix_size; ++i) {
        if(str_pref[i] != patt_pref[i])
            return false;
    }
    if(patt_has_prefix_lesser) // prefix lesser than string_t::PREFIX_LENGTH
        return true;

    // check the remaining chars after prefix early out
    bool equal;
    uint32_t num_char_equals = string_t::PREFIX_LENGTH;
    const char *str_data = str.GetData();
    const char *patt_data = pattern.GetData();
    for(idx_t i = string_t::PREFIX_LENGTH; i < pattern_len; ++i) {
        equal = (str_data[i] == patt_data[i]); // removed branch
        num_char_equals += equal;
    }
    return (num_char_equals == pattern_len);
}

static bool prefix4(const string_t &str, const string_t &pattern) {
    uint32_t pattern_len = pattern.GetSize();
    if(pattern_len == 0)
        return true;

    uint32_t str_len = str.GetSize();
    if(pattern_len > str_len)
        return false;

    // prefix early out
    const char *str_pref  = str.GetPrefix();
    const char *patt_pref = pattern.GetPrefix();
    bool patt_has_prefix_lesser = (pattern_len < string_t::PREFIX_LENGTH);
    uint32_t prefix_size = patt_has_prefix_lesser ? pattern_len : string_t::PREFIX_LENGTH;
    for(idx_t i=0; i < prefix_size; ++i) {
        if( str_pref[i] != patt_pref[i] )
            return false;
    }
    if(patt_has_prefix_lesser) // prefix lesser than string_t::PREFIX_LENGTH
        return true;

    // check the remaining chars after prefix early out
    const char *str_data = str.GetData();
    const char *patt_data = pattern.GetData();
    for(idx_t i = string_t::PREFIX_LENGTH; i < pattern_len; ++i) {
        if( str_data[i] != patt_data[i] )
            return false;
    }
    return true;
}

bool prefix5(const string_t &str, const string_t &prefix) {
    auto str_size = str.GetSize();
    auto prefix_length = prefix.GetSize();
    if (prefix_length > str_size) {
        return false;
    }
    if (prefix_length <= string_t::PREFIX_LENGTH) {
        // short prefix
        if (prefix_length == 0) {
            // length = 0, return true
            return true;
        }
        // otherwise we know entire result by comparing the prefix length
        return memcmp(str.GetPrefix(), prefix.GetPrefix(), prefix_length) == 0;
    } else {
        if (memcmp(str.GetPrefix(), prefix.GetPrefix(), string_t::PREFIX_LENGTH) != 0) {
            // early out
            return false;
        }
        // compare the rest of the prefix
        return memcmp(str.GetData() + string_t::PREFIX_LENGTH, prefix.GetData() + string_t::PREFIX_LENGTH, prefix_length - string_t::PREFIX_LENGTH) == 0;
    }
}

static bool prefix6(const string_t &str, const string_t &pattern) {
    auto str_size = str.GetSize();
    auto patt_length = pattern.GetSize();
    if (patt_length > str_size) {
        return false;
    }
    if (patt_length <= string_t::PREFIX_LENGTH) {
        // short prefix
        if (patt_length == 0) {
            // length = 0, return true
            return true;
        }

        // prefix early out
        const char *str_pref  = str.GetPrefix();
        const char *patt_pref = pattern.GetPrefix();
        for(idx_t i=0; i < patt_length; ++i) {
            if(str_pref[i] != patt_pref[i])
                return false;
        }
        return true;
    } else {
        const char *str_pref  = str.GetPrefix();
        const char *patt_pref = pattern.GetPrefix();
        for(idx_t i=0; i < string_t::PREFIX_LENGTH; ++i) {
            if(str_pref[i] != patt_pref[i]) {
                // early out
                return false;
            }
        }

        // check the remaining chars after prefix early out
        const char *str_data = str.GetData();
        const char *patt_data = pattern.GetData();
        for(idx_t i = string_t::PREFIX_LENGTH; i < patt_length; ++i) {
            if(str_data[i] != patt_data[i])
                return false;
        }
        return true;
    }
}

static bool prefix7(const string_t &str, const string_t &pattern) {
    auto str_size = str.GetSize();
    auto patt_length = pattern.GetSize();
    if (patt_length > str_size) {
        return false;
    }
    if (patt_length <= string_t::PREFIX_LENGTH) {
        // short prefix
        if (patt_length == 0) {
            // length = 0, return true
            return true;
        }

        // prefix early out
        const char *str_pref  = str.GetPrefix();
        const char *patt_pref = pattern.GetPrefix();
        for(idx_t i=0; i < patt_length; ++i) {
            if(str_pref[i] != patt_pref[i])
                return false;
        }
        return true;
    } else {
        // prefix early out
        const char *str_pref  = str.GetPrefix();
        const char *patt_pref = pattern.GetPrefix();
        for(idx_t i=0; i < string_t::PREFIX_LENGTH; ++i) {
            if(str_pref[i] != patt_pref[i]) {
                // early out
                return false;
            }
        }
        // compare the rest of the prefix
        return memcmp(str.GetData() + string_t::PREFIX_LENGTH, pattern.GetData() + string_t::PREFIX_LENGTH, patt_length - string_t::PREFIX_LENGTH) == 0;
    }
}

void PrefixFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("prefix",                             // name of the function
	                               {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
	                               SQLType::BOOLEAN,                     // return type
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, PrefixOperator, true>));

    set.AddFunction(ScalarFunction("prefix2",                            // name of the function
                                   {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
                                   SQLType::BOOLEAN,                     // return type
                                   ScalarFunction::BinaryFunction<string_t, string_t, bool, Prefix2Operator, true>));

    set.AddFunction(ScalarFunction("prefix3",                            // name of the function
                                   {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
                                   SQLType::BOOLEAN,                     // return type
                                   ScalarFunction::BinaryFunction<string_t, string_t, bool, Prefix3Operator, true>));

    set.AddFunction(ScalarFunction("prefix4",                            // name of the function
                                   {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
                                   SQLType::BOOLEAN,                     // return type
                                   ScalarFunction::BinaryFunction<string_t, string_t, bool, Prefix4Operator, true>));

    set.AddFunction(ScalarFunction("prefix5",                            // name of the function
                                   {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
                                   SQLType::BOOLEAN,                     // return type
                                   ScalarFunction::BinaryFunction<string_t, string_t, bool, Prefix5Operator, true>));

    set.AddFunction(ScalarFunction("prefix6",                            // name of the function
                                   {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
                                   SQLType::BOOLEAN,                     // return type
                                   ScalarFunction::BinaryFunction<string_t, string_t, bool, Prefix6Operator, true>));

    set.AddFunction(ScalarFunction("prefix7",                            // name of the function
                                   {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
                                   SQLType::BOOLEAN,                     // return type
                                   ScalarFunction::BinaryFunction<string_t, string_t, bool, Prefix7Operator, true>));
}

} // namespace duckdb
