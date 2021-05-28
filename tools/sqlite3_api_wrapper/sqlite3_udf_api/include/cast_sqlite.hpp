#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/constants.hpp"

#include "udf_struct_sqlite3.h"

using namespace duckdb;
using namespace std;

struct CastSQLite {
    template <class SRC>
    static inline sqlite3_value Operation(SRC input) {
        return (sqlite3_value)input;
    }

    // static void ToVectorInt(int32_t *__restrict data, sqlite3_value *__restrict result, idx_t count);
	template<class T>
	static void ToVectorSQLiteValue(T *__restrict data, sqlite3_value *__restrict result, idx_t count) {
		for(idx_t i=0; i < count; ++i) {
			result[i] = CastSQLite::Operation(data[i]);
		}
	}
    static void ToVector(LogicalType type, VectorData &vec_data, vector<sqlite3_value> &result);
};

template <>
sqlite3_value CastSQLite::Operation(int32_t input);

template <>
sqlite3_value CastSQLite::Operation(double input);
