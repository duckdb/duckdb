
#include "execution/vector/vector_operations.hpp"
#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/types/operators.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Templated Looping Functions
//===--------------------------------------------------------------------===//
template <class T, class RES, class OP>
void _templated_unary_fold(Vector &left, Vector &result) {
	T *ldata = (T *)left.data;
	RES *result_data = (RES *)result.data;
	result_data[0] = 0;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			result_data[0] =
			    OP::Operation(result_data[0], ldata[left.sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			result_data[0] = OP::Operation(result_data[0], ldata[i]);
		}
	}
	result.count = 1;
}

template <class T, class RES, class OP>
void _templated_unary_loop(Vector &left, Vector &result) {
	T *ldata = (T *)left.data;
	RES *result_data = (RES *)result.data;
	if (left.sel_vector) {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = OP::Operation(ldata[left.sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < left.count; i++) {
			result_data[i] = OP::Operation(ldata[i]);
		}
	}
	result.count = left.count;
}

template <class T, class RES, class OP>
void _templated_binary_loop(Vector &left, Vector &right, Vector &result) {
	T *ldata = (T *)left.data;
	T *rdata = (T *)right.data;
	RES *result_data = (RES *)result.data;
	if (left.count == right.count) {
		if (left.sel_vector && right.sel_vector) {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] = OP::Operation(ldata[left.sel_vector[i]],
				                               rdata[right.sel_vector[i]]);
			}
		} else if (left.sel_vector) {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] =
				    OP::Operation(ldata[left.sel_vector[i]], rdata[i]);
			}
		} else if (right.sel_vector) {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] =
				    OP::Operation(ldata[i], rdata[right.sel_vector[i]]);
			}
		} else {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] = OP::Operation(ldata[i], rdata[i]);
			}
		}
		result.count = left.count;
	} else if (left.count == 1) {
		if (right.sel_vector) {
			for (size_t i = 0; i < right.count; i++) {
				result_data[i] =
				    OP::Operation(ldata[0], rdata[right.sel_vector[i]]);
			}
		} else {
			for (size_t i = 0; i < right.count; i++) {
				result_data[i] = OP::Operation(ldata[0], rdata[i]);
			}
		}
		result.count = right.count;
	} else if (right.count == 1) {
		if (left.sel_vector) {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] =
				    OP::Operation(ldata[left.sel_vector[i]], rdata[0]);
			}
		} else {
			for (size_t i = 0; i < left.count; i++) {
				result_data[i] = OP::Operation(ldata[i], rdata[0]);
			}
		}
		result.count = left.count;
	} else {
		throw Exception("Vector lengths don't match");
	}
}

//===--------------------------------------------------------------------===//
// Type Switches
//===--------------------------------------------------------------------===//
template <class OP> void _generic_unary_loop(Vector &left, Vector &result) {
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_loop<int8_t, int8_t, OP>(left, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_loop<int16_t, int16_t, OP>(left, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_loop<int32_t, int32_t, OP>(left, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_loop<int64_t, int64_t, OP>(left, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_loop<double, double, OP>(left, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class OP, class RES>
void _fixed_return_unary_loop(Vector &left, Vector &result) {
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_loop<int8_t, RES, OP>(left, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_loop<int16_t, RES, OP>(left, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_loop<int32_t, RES, OP>(left, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_loop<int64_t, RES, OP>(left, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_loop<double, RES, OP>(left, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class OP>
void _generic_unary_fold_loop(Vector &left, Vector &result) {
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_fold<int8_t, int8_t, OP>(left, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_fold<int16_t, int16_t, OP>(left, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_fold<int32_t, int32_t, OP>(left, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_fold<int64_t, int64_t, OP>(left, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_fold<double, double, OP>(left, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}
template <class OP, class RES>
void _fixed_return_unary_fold_loop(Vector &left, Vector &result) {
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_unary_fold<int8_t, RES, OP>(left, result);
		break;
	case TypeId::SMALLINT:
		_templated_unary_fold<int16_t, RES, OP>(left, result);
		break;
	case TypeId::INTEGER:
		_templated_unary_fold<int32_t, RES, OP>(left, result);
		break;
	case TypeId::BIGINT:
		_templated_unary_fold<int64_t, RES, OP>(left, result);
		break;
	case TypeId::DECIMAL:
		_templated_unary_fold<double, RES, OP>(left, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class OP>
void _generic_binary_loop(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw NotImplementedException("Type cast not implemented here!");
	}
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_binary_loop<int8_t, int8_t, OP>(left, right, result);
		break;
	case TypeId::SMALLINT:
		_templated_binary_loop<int16_t, int16_t, OP>(left, right, result);
		break;
	case TypeId::INTEGER:
		_templated_binary_loop<int32_t, int32_t, OP>(left, right, result);
		break;
	case TypeId::BIGINT:
		_templated_binary_loop<int64_t, int64_t, OP>(left, right, result);
		break;
	case TypeId::DECIMAL:
		_templated_binary_loop<double, double, OP>(left, right, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

template <class OP, class RES>
void _fixed_return_binary_loop(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw NotImplementedException("Type cast not implemented here!");
	}
	switch (left.type) {
	case TypeId::TINYINT:
		_templated_binary_loop<int8_t, RES, OP>(left, right, result);
		break;
	case TypeId::SMALLINT:
		_templated_binary_loop<int16_t, RES, OP>(left, right, result);
		break;
	case TypeId::INTEGER:
		_templated_binary_loop<int32_t, RES, OP>(left, right, result);
		break;
	case TypeId::BIGINT:
		_templated_binary_loop<int64_t, RES, OP>(left, right, result);
		break;
	case TypeId::DECIMAL:
		_templated_binary_loop<double, RES, OP>(left, right, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//
void VectorOperations::Add(Vector &left, Vector &right, Vector &result) {
	_generic_binary_loop<operators::Addition>(left, right, result);
}

void VectorOperations::Subtract(Vector &left, Vector &right, Vector &result) {
	_generic_binary_loop<operators::Subtraction>(left, right, result);
}

void VectorOperations::Multiply(Vector &left, Vector &right, Vector &result) {
	_generic_binary_loop<operators::Multiplication>(left, right, result);
}

void VectorOperations::Divide(Vector &left, Vector &right, Vector &result) {
	_generic_binary_loop<operators::Division>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
void VectorOperations::Equals(Vector &left, Vector &right, Vector &result) {
	_fixed_return_binary_loop<operators::Equals, bool>(left, right, result);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result) {
	_fixed_return_binary_loop<operators::NotEquals, bool>(left, right, result);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right,
                                   Vector &result) {
	_fixed_return_binary_loop<operators::GreaterThan, bool>(left, right,
	                                                        result);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right,
                                         Vector &result) {
	_fixed_return_binary_loop<operators::GreaterThanEquals, bool>(left, right,
	                                                              result);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result) {
	_fixed_return_binary_loop<operators::LessThan, bool>(left, right, result);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right,
                                      Vector &result) {
	_fixed_return_binary_loop<operators::LessThanEquals, bool>(left, right,
	                                                           result);
}

void VectorOperations::And(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw NotImplementedException("FIXME cast");
	}

	if (left.count == right.count) {
		_templated_binary_loop<bool, bool, operators::And>(left, right, result);
	} else {
		throw Exception("Vector lengths don't match");
	}
}

void VectorOperations::Or(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw NotImplementedException("FIXME cast");
	}

	if (left.count == right.count) {
		_templated_binary_loop<bool, bool, operators::Or>(left, right, result);
	} else {
		throw Exception("Vector lengths don't match");
	}
}

void VectorOperations::Sum(Vector &left, Vector &result) {
	_generic_unary_fold_loop<operators::Addition>(left, result);
}

void VectorOperations::Count(Vector &left, Vector &result) {
	Vector count_vector(Value((int32_t)left.count));
	VectorOperations::Add(result, count_vector, result);
}

void VectorOperations::Average(Vector &left, Vector &result) {
	Vector count_vector(Value((int32_t)left.count));
	VectorOperations::Sum(left, result);
	VectorOperations::Divide(result, count_vector, result);
}

void VectorOperations::Max(Vector &left, Vector &result) {
	_generic_unary_fold_loop<operators::Max>(left, result);
}

void VectorOperations::Min(Vector &left, Vector &result) {
	_generic_unary_fold_loop<operators::Min>(left, result);
}



//===--------------------------------------------------------------------===//
// Hash functions
//===--------------------------------------------------------------------===//

namespace operators {
struct Hash {
	template <class T> static inline int32_t Operation(T left) {
		return duckdb::Hash(left);
	}
};
struct CombineHash {
	static inline int32_t Operation(int32_t left, int32_t right) {
		return left ^ right;
	}
};
}

void VectorOperations::Hash(Vector &left, Vector &result) {
	_fixed_return_unary_loop<operators::Hash, int32_t>(left, result);
}

void VectorOperations::CombineHash(Vector &left, Vector &right,
                                   Vector &result) {
	if (left.type != TypeId::INTEGER) {
		throw NotImplementedException("Left argument must be 32-bit integer hash");
	}
	VectorOperations::Hash(right, result);
	_templated_binary_loop<int32_t, int32_t, operators::CombineHash>(left, result, result);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
template <class T> void _copy_loop(Vector &source, void *target) {
	T *sdata = (T *)source.data;
	T *result = (T *)target;
	if (source.sel_vector) {
		for (size_t i = 0; i < source.count; i++) {
			result[i] = sdata[source.sel_vector[i]];
		}
	} else {
		for (size_t i = 0; i < source.count; i++) {
			result[i] = sdata[i];
		}
	}
}

void VectorOperations::Copy(Vector &source, void *target) {
	if (source.count == 0)
		return;
	switch (source.type) {
	case TypeId::TINYINT:
		_copy_loop<int8_t>(source, target);
		break;
	case TypeId::SMALLINT:
		_copy_loop<int16_t>(source, target);
		break;
	case TypeId::INTEGER:
		_copy_loop<int32_t>(source, target);
		break;
	case TypeId::BIGINT:
		_copy_loop<int64_t>(source, target);
		break;
	case TypeId::DECIMAL:
		_copy_loop<double>(source, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for copy");
	}
}
