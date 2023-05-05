//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/limits.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

// Undef annoying windows macro
#undef max

#include <limits>

namespace duckdb {

template <class T>
struct NumericLimits {
	static constexpr T Minimum() {
		return std::numeric_limits<T>::lowest();
	};
	static constexpr T Maximum() {
		return std::numeric_limits<T>::max();
	};
	DUCKDB_API static bool IsSigned();
	DUCKDB_API static idx_t Digits();
};

template <>
struct NumericLimits<int8_t> {
	static constexpr int8_t Minimum() {
		return std::numeric_limits<int8_t>::lowest();
	};
	static constexpr int8_t Maximum() {
		return std::numeric_limits<int8_t>::max();
	};
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 3;
	}
};
template <>
struct NumericLimits<int16_t> {
	static constexpr int16_t Minimum() {
		return std::numeric_limits<int16_t>::lowest();
	};
	static constexpr int16_t Maximum() {
		return std::numeric_limits<int16_t>::max();
	};
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 5;
	}
};
template <>
struct NumericLimits<int32_t> {
	static constexpr int32_t Minimum() {
		return std::numeric_limits<int32_t>::lowest();
	};
	static constexpr int32_t Maximum() {
		return std::numeric_limits<int32_t>::max();
	};
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 10;
	}
};

template <>
struct NumericLimits<int64_t> {
	static constexpr int64_t Minimum() {
		return std::numeric_limits<int64_t>::lowest();
	};
	static constexpr int64_t Maximum() {
		return std::numeric_limits<int64_t>::max();
	};
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 19;
	}
};
template <>
struct NumericLimits<hugeint_t> {
	static constexpr hugeint_t Minimum() {
		return {std::numeric_limits<int64_t>::lowest(), 1};
	};
	static constexpr hugeint_t Maximum() {
		return {std::numeric_limits<int64_t>::max(), std::numeric_limits<uint64_t>::max()};
	};
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 39;
	}
};

template <>
struct NumericLimits<uint8_t> {
	static constexpr uint8_t Minimum() {
		return std::numeric_limits<uint8_t>::lowest();
	};
	static constexpr uint8_t Maximum() {
		return std::numeric_limits<uint8_t>::max();
	};
	static bool IsSigned() {
		return false;
	}
	static idx_t Digits() {
		return 3;
	}
};

template <>
struct NumericLimits<uint16_t> {
	static constexpr uint16_t Minimum() {
		return std::numeric_limits<uint16_t>::lowest();
	};
	static constexpr uint16_t Maximum() {
		return std::numeric_limits<uint16_t>::max();
	};
	static bool IsSigned() {
		return false;
	}
	static idx_t Digits() {
		return 5;
	}
};
template <>
struct NumericLimits<uint32_t> {
	static constexpr uint32_t Minimum() {
		return std::numeric_limits<uint32_t>::lowest();
	};
	static constexpr uint32_t Maximum() {
		return std::numeric_limits<uint32_t>::max();
	};
	static bool IsSigned() {
		return false;
	}
	static idx_t Digits() {
		return 10;
	}
};
template <>
struct NumericLimits<uint64_t> {
	static constexpr uint64_t Minimum() {
		return std::numeric_limits<uint64_t>::lowest();
	};
	static constexpr uint64_t Maximum() {
		return std::numeric_limits<uint64_t>::max();
	};
	static bool IsSigned() {
		return false;
	}
	static idx_t Digits() {
		return 20;
	}
};
template <>
struct NumericLimits<float> {
	static constexpr float Minimum() {
		return std::numeric_limits<float>::lowest();
	};
	static constexpr float Maximum() {
		return std::numeric_limits<float>::max();
	};
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 127;
	}
};
template <>
struct NumericLimits<double> {
	static constexpr double Minimum() {
		return std::numeric_limits<double>::lowest();
	};
	static constexpr double Maximum() {
		return std::numeric_limits<double>::max();
	};
	static bool IsSigned() {
		return true;
	}
	static idx_t Digits() {
		return 250;
	}
};

} // namespace duckdb
