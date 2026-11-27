//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/relation_statistics/cardinality_value.hpp
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/common.hpp"

#include <cmath>
#include <limits>
#include <type_traits>

namespace duckdb {

//! Retain scaled numerator and denominator products until the final division.
//! Inputs are finite nonnegative row/domain counts or selectivity factors; only accumulated products need extra range.
class CardinalityValue {
public:
	// Scalar inputs can widen to a scaled point; materializing a point remains explicit.
	CardinalityValue(double value = 1) : CardinalityValue(Product(value), Product(1)) { // NOLINT
	}

	template <class T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
	CardinalityValue(T value) : CardinalityValue(static_cast<double>(value)) { // NOLINT
	}

	bool IsZero() const {
		return numerator.significand == 0;
	}

	//! Rounded numeric equality need not make operands interchangeable in subsequent comparisons.
	bool HasSameRepresentation(const CardinalityValue &other) const {
		return numerator.significand == other.numerator.significand && numerator.exponent == other.numerator.exponent &&
		       denominator == other.denominator &&
		       std::signbit(numerator.significand) == std::signbit(other.numerator.significand);
	}

	double GetLog() const {
		if (IsZero()) {
			return -std::numeric_limits<double>::infinity();
		}
		return std::log(numerator.significand) - std::log(denominator) +
		       static_cast<double>(numerator.exponent) * std::log(2.0);
	}

	double GetDouble() const {
		if (IsZero()) {
			return 0;
		}
		Product quotient(numerator.significand / denominator);
		quotient.exponent += numerator.exponent;
		if (quotient.exponent > std::numeric_limits<double>::max_exponent) {
			// A final overflow remains infinity. Saturating here would also tie costs and can overflow when adding
			// them.
			return std::numeric_limits<double>::infinity();
		}
		if (quotient.exponent < std::numeric_limits<double>::min_exponent - std::numeric_limits<double>::digits) {
			return 0;
		}
		return std::ldexp(quotient.significand, static_cast<int>(quotient.exponent));
	}

	idx_t GetIndex() const {
		auto value = GetDouble();
		// Keep a positive estimate from becoming an empty input in subsequent optimization.
		if (value < 1) {
			return IsZero() ? 0 : 1;
		}
		auto maximum = std::numeric_limits<idx_t>::max();
		return value >= static_cast<double>(maximum) ? maximum : static_cast<idx_t>(value);
	}

	friend CardinalityValue operator*(const CardinalityValue &left, const CardinalityValue &right) {
		return CardinalityValue(left.numerator * right.numerator, Product(left.denominator * right.denominator));
	}

	friend CardinalityValue operator/(const CardinalityValue &left, const CardinalityValue &right) {
		D_ASSERT(!right.IsZero());
		return CardinalityValue(left.numerator * right.denominator, right.numerator * left.denominator);
	}

	friend CardinalityValue operator+(const CardinalityValue &left, const CardinalityValue &right) {
		return CardinalityValue(
		    (left.numerator * right.denominator).Add(right.numerator * left.denominator, Product::Operation::ADD),
		    Product(left.denominator * right.denominator));
	}

	//! Subtraction of nonnegative estimates, clamped at zero.
	friend CardinalityValue operator-(const CardinalityValue &left, const CardinalityValue &right) {
		if (!(left > right)) {
			return CardinalityValue(0);
		}
		return CardinalityValue(
		    (left.numerator * right.denominator).Add(right.numerator * left.denominator, Product::Operation::SUBTRACT),
		    Product(left.denominator * right.denominator));
	}

	CardinalityValue &operator*=(const CardinalityValue &other) {
		*this = *this * other;
		return *this;
	}

	CardinalityValue &operator+=(const CardinalityValue &other) {
		*this = *this + other;
		return *this;
	}

	friend bool operator>(const CardinalityValue &left, const CardinalityValue &right) {
		// Equal power-of-two denominators cancel without rounding.
		if (left.denominator == 0.5 && right.denominator == 0.5) {
			return left.numerator > right.numerator;
		}
		return left.numerator * right.denominator > right.numerator * left.denominator;
	}

	friend bool operator<(const CardinalityValue &left, const CardinalityValue &right) {
		return right > left;
	}

	friend bool operator==(const CardinalityValue &left, const CardinalityValue &right) {
		return !(left > right) && !(right > left);
	}

	friend bool operator!=(const CardinalityValue &left, const CardinalityValue &right) {
		return !(left == right);
	}

	friend bool operator<=(const CardinalityValue &left, const CardinalityValue &right) {
		return !(left > right);
	}

	friend bool operator>=(const CardinalityValue &left, const CardinalityValue &right) {
		return !(left < right);
	}

private:
	struct Product {
		enum class Operation { ADD, SUBTRACT };

		explicit Product(double value) {
			D_ASSERT(std::isfinite(value) && value >= 0);
			int value_exponent;
			significand = std::frexp(value, &value_exponent);
			exponent = value_exponent;
		}

		Product operator*(const Product &other) const {
			auto result = *this * other.significand;
			if (result.significand != 0) {
				result.exponent += other.exponent;
			}
			return result;
		}

		Product operator*(double other) const {
			Product result(significand * other);
			if (result.significand != 0) {
				result.exponent += exponent;
			}
			return result;
		}

		Product Add(const Product &other, Operation operation) const {
			if (other.significand == 0) {
				return *this;
			}
			if (significand == 0) {
				D_ASSERT(operation == Operation::ADD);
				return other;
			}
			auto common_exponent = MaxValue(exponent, other.exponent);
			auto scale = [&](const Product &value) {
				auto distance = common_exponent - value.exponent;
				return distance > std::numeric_limits<double>::digits
				           ? 0.0
				           : std::ldexp(value.significand, -static_cast<int>(distance));
			};
			Product result(scale(*this) + (operation == Operation::SUBTRACT ? -scale(other) : scale(other)));
			if (result.significand != 0) {
				result.exponent += common_exponent;
			}
			return result;
		}

		bool operator>(const Product &other) const {
			if (significand == 0 || other.significand == 0) {
				return significand > other.significand;
			}
			return exponent == other.exponent ? significand > other.significand : exponent > other.exponent;
		}

		double significand;
		int64_t exponent;
	};

	CardinalityValue(Product numerator, Product denominator)
	    : numerator(numerator), denominator(denominator.significand) {
		// Remove common powers of two without dividing the significands.
		this->numerator.exponent -= denominator.exponent;
	}

	Product numerator;
	double denominator;
};

} // namespace duckdb
