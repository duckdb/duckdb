#include "duckdb/optimizer/join_order/join_order_rank.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

JoinOrderRank::JoinOrderRank(double log_multiplier) : log_multiplier(log_multiplier), log_work(log_multiplier) {
	D_ASSERT(!std::isnan(log_multiplier));
}

double JoinOrderRank::AddLogValues(double left, double right) {
	if (left < right) {
		std::swap(left, right);
	}
	if (left == -std::numeric_limits<double>::infinity()) {
		return left;
	}
	return left + std::log1p(std::exp(right - left));
}

void JoinOrderRank::Append(const JoinOrderRank &other) {
	log_work = AddLogValues(log_work, log_multiplier + other.log_work);
	log_multiplier += other.log_multiplier;
}

void JoinOrderRank::AddIndependentWork(double work) {
	log_work = AddLogValues(log_work, work);
}

int32_t JoinOrderRank::Sign() const {
	return log_multiplier < 0 ? -1 : log_multiplier > 0 ? 1 : 0;
}

double JoinOrderRank::LogMagnitude() const {
	if (log_multiplier < 0) {
		return std::log(-std::expm1(log_multiplier)) - log_work;
	}
	return log_multiplier + std::log(-std::expm1(-log_multiplier)) - log_work;
}

bool JoinOrderRank::operator<(const JoinOrderRank &other) const {
	auto sign = Sign();
	auto other_sign = other.Sign();
	if (sign != other_sign) {
		return sign < other_sign;
	}
	if (sign == 0) {
		return false;
	}
	auto magnitude = LogMagnitude();
	auto other_magnitude = other.LogMagnitude();
	return sign < 0 ? magnitude > other_magnitude : magnitude < other_magnitude;
}

} // namespace duckdb
