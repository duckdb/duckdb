#include "random.hh"

namespace smith {
std::mt19937_64 rng;
}

int d6() {
	static std::uniform_int_distribution<> pick(1, 6);
	return pick(smith::rng);
}

int d9() {
	static std::uniform_int_distribution<> pick(1, 9);
	return pick(smith::rng);
}

int d12() {
	static std::uniform_int_distribution<> pick(1, 12);
	return pick(smith::rng);
}

int d20() {
	static std::uniform_int_distribution<> pick(1, 20);
	return pick(smith::rng);
}

int d42() {
	static std::uniform_int_distribution<> pick(1, 42);
	return pick(smith::rng);
}

int d100() {
	static std::uniform_int_distribution<> pick(1, 100);
	return pick(smith::rng);
}
