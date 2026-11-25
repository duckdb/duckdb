#include "catch.hpp"
#include "duckdb/common/buffer_ptr.hpp"

#include <thread>
#include <vector>

namespace duckdb {
namespace {

struct BufferPtrCounter {
	explicit BufferPtrCounter(int &destroyed_p) : destroyed(destroyed_p) {
	}
	~BufferPtrCounter() {
		destroyed++;
	}
	int &destroyed;
	int first = 11;
	int second = 22;
};

struct BufferPtrLeft {
	int left = 1;
};
struct BufferPtrRight {
	int right = 2;
};
struct BufferPtrDerived : BufferPtrLeft, BufferPtrRight, BufferPtrCounter {
	explicit BufferPtrDerived(int &destroyed) : BufferPtrCounter(destroyed) {
	}
};
struct alignas(256) BufferPtrAligned {
	int value = 42;
};

struct BufferPtrSelf : enable_buffer_from_this<BufferPtrSelf> {
	explicit BufferPtrSelf(int &rejected_p) : rejected(&rejected_p) {
		try {
			buffer_from_this();
		} catch (const std::bad_weak_ptr &) {
			(*rejected)++;
		}
	}
	~BufferPtrSelf() {
		try {
			buffer_from_this();
		} catch (const std::bad_weak_ptr &) {
			(*rejected)++;
		}
	}
	int *rejected;
};
struct BufferPtrSelfDerived : BufferPtrLeft, BufferPtrSelf {
	explicit BufferPtrSelfDerived(int &rejected) : BufferPtrSelf(rejected) {
	}
};

// Instantiating and destroying a handle does not require a complete payload.
struct BufferPtrIncomplete;
void CheckIncompleteBuffer() {
	buffer_ptr<BufferPtrIncomplete> first;
	auto second = first;
	second.reset();
}

} // namespace

TEST_CASE("Buffer pointers preserve aliases and destroy their owner exactly once", "[buffer_ptr]") {
	CheckIncompleteBuffer();
	int destroyed = 0;
	auto owner = make_buffer<BufferPtrCounter>(destroyed);
	buffer_ptr<int> first(owner, &owner->first), second(owner, &owner->second);
	first = second;
	REQUIRE(first.get() == second.get());
	REQUIRE(*first == 22);
	REQUIRE(owner.use_count() == 3);
	buffer_ptr<int> null_alias(owner, nullptr);
	REQUIRE_FALSE(null_alias);
	REQUIRE(null_alias.use_count() == 4);
	owner.reset();
	first.reset();
	second.reset();
	REQUIRE(destroyed == 0);
	null_alias.reset();
	REQUIRE(destroyed == 1);

	int value = 99;
	auto one = make_buffer<BufferPtrCounter>(destroyed);
	auto two = make_buffer<BufferPtrCounter>(destroyed);
	buffer_ptr<int> a(one, &value), b(two, &value);
	one.reset();
	a = b; // Equal stored pointers do not imply equal owners.
	REQUIRE(destroyed == 2);
	REQUIRE(two.use_count() == 3);
	two.reset();
	b.reset();
	REQUIRE(*a == 99);
	a.reset();
	REQUIRE(destroyed == 3);

	buffer_ptr<int> borrowed(buffer_ptr<int>(), &value);
	REQUIRE(*borrowed == 99);
	REQUIRE(borrowed.use_count() == 0);
}

TEST_CASE("Buffer pointer assignment acquires embedded sources before releasing the destination", "[buffer_ptr]") {
	struct Node {
		buffer_ptr<Node> child;
		int value = 17;
	};
	for (bool move : {false, true}) {
		auto parent = make_buffer<Node>();
		parent->child = make_buffer<Node>();
		auto expected = parent->child.get();
		if (move) {
			parent = std::move(parent->child);
		} else {
			parent = parent->child;
		}
		REQUIRE(parent.get() == expected);
		REQUIRE(parent.use_count() == 1);
		auto &same = parent;
		parent = same;
		parent = std::move(same);
		REQUIRE(parent.get() == expected);
		REQUIRE(parent->value == 17);
		REQUIRE(parent.use_count() == 1);
	}
}

TEST_CASE("Buffer pointer conversions retain adjusted pointers and concrete destruction", "[buffer_ptr]") {
	int destroyed = 0;
	auto derived = make_buffer<BufferPtrDerived>(destroyed);
	buffer_ptr<BufferPtrRight> right = derived;
	REQUIRE(static_cast<void *>(right.get()) != static_cast<void *>(derived.get()));
	REQUIRE(right->right == 2);
	buffer_ptr<const BufferPtrRight> constant = std::move(right);
	REQUIRE_FALSE(right);
	buffer_ptr<const void> erased = constant;
	derived.reset();
	constant.reset();
	REQUIRE(destroyed == 0);
	erased.reset();
	REQUIRE(destroyed == 1);
	auto aligned = make_buffer<BufferPtrAligned>();
	REQUIRE(reinterpret_cast<uintptr_t>(aligned.get()) % alignof(BufferPtrAligned) == 0);
	REQUIRE(aligned->value == 42);

	static_assert(!std::is_constructible<buffer_ptr<int>, int *>::value, "owners must come from make_buffer");
	static_assert(!std::is_constructible<buffer_ptr<int>, std::shared_ptr<int>>::value, "ownership families differ");
	static_assert(!std::is_constructible<buffer_ptr<int>, buffer_ptr<const int>>::value, "cannot remove const");
	static_assert(std::is_nothrow_move_constructible<buffer_ptr<int>>::value, "moving ownership cannot throw");
}

TEST_CASE("Buffer construction failure cleans up constructed members", "[buffer_ptr]") {
	struct Failing {
		explicit Failing(int &destroyed) : counter(destroyed) {
			throw 42;
		}
		BufferPtrCounter counter;
	};
	int destroyed = 0;
	REQUIRE_THROWS_AS(make_buffer<Failing>(destroyed), int);
	REQUIRE(destroyed == 1);
}

TEST_CASE("Buffer from this retains a live owner but cannot inherit or resurrect one", "[buffer_ptr]") {
	int rejected = 0;
	{
		auto original = make_buffer<BufferPtrSelfDerived>(rejected);
		REQUIRE(rejected == 1); // Constructor cannot acquire an owner.
		auto retained = original->buffer_from_this();
		REQUIRE(retained.get() == static_cast<BufferPtrSelf *>(original.get()));
		const auto &constant = *original;
		auto const_retained = constant.buffer_from_this();
		REQUIRE(const_retained.use_count() == 3);
		original.reset();
		REQUIRE(retained.use_count() == 2);
		BufferPtrSelf copied(*retained);
		BufferPtrSelf moved(std::move(*retained));
		REQUIRE_THROWS_AS(copied.buffer_from_this(), std::bad_weak_ptr);
		REQUIRE_THROWS_AS(moved.buffer_from_this(), std::bad_weak_ptr);
		REQUIRE(retained->buffer_from_this().get() == retained.get());
		auto destination = make_buffer<BufferPtrSelf>(rejected);
		*destination = *retained;
		REQUIRE(destination->buffer_from_this().get() == destination.get());
		*destination = std::move(*retained);
		REQUIRE(destination->buffer_from_this().get() == destination.get());
	}
	REQUIRE(rejected == 6); // Two rejected constructors and four destructors.
}

TEST_CASE("Buffer final release observes writes from every previous owner", "[buffer_ptr]") {
	struct Published {
		explicit Published(std::atomic<int> &completed_p) : completed(completed_p) {
		}
		~Published() {
			for (auto value : values) {
				if (value != 123) {
					completed.store(-1000);
					return;
				}
			}
			completed.fetch_add(1);
		}
		int values[4] {};
		std::atomic<int> &completed;
	};
	std::atomic<int> completed {0};
	for (int iteration = 0; iteration < 100; iteration++) {
		auto owner = make_buffer<Published>(completed);
		std::atomic<int> ready {0};
		std::atomic<bool> start {false};
		std::vector<std::thread> threads;
		for (int t = 0; t < 4; t++) {
			threads.emplace_back([local = owner, t, &ready, &start]() mutable {
				ready.fetch_add(1);
				while (!start.load()) {
					std::this_thread::yield();
				}
				for (int copy = 0; copy < 100; copy++) {
					auto other = local;
					local = other;
				}
				local->values[t] = 123;
				local.reset();
			});
		}
		while (ready.load() != 4) {
			std::this_thread::yield();
		}
		owner.reset();
		start = true;
		for (auto &thread : threads) {
			thread.join();
		}
		REQUIRE(completed.load() == iteration + 1);
	}
}

} // namespace duckdb
