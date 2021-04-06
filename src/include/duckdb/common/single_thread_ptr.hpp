//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/single_thread_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

class RefCounter {
public:
	uint32_t pn;
	RefCounter() : pn(1) {
	}
	void inc() {
		++pn;
	}
	void dec() {
		--pn;
	}
	uint32_t getPn() const {
		return pn;
	}
	virtual ~RefCounter() {
	}
};

namespace duckdb {
template <typename T>
class single_thread_ptr {
public:
	T *ptr;                // contained pointer
	RefCounter *ref_count; // reference counter

public:
	// Default constructor, constructs an empty single_thread_ptr.
	constexpr single_thread_ptr() : ptr(nullptr), ref_count(nullptr) {
	}
	// Construct empty single_thread_ptr.
	constexpr single_thread_ptr(std::nullptr_t) : ptr(nullptr), ref_count(nullptr) {
	}
	// Construct a single_thread_ptr that wraps raw pointer.

	single_thread_ptr(RefCounter *r, T *p) {
		ptr = p;
		ref_count = r;
	}

	template <class U>
	single_thread_ptr(RefCounter *r, U *p) {
		ptr = p;
		ref_count = r;
	}

	// Copy  constructor.
	single_thread_ptr(const single_thread_ptr &sp) : ptr(nullptr), ref_count(nullptr) {
		if (sp.ptr) {
			ptr = sp.ptr;
			ref_count = sp.ref_count;
			ref_count->inc();
		}
	}

	// Conversion constructor.
	template <typename U>
	single_thread_ptr(const single_thread_ptr<U> &sp) : ptr(nullptr), ref_count(nullptr) {
		if (sp.ptr) {
			ptr = sp.ptr;
			ref_count = sp.ref_count;
			ref_count->inc();
		}
	}

	// move  constructor.
	single_thread_ptr(single_thread_ptr &&sp) noexcept : ptr {sp.ptr}, ref_count {sp.ref_count} {
		sp.ptr = nullptr;
		sp.ref_count = nullptr;
	}

	// move  constructor.
	template <class U>
	single_thread_ptr(single_thread_ptr<U> &&sp) noexcept : ptr {sp.ptr}, ref_count {sp.ref_count} {
		sp.ptr = nullptr;
		sp.ref_count = nullptr;
	}

	// No effect if single_thread_ptr is empty or use_count() > 1, otherwise release the resources.
	~single_thread_ptr() {
		release();
	}

	void release() {
		if (ptr && ref_count) {
			ref_count->dec();
			if ((ref_count->getPn()) == 0) {
				delete ref_count;
			}
		}
		ref_count = nullptr;
		ptr = nullptr;
	}

	// Copy assignment.
	single_thread_ptr &operator=(single_thread_ptr sp) noexcept {
		std::swap(this->ptr, sp.ptr);
		std::swap(this->ref_count, sp.ref_count);
		return *this;
	}

	// Dereference pointer to managed object.
	T &operator*() const noexcept {
		return *ptr;
	}
	T *operator->() const noexcept {
		return ptr;
	}

	// Return the contained pointer.
	T *get() const noexcept {
		return ptr;
	}

	// Return use count (use count == 0 if single_thread_ptr is empty).
	long use_count() const noexcept {
		if (ptr)
			return ref_count->getPn();
		else
			return 0;
	}

	// Check if there is an associated managed object.
	explicit operator bool() const noexcept {
		return (ptr);
	}

	// Resets single_thread_ptr to empty.
	void reset() noexcept {
		release();
	}
};

template <class T>
struct _object_and_block : public RefCounter {
	T object;

	template <class... Args>
	explicit _object_and_block(Args &&... args) : object(std::forward<Args>(args)...) {
	}
};

// Operator overloading.
template <typename T, typename U>
inline bool operator==(const single_thread_ptr<T> &sp1, const single_thread_ptr<U> &sp2) {
	return sp1.get() == sp2.get();
}

template <typename T>
inline bool operator==(const single_thread_ptr<T> &sp, std::nullptr_t) noexcept {
	return !sp;
}

template <typename T, typename U>
inline bool operator!=(const single_thread_ptr<T> &sp1, const single_thread_ptr<U> &sp2) {
	return sp1.get() != sp2.get();
}

template <typename T>
inline bool operator!=(const single_thread_ptr<T> &sp, std::nullptr_t) noexcept {
	return sp.get();
}

template <typename T>
inline bool operator!=(std::nullptr_t, const single_thread_ptr<T> &sp) noexcept {
	return sp.get();
}

template <class T, class... Args>
single_thread_ptr<T> single_thread_make_shared(Args &&... args) {
	auto tmp_object = new _object_and_block<T>(std::forward<Args>(args)...);
	return single_thread_ptr<T>(tmp_object, &(tmp_object->object));
}
} // namespace duckdb