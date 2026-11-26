//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/buffer_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/memory_safety.hpp"

#include <atomic>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

namespace duckdb {

template <class T>
class buffer_ptr;
template <class T>
class enable_buffer_from_this;
template <class T, class... ARGS>
buffer_ptr<T> make_buffer(ARGS &&...args);

namespace buffer_ptr_detail {

struct ControlBlock {
	explicit ControlBlock(void (*destroy_p)(ControlBlock *)) noexcept : destroy(destroy_p) {
	}

	void AddReference() noexcept {
		auto previous = references.fetch_add(1, std::memory_order_relaxed);
		// Leave ample headroom for concurrent increments before terminating.
		if (previous == 0 || previous >= (std::numeric_limits<uint64_t>::max)() / 2) {
			std::terminate();
		}
	}

	void Release() noexcept {
		// Acquiring an owner requires an existing strong owner. With no weak owners,
		// a sole owner cannot race with an increment and needs no atomic decrement.
		if (references.load(std::memory_order_acquire) == 1) {
			// buffer_from_this must also reject acquisition during destruction.
			references.store(0, std::memory_order_relaxed);
			destroy(this);
			return;
		}
		if (references.fetch_sub(1, std::memory_order_release) == 1) {
			// Synchronize with the release sequence before destroying the payload.
			(void)references.load(std::memory_order_acquire);
			destroy(this);
		}
	}

	std::atomic<uint64_t> references {1};
	void (*destroy)(ControlBlock *);
};

template <class T>
struct Allocation : ControlBlock {
	template <class... ARGS>
	explicit Allocation(ARGS &&...args) : ControlBlock(Destroy), value(std::forward<ARGS>(args)...) {
	}

	static void Destroy(ControlBlock *block) {
		delete static_cast<Allocation *>(block);
	}

	T value;
};

} // namespace buffer_ptr_detail

//! Strong-only, atomic shared ownership for buffers. Independent handles can be
//! used concurrently; modifying the same handle requires external synchronization.
//! Create owners with make_buffer. Aliases retain an owner but may point elsewhere.
template <class T>
class buffer_ptr { // NOLINT: mimic std casing
public:
	using element_type = T;
#if defined(_LIBCPP_VERSION)
	using __trivially_relocatable = buffer_ptr; // NOLINT: libc++ extension point
#endif

	buffer_ptr() noexcept = default;
	buffer_ptr(std::nullptr_t) noexcept { // NOLINT: allow implicit nullptr
	}
	buffer_ptr(const buffer_ptr &other) noexcept : buffer_ptr(other, other.ptr) {
	}
	template <class U, typename std::enable_if<std::is_convertible<U *, T *>::value, int>::type = 0>
	buffer_ptr(const buffer_ptr<U> &other) noexcept // NOLINT: allow implicit conversion
	    : buffer_ptr(other, other.ptr) {
	}
	buffer_ptr(buffer_ptr &&other) noexcept : buffer_ptr(std::move(other), other.ptr) {
	}
	template <class U, typename std::enable_if<std::is_convertible<U *, T *>::value, int>::type = 0>
	buffer_ptr(buffer_ptr<U> &&other) noexcept // NOLINT: allow implicit conversion
	    : buffer_ptr(std::move(other), other.ptr) {
	}
	template <class U>
	buffer_ptr(const buffer_ptr<U> &owner, T *ptr_p) noexcept : ptr(ptr_p), control(owner.control) {
		if (control) {
			control->AddReference();
		}
	}
	template <class U>
	// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved): steals the owner's control block
	buffer_ptr(buffer_ptr<U> &&owner, T *ptr_p) noexcept : ptr(ptr_p), control(owner.control) {
		owner.ptr = nullptr;
		owner.control = nullptr;
	}
	~buffer_ptr() {
		if (control) {
			control->Release();
		}
	}

	buffer_ptr &operator=(const buffer_ptr &other) noexcept {
		if (this == &other) {
			return *this;
		}
		return Assign(other);
	}
	template <class U, typename std::enable_if<std::is_convertible<U *, T *>::value, int>::type = 0>
	buffer_ptr &operator=(const buffer_ptr<U> &other) noexcept {
		return Assign(other);
	}
	buffer_ptr &operator=(buffer_ptr &&other) noexcept {
		buffer_ptr(std::move(other)).swap(*this);
		return *this;
	}
	template <class U, typename std::enable_if<std::is_convertible<U *, T *>::value, int>::type = 0>
	buffer_ptr &operator=(buffer_ptr<U> &&other) noexcept {
		buffer_ptr(std::move(other)).swap(*this);
		return *this;
	}

	void reset() noexcept { // NOLINT: mimic std casing
		buffer_ptr().swap(*this);
	}
	void swap(buffer_ptr &other) noexcept { // NOLINT: mimic std casing
		std::swap(ptr, other.ptr);
		std::swap(control, other.control);
	}
	T *get() const noexcept { // NOLINT: mimic std casing
		return ptr;
	}
	uint64_t use_count() const noexcept { // NOLINT: mimic std casing
		return control ? control->references.load(std::memory_order_relaxed) : 0;
	}
	explicit operator bool() const noexcept {
		return ptr != nullptr;
	}
	typename std::add_lvalue_reference<T>::type operator*() const {
		AssertNotNull();
		return *ptr;
	}
	T *operator->() const {
		AssertNotNull();
		return ptr;
	}

private:
	template <class U>
	friend class buffer_ptr;
	template <class U>
	friend class enable_buffer_from_this;
	template <class U, class... ARGS>
	friend buffer_ptr<U> make_buffer(ARGS &&...args);

	buffer_ptr(T *ptr_p, buffer_ptr_detail::ControlBlock *control_p) noexcept : ptr(ptr_p), control(control_p) {
	}
	template <class U>
	buffer_ptr &Assign(const buffer_ptr<U> &other) noexcept {
		if (control == other.control) {
			// Two aliases of the same owner can have different stored pointers.
			ptr = other.ptr;
		} else {
			// Acquire first: other can be a member of the object being released.
			buffer_ptr(other).swap(*this);
		}
		return *this;
	}
	void AssertNotNull() const {
#if !defined(DUCKDB_DEBUG_NO_SAFETY) && !defined(DUCKDB_CLANG_TIDY)
		if (!ptr) {
			ThrowNullSharedPtrDereference();
		}
#endif
	}
	template <class U>
	void EnableOwner(const enable_buffer_from_this<U> *object) noexcept {
		object->control = control;
	}
	void EnableOwner(...) noexcept {
	}

	T *ptr = nullptr;
	buffer_ptr_detail::ControlBlock *control = nullptr;
};

template <class T, class U>
bool operator==(const buffer_ptr<T> &left, const buffer_ptr<U> &right) noexcept {
	return left.get() == right.get();
}
template <class T, class U>
bool operator!=(const buffer_ptr<T> &left, const buffer_ptr<U> &right) noexcept {
	return !(left == right);
}
template <class T>
bool operator==(const buffer_ptr<T> &ptr, std::nullptr_t) noexcept {
	return !ptr;
}
template <class T>
bool operator==(std::nullptr_t, const buffer_ptr<T> &ptr) noexcept {
	return !ptr;
}
template <class T>
bool operator!=(const buffer_ptr<T> &ptr, std::nullptr_t) noexcept {
	return bool(ptr);
}
template <class T>
bool operator!=(std::nullptr_t, const buffer_ptr<T> &ptr) noexcept {
	return bool(ptr);
}

//! Acquire another strong owner while an existing owner keeps this object alive.
//! This is not a weak reference: it cannot be used concurrently with final release.
template <class T>
class enable_buffer_from_this { // NOLINT: mimic std casing
public:
	buffer_ptr<T> buffer_from_this() { // NOLINT: mimic std casing
		AddReference();
		return buffer_ptr<T>(static_cast<T *>(this), control);
	}
	buffer_ptr<const T> buffer_from_this() const { // NOLINT: mimic std casing
		AddReference();
		return buffer_ptr<const T>(static_cast<const T *>(this), control);
	}

protected:
	// Protected like std::enable_shared_from_this, so any class in a hierarchy can derive from it
	// NOLINTNEXTLINE(bugprone-crtp-constructor-accessibility)
	enable_buffer_from_this() noexcept = default;
	// NOLINTNEXTLINE(bugprone-crtp-constructor-accessibility)
	enable_buffer_from_this(const enable_buffer_from_this &) noexcept {
	}
	// NOLINTNEXTLINE(bugprone-unhandled-self-assignment): assignment keeps the owner, so it is a no-op
	enable_buffer_from_this &operator=(const enable_buffer_from_this &) noexcept {
		// Assignment changes the payload, not its owner.
		return *this;
	}
	~enable_buffer_from_this() = default;

private:
	template <class U>
	friend class buffer_ptr;

	void AddReference() const {
		if (!control || control->references.load(std::memory_order_relaxed) == 0) {
			throw std::bad_weak_ptr();
		}
		control->AddReference();
	}
	mutable buffer_ptr_detail::ControlBlock *control = nullptr;
};

template <class T, class... ARGS>
buffer_ptr<T> make_buffer(ARGS &&...args) { // NOLINT: mimic std casing
	auto allocation = new buffer_ptr_detail::Allocation<T>(std::forward<ARGS>(args)...);
	buffer_ptr<T> result(&allocation->value, allocation);
	result.EnableOwner(&allocation->value);
	return result;
}

} // namespace duckdb
