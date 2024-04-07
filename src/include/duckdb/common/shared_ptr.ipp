
namespace duckdb {

template <typename T>
class weak_ptr;

template <class T>
class enable_shared_from_this;

template <typename T>
class shared_ptr {
private:
	template <class U>
	friend class weak_ptr;
	template <class U>
	friend class shared_ptr;

private:
	std::shared_ptr<T> internal;

public:
	// Constructors
	shared_ptr() : internal() {
	}
	shared_ptr(std::nullptr_t) : internal(nullptr) {
	} // Implicit conversion
	template <class U>
	explicit shared_ptr(U *ptr) : internal(ptr) {
		__enable_weak_this(internal.get(), internal.get());
	}
	// Constructor with custom deleter
	template <typename Deleter>
	shared_ptr(T *ptr, Deleter deleter) : internal(ptr, deleter) {
		__enable_weak_this(internal.get(), internal.get());
	}
	template <class U>
	shared_ptr(const shared_ptr<U> &__r, T *__p) noexcept : internal(__r.internal, __p) {
	}

	shared_ptr(const shared_ptr &other) : internal(other.internal) {
	}

	shared_ptr(std::shared_ptr<T> other) : internal(other) {
		// FIXME: should we __enable_weak_this here?
		// *our* enable_shared_from_this hasn't initialized yet, so I think so?
		__enable_weak_this(internal.get(), internal.get());
	}
	shared_ptr(shared_ptr<T> &&other) : internal(other.internal) {
	}

	template <class U>
	explicit shared_ptr(weak_ptr<U> other) : internal(other.internal) {
	}

#if _LIBCPP_STD_VER <= 14 || defined(_LIBCPP_ENABLE_CXX17_REMOVED_AUTO_PTR)
	template <class U, std::enable_if<std::is_convertible<U *, T *>::value, int> = 0>
	shared_ptr(std::auto_ptr<U> &&__r) : internal(__r.release()) {
		__enable_weak_this(internal.get(), internal.get());
	}
#endif

	template <class U, class DELETER, bool SAFE,
	          typename std::enable_if<__compatible_with<U, T>::value &&
	                                      std::is_convertible<typename unique_ptr<U, DELETER>::pointer, T *>::value,
	                                  int>::type = 0>
	shared_ptr(unique_ptr<U, DELETER, SAFE> &&other) : internal(other.release()) {
		__enable_weak_this(internal.get(), internal.get());
	}

	// Destructor
	~shared_ptr() = default;

	// Assignment operators
	shared_ptr &operator=(const shared_ptr &other) {
		internal = other.internal;
		return *this;
	}

	template <class U, class DELETER, bool SAFE,
	          typename std::enable_if<__compatible_with<U, T>::value &&
	                                      std::is_convertible<typename unique_ptr<U, DELETER>::pointer, T *>::value,
	                                  int>::type = 0>
	shared_ptr<T> &operator=(unique_ptr<U, DELETER, SAFE> &&__r) {
		shared_ptr(std::move(__r)).swap(*this);
		return *this;
	}

	// Modifiers
	void reset() {
		internal.reset();
	}

	template <typename U>
	void reset(U *ptr) {
		internal.reset(ptr);
	}

	template <typename U, typename Deleter>
	void reset(U *ptr, Deleter deleter) {
		internal.reset(ptr, deleter);
	}

	void swap(shared_ptr &r) noexcept {
		internal.swap(r.internal);
	}

	// Observers
	T *get() const {
		return internal.get();
	}

	long use_count() const {
		return internal.use_count();
	}

	explicit operator bool() const noexcept {
		return internal.operator bool();
	}

	template <class U>
	operator shared_ptr<U>() const noexcept {
		return shared_ptr<U>(internal);
	}

	// Element access
	std::__add_lvalue_reference_t<T> operator*() const {
		return *internal;
	}

	T *operator->() const {
		return internal.operator->();
	}

	// Relational operators
	template <typename U>
	bool operator==(const shared_ptr<U> &other) const noexcept {
		return internal == other.internal;
	}

	bool operator==(std::nullptr_t) const noexcept {
		return internal == nullptr;
	}

	template <typename U>
	bool operator!=(const shared_ptr<U> &other) const noexcept {
		return internal != other.internal;
	}
	bool operator!=(std::nullptr_t) const noexcept {
		return internal != nullptr;
	}

	template <typename U>
	bool operator<(const shared_ptr<U> &other) const noexcept {
		return internal < other.internal;
	}

	template <typename U>
	bool operator<=(const shared_ptr<U> &other) const noexcept {
		return internal <= other.internal;
	}

	template <typename U>
	bool operator>(const shared_ptr<U> &other) const noexcept {
		return internal > other.internal;
	}

	template <typename U>
	bool operator>=(const shared_ptr<U> &other) const noexcept {
		return internal >= other.internal;
	}

	template <typename U, typename S>
	friend shared_ptr<S> shared_ptr_cast(shared_ptr<U> src);

private:
	// This overload is used when the class inherits from 'enable_shared_from_this<U>'
	template <class U, class _OrigPtr,
	          typename std::enable_if<std::is_convertible<_OrigPtr *, const enable_shared_from_this<U> *>::value,
	                                  int>::type = 0>
	void __enable_weak_this(const enable_shared_from_this<U> *__e, _OrigPtr *__ptr) noexcept {
		typedef typename std::remove_cv<U>::type NonConstU;
		if (__e && __e->__weak_this_.expired()) {
			// __weak_this__ is the mutable variable returned by 'shared_from_this'
			// it is initialized here
			__e->__weak_this_ = shared_ptr<NonConstU>(*this, const_cast<NonConstU *>(static_cast<const U *>(__ptr)));
		}
	}

	void __enable_weak_this(...) noexcept {
	}
};

} // namespace duckdb
