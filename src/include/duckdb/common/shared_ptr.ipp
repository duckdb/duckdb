namespace duckdb {

template <typename T, bool SAFE = true>
class weak_ptr;

template <class T>
class enable_shared_from_this;

template <typename T, bool SAFE = true>
class shared_ptr {
public:
	using original = std::shared_ptr<T>;
	using element_type = typename original::element_type;
	using weak_type = weak_ptr<T, SAFE>;

private:
	static inline void AssertNotNull(const bool null) {
#if defined(DUCKDB_DEBUG_NO_SAFETY) || defined(DUCKDB_CLANG_TIDY)
		return;
#else
		if (DUCKDB_UNLIKELY(null)) {
			throw duckdb::InternalException("Attempted to dereference shared_ptr that is NULL!");
		}
#endif
	}

private:
	template <class U, bool SAFE_P>
	friend class weak_ptr;

	template <class U, bool SAFE_P>
	friend class shared_ptr;

	template <typename U, typename S>
	friend shared_ptr<S> shared_ptr_cast(shared_ptr<U> src);

private:
	original internal;

public:
	// Constructors
	shared_ptr() : internal() {
	}
	shared_ptr(std::nullptr_t) : internal(nullptr) {
	}

	// From raw pointer of type U convertible to T
	template <class U, typename std::enable_if<__compatible_with<U, T>::value, int>::type = 0>
	explicit shared_ptr(U *ptr) : internal(ptr) {
		__enable_weak_this(internal.get(), internal.get());
	}
	// From raw pointer of type T with custom Deleter
	template <typename Deleter>
	shared_ptr(T *ptr, Deleter deleter) : internal(ptr, deleter) {
		__enable_weak_this(internal.get(), internal.get());
	}
	// Aliasing constructor: shares ownership information with __r but contains __p instead
	// When the created shared_ptr goes out of scope, it will call the Deleter of __r, will not delete __p
	template <class U>
	shared_ptr(const shared_ptr<U> &__r, T *__p) noexcept : internal(__r.internal, __p) {
	}
#if _LIBCPP_STD_VER >= 20
	template <class U>
	shared_ptr(shared_ptr<U> &&__r, T *__p) noexcept : internal(std::move(__r.internal), __p) {
	}
#endif

	// Copy constructor, share ownership with __r
	template <class U, typename std::enable_if<__compatible_with<U, T>::value, int>::type = 0>
	shared_ptr(const shared_ptr<U> &__r) noexcept : internal(__r.internal) {
	}
	shared_ptr(const shared_ptr &other) : internal(other.internal) {
	}
	// Move constructor, share ownership with __r
	template <class U, typename std::enable_if<__compatible_with<U, T>::value, int>::type = 0>
	shared_ptr(shared_ptr<U> &&__r) noexcept : internal(std::move(__r.internal)) {
	}
	shared_ptr(shared_ptr<T> &&other) : internal(std::move(other.internal)) {
	}

	// Construct from std::shared_ptr
	explicit shared_ptr(std::shared_ptr<T> other) : internal(other) {
		// FIXME: should we __enable_weak_this here?
		// *our* enable_shared_from_this hasn't initialized yet, so I think so?
		__enable_weak_this(internal.get(), internal.get());
	}

	// Construct from weak_ptr
	template <class U>
	explicit shared_ptr(weak_ptr<U> other) : internal(other.internal) {
	}

	// Construct from auto_ptr
#if _LIBCPP_STD_VER <= 14 || defined(_LIBCPP_ENABLE_CXX17_REMOVED_AUTO_PTR)
	template <class U, std::enable_if<std::is_convertible<U *, T *>::value, int> = 0>
	shared_ptr(std::auto_ptr<U> &&__r) : internal(__r.release()) {
		__enable_weak_this(internal.get(), internal.get());
	}
#endif

	// Construct from unique_ptr, takes over ownership of the unique_ptr
	template <class U, class DELETER, bool SAFE_P,
	          typename std::enable_if<__compatible_with<U, T>::value &&
	                                      std::is_convertible<typename unique_ptr<U, DELETER>::pointer, T *>::value,
	                                  int>::type = 0>
	shared_ptr(unique_ptr<U, DELETER, SAFE_P> &&other) : internal(std::move(other)) {
		__enable_weak_this(internal.get(), internal.get());
	}

	// Destructor
	~shared_ptr() = default;

	// Assign from shared_ptr copy
	shared_ptr<T> &operator=(const shared_ptr &other) noexcept {
		// Create a new shared_ptr using the copy constructor, then swap out the ownership to *this
		shared_ptr(other).swap(*this);
		return *this;
	}
	template <class U, typename std::enable_if<__compatible_with<U, T>::value, int>::type = 0>
	shared_ptr<T> &operator=(const shared_ptr<U> &other) {
		shared_ptr(other).swap(*this);
		return *this;
	}

	// Assign from moved shared_ptr
	shared_ptr<T> &operator=(shared_ptr &&other) noexcept {
		// Create a new shared_ptr using the move constructor, then swap out the ownership to *this
		shared_ptr(std::move(other)).swap(*this);
		return *this;
	}
	template <class U, typename std::enable_if<__compatible_with<U, T>::value, int>::type = 0>
	shared_ptr<T> &operator=(shared_ptr<U> &&other) {
		shared_ptr(std::move(other)).swap(*this);
		return *this;
	}

	// Assign from moved unique_ptr
	template <class U, class DELETER, bool SAFE_P,
	          typename std::enable_if<__compatible_with<U, T>::value &&
	                                      std::is_convertible<typename unique_ptr<U, DELETER>::pointer, T *>::value,
	                                  int>::type = 0>
	shared_ptr<T> &operator=(unique_ptr<U, DELETER, SAFE_P> &&__r) {
		shared_ptr(std::move(__r)).swap(*this);
		return *this;
	}

#ifdef DUCKDB_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	void
	reset() {
		internal.reset();
	}
#ifdef DUCKDB_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	template <typename U>
	void reset(U *ptr) {
		internal.reset(ptr);
	}
#ifdef DUCKDB_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	template <typename U, typename Deleter>
	void reset(U *ptr, Deleter deleter) {
		internal.reset(ptr, deleter);
	}

	void swap(shared_ptr &r) noexcept {
		internal.swap(r.internal);
	}

	T *get() const {
		return internal.get();
	}

	long use_count() const {
		return internal.use_count();
	}

	explicit operator bool() const noexcept {
		return internal.operator bool();
	}

	std::__add_lvalue_reference_t<T> operator*() const {
		if (MemorySafety<SAFE>::ENABLED) {
			const auto ptr = internal.get();
			AssertNotNull(!ptr);
			return *ptr;
		} else {
			return *internal;
		}
	}

	T *operator->() const {
		if (MemorySafety<SAFE>::ENABLED) {
			const auto ptr = internal.get();
			AssertNotNull(!ptr);
			return ptr;
		} else {
			return internal.operator->();
		}
	}

	// Relational operators
	template <typename U>
	bool operator==(const shared_ptr<U> &other) const noexcept {
		return internal == other.internal;
	}
	template <typename U>
	bool operator!=(const shared_ptr<U> &other) const noexcept {
		return internal != other.internal;
	}

	bool operator==(std::nullptr_t) const noexcept {
		return internal == nullptr;
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
