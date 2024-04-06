
namespace duckdb {

template <typename T>
class weak_ptr;

template <typename T>
class shared_ptr {
private:
	template <class U>
	friend class weak_ptr;
	std::shared_ptr<T> internal;
	template <class U>
	friend class shared_ptr;

public:
	// Constructors
	shared_ptr() : internal() {
	}
	shared_ptr(std::nullptr_t) : internal(nullptr) {
	} // Implicit conversion
	template <class U>
	explicit shared_ptr(U *ptr) : internal(ptr) {
	}
	// Constructor with custom deleter
	template <typename Deleter>
	shared_ptr(T *ptr, Deleter deleter) : internal(ptr, deleter) {
	}
	template <class U>
	shared_ptr(const shared_ptr<U> &__r, T *__p) noexcept : internal(__r.internal, __p) {
	}

	shared_ptr(const shared_ptr &other) : internal(other.internal) {
	}

	shared_ptr(std::shared_ptr<T> other) : internal(std::move(other)) {
	}
	shared_ptr(shared_ptr<T> &&other) : internal(std::move(other.internal)) {
	}

	template <class U>
	explicit shared_ptr(weak_ptr<U> other) : internal(other.internal) {
	}

#if _LIBCPP_STD_VER <= 14 || defined(_LIBCPP_ENABLE_CXX17_REMOVED_AUTO_PTR)
	template <class U, std::enable_if<std::is_convertible<U *, T *>::value, int> = 0>
	shared_ptr(std::auto_ptr<U> &&__r) : internal(__r.release()) {
	}
#endif

	template <class U, class DELETER, bool SAFE,
	          typename std::enable_if<__compatible_with<U, T>::value &&
	                                      std::is_convertible<typename unique_ptr<U, DELETER>::pointer, T *>::value,
	                                  int>::type = 0>
	shared_ptr(unique_ptr<U, DELETER, SAFE> &&other) : internal(other.release()) {
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
};

} // namespace duckdb
