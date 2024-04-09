namespace duckdb {

template <class T>
class enable_shared_from_this {
public:
	template <class U, bool SAFE>
	friend class shared_ptr;

private:
	mutable weak_ptr<T> __weak_this_;

protected:
	constexpr enable_shared_from_this() noexcept {
	}
	enable_shared_from_this(enable_shared_from_this const &) noexcept {
	}
	enable_shared_from_this &operator=(enable_shared_from_this const &) noexcept {
		return *this;
	}
	~enable_shared_from_this() {
	}

public:
	shared_ptr<T> shared_from_this() {
		return shared_ptr<T>(__weak_this_);
	}
	shared_ptr<T const> shared_from_this() const {
		return shared_ptr<const T>(__weak_this_);
	}

#if _LIBCPP_STD_VER >= 17
	weak_ptr<T> weak_from_this() noexcept {
		return __weak_this_;
	}

	weak_ptr<const T> weak_from_this() const noexcept {
		return __weak_this_;
	}
#endif // _LIBCPP_STD_VER >= 17
};

} // namespace duckdb
