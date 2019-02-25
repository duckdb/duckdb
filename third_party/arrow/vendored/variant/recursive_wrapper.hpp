// Vendored from https://github.com/mapbox/variant at tag v1.1.5

#ifndef MAPBOX_UTIL_RECURSIVE_WRAPPER_HPP
#define MAPBOX_UTIL_RECURSIVE_WRAPPER_HPP

// Based on variant/recursive_wrapper.hpp from boost.
//
// Original license:
//
// Copyright (c) 2002-2003
// Eric Friedman, Itay Maman
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#include <cassert>
#include <utility>

namespace mapbox {
namespace util {

template <typename T>
class recursive_wrapper
{

    T* p_;

    void assign(T const& rhs)
    {
        this->get() = rhs;
    }

public:
    using type = T;

    /**
     * Default constructor default initializes the internally stored value.
     * For POD types this means nothing is done and the storage is
     * uninitialized.
     *
     * @throws std::bad_alloc if there is insufficient memory for an object
     *         of type T.
     * @throws any exception thrown by the default constructur of T.
     */
    recursive_wrapper()
        : p_(new T){}

    ~recursive_wrapper() noexcept { delete p_; }

    recursive_wrapper(recursive_wrapper const& operand)
        : p_(new T(operand.get())) {}

    recursive_wrapper(T const& operand)
        : p_(new T(operand)) {}

    recursive_wrapper(recursive_wrapper&& operand)
        : p_(new T(std::move(operand.get()))) {}

    recursive_wrapper(T&& operand)
        : p_(new T(std::move(operand))) {}

    inline recursive_wrapper& operator=(recursive_wrapper const& rhs)
    {
        assign(rhs.get());
        return *this;
    }

    inline recursive_wrapper& operator=(T const& rhs)
    {
        assign(rhs);
        return *this;
    }

    inline void swap(recursive_wrapper& operand) noexcept
    {
        T* temp = operand.p_;
        operand.p_ = p_;
        p_ = temp;
    }

    recursive_wrapper& operator=(recursive_wrapper&& rhs) noexcept
    {
        swap(rhs);
        return *this;
    }

    recursive_wrapper& operator=(T&& rhs)
    {
        get() = std::move(rhs);
        return *this;
    }

    T& get()
    {
        assert(p_);
        return *get_pointer();
    }

    T const& get() const
    {
        assert(p_);
        return *get_pointer();
    }

    T* get_pointer() { return p_; }

    const T* get_pointer() const { return p_; }

    operator T const&() const { return this->get(); }

    operator T&() { return this->get(); }

}; // class recursive_wrapper

template <typename T>
inline void swap(recursive_wrapper<T>& lhs, recursive_wrapper<T>& rhs) noexcept
{
    lhs.swap(rhs);
}
} // namespace util
} // namespace mapbox

#endif // MAPBOX_UTIL_RECURSIVE_WRAPPER_HPP
