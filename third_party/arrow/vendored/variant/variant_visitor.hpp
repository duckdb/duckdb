// Vendored from https://github.com/mapbox/variant at tag v1.1.5

#ifndef MAPBOX_UTIL_VARIANT_VISITOR_HPP
#define MAPBOX_UTIL_VARIANT_VISITOR_HPP

namespace mapbox {
namespace util {

template <typename... Fns>
struct visitor;

template <typename Fn>
struct visitor<Fn> : Fn
{
    using type = Fn;
    using Fn::operator();

    visitor(Fn fn) : Fn(fn) {}
};

template <typename Fn, typename... Fns>
struct visitor<Fn, Fns...> : Fn, visitor<Fns...>
{
    using type = visitor;
    using Fn::operator();
    using visitor<Fns...>::operator();

    visitor(Fn fn, Fns... fns) : Fn(fn), visitor<Fns...>(fns...) {}
};

template <typename... Fns>
visitor<Fns...> make_visitor(Fns... fns)
{
    return visitor<Fns...>(fns...);
}

} // namespace util
} // namespace mapbox

#endif // MAPBOX_UTIL_VARIANT_VISITOR_HPP
