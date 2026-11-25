#pragma once

#include <complex>
#include <cstdint>
#include <optional>
#include <string_view>

namespace dataflow {

enum class ScalarType : std::uint8_t {
    byte,
    i8,
    u8,
    i16,
    u16,
    i32,
    u32,
    f32,
    f64,
    c64,
    c128
};

template <typename... Ts> struct ScalarTypeList {};

template <typename T> struct ScalarID {};

template <> struct ScalarID<std::byte> {
    static constexpr auto value = ScalarType::byte;
    static constexpr std::string_view name = "byte";
};

template <> struct ScalarID<std::int8_t> {
    static constexpr auto value = ScalarType::i8;
    static constexpr std::string_view name = "i8";
};

template <> struct ScalarID<std::uint8_t> {
    static constexpr auto value = ScalarType::u8;
    static constexpr std::string_view name = "u8";
};

template <> struct ScalarID<std::int16_t> {
    static constexpr auto value = ScalarType::i16;
    static constexpr std::string_view name = "i16";
};

template <> struct ScalarID<std::uint16_t> {
    static constexpr auto value = ScalarType::u16;
    static constexpr std::string_view name = "u16";
};

template <> struct ScalarID<std::int32_t> {
    static constexpr auto value = ScalarType::i32;
    static constexpr std::string_view name = "i32";
};

template <> struct ScalarID<std::uint32_t> {
    static constexpr auto value = ScalarType::u32;
    static constexpr std::string_view name = "u32";
};

template <> struct ScalarID<float> {
    static constexpr auto value = ScalarType::f32;
    static constexpr std::string_view name = "f32";
};

template <> struct ScalarID<double> {
    static constexpr auto value = ScalarType::f64;
    static constexpr std::string_view name = "f64";
};

template <> struct ScalarID<std::complex<float>> {
    static constexpr auto value = ScalarType::c64;
    static constexpr std::string_view name = "c64";
};

template <> struct ScalarID<std::complex<double>> {
    static constexpr auto value = ScalarType::c128;
    static constexpr std::string_view name = "c128";
};

template <typename T> static constexpr auto scalar_id_v = ScalarID<T>::value;

using SupportedScalarTypes = ScalarTypeList<std::byte,
    std::int8_t,
    std::uint8_t,
    std::int16_t,
    std::uint16_t,
    std::int32_t,
    std::uint32_t,
    float,
    double,
    std::complex<float>,
    std::complex<double>>;

template <typename... Ts>
[[nodiscard]] inline std::optional<ScalarType> scalar_type_from_name(
    std::string_view name,
    ScalarTypeList<Ts...>) {
    std::optional<ScalarType> result;
    static_cast<void>(
        ((name == ScalarID<Ts>::name ? (result = ScalarID<Ts>::value, true)
                                     : false) ||
            ...));
    return result;
}

[[nodiscard]] inline std::optional<ScalarType> scalar_type_from_name(
    std::string_view name) {
    return scalar_type_from_name(name, SupportedScalarTypes{});
}

} // namespace dataflow
