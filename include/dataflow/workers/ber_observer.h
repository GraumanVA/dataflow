#pragma once

#include <dataflow/observer.h>
#include <dataflow/interfaces/worker_factory.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace dataflow {

struct BerObserverConfig {
    std::size_t transaction_size = 1;
    std::size_t bits_per_element = 0;
    std::string stop_mode = "bit_errors";
    std::uint64_t target_bit_errors = 100;
    double confidence = 0.95;
    double max_half_width = 1e-4;
    std::uint64_t min_bits = 100;
};

enum class BerStopMode {
    bit_errors,
    confidence_interval
};

template <typename T>
    requires(std::is_integral_v<T> || std::is_same_v<T, std::byte>)
class BerObserver final : public ConfiguredSizedTypedObserver<BerObserver<T>,
                              BerObserverConfig,
                              T,
                              T> {
    struct Accumulator {
        std::uint64_t bits{};
        std::uint64_t errors{};
    };

    BerStopMode mode_;
    Accumulator accumulator_;

    static BerStopMode parse_mode(const std::string &mode) {
        if (mode == "bit_errors") {
            return BerStopMode::bit_errors;
        }
        if (mode == "confidence_interval") {
            return BerStopMode::confidence_interval;
        }
        throw std::invalid_argument(
            "BER stop_mode must be 'bit_errors' or 'confidence_interval'");
    }

    static double normal_quantile(double probability) {
        // Peter J. Acklam's inverse-normal rational approximation.
        constexpr double a1 = -3.969683028665376e+01;
        constexpr double a2 = 2.209460984245205e+02;
        constexpr double a3 = -2.759285104469687e+02;
        constexpr double a4 = 1.383577518672690e+02;
        constexpr double a5 = -3.066479806614716e+01;
        constexpr double a6 = 2.506628277459239e+00;
        constexpr double b1 = -5.447609879822406e+01;
        constexpr double b2 = 1.615858368580409e+02;
        constexpr double b3 = -1.556989798598866e+02;
        constexpr double b4 = 6.680131188771972e+01;
        constexpr double b5 = -1.328068155288572e+01;
        constexpr double c1 = -7.784894002430293e-03;
        constexpr double c2 = -3.223964580411365e-01;
        constexpr double c3 = -2.400758277161838e+00;
        constexpr double c4 = -2.549732539343734e+00;
        constexpr double c5 = 4.374664141464968e+00;
        constexpr double c6 = 2.938163982698783e+00;
        constexpr double d1 = 7.784695709041462e-03;
        constexpr double d2 = 3.224671290700398e-01;
        constexpr double d3 = 2.445134137142996e+00;
        constexpr double d4 = 3.754408661907416e+00;
        constexpr double low = 0.02425;
        constexpr double high = 1.0 - low;

        if (probability < low) {
            const auto q = std::sqrt(-2.0 * std::log(probability));
            return (((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6) /
                   ((((d1 * q + d2) * q + d3) * q + d4) * q + 1.0);
        }
        if (probability <= high) {
            const auto q = probability - 0.5;
            const auto r = q * q;
            return (((((a1 * r + a2) * r + a3) * r + a4) * r + a5) * r + a6) *
                   q /
                   (((((b1 * r + b2) * r + b3) * r + b4) * r + b5) * r + 1.0);
        }
        const auto q = std::sqrt(-2.0 * std::log(1.0 - probability));
        return -(((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6) /
               ((((d1 * q + d2) * q + d3) * q + d4) * q + 1.0);
    }

    [[nodiscard]] std::pair<double, double> confidence_interval() const {
        const auto totals = accumulator_;
        const auto &config = this->observer_config();
        if (totals.bits == 0) {
            return {0.0, 1.0};
        }
        const auto n = static_cast<double>(totals.bits);
        const auto probability = static_cast<double>(totals.errors) / n;
        const auto z = normal_quantile(0.5 + config.confidence / 2.0);
        const auto z_squared = z * z;
        const auto denominator = 1.0 + z_squared / n;
        const auto center = (probability + z_squared / (2.0 * n)) / denominator;
        const auto half_width =
            z *
            std::sqrt(probability * (1.0 - probability) / n +
                      z_squared / (4.0 * n * n)) /
            denominator;
        return {std::max(0.0, center - half_width),
            std::min(1.0, center + half_width)};
    }

    static std::string format_rate(double value) {
        std::ostringstream output;
        output << std::scientific << std::setprecision(6) << value;
        return output.str();
    }

    [[nodiscard]] static constexpr std::size_t storage_bits_per_element()
        noexcept {
        return sizeof(T) * 8U;
    }

    [[nodiscard]] static std::size_t bits_per_element(
        const BerObserverConfig &config) noexcept {
        return config.bits_per_element == 0 ? storage_bits_per_element()
                                            : config.bits_per_element;
    }

    template <typename Unsigned>
    static unsigned int count_low_bit_errors(Unsigned difference,
        std::size_t used_bits) noexcept {
        constexpr auto width = sizeof(Unsigned) * 8U;
        if (used_bits < width) {
            const auto mask =
                static_cast<Unsigned>((Unsigned{1} << used_bits) - 1U);
            difference = static_cast<Unsigned>(difference & mask);
        }
        return std::popcount(difference);
    }

    static unsigned int bit_errors(T left,
        T right,
        std::size_t used_bits) noexcept {
        if constexpr (std::is_same_v<T, std::byte>) {
            return count_low_bit_errors(
                std::to_integer<std::uint8_t>(left ^ right), used_bits);
        } else {
            using Unsigned = std::make_unsigned_t<T>;
            const auto difference = static_cast<Unsigned>(
                static_cast<Unsigned>(left) ^ static_cast<Unsigned>(right));
            return count_low_bit_errors(difference, used_bits);
        }
    }

public:
    using Config = BerObserverConfig;
    using Base =
        ConfiguredSizedTypedObserver<BerObserver<T>, BerObserverConfig, T, T>;
    using typename Base::input_view;
    using typename Base::output_view;

    static void validate(const BerObserverConfig &config) {
        const auto mode = parse_mode(config.stop_mode);
        if (config.transaction_size == 0) {
            throw std::invalid_argument(
                "BER transaction size must be positive");
        }
        if (config.bits_per_element > storage_bits_per_element()) {
            throw std::invalid_argument(
                "BER bits_per_element exceeds the input element width");
        }
        if (mode == BerStopMode::bit_errors && config.target_bit_errors == 0) {
            throw std::invalid_argument(
                "BER target_bit_errors must be positive");
        }
        if (mode == BerStopMode::confidence_interval) {
            if (!(config.confidence > 0.0 && config.confidence < 1.0)) {
                throw std::invalid_argument(
                    "BER confidence must be between zero and one");
            }
            if (!(config.max_half_width > 0.0)) {
                throw std::invalid_argument(
                    "BER max_half_width must be positive");
            }
            if (config.min_bits == 0) {
                throw std::invalid_argument("BER min_bits must be positive");
            }
        }
    }

    [[nodiscard]] static WorkerPorts ports(const BerObserverConfig &config) {
        return {{port<T>("reference", config.transaction_size),
                    port<T>("received", config.transaction_size)},
            {}};
    }

    explicit BerObserver(const BerObserverConfig &config)
        : Base(config), mode_(parse_mode(config.stop_mode)) {
    }

    void work_one(std::span<input_view> inputs,
        std::span<output_view>) override {
        const auto &config = this->observer_config();
        const auto transaction_size = config.transaction_size;
        const auto used_bits = bits_per_element(config);
        for (std::size_t i{}; i < transaction_size; ++i) {
            accumulator_.errors +=
                bit_errors(inputs[0][i], inputs[1][i], used_bits);
        }
        accumulator_.bits +=
            static_cast<std::uint64_t>(transaction_size) * used_bits;
    }

    void merge_from_same(const BerObserver &other) {
        accumulator_.bits += other.accumulator_.bits;
        accumulator_.errors += other.accumulator_.errors;
    }

    [[nodiscard]] bool criterion_satisfied() const noexcept override {
        const auto totals = accumulator_;
        const auto &config = this->observer_config();
        if (mode_ == BerStopMode::bit_errors) {
            return totals.errors >= config.target_bit_errors;
        }
        if (totals.bits < config.min_bits || totals.bits == 0) {
            return false;
        }
        const auto interval = confidence_interval();
        return (interval.second - interval.first) / 2.0 <=
               config.max_half_width;
    }

    [[nodiscard]] bool has_completion_criterion() const noexcept override {
        return true;
    }

    [[nodiscard]] std::vector<StatisticValue> statistics() const override {
        const auto totals = accumulator_;
        const auto rate = totals.bits == 0
                              ? 0.0
                              : static_cast<double>(totals.errors) /
                                    static_cast<double>(totals.bits);
        std::vector<StatisticValue> result{
            {"bits", std::to_string(totals.bits)},
            {"bit_errors", std::to_string(totals.errors)},
            {"ber", format_rate(rate)}};
        if (mode_ == BerStopMode::bit_errors) {
            return result;
        }
        const auto interval = confidence_interval();
        result.push_back({"ci_lower", format_rate(interval.first)});
        result.push_back({"ci_upper", format_rate(interval.second)});
        return result;
    }

    [[nodiscard]] std::string criterion_description() const override {
        const auto &config = this->observer_config();
        if (mode_ == BerStopMode::bit_errors) {
            return "bit errors >= " + std::to_string(config.target_bit_errors);
        }
        return std::to_string(config.confidence * 100.0) +
               "% Wilson CI half-width <= " +
               format_rate(config.max_half_width) + ", min bits " +
               std::to_string(config.min_bits);
    }
};

} // namespace dataflow
