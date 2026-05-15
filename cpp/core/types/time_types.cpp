#include "time_types.h"

namespace sgt {

std::string TimeToStr(uint64_t uint_time) {
    int64_t seconds = uint_time / 1000000;
    int64_t microseconds = uint_time % 1000000;
    auto time_point_seconds = std::chrono::system_clock::from_time_t(seconds);
    auto time_point = time_point_seconds + std::chrono::microseconds(microseconds);
    std::time_t time_t_val = std::chrono::system_clock::to_time_t(time_point);
    std::tm* tm_val = std::gmtime(&time_t_val);

    std::stringstream ss;
    ss << std::put_time(tm_val, "%F %T") << std::format(".{:06d}", microseconds);
    return ss.str();
}

std::string TimeNanoToStr(uint64_t uint_time) {
    int64_t seconds = uint_time / 1000000000;
    int64_t nanoseconds = uint_time % 1000000000;
    auto time_point_seconds = std::chrono::system_clock::from_time_t(seconds);
    auto time_point = time_point_seconds + std::chrono::nanoseconds(nanoseconds);
    std::time_t time_t_val = std::chrono::system_clock::to_time_t(time_point);
    std::tm* tm_val = std::gmtime(&time_t_val);

    std::stringstream ss;
    ss << std::put_time(tm_val, "%F %T") << std::format(".{:09d}", nanoseconds);
    return ss.str();
}

std::string ToDatetime(uint64_t timestamp_us) {
    auto tp = std::chrono::system_clock::time_point(std::chrono::microseconds(timestamp_us));
    const uint64_t microseconds = timestamp_us % 1000000;
    std::time_t time_t_val = std::chrono::system_clock::to_time_t(tp);
    std::tm tm_val = *std::gmtime(&time_t_val);
    std::ostringstream oss;
    oss << std::put_time(&tm_val, "%F %T") << std::format(".{:06d}", microseconds);
    return oss.str();
}

} // namespace sgt
