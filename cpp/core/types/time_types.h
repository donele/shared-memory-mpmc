#pragma once

#include "time.h"

#include <chrono>
#include <iostream>
#include <sstream>

namespace sgt {

using TimePoint = std::chrono::system_clock::time_point;
using TimeDurationNano = std::chrono::nanoseconds;
using TimeDurationMicro = std::chrono::microseconds;
using TimeDurationMilli = std::chrono::milliseconds;
using TimeDurationSec = std::chrono::seconds;
using TimeDurationMin = std::chrono::minutes;
using TimeDurationHour = std::chrono::hours;
using Date = std::chrono::year_month_day;
using Days = std::chrono::days; 
using Hours = std::chrono::hours; 
//using SysDays = std::chrono::sys_days; 
inline TimePoint DateToTimepoint(const Date& date) {
    return std::chrono::sys_days(date);
}

inline Date DateFromDatestr(const std::string& datestr) {
    std::istringstream ss(datestr);
    std::tm tm0 = {};
    ss >> std::get_time(&tm0, "%Y%m%d");
    if(ss.fail()) {
        std::cerr << "Failed to parse: " << datestr << std::endl;
        exit(1);
    }
    auto tp = std::chrono::system_clock::from_time_t(mktime(&tm0));
    //SysDays d = std::chrono::floor<std::chrono::days>(tp);
    Date d = std::chrono::floor<std::chrono::days>(tp);
    return d;
}

inline TimeDurationSec DurationSecFromStr(const std::string& timestr) {
    std::tm tm0 = {};
    std::istringstream ss0("00:00:00");
    ss0 >> std::get_time(&tm0, "%H:%M:%S");

    if(ss0.fail()) {
        std::cerr << "Failed to parse: " << timestr << std::endl;
        exit(1);
    }

    std::tm tm1 = {};
    std::istringstream ss1(timestr);
    ss1 >> std::get_time(&tm1, "%H:%M:%S");

    if(ss1.fail()) {
        std::cerr << "Failed to parse: " << timestr << std::endl;
        exit(1);
    }
    auto tp0 = std::chrono::system_clock::from_time_t(mktime(&tm0));
    auto tp1 = std::chrono::system_clock::from_time_t(mktime(&tm1));
    TimeDurationSec duration = std::chrono::duration_cast<std::chrono::seconds>(tp1 - tp0);
    return duration;
}

inline Date AddDays(const Date& date, int days) {
    Date d = std::chrono::sys_days(date) + Days{days};
    return d;
}

inline TimePoint AddHours(const TimePoint& tp, int hours) {
    TimePoint d = tp + Hours{hours};
    return d;
}

inline TimePoint date_to_timepoint(const Date& date) {
    TimePoint tp = DateToTimepoint(date);
    return tp;
}

inline std::string ToString(const Date& date) {
    return std::format("{:%Y%m%d}", date);
}

inline std::string tp_to_datestr(const TimePoint& tp) {
    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
    std::tm tm0 = *std::gmtime(&tt);
    std::ostringstream oss;
    oss << std::put_time(&tm0, "%Y%m%d");
    std::string str = oss.str();
    return str;
}

inline std::string ToString(const TimePoint& tp) {
    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
    std::tm tm0 = *std::gmtime(&tt);
    std::ostringstream oss;
    oss << std::put_time(&tm0, "%Y%m%d %H:%M:%S");
    std::string str = oss.str();
    return str;
}

std::string TimeToStr(uint64_t uint_time);
std::string TimeNanoToStr(uint64_t uint_time);
std::string ToDatetime(uint64_t timestamp_us);

}
