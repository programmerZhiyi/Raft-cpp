#include "config.h"
#include <ctime>
#include <boost/serialization/extended_type_info_typeid.hpp>
#include <cstdio>
#include <iostream>
#include <thread>
#include <random>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

void DPrintf(const char *format, ...) {
    if (DEBUG) {
      // 获取当前的日期，然后取日志信息，写入相应的日志文件当中 a+
      time_t now = time(nullptr);
      tm *nowtm = localtime(&now);
      va_list args;
      va_start(args, format);
      std::printf("[%d-%d-%d-%d-%d-%d] ", nowtm->tm_year + 1900, nowtm->tm_mon + 1, nowtm->tm_mday, nowtm->tm_hour,
                  nowtm->tm_min, nowtm->tm_sec);
      std::vprintf(format, args);
      std::printf("\n");
      va_end(args);
    }
}

void myAssert(bool condition, std::string message) {
    if (!condition) {
      std::cerr << "Error: " << message << std::endl;
      std::exit(EXIT_FAILURE);
    }
}

std::chrono::_V2::system_clock::time_point now() {
    return std::chrono::high_resolution_clock::now(); // 获取当前时间
}

void sleepNMilliseconds(int N) {
    std::this_thread::sleep_for(std::chrono::milliseconds(N)); // 睡眠N毫秒
}

std::chrono::milliseconds getRandomizedElectionTimeout() {
    std::random_device rd; // 获取随机数种子
    std::mt19937 rng(rd()); // 随机数引擎
    std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime); // 随机数分布

    return std::chrono::milliseconds(dist(rng)); // 生成随机数
}

std::string Op::asString() const {
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);

    oa << *this;

    return ss.str();
}

bool Op::parseFromString(std::string str) {
    std::stringstream iss(str);
    boost::archive::text_iarchive ia(iss);
    ia >> *this;
    return true;
}

std::ostream& operator<<(std::ostream& os, const Op& obj) {
    os << "[MyClass:Operation{" + obj.Operation + "},Key{" + obj.Key + "},Value{" + obj.Value + "},ClientId{" +
            obj.ClientId + "},RequestId{" + std::to_string(obj.RequestId) + "}";  // 在这里实现自定义的输出格式
    return os;
}