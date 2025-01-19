//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"
#include <algorithm>
#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <stdexcept>

void TimestampColumnVector::add(std::string &value) {
    std::cout<<"TimestampColumnVector::add(std::string &value)"<<std::endl;
    std::tm time = {};
    int millis = 0, micros = 0;
    char t;

    // 解析时间戳字符串：%Y-%m-%d %H:%M:%S 格式
    std::istringstream ss(value);
    ss >> std::get_time(&time, "%Y-%m-%d %H:%M:%S");
    std::cout << "Parsed time: " << (1900 + time.tm_year) << "-" << (1 + time.tm_mon) << "-" << time.tm_mday 
              << " " << time.tm_hour << ":" << time.tm_min << ":" << time.tm_sec << std::endl;
    // 检查是否有额外的毫秒和微秒部分

    // 检查解析是否失败
    if (!ss.eof()) {
        std::cout << "Remaining data in stream: " << ss.str().substr(ss.tellg()) << std::endl;
        throw std::invalid_argument("Extra characters after timestamp!");
    }
    // 获取时间点（秒级别 Unix 时间戳）
    auto timePoint = std::chrono::system_clock::from_time_t(std::mktime(&time));

    // 计算自 1970-01-01 00:00:00 UTC 以来的微秒数
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(timePoint.time_since_epoch());

    // 计算时间戳的微秒值，加入毫秒和微秒部分
    add(duration.count() + millis * 1000 + micros);
}

// void TimestampColumnVector::add(bool value) {
//     add(value ? 1 : 0);
// }

void TimestampColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    times[index] = value;
    
    isNull[index] = false;
}

// void TimestampColumnVector::add(int value) {
//     if (writeIndex >= length) {
//         ensureSize(writeIndex * 2, true);
//     }
//     int index = writeIndex++;
//     times[index] = value;
//     isNull[index] = false;
// }

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
            long *oldVector = times;
            posix_memalign(reinterpret_cast<void **>(&times), 64,
                           size * sizeof(long));
            if (preserveData) {
                std::copy(oldVector, oldVector + length, times);
            }
            delete[] oldVector;
            memoryUsage += (long) sizeof(long) * (size - length);
            resize(size);

    }
}





TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    this->precision = precision;
    // if(encoding) {
    posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
    // } else {
        // this->times = nullptr;
    // }
}


void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
    isNull[elementNum] = false;
}