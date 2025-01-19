//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"
#include <algorithm>
#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <ctime>
void DateColumnVector::add(std::string &value) {
    std::cout<<"DateColumnVector::add(std::string &value)"<<std::endl;
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }

    int year, month, day;
    if (!parseDate(value, year, month, day)) {
        std::cerr << "Invalid date format!" << std::endl;
        return;
    }

    auto days_since_epoch = calculateDaysSinceEpoch(year, month, day);
    add(days_since_epoch);
}

bool DateColumnVector::parseDate(const std::string &val, int &year, int &month, int &day) {
        std::istringstream ss(val);
        char ch1, ch2;
        ss >> year >> ch1 >> month >> ch2 >> day;
        return !(ss.fail() || ch1 != '-' || ch2 != '-');
}

int DateColumnVector::calculateDaysSinceEpoch(int year, int month, int day) {
    using namespace std::chrono;
        // 构建一个 tm 结构
    struct tm time = {};
    time.tm_year = year - 1900;  // 年份相对 1900
    time.tm_mon = month - 1;     // 月份从 0 开始
    time.tm_mday = day+1;

    time_t timestamp = mktime(&time);  // 将 tm 转换为时间戳
    if (timestamp == -1) {
        std::cerr << "Invalid date!" << std::endl;
        return -1;
    }

        // 计算自 Unix 纪元（1970-01-01）以来的天数
    time_t epoch_ts = 0;  // Unix 纪元
    auto diff = std::chrono::seconds(timestamp - epoch_ts);
    return duration_cast<std::chrono::hours>(diff).count() / 24;
}

void DateColumnVector::add(bool value) {
    add(value ? 1 : 0);
}

// void DateColumnVector::add(int64_t value) {
//     if (writeIndex >= length) {
//         ensureSize(writeIndex * 2, true);
//     }
//     int index = writeIndex++;
//     dates[index] = value;
//     isNull[index] = false;
// }

void DateColumnVector::add(int value) {
    std::cout<<"DateColumnVector::add(int value)"<<std::endl;
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    dates[index] = value;
    isNull[index] = false;
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    std::cout<<"DateColumnVector::ensureSize(uint64_t size, bool preserveData)"<<std::endl;
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
            int *oldVector = dates;
            posix_memalign(reinterpret_cast<void **>(&dates), 32,
                           size * sizeof(int));
            if (preserveData) {
                std::copy(oldVector, oldVector + length, dates);
            }
            delete[] oldVector;
            memoryUsage += (long) sizeof(long) * (size - length);
            resize(size);
    }
}




DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
	// if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	// } else {
	// 	this->dates = nullptr;
	// }
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
	if(elementNum >= writeIndex) {
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
	// TODO: isNull
    isNull[elementNum] = false;
}

void * DateColumnVector::current() {
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}
