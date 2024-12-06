#ifndef HELLO_CPP2_UTIL_H
#define HELLO_CPP2_UTIL_H

#include "def.h"
#include <cassert>
#include <cstdio>
#include <unistd.h>
#include <thread>
#include <ctime>
#include <string>
#include "cstring"
#include <netinet/in.h>
#include "sys/stat.h"
#include <sys/mman.h>
#include <fcntl.h>


static void fill_time_str(char *timeStr) {
    struct timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm *time_info = localtime(&ts.tv_sec);
    sprintf(timeStr, "%.4d-%.2d-%.2d %.2d:%.2d:%.2d.%.3ld",
            time_info->tm_year + 1900, time_info->tm_mon + 1, time_info->tm_mday, time_info->tm_hour, time_info->tm_min,
            time_info->tm_sec, ts.tv_nsec / 1000000);

}

static void log_debug(const char *cs) {
    char timeStr[24];
    fill_time_str(timeStr);
    std::thread::id thread_id = std::this_thread::get_id();
//    printf("%s %d : %s %s \n", timeStr, thread_id, __FUNCTION__, cs);
    printf("%s %d: %s \n", timeStr, thread_id, cs);
    fflush(stdout);
}

static void log_debug(const std::string str) {
    log_debug(str.c_str());
}

static void log_debug(const std::string str, uint64_t value) {
    std::string msg = str;
    msg.append(std::to_string(value));
    log_debug(msg.c_str());
}

static void log_debug_int(const std::string str, int value) {
    std::string msg = str;
    msg.append(std::to_string(value));
    log_debug(msg.c_str());
}

static void log_debug(const std::string str, const std::string str1, const std::string str2) {
    std::string msg = str;
    msg.append(str1);
    msg.append(str2);
    log_debug(msg.c_str());
}

static void log_debug(const std::string str, uint64_t value, const std::string str2, uint64_t value2) {
    std::string msg = str;
    msg.append(std::to_string(value));
    msg.append(str2);
    msg.append(std::to_string(value2));
    log_debug(msg.c_str());
}

static void log_debug(const std::string str, const char *cs) {
    std::string msg = str;
    msg.append(cs);
    log_debug(msg.c_str());
}

static void log_debug_double(const std::string str, double value) {
    std::string msg = str;
    msg.append(std::to_string(value));
    log_debug(msg.c_str());
}

static void print_row(const void *data) {
    std::string msg = "row: {";
    auto *arr = (uint64_t *) data;
    for (int i = 0; i < 34; ++i) {
        msg.append(std::to_string(arr[i]));
        msg.append(", ");
    }
    msg.append("}");
    log_debug(msg.c_str());
}

static void print_query(int32_t select_column, int32_t where_column, const void *column_key, size_t column_key_len) {
    auto *p = (uint64_t *) column_key;
    std::string msg = "query: select: ";
    msg.append(std::to_string(select_column));
    msg.append(", where: ");
    msg.append(std::to_string(where_column));
    msg.append(", key: {");
    auto size = column_key_len / 8;
    for (int i = 0; i < size; ++i) {
        msg.append(std::to_string(p[i]));
        msg.append(",");
    }
    msg.append("}");
    log_debug(msg.c_str());
}

static void fill_file(int fd, uint64_t size) {
    fallocate64(fd, 0, 0, size);
}

static uint64_t get_file_size(int fd) {
    struct stat stat_buf{};
    fstat(fd, &stat_buf);
    return stat_buf.st_size;
}

static void *init_mmap_file2(const char *file_name, int fd, uint64_t file_size, char *buf) {
    auto real_file_size = get_file_size(fd);
    if (real_file_size == 0) {
        lseek(fd, file_size - 1, SEEK_END);
        write(fd, "", 1);
        log_debug("init_mmap_file: ", file_name);
    }
    return mmap(buf, file_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
}

static void *init_mmap_file(const char *file_name, int fd, uint64_t file_size) {
    auto real_file_size = get_file_size(fd);
    if (real_file_size == 0) {
        lseek(fd, file_size - 1, SEEK_END);
        write(fd, "", 1);
        log_debug("init_mmap_file: ", file_name);
    }
    return mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
}

static void bind_core(const char *flag, int index) {
    cpu_set_t mask;
    auto core_index = index % 4;
    CPU_ZERO(&mask);
    CPU_SET(core_index, &mask);
    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) != 0) {
        assert(false);
    } else {
        if (OFFLINE) {
            std::string msg("bind_cpu_success: ");
            msg.append(flag);
            msg.append(", core_index: ");
            msg.append(std::to_string(core_index));
            log_debug(msg);
        }
    }
}

static void clean_work_dir(const std::string &path) {
    bool clean_dir = true;
    if (OFFLINE && clean_dir) {
        std::string cmd = "rm -rf ";
        cmd.append(path);
        cmd.append("*");
        log_debug(cmd);
        system(cmd.c_str());
    }
}

static uint64_t native_current_milliseconds() {
    struct timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static uint64_t current_milliseconds() {
    return native_current_milliseconds();
}

static uint64_t current_us_time() {
    struct timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000UL + ts.tv_nsec / 1000;
}

static uint64_t current_ns_time() {
    struct timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000000UL + ts.tv_nsec;
}

static void benchmark(std::string name, void (*func)()) {
    uint64_t start_time = current_milliseconds();
    (*func)();
    uint64_t cost = current_milliseconds() - start_time;
    std::string msg = "benchmark [";
    msg.append(name);
    msg.append("] cost: ");
    msg.append(std::to_string(cost));
    log_debug(msg);
}

static std::string column_name(int col) {
    if (col == 0) {
        return "Id";
    }
    if (col == 1) {
        return "UserId";
    }
    if (col == 2) {
        return "Name";
    }
    if (col == 3) {
        return "Salary";
    }
    return "error";
}

static uint32_t get_client_index(sockaddr_in *addr) {
    return addr->sin_addr.s_addr;
}

inline static void put_long(void *dst, uint64_t value) {
    ((uint64_t *) dst)[0] = value;
}

inline static void put_int(void *dst, uint32_t value) {
    ((uint32_t *) dst)[0] = value;
}

inline static void put_short(void *dst, uint16_t value) {
    ((uint16_t *) dst)[0] = value;
}

inline static void put_byte(void *dst, uint8_t value) {
    ((uint8_t *) dst)[0] = value;
}

inline static void sleep_and_log(int mill) {
    log_debug("sleep ", mill);
    std::this_thread::sleep_for(std::chrono::milliseconds(mill));
}

inline static int mix32(int k) {
    k = (k ^ k >> 16) * -2048144789;
    k = (k ^ k >> 13) * -1028477387;
    return k ^ k >> 16;
}

inline static uint64_t mix64(uint64_t z) {
    z = (z ^ (z >> 32)) * 0x4cd6944c5cc20b6dL;
    z = (z ^ (z >> 29)) * 0xfc12c5b19d3259e9L;
    return z ^ (z >> 32);
}

inline static uint64_t mix64_2(uint64_t z) {
    uint64_t h = z * 0x9e3779b97f4a7c15L;
    return (h ^ (h >> 32));
}

inline static uint32_t mix32(uint32_t k) {
    k = (k ^ (k >> 16)) * 0x85ebca6b;
    k = (k ^ (k >> 13)) * 0xc2b2ae35;
    return k ^ (k >> 16);
}

static uint64_t page_hash(uint64_t *buf) {
    uint64_t hash = 0;
    int count = PAGE_SIZE / 8;
    for (int i = 0; i < count; ++i) {
        hash += buf[i] * 31;
    }
    return hash;
}

inline double divide_d(double d1, double d2) {
    return d1 / d2;
}

static void store_value(const char *name, int value) {
    std::string file_name(root_path);
    file_name.append(name);
    auto fd = open(file_name.c_str(), O_CREAT | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR);
    auto buf = static_cast<int32_t *>(memalign(DIRECT_BLOCK, DIRECT_BLOCK));
    buf[0] = value;
    pwrite(fd, buf, DIRECT_BLOCK, 0);
    close(fd);
    free(buf);
}

static int read_value(const char *name, int default_value) {
    std::string file_name(root_path);
    file_name.append(name);
    int value = default_value;
    if (access(file_name.c_str(), F_OK) == 0) {
        auto fd = open(file_name.c_str(), O_CREAT | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR);
        auto buf = static_cast<int32_t *>(memalign(DIRECT_BLOCK, DIRECT_BLOCK));
        pread(fd, buf, DIRECT_BLOCK, 0);
        value = buf[0];
        close(fd);
        free(buf);
    }
    return value;
}

static int open_file_fd(const char * file_name){
    std::string full_path;
    full_path.append(root_path);
    full_path.append(file_name);
    return open(full_path.c_str(), O_CREAT | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR);
}

static int open_file_fd(const char * file_name, int index){
    std::string full_path;
    full_path.append(root_path);
    full_path.append(file_name);
    full_path.append(std::to_string(index));
    return open(full_path.c_str(), O_CREAT | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR);
}

#endif //HELLO_CPP2_UTIL_H
