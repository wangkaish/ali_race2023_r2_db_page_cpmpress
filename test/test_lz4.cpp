//
// Created by test on 8/7/23.
//

#define ZSTD_STATIC_LINKING_ONLY

#include "stdio.h"
#include <iostream>
#include <fstream>
#include <cassert>
#include <cstring>
#include <sstream>
#include "malloc.h"
#include "../page_engine/util.h"
#include "../page_engine/zstd/zstd.h"
#include "../page_engine/compress.h"
#include "../page_engine/meta_index.h"
#include "../page_engine/flat_map.h"


void test_mmap_write() {
    int file_size = 1024 * 4;
    char *buf = static_cast<char *>(memalign(1024 * 4, file_size * 2));
    std::string file_name = "/home/test/temp/trace/aaa.txt";
    int buf_fd = open(file_name.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    char *buf_file_no_padding = (char *) init_mmap_file2(file_name.c_str(), buf_fd, file_size, buf);

    for (int i = 0; i < 10; ++i) {
        buf_file_no_padding[i] = 'a' + i;
    }
//    close(buf_fd);
}

void test_mmap_read() {
    int file_size = 1024 * 4;
    char *buf = static_cast<char *>(memalign(1024 * 4, file_size * 2));
    std::string file_name = "/home/test/temp/trace/aaa.txt";
    int buf_fd = open(file_name.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    char *buf_file_no_padding = (char *) init_mmap_file2(file_name.c_str(), buf_fd, file_size, buf);

    for (int i = 0; i < 10; ++i) {
        log_debug("i: ", i, ": ", buf_file_no_padding[i]);
    }
//    close(buf_fd);
}

void test_file_read() {
    std::string file_name = "/home/test/temp/trace/meta_data_0";
    int buf_fd = open(file_name.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    auto file_size = get_file_size(buf_fd);
    log_debug("file_size: ", file_size);
//    close(buf_fd);
}

void print_statm() {
//    std::string path = "/proc/self/statm";
//    FILE *file = fopen(path.c_str(), "r");
//    char buf[128];
//    int n = fread(buf, 1, 128, file);
//    std::string xx(buf, 0, n - 1);
//    log_debug(xx);
//    log_debug("current: ", get_rss_memory_used());

}

void test_read_proc_memory() {
    print_statm();
    log_debug("---------------------------------------1");
    char *buf[10];
    int count = 10;
    int block_size = 1024 * 128;

    for (int i = 0; i < count; ++i) {
        buf[i] = static_cast<char *>(memalign(DIRECT_BLOCK, block_size));
    }

    for (int i = 0; i < count; ++i) {
        print_statm();
        char *data = buf[i];
        for (int j = 0; j < block_size; ++j) {
            data[j] = 1;
        }
    }
    log_debug("---------------------------------------");

    for (int i = 0; i < count; ++i) {
        print_statm();
        free(buf[i]);
    }

    log_debug("---------------------------------------");
    print_statm();
}

void test_read_proc_memory2() {
    print_statm();
    log_debug("--------------------------------------- 2");
    int count = 10;
    int block_size = 1024 * 128;
    char *buf = static_cast<char *>(memalign(DIRECT_BLOCK, block_size * count));

    for (int i = 0; i < count; ++i) {
        print_statm();
        char *data = buf + block_size * i;
        for (int j = 0; j < block_size; ++j) {
            data[j] = 1;
        }
    }
    log_debug("---------------------------------------");

    for (int i = 1; i < count; ++i) {
        print_statm();
        int new_buf_len = block_size * (count - i);
        char *buf_old = buf;
        buf = static_cast<char *>(memalign(DIRECT_BLOCK, new_buf_len));
        memcpy(buf, buf_old, new_buf_len);
        free(buf_old);
    }

    log_debug("---------------------------------------");
    print_statm();
}

void test_read_page() {
    //page_no: 2, pos: 50952, compressed_size: 218, seg_read_index_1: 3, seg_read_index_2: 3, instance_index: 0
    std::string file_name = "/home/test/temp/trace/meta_data_0";
    uint64_t pos = 50952;
    int compressed_size = 218;
    int buf_fd = open(file_name.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    char compressed_buf[PAGE_SIZE];
    char result_buf[PAGE_SIZE];
    int read_ret = pread(buf_fd, compressed_buf, compressed_size, pos);
    log_debug("read_ret: ", read_ret);
    Compressor compressor;
    int decoded = compressor.decompress(compressed_buf, compressed_size, result_buf);
    log_debug("decoded: ", decoded);
    auto hash = page_hash((uint64_t *) result_buf);
    log_debug("hash: ", hash);
}

static uint32_t test_get_local_index(uint32_t *meta_index_array, uint32_t page_no) {
    int count = (int) meta_index_array[PAGE_COUNT_SLOT];
    uint32_t *_meta_index_array = meta_index_array + (PAGE_SIZE / 4) - count;
    for (int i = count - 4; i >= 0; i -= 2) {
        if (_meta_index_array[i] == page_no) {
            return _meta_index_array[i + 1];
        }
    }
    if (OFFLINE) {
        log_debug("get_local_index_error: ", page_no);
    }
    assert(false);
}

void test_read_page_hi() {
    // page_no: 1920, seg_read_index: 91, local_pos: 5964, local_len: 149, local_seg_write_index: 1, instance_index: 2, hash: 10091427282169328932
    // compressor.decompress( ((char *)(compressed_buf)) + 1024,7852,result_buf)
    uint32_t page_no = 1920;
    std::string file_name = "/home/test/temp/trace/meta_data_2";
    uint64_t pos = 91 << SEG_READ_MOVE;
    int buf_fd = open(file_name.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    char compressed_buf[PAGE_SIZE];
    uint32_t meta_buf[PAGE_SIZE / 4];
    char result_buf[PAGE_SIZE];
    int read_ret = pread(buf_fd, compressed_buf, PAGE_SIZE, pos);
    log_debug("read_ret: ", read_ret);
    memcpy(meta_buf, compressed_buf, PAGE_SIZE);
    auto local_index = test_get_local_index(meta_buf, page_no);
    auto local_pos = local_index >> 16;
    auto local_length = local_index & 0xFFFF;
    Compressor compressor;
    int decoded = compressor.decompress(compressed_buf + local_pos, local_length, result_buf);
    log_debug("decoded: ", decoded);
    auto hash = page_hash((uint64_t *) result_buf);
    log_debug("hash: ", hash);


}


static void read_meta_index() {
    std::string file_name = "/home/test/temp/trace/";
    root_path = file_name;

    MetaIndex meta_index;
    meta_index.init();

}

static void test_flat_map() {
    FlatMap map{};
    int count = FLAT_MAP_CAPACITY;
    for (int i = 1; i < count; ++i) {
        map.put(i, i);
    }
    for (int i = 1; i < count; ++i) {
        auto value = map.get(i);
        log_debug("key: ", i, ", value: ", value);
    }
    for (int i = 1; i < count; ++i) {
        map.remove(i);
    }

    log_debug("size: ", map.size());

}

int main(int argc, char *argv[]) {

//    log_debug("v: ", log2(8));
//    log_debug("v: ", bz3_bound(PAGE_SIZE));
//    test_bz32_raw();
//    test_bz3_low_level();
//    uint32_t x = 123456;
//    uint64_t x2 = x << 32;
//    uint64_t x3 = ((uint64_t) x) << 32;
//    log_debug("x2: ", x2);
//    log_debug("x3: ", x3);

    // log2 1024 = 10;

//    log_debug("page_size: ", log2(1024));
//
//    for (int i = 0; i < 15; ++i) {
//        std::cout << "static constexpr int log2_" << (1 << i) << "k = " << (10 + i) << "; \n";
//    }

//    test_mmap_write();
//    test_file_read();

//    test_read_proc_memory();

    test_read_page_hi();
    return 0;
}