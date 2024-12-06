#ifndef HELLO_CPP2_DEF_H
#define HELLO_CPP2_DEF_H

#include <cstdint>
#include <cmath>
#include <fcntl.h>
#include "env.h"
#include <string>
#include "cstring"
#include "malloc.h"
#include "unistd.h"
#include "log2_constants.h"

struct DiskIndex {
    uint32_t page_no;
    uint32_t index;
};

static std::string root_path;

static constexpr int DIRECT_BLOCK = 512;
static constexpr int DIRECT_BLOCK_MOVE = log2_512;
static const int COMPRESS_LEVEL_HI_SPEED = -1;

static const int INSTANCE_COUNT = 12;
static const uint32_t PAGE_COUNT = 655360;
static const uint32_t PAGE_SIZE = 1024 * 16;
static const int SEG_WRITE_SIZE = 1024 * 32;
static const int SEG_WRITE_MOVE = log2(SEG_WRITE_SIZE);
static const int SEG_WRITE_SIZE_ALL = SEG_WRITE_SIZE + PAGE_SIZE;
static const int SEG_READ_SIZE = 1024 * 16;
static const int SEG_READ_MOVE = log2(SEG_READ_SIZE);
static const int META_BUF_SIZE = 1024 * 32;
static const int META_BUF_MOVE = log2(META_BUF_SIZE);
static const int MAX_META_SIZE = (META_BUF_SIZE / sizeof(DiskIndex));
static const int FLAT_MAP_CAPACITY = 1024;
static const int READ_CACHE_MOVE = SEG_READ_SIZE == SEG_WRITE_SIZE ? 0 : 1;
static const int CACHE_HIT_RATE_THRESHOLD = 80;

static const int HI_SEG_SIZE = 1024 * 16;
static const int HI_SEG_MOVE = log2(HI_SEG_SIZE);
static const int HI_SEG_SIZE_ALL = HI_SEG_SIZE + PAGE_SIZE;
static const int HI_PAGE_COUNT_SLOT = (HI_SEG_SIZE / 4) - 2;
static const int HI_SEG_KEY_COUNT = 256;
static const int HI_PAGE_META_PADDING = 1024 * 2;
static const int HI_LOCAL_META_COUNT = HI_PAGE_META_PADDING / 4;

static const bool SLEEP_WRITE = false;
static const bool RECYCLE_ENABLED = true;
static const bool BIND_CORE_ENABLED = false;
static const bool RW_BY_ABSTRACT_METHOD = true;

static bool HI_COMPRESS = false;
static int COMPRESS_LEVEL = 1;
static int STATIC_CACHE_SIZE = 0;
static int STATIC_CACHE_COUNT = 0;
static bool SAMPLE_COMPRESS_RATIO = false;

static const bool DEBUG = false;
static const uint32_t DEBUG_PAGE_NO = 2;

static void update_compress_ratio(int compress_ratio) {
    auto compress_ratio_low = compress_ratio >> 16;
    auto compress_ratio_hi = compress_ratio & 0xFFFF;
    if (compress_ratio_low < 100) {
        HI_COMPRESS = true;
        STATIC_CACHE_SIZE = 1024 * (1024 * 3);
    } else {
        STATIC_CACHE_SIZE = 1024 * (1024 * 2 + 512);
    }
    if (OFFLINE) {
        HI_COMPRESS = true;
        STATIC_CACHE_SIZE = 1024 * (64);
    }
    auto compress_compare = compress_ratio_hi * 100 / compress_ratio_low;
    if (compress_compare < 120) {
        COMPRESS_LEVEL = COMPRESS_LEVEL_HI_SPEED;
    }
    STATIC_CACHE_COUNT = STATIC_CACHE_SIZE / SEG_READ_SIZE;
//    HI_COMPRESS = true;
}

static uint64_t build_disk_index(uint64_t instance_index, uint64_t length, uint64_t file_pos) {
    return (instance_index << 56) | (length << 40) | file_pos;
}

static int get_instance_index_from_disk_index(uint64_t index) {
    return (int) (index >> 56);
}

static int get_length_from_disk_index(uint64_t index) {
    return (int) (index >> 40) & 0xFFFF;
}

static uint64_t get_file_pos_from_disk_index(uint64_t index) {
    return index & 0xFFFFFFFFFFL;
}


static uint32_t build_index_hi(uint32_t instance_index, uint32_t seg_index) {
    return (instance_index << 16) | seg_index;
}

static uint32_t get_instance_from_index_hi(uint32_t index) {
    return index >> 16;
}

static uint32_t get_seg_index_from_index_hi(uint32_t index) {
    return index & 0xFFFF;
}


static uint32_t build_cache_index(uint32_t instance_index, uint32_t seg_index) {
    return (instance_index << 25) | (1 << 24) | (seg_index);
}

static uint32_t get_instance_index_from_cache_index(uint32_t cache_index) {
    return (cache_index >> 25);
}

static uint32_t get_seg_index_from_cache_index(uint32_t cache_index) {
    return (cache_index & 0xFFFFFF);
}

#endif //HELLO_CPP2_DEF_H
