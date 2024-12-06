//
// Created by test on 8/21/22.
//

#ifndef INTERFACE_SHARED_DATA_H
#define INTERFACE_SHARED_DATA_H

#include "def.h"
#include "atomic"
#include "util.h"
#include "meta_index.h"

static MetaIndex meta_index;

static const int CASE_1000 = ONLINE ? 1000 : 200;

static int data_fd_array[INSTANCE_COUNT];

volatile bool cache_enabled = true;
volatile bool sample_read = true; //TODO not volatile
volatile int disk_hit_1000 = 0;

std::atomic<int> page_read_count = {0};
std::atomic<int> disk_read_count = {0};
std::atomic<int> all_compressed_size = {0};

void sample_disk_read() {
    if (sample_read) {
        disk_read_count.fetch_add(1);
    }
}

void print_read_log() {
    if (OFFLINE) {
        std::string str("sample_read: disk_hit_1000: ");
        str.append(std::to_string(disk_hit_1000));
        log_debug(str.c_str());
    }
}

void sample_page_read() {
    if (sample_read) {
        auto r_cnt = page_read_count.fetch_add(1);
        if (r_cnt == CASE_1000) {
            disk_hit_1000 = disk_read_count;
            cache_enabled = disk_hit_1000 < 1000;
//            print_read_log();
            sample_read = false;
        }
    }
}

#endif //INTERFACE_SHARED_DATA_H
