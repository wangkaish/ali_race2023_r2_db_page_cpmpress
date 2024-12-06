// Copyright [2023] Alibaba Cloud All rights reserved

#include <mutex>
#include "dummy_engine.h"
#include "util.h"
#include "atomic"
#include "instance_proxy.h"

/*
 * Dummy sample of page engine
 */
int sleep_count = 0;
volatile bool sample_compress_ratio_pending = true;
std::atomic<bool> sync_flush_disk_lock = {false};
std::atomic<int> active_thread_count = {0};
std::atomic<int> active_read_thread_count = {0};
std::atomic<int> compressed_size_sample = {0};
std::atomic<int> compressed_size_hi_sample = {0};
std::atomic<int> sample_compress_ratio_active_thread_count = {INSTANCE_COUNT};

void (*v_page_read)(uint32_t, char *);

void (*v_page_write)(uint32_t, char *);

thread_local InstanceProxy *instance = nullptr;

void page_write_hi(uint32_t page_no, char *buf) {
    instance->page_write_hi(page_no, buf);
}

void page_write_low(uint32_t page_no, char *buf) {
    instance->page_write_low(page_no, buf);
}

void page_read_hi(uint32_t page_no, char *buf) {
    instance->page_read_hi(page_no, buf);
}

void page_read_low(uint32_t page_no, char *buf) {
    instance->page_read_low(page_no, buf);
}

void update_read_write_fun() {
    if (RW_BY_ABSTRACT_METHOD) {
        if (HI_COMPRESS) {
            v_page_read = &page_read_hi;
            v_page_write = &page_write_hi;
        } else {
            v_page_read = &page_read_low;
            v_page_write = &page_write_low;
        }
    }
}

RetCode PageEngine::Open(const std::string &path, PageEngine **eptr) {
    return DummyEngine::Open(path, eptr);
}

RetCode DummyEngine::Open(const std::string &path, PageEngine **eptr) {
    auto *engine = new DummyEngine(path);
    *eptr = engine;
    return kSucc;
}

DummyEngine::DummyEngine(const std::string &path) {
    this->path = path;
    if (OFFLINE) {
        log_debug("pid: ", getpid());
    }
    root_path = path;
    meta_index.init();
    if (!SAMPLE_COMPRESS_RATIO) {
        for (int i = 0; i < INSTANCE_COUNT; ++i) {
            instances[i].init(i);
        }
        update_read_write_fun();
    }
}

DummyEngine::~DummyEngine() {
    if (OFFLINE) {
        log_debug("all_compressed_size: ", all_compressed_size);
    }
}

void select_instance() {
    static std::atomic<int> instance_auto = {0};
    auto index = instance_auto.fetch_add(1);
    instance = &instances[index];
    instance->instance_index = index;
    if (BIND_CORE_ENABLED) {
        std::string str("instance_");
        str.append(std::to_string(index));
        bind_core(str.c_str(), index);
    }
    if (OFFLINE) {
        log_debug("select_file_channel: ", index);
    }
}

void sync_flush_disk() {
    bool old = false;
    if (sync_flush_disk_lock.compare_exchange_weak(old, true)) {
        if (sleep_count == 3) {
            usleep(1);
        } else {
            usleep(10);
        }
        if (active_thread_count == 0) {
            if (sleep_count < 3) {
                sleep_count++;
            }
            if (OFFLINE) {
                log_debug("start_idle_flush__________________________");
            }
            for (int i = 0; i < INSTANCE_COUNT; ++i) {
                instances[i].idle_flush_meta_data();
            }
            auto meta_instance = &instances[0];
            for (int i = 1; i < INSTANCE_COUNT; ++i) {
                meta_instance->copy_meta_index(&instances[i]);
            }
            meta_instance->idle_flush_meta_index();
            if (OFFLINE) {
                log_debug("finish_idle_flush__________________________");
            }
        } else {
            if (sleep_count != 0) {
                sleep_count = 0;
            }
        }
        sync_flush_disk_lock.store(false);
    } else {
        if (sleep_count != 0) {
            sleep_count = 0;
        }
    }
}

void flush_disk() {
    if (active_thread_count.fetch_add(-1) == 1) {
        sync_flush_disk();
    }
}

void wait_for_flush_disk() {
    active_thread_count.fetch_add(1);
    if (sync_flush_disk_lock) {
        for (; sync_flush_disk_lock;) {
            usleep(10);
        }
    }
}

void do_sample_compress_ratio(char *src_buf) {
    if (SAMPLE_COMPRESS_RATIO) {
        char temp_buf[PAGE_SIZE];
        auto compressor = instance->compressor;
        auto compressed_size_low = compressor->compress(src_buf, PAGE_SIZE, temp_buf, 1);
        auto compressed_size_hi = compressor->compress(src_buf, PAGE_SIZE, temp_buf, COMPRESS_LEVEL_HI_SPEED);
        compressed_size_sample.fetch_add(compressed_size_low);
        compressed_size_hi_sample.fetch_add(compressed_size_hi);
        auto num = sample_compress_ratio_active_thread_count.fetch_add(-1);
        if (num == 1) {
            auto compress_ratio_low = (int) (compressed_size_sample * 1000 / (INSTANCE_COUNT * PAGE_SIZE));
            auto compress_ratio_hi = (int) (compressed_size_hi_sample * 1000 / (INSTANCE_COUNT * PAGE_SIZE));
            auto complex_ratio = (compress_ratio_low << 16) | compress_ratio_hi;
            update_compress_ratio(complex_ratio);
            store_value("compress_ratio", complex_ratio);
            meta_index.allocate_index_memory();
            for (int i = 0; i < INSTANCE_COUNT; ++i) {
                instances[i].init(i);
            }
            update_read_write_fun();
            sample_compress_ratio_pending = false;
            if (OFFLINE) {
                std::string str("compress_ratio_low: ");
                str.append(std::to_string(compress_ratio_low));
                str.append(", compress_ratio_hi: ");
                str.append(std::to_string(compress_ratio_hi));
                str.append(", compress_level: ");
                str.append(std::to_string(COMPRESS_LEVEL));
                str.append(", hi_compress: ");
                str.append(std::to_string(HI_COMPRESS));
                log_debug(str.c_str());
            }
        } else {
            for (; sample_compress_ratio_pending;) {
                usleep(1);
            }
        }
    }
}

RetCode DummyEngine::pageWrite(uint32_t page_no, const void *buf) {
    if (instance == nullptr) {
        select_instance();
        do_sample_compress_ratio((char *) buf);
    }
    if (SLEEP_WRITE) {
        if (active_read_thread_count >= 8) {
            usleep(1000 * 100);
        }
    }
    wait_for_flush_disk();
    if (RW_BY_ABSTRACT_METHOD) {
        (v_page_write)(page_no, (char *) buf);
    } else {
        instance->page_write(page_no, (char *) buf);
    }
    flush_disk();
    return kSucc;
}

RetCode DummyEngine::pageRead(uint32_t page_no, void *buf) {
    if (instance == nullptr) {
        select_instance();
    }
    if (SLEEP_WRITE) {
        active_read_thread_count.fetch_add(1);
    }
    wait_for_flush_disk();
    if (RW_BY_ABSTRACT_METHOD) {
        (v_page_read)(page_no, (char *) buf);
    } else {
        instance->page_read(page_no, (char *) buf);
    }
    if (SLEEP_WRITE) {
        active_read_thread_count.fetch_add(-1);
    }
    flush_disk();
    return kSucc;
}
