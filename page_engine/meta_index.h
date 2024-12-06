//
// Created by test on 23-10-4.
//

#ifndef PAGE_ENGINE_META_INDEX_H
#define PAGE_ENGINE_META_INDEX_H

#include "atomic"
#include "def.h"
#include <string>
#include <fcntl.h>
#include "util.h"

class MetaIndex {

public:

    int meta_fd = -1;
    int last_meta_count = 0;
    std::atomic<int> meta_seg_sequence = {0};
    std::atomic<uint32_t> *meta_array4;
    std::atomic<uint64_t> *meta_array8;
    DiskIndex *meta_index_buf;
    uint64_t instance_file_size_array[INSTANCE_COUNT];

    void init() {
        auto start_time = native_current_milliseconds();
        this->meta_fd = open_file_fd("meta_index");
        auto meta_file_size = get_file_size(meta_fd);

        if (meta_file_size > 0) {
            SAMPLE_COMPRESS_RATIO = false;
            int compress_ratio = read_value("compress_ratio", -1);
            if (compress_ratio == -1) {
                assert(false);
            } else {
                update_compress_ratio(compress_ratio);
            }
            allocate_index_memory();
            auto meta_buf_count = meta_file_size >> META_BUF_MOVE;
            if (HI_COMPRESS) {
                for (int i = 0; i < meta_buf_count; ++i) {
                    pread(meta_fd, meta_index_buf, META_BUF_SIZE, i << META_BUF_MOVE);
                    last_meta_count = recover_meta_index_hi(meta_index_buf);
                }
            } else {
                for (int i = 0; i < meta_buf_count; ++i) {
                    pread(meta_fd, meta_index_buf, META_BUF_SIZE, i << META_BUF_MOVE);
                    last_meta_count = recover_meta_index_low(meta_index_buf);
                }
            }
            if (last_meta_count == MAX_META_SIZE) {
                last_meta_count = 0;
                memset(meta_index_buf, 0, META_BUF_SIZE);
            }
            this->meta_seg_sequence = meta_buf_count;
        } else {
            SAMPLE_COMPRESS_RATIO = true;
        }
        auto end_time = native_current_milliseconds();
        if (OFFLINE) {
            log_debug("init_meta_index_cost: ", (end_time - start_time));
        }
    }

    void allocate_index_memory() {
        meta_index_buf = static_cast<DiskIndex *>(memalign(DIRECT_BLOCK, META_BUF_SIZE));
        if (HI_COMPRESS) {
            meta_array4 = reinterpret_cast<std::atomic<uint32_t> *>(malloc(PAGE_COUNT * 4));
        } else {
            meta_array8 = reinterpret_cast<std::atomic<uint64_t> *>(malloc(PAGE_COUNT * 8));
        }
    }

    int recover_meta_index_hi(DiskIndex *item_array) const {
        int count = 0;
        std::string str;
        for (int i = 0; i < MAX_META_SIZE; ++i) {
            auto item = &item_array[i];
            auto raw_page_no = item->page_no;
            if (raw_page_no != 0) {
                count++;
                auto page_no = raw_page_no >> 1;
                auto index = item->index;
                set_index_hi(page_no, index);
                if (OFFLINE) {
                    str.append(std::to_string(page_no));
                    str.append(": ");
                    str.append(std::to_string(get_file_pos_from_disk_index(index)));
                    str.append(", ");
                }
            } else {
                break;
            }
        }
        if (OFFLINE) {
//            log_debug(str);
        }
        return count;
    }

    int recover_meta_index_low(DiskIndex *item_array) {
        int count = 0;
        std::string str;
        for (int i = 0; i < MAX_META_SIZE; ++i) {
            auto item = &item_array[i];
            auto raw_page_no = item->page_no;
            if (raw_page_no != 0) {
                count++;
                auto page_no = raw_page_no >> 1;
                auto raw_index = item->index;
                auto instance_index = raw_index >> 16;
                auto length = raw_index & 0xFFFF;
                auto index = build_disk_index(instance_index, length, instance_file_size_array[instance_index]);
                instance_file_size_array[instance_index] += length;
                set_index_low(page_no, index);
                if (OFFLINE) {
                    str.append(std::to_string(page_no));
                    str.append(": ");
                    str.append(std::to_string(get_file_pos_from_disk_index(index)));
                    str.append(", ");
                }
            } else {
                break;
            }
        }
        if (OFFLINE) {
//            log_debug(str);
        }
        return count;
    }

    uint64_t get_instance_file_size(int instance_index) {
        return instance_file_size_array[instance_index];
    }

    void set_index_low(int page_no, uint64_t value) const {
        meta_array8[page_no] = value;
    }

    uint64_t get_index_low(int page_no) const {
        return meta_array8[page_no];
    }

    void set_index_hi(int page_no, uint32_t index) const {
        meta_array4[page_no] = index;
    }

    uint32_t get_index_hi(int page_no) const {
        return meta_array4[page_no];
    }

    int get_meta_fd() const {
        return meta_fd;
    }

    uint64_t get_next_seg_index() {
        return this->meta_seg_sequence.fetch_add(1);
    }

};


#endif //PAGE_ENGINE_META_INDEX_H
