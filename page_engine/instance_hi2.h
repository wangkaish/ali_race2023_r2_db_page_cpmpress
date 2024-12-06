//
// Created by wangkai on 2022/7/13.
//

#ifndef INTERFACE_INSTANCE_HI2_H
#define INTERFACE_INSTANCE_HI2_H

#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include "sys/stat.h"
#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include "sys/stat.h"
#include "malloc.h"
#include <cstring>
#include <sys/mman.h>
#include <cmath>
#include "def.h"
#include "util.h"
#include "compress.h"
#include "shared_data.h"
#include "meta_index.h"
#include "flat_map.h"


class InstanceHi2 {
public:


    int page_count = 0;
    int meta_data_fd = -1;
    int instance_index = 0;
    int file_buf_size = 0;
    int persisted_file_buf_size = 0;
    int meta_index_size = 0;
    int persisted_meta_index_size = 0;
    int meta_seg_index = -1;
    uint32_t cache_slot_seq = 0;
    uint64_t seg_write_index = 0;
    DiskIndex *index_write_buf;
    char *data_write_buf = static_cast<char *>(memalign(DIRECT_BLOCK, HI_SEG_SIZE_ALL));
    uint32_t *data_meta_buf;
    char *cache_buf = static_cast<char *>(memalign(DIRECT_BLOCK, STATIC_CACHE_SIZE));
    FlatMap seg_key_map;
    int64_t seg_key_array[HI_SEG_KEY_COUNT];

    void init(int index) {
        this->instance_index = index;
        if (index == 0) {
            this->meta_seg_index = meta_index.meta_seg_sequence - 1;
            this->meta_index_size = meta_index.last_meta_count;
            this->index_write_buf = meta_index.meta_index_buf;
        } else {
            this->index_write_buf = static_cast<DiskIndex *>(memalign(DIRECT_BLOCK, META_BUF_SIZE));
        }

        this->meta_data_fd = open_file_fd("meta_data_", index);

        data_fd_array[index] = meta_data_fd;

        for (int j = 0; j < HI_SEG_KEY_COUNT; ++j) {
            seg_key_array[j] = -1;
        }
        auto file_size = get_file_size(meta_data_fd);
        this->seg_write_index = file_size >> HI_SEG_MOVE;
        this->data_meta_buf = (uint32_t *) (this->data_write_buf + HI_SEG_SIZE_ALL - HI_PAGE_META_PADDING);

        if (OFFLINE) {
            std::string str = "recover_index: ";
            str.append(std::to_string(index));
            str.append(", seg_write_index: ");
            str.append(std::to_string(this->seg_write_index));
            str.append(", file_pos: ");
            str.append(std::to_string(file_size));
            str.append(", STATIC_CACHE_COUNT: ");
            str.append(std::to_string(STATIC_CACHE_COUNT));
            log_debug(str.c_str());
        }
    }

    inline int next_cache_index() {
        uint32_t next_index = cache_slot_seq++;
        return (int) (next_index % STATIC_CACHE_COUNT);
    }

    inline void idle_flush_meta_data() {
        if (this->persisted_file_buf_size < this->file_buf_size) {
            auto old_persisted_file_buf_size = persisted_file_buf_size;
            this->persisted_file_buf_size = this->file_buf_size;
            copy_meta_to_data(this->data_write_buf);
            pwrite(meta_data_fd, this->data_write_buf, HI_SEG_SIZE, this->seg_write_index << HI_SEG_MOVE);
            if (OFFLINE) {
                std::string str("idle_data_write: offset: ");
                str.append(std::to_string((this->seg_write_index << HI_SEG_MOVE)));
                str.append(", persisted_file_buf_size: ");
                str.append(std::to_string(old_persisted_file_buf_size));
                str.append(", file_buf_size: ");
                str.append(std::to_string(file_buf_size));
                str.append(", instance_index: ");
                str.append(std::to_string(instance_index));
                log_debug(str);
            }
        }
    }

    inline void clean_meta_index_buf() {
        this->meta_index_size = 0;
        this->persisted_meta_index_size = 0;
        memset(index_write_buf, 0, META_BUF_SIZE);
    }

    inline void copy_meta_index(InstanceHi2 *p_instance) {
        auto p_index_size = p_instance->meta_index_size;
        if (p_index_size > 0) {
            for (int i = 0; i < p_index_size; ++i) {
                write_meta_index(&p_instance->index_write_buf[i]);
            }
            p_instance->clean_meta_index_buf();
            if (OFFLINE) {
                log_debug("copy_meta_index_size: ", p_index_size);
            }
        }
    }

    inline void write_meta_index(DiskIndex *p_item) {
        auto item = &index_write_buf[meta_index_size++];
        item->page_no = p_item->page_no;
        item->index = p_item->index;
        if (meta_index_size == MAX_META_SIZE) {
            flush_meta_index();
        }
    }

    inline void idle_flush_meta_index() {
        if (this->persisted_meta_index_size < this->meta_index_size) {
            int meta_seg_write_index;
            if (meta_seg_index == -1) {
                meta_seg_write_index = meta_index.get_next_seg_index();
                this->meta_seg_index = meta_seg_write_index;
            } else {
                meta_seg_write_index = meta_seg_index;
            }
            auto old_persisted_meta_index_size = persisted_meta_index_size;

            this->persisted_meta_index_size = this->meta_index_size;
            pwrite(meta_index.get_meta_fd(), index_write_buf, META_BUF_SIZE, meta_seg_write_index << META_BUF_MOVE);
            if (OFFLINE) {
                std::string str("idle_meta_write: seg_index: ");
                str.append(std::to_string(meta_seg_write_index));
                str.append(", persisted_meta_index_size: ");
                str.append(std::to_string(old_persisted_meta_index_size));
                str.append(", meta_buf_size: ");
                str.append(std::to_string(meta_index_size));
                log_debug(str);
            }
        }
    }

    inline void flush_meta_index() {
        int meta_seg_write_index;
        if (meta_seg_index == -1) {
            meta_seg_write_index = meta_index.get_next_seg_index();
        } else {
            meta_seg_write_index = meta_seg_index;
            meta_seg_index = -1;
        }
        pwrite(meta_index.get_meta_fd(), index_write_buf, META_BUF_SIZE, meta_seg_write_index << META_BUF_MOVE);
        if (OFFLINE) {
            std::string str("meta_write: seg_index: ");
            str.append(std::to_string(meta_seg_write_index));
            str.append(", meta_buf_size: ");
            str.append(std::to_string(meta_index_size));
            log_debug(str);
        }
        this->clean_meta_index_buf();
    }

    inline void flush_meta_data() {
//        store_write_buf_to_cache(this->data_write_buf, this->seg_write_index);
        pwrite(meta_data_fd, this->data_write_buf, HI_SEG_SIZE, this->seg_write_index << HI_SEG_MOVE);
        if (OFFLINE) {
            std::string str("data_write: offset: ");
            str.append(std::to_string((this->seg_write_index << HI_SEG_MOVE)));
            str.append(", seg_index: ");
            str.append(std::to_string(seg_write_index));
            log_debug(str);
        }
        this->seg_write_index++;
        this->persisted_file_buf_size = 0;
        if (OFFLINE) {
            log_debug("update_seg_write_index: ", this->seg_write_index);
        }
    }

    void copy_meta_to_data(char *data_addr) const {
        this->data_meta_buf[this->page_count] = this->page_count + 2;
        auto page_count_size = ((page_count + 2) << 2);
        auto meta_dest = data_addr + HI_SEG_SIZE - page_count_size;
        memcpy(meta_dest, this->data_meta_buf, page_count_size);
    }

    inline void page_write(Compressor *compressor, uint32_t page_no, char *buf) {
        auto dest = this->data_write_buf + file_buf_size;
        int compressed_size = compressor->compress(buf, PAGE_SIZE, dest, COMPRESS_LEVEL);

        if (DEBUG && DEBUG_PAGE_NO == page_no) {
            log_debug("debug_entry: ", page_no);
        }
        do_page_write(page_no, buf, dest, compressed_size);
    }

    inline void do_page_write(uint32_t page_no, const char *buf, const char *dest, int compressed_size) {
        auto length = file_buf_size + compressed_size + (page_count << 2) + 16;
        uint32_t index;
        uint32_t local_index;
        if (length >= HI_SEG_SIZE) {
            auto data_addr = data_write_buf;
            if (length > HI_SEG_SIZE) {
                memmove(data_write_buf + HI_SEG_SIZE, dest, compressed_size);
                copy_meta_to_data(data_addr);
                page_count = 0;
                file_buf_size = 0;
                flush_meta_data();
                memmove(data_write_buf, data_write_buf + HI_SEG_SIZE, compressed_size);

                index = this->seg_write_index;
                local_index = (file_buf_size << 16) | compressed_size;
                data_meta_buf[page_count++] = page_no;
                data_meta_buf[page_count++] = local_index;
                file_buf_size += compressed_size;
            } else {
                index = this->seg_write_index;
                local_index = (file_buf_size << 16) | compressed_size;
                data_meta_buf[page_count++] = page_no;
                data_meta_buf[page_count++] = local_index;

                copy_meta_to_data(data_addr);
                page_count = 0;
                file_buf_size = 0;
                flush_meta_data();
            }
        } else {
            index = this->seg_write_index;
            local_index = (file_buf_size << 16) | compressed_size;
            data_meta_buf[page_count++] = page_no;
            data_meta_buf[page_count++] = local_index;
            file_buf_size += compressed_size;
        }

        auto item = &index_write_buf[meta_index_size++];
        auto full_index = build_index_hi(instance_index, index);
        item->page_no = (page_no << 1) | 1;
        item->index = full_index;

        if (meta_index_size == MAX_META_SIZE) {
            flush_meta_index();
        }
        meta_index.set_index_hi(page_no, full_index);

        if (page_count >= HI_LOCAL_META_COUNT) {
            if (OFFLINE) {
                log_debug("page_count >= LOCAL_META_COUNT: page_count: ", page_count);
            }
            assert(false);
        }

        if (OFFLINE) {
            uint64_t hash = 0;
            if (buf != nullptr) {
                hash = page_hash((uint64_t *) buf);
            }
            all_compressed_size.fetch_add(compressed_size);
            std::string str("write ================ page_no: ");
            str.append(std::to_string(page_no));
            str.append(", seg_read_index: ");
            str.append(std::to_string(index));
            str.append(", local_pos: ");
            str.append(std::to_string(local_index >> 16));
            str.append(", local_len: ");
            str.append(std::to_string(local_index & 0xFFFF));
            str.append(", instance_index: ");
            str.append(std::to_string(instance_index));
            str.append(", hash: ");
            str.append(std::to_string(hash));
            log_debug(str);
        }
    }

    uint32_t get_local_index_from_memory(uint32_t *meta_index_array, uint32_t page_no) const {
        int count = this->page_count;
        for (int i = count - 2; i >= 0; i -= 2) {
            if (meta_index_array[i] == page_no) {
                return meta_index_array[i + 1];
            }
        }
        if (OFFLINE) {
            for (int i = 0; i < count; i += 2) {
                log_debug("i: ", i, ", page_no: ", meta_index_array[i]);
            }
            log_debug("get_local_index_from_memory_error: ", page_no);
        }
        assert(false);
    }

    static uint32_t get_local_index_from_disk(uint32_t *meta_index_array, uint32_t page_no) {
        int count = (int) meta_index_array[HI_PAGE_COUNT_SLOT];
        uint32_t *_meta_index_array = meta_index_array + (HI_SEG_SIZE / 4) - count;
        for (int i = count - 4; i >= 0; i -= 2) {
            if (_meta_index_array[i] == page_no) {
                return _meta_index_array[i + 1];
            }
        }
        if (OFFLINE) {
            log_debug("get_local_index_from_disk_error: ", page_no, ", count: ", count);
            for (int i = 0; i < count; i += 2) {
                log_debug("i: ", i, ", page_no: ", meta_index_array[i]);
            }
        }
        assert(false);
    }

    void page_read(Compressor *compressor, uint32_t page_no, char *buf) {
        if (OFFLINE) {
            log_debug("pre_read_page_no: ", page_no);
        }
        if (DEBUG && DEBUG_PAGE_NO == page_no) {
            log_debug("debug_entry: ", page_no);
        }

        auto index = meta_index.get_index_hi((int) page_no);
        auto dst_instance_index = get_instance_from_index_hi(index);
        auto dst_seg_read_index = get_seg_index_from_index_hi(index);
        auto read_data = read_data_from_cache(dst_instance_index, dst_seg_read_index);
        uint32_t local_index;
        if (read_data != this->data_write_buf) {
            local_index = get_local_index_from_disk((uint32_t *) read_data, page_no);
        } else {
            local_index = get_local_index_from_memory((uint32_t *) (this->data_meta_buf), page_no);
        }
        auto local_pos = local_index >> 16;
        auto local_length = local_index & 0xFFFF;
        int decoded_len = compressor->decompress(read_data + local_pos, local_length, buf);

        if (OFFLINE) {
            auto hash = page_hash((uint64_t *) buf);
            std::string str("read ================ page_no: ");
            str.append(std::to_string(page_no));
            str.append(", local_pos: ");
            str.append(std::to_string(local_pos));
            str.append(", compressed_size: ");
            str.append(std::to_string(local_length));
            str.append(", decode_len: ");
            str.append(std::to_string(decoded_len));
            str.append(", seg_read_index: ");
            str.append(std::to_string(dst_seg_read_index));
            str.append(", hash: ");
            str.append(std::to_string(hash));
            log_debug(str);
        }
    }

    static uint32_t compute_cache_hit_rate(
            uint32_t *meta_index_array, uint32_t cache_instance_index, uint32_t cache_seg_index) {
        int cache_hit = 0;
        int count = (int) meta_index_array[HI_PAGE_COUNT_SLOT];
        uint32_t *_meta_index_array = meta_index_array + (HI_SEG_SIZE / 4) - count;
        int limit = count - 2;
        for (int i = 0; i < limit; i += 2) {
            auto page_no = _meta_index_array[i];
            auto index = meta_index.get_index_hi(page_no);
            auto dst_instance_index = get_instance_from_index_hi(index);
            auto dst_seg_read_index = get_seg_index_from_index_hi(index);
            if (dst_instance_index == cache_instance_index && dst_seg_read_index == cache_seg_index) {
                cache_hit++;
            }
        }
        return (cache_hit << 16) | ((count - 2) >> 1);
    }

    void recycle_cache_to_disk(int slot, uint32_t cache_index) {
        auto addr = cache_buf + (slot << HI_SEG_MOVE);
        auto meta_index_array = reinterpret_cast<uint32_t *>(addr);
        auto cache_instance_index = get_instance_index_from_cache_index(cache_index);
        auto cache_seg_index = get_seg_index_from_cache_index(cache_index);
        auto complex_result = compute_cache_hit_rate(meta_index_array, cache_instance_index, cache_seg_index);
        auto cache_hit = complex_result >> 16;
        auto all_count = complex_result & 0xFFFF;
        auto cache_hit_rate = cache_hit * 100 / all_count;
        if (OFFLINE) {
            log_debug("recycle_cache_to_disk: cache_hit_rate: ", cache_hit_rate);
        }
        if (cache_hit_rate < CACHE_HIT_RATE_THRESHOLD && all_count >= 3) {
            int count = (int) meta_index_array[HI_PAGE_COUNT_SLOT];
            uint32_t *_meta_index_array = meta_index_array + (HI_SEG_SIZE / 4) - count;
            int limit = count - 2;
            for (int i = limit - 2; i >= 0; i -= 2) {
                auto page_no = _meta_index_array[i];
                auto dst_index = meta_index.get_index_hi(page_no);
                auto dst_instance_index = get_instance_from_index_hi(dst_index);
                auto dst_seg_read_index = get_seg_index_from_index_hi(dst_index);
                if (dst_instance_index == cache_instance_index && dst_seg_read_index == cache_seg_index) {
                    auto local_index = _meta_index_array[i + 1];
                    auto local_pos = local_index >> 16;
                    auto local_length = local_index & 0xFFFF;
                    auto dest = this->data_write_buf + file_buf_size;
                    auto src = addr + local_pos;
                    memcpy(dest, src, local_length);
                    do_page_write(page_no, nullptr, dest, local_length);
                }
            }
        }
    }

    inline void store_write_buf_to_cache(char *data, uint32_t seg_read_index) {
        auto complex_seg_index = (this->instance_index << 28) | seg_read_index;
        auto slot = next_cache_index();
        if (seg_key_array[slot] != -1) {
            seg_key_map.remove(seg_key_array[slot]);
        }
        seg_key_array[slot] = complex_seg_index;
        seg_key_map.put(complex_seg_index, slot);
        auto addr = (cache_buf + (slot << HI_SEG_MOVE));
        memcpy(addr, data, HI_SEG_SIZE);
    }

    inline char *read_data_from_cache(uint32_t dst_instance_index, uint32_t seg_index) {
        if (seg_index == this->seg_write_index && dst_instance_index == this->instance_index) {
            return data_write_buf;
        }
        auto cache_index = build_cache_index(dst_instance_index, seg_index);
        auto find = seg_key_map.get(cache_index);
        if (find == -1) {
            auto slot = next_cache_index();
            if (seg_key_array[slot] != -1) {
                auto cache_index_temp = seg_key_array[slot];
                seg_key_map.remove(cache_index_temp);
                if (RECYCLE_ENABLED) {
                    recycle_cache_to_disk(slot, cache_index_temp);
                }
            }
            seg_key_array[slot] = cache_index;
            seg_key_map.put(cache_index, slot);
            auto addr = (cache_buf + (slot << HI_SEG_MOVE));
            auto file_fd = data_fd_array[dst_instance_index];
            pread(file_fd, addr, HI_SEG_SIZE, seg_index << HI_SEG_MOVE);
            if (OFFLINE) {
                std::string str("data_read: seg_index: ");
                str.append(std::to_string(seg_index));
                str.append(", file_index: ");
                str.append(std::to_string(dst_instance_index));
                str.append(", slot: ");
                str.append(std::to_string(slot));
                log_debug(str);
            }
            return addr;
        } else {
            if (OFFLINE) {
                log_debug("read_from_cache");
            }
            auto slot = find;
            return (cache_buf + (slot << HI_SEG_MOVE));
        }
    }

};

#endif //INTERFACE_INSTANCE_HI2_H
