//
// Created by wangkai on 2022/7/13.
//

#ifndef INTERFACE_INSTANCE_LOW_H
#define INTERFACE_INSTANCE_LOW_H

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

class InstanceLow {
public:

    static const int SEG_KEY_COUNT = 256;

    int meta_data_fd = -1;
    int instance_index = 0;
    int file_buf_size = 0;
    int persisted_file_buf_size = 0;
    int meta_index_size = 0;
    int persisted_meta_index_size = 0;
    int meta_seg_index = -1;
    uint32_t cache_index = 0;
    uint64_t file_size = 0;
    uint64_t seg_write_index = 0;
    DiskIndex *index_write_buf;
    char *data_write_buf = static_cast<char *>(memalign(DIRECT_BLOCK, SEG_WRITE_SIZE_ALL));
    char *cache_buf = static_cast<char *>(memalign(DIRECT_BLOCK, STATIC_CACHE_SIZE));
    char *codec_buf = static_cast<char *>(memalign(DIRECT_BLOCK, PAGE_SIZE));
    FlatMap seg_key_map;
    int64_t seg_key_array[SEG_KEY_COUNT];

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
        this->file_size = meta_index.get_instance_file_size(index);
        data_fd_array[index] = meta_data_fd;

        for (int j = 0; j < SEG_KEY_COUNT; ++j) {
            seg_key_array[j] = -1;
        }
        this->seg_write_index = this->file_size >> SEG_WRITE_MOVE;
        this->file_buf_size = this->file_size & (SEG_WRITE_SIZE - 1);
        if (this->file_buf_size != 0) {
            this->persisted_file_buf_size = this->file_buf_size;
            pread(this->meta_data_fd, this->data_write_buf, SEG_WRITE_SIZE, this->seg_write_index << SEG_WRITE_MOVE);
        }

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

    static inline void local_sample_disk_read() {
        sample_disk_read();
    }

    inline int next_cache_index() {
        uint32_t next_index = cache_index++;
        return (int) (next_index % STATIC_CACHE_COUNT);
    }

    void next_seg_write_index() {
        this->seg_write_index++;
        this->file_buf_size = 0;
        this->persisted_file_buf_size = 0;
        this->file_size = (this->seg_write_index << SEG_WRITE_MOVE);
        if (OFFLINE) {
            log_debug("update_seg_write_index: ", this->seg_write_index);
        }
    }

    inline void flush_meta_data() {
        pwrite(meta_data_fd, this->data_write_buf, SEG_WRITE_SIZE, this->seg_write_index << SEG_WRITE_MOVE);
        if (OFFLINE) {
            std::string str("data_write: offset: ");
            str.append(std::to_string((this->seg_write_index << SEG_WRITE_MOVE)));
            str.append(", seg_index: ");
            str.append(std::to_string(seg_write_index));
            log_debug(str);
        }
        this->next_seg_write_index();
    }

    inline void idle_flush_meta_data() {
        if (this->persisted_file_buf_size < this->file_buf_size) {
            auto old_persisted_file_buf_size = persisted_file_buf_size;
            this->persisted_file_buf_size = this->file_buf_size;
            pwrite(meta_data_fd, this->data_write_buf, SEG_WRITE_SIZE, this->seg_write_index << SEG_WRITE_MOVE);
            if (OFFLINE) {
                std::string str("idle_data_write: offset: ");
                str.append(std::to_string((this->seg_write_index << SEG_WRITE_MOVE)));
                str.append(", persisted_file_buf_size: ");
                str.append(std::to_string(old_persisted_file_buf_size));
                str.append(", file_buf_size: ");
                str.append(std::to_string(file_buf_size));
                log_debug(str);
            }
        }
    }

    inline void clean_meta_index_buf() {
        this->meta_index_size = 0;
        this->persisted_meta_index_size = 0;
        memset(index_write_buf, 0, META_BUF_SIZE);
    }

    inline void copy_meta_index(InstanceLow *p_instance) {
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

    inline void page_write(Compressor *compressor, uint32_t page_no, char *buf) {
        auto dest = this->data_write_buf + file_buf_size;
        int compressed_size = compressor->compress(buf, PAGE_SIZE, dest, COMPRESS_LEVEL);
        uint64_t index = build_disk_index(this->instance_index, compressed_size, this->file_size);
        auto length = file_buf_size + compressed_size;
        if (length >= SEG_WRITE_SIZE) {
            flush_meta_data();
            auto remain = length - SEG_WRITE_SIZE;
            if (remain > 0) {
                memcpy(this->data_write_buf, this->data_write_buf + SEG_WRITE_SIZE, remain);
                this->file_buf_size += remain;
                this->file_size += remain;
            }
        } else {
            this->file_buf_size += compressed_size;
            this->file_size += compressed_size;
        }

        auto item = &index_write_buf[meta_index_size++];
        item->page_no = (page_no << 1) | 1;
        item->index = (instance_index << 16) | compressed_size;

        if (meta_index_size == MAX_META_SIZE) {
            flush_meta_index();
        }
        meta_index.set_index_low(page_no, index);

        if (OFFLINE) {
            all_compressed_size.fetch_add(compressed_size);
            std::string str("write ================ page_no: ");
            str.append(std::to_string(page_no));
            str.append(", compressed_size: ");
            str.append(std::to_string(compressed_size));
            str.append(", file_pos: ");
            str.append(std::to_string(index & 0xFFFFFFFFFFL));
            str.append(", seg_write_index: ");
            str.append(std::to_string((index & 0xFFFFFFFFFFL) >> SEG_WRITE_MOVE));
            str.append(", file_index: ");
            str.append(std::to_string(instance_index));
            str.append(", index_1: ");
            str.append(std::to_string(index >> 32));
            str.append(", index_2: ");
            str.append(std::to_string(index & 0xFFFFFFFF));
            log_debug(str);
        }
    }

    void page_read(Compressor *compressor, uint32_t page_no, char *buf) {
        if (OFFLINE) {
            log_debug("pre_read_page_no: ", page_no);
        }
        auto index = meta_index.get_index_low((int) page_no);
        auto pos = get_file_pos_from_disk_index(index);
        auto compressed_size = get_length_from_disk_index(index);
        int decoded_len;
        auto dst_instance_index = get_instance_index_from_disk_index(index);
        if (cache_enabled) {
            auto seg_read_index_1 = pos >> SEG_READ_MOVE;
            auto seg_read_index_2 = (pos + compressed_size) >> SEG_READ_MOVE;
            if (seg_read_index_1 == seg_read_index_2) {
                auto read_data = read_data_from_cache_ignore_keep(dst_instance_index, seg_read_index_1);
                char *codec_buf_offset = read_data + (pos & (SEG_READ_SIZE - 1));
                decoded_len = compressor->decompress((char *) codec_buf_offset, compressed_size, buf);
            } else {
                int remain = (pos + compressed_size) & (SEG_READ_SIZE - 1);
                int len = compressed_size - remain;
                int slot_1;
                int src_data_1_off = SEG_READ_SIZE - len;
                auto read_data1 = read_data_from_cache_1(dst_instance_index, seg_read_index_1, &slot_1);
                auto read_data2 = read_data_from_cache_2(
                        dst_instance_index, seg_read_index_2, slot_1, codec_buf, src_data_1_off, len);
                if ((read_data1 + SEG_READ_SIZE) == read_data2) {
                    char *codec_buf_offset = read_data1 + src_data_1_off;
                    decoded_len = compressor->decompress(codec_buf_offset, compressed_size, buf);
                } else {
                    if (read_data1 != read_data2) {
                        memcpy(codec_buf, read_data1 + src_data_1_off, len);
                    }
                    memcpy(codec_buf + len, read_data2, remain);
                    decoded_len = compressor->decompress(codec_buf, compressed_size, buf);
                }
            }
        } else {
            auto file_fd = data_fd_array[dst_instance_index];
            auto seg_write_index_1 = pos >> SEG_WRITE_MOVE;
            auto seg_write_index_2 = (pos + compressed_size) >> SEG_WRITE_MOVE;
            if (seg_write_index_2 == seg_write_index && dst_instance_index == instance_index) {
                if (seg_write_index_1 == seg_write_index_2) {
                    auto codec_offset = data_write_buf + (pos - (seg_write_index << SEG_WRITE_MOVE));
                    decoded_len = compressor->decompress(codec_offset, compressed_size, buf);
                    if (OFFLINE) {
                        log_debug("read_from_memory_no_cache");
                    }
                } else {
                    auto remain = (pos + compressed_size - (seg_write_index << SEG_WRITE_MOVE));
                    auto seg_read_start = (pos >> DIRECT_BLOCK_MOVE) << DIRECT_BLOCK_MOVE;
                    auto seg_read_end = seg_write_index << SEG_WRITE_MOVE;
                    auto read_len = (seg_read_end - seg_read_start);
                    pread(file_fd, codec_buf, read_len, seg_read_start);
                    memcpy(codec_buf + read_len, data_write_buf, remain);
                    auto codec_offset = codec_buf + (pos & (DIRECT_BLOCK - 1));
                    decoded_len = compressor->decompress(codec_offset, compressed_size, buf);
                    if (OFFLINE) {
                        log_debug("read_from_disk_memory_no_cache");
                    }
                }
            } else {
                auto seg_read_start = (pos >> DIRECT_BLOCK_MOVE) << DIRECT_BLOCK_MOVE;
                auto seg_read_end =
                        ((pos + compressed_size + DIRECT_BLOCK - 1) >> DIRECT_BLOCK_MOVE) << DIRECT_BLOCK_MOVE;
                auto read_len = (seg_read_end - seg_read_start);
                pread(file_fd, codec_buf, read_len, seg_read_start);
                auto codec_offset = codec_buf + (pos & (DIRECT_BLOCK - 1));
                decoded_len = compressor->decompress(codec_offset, compressed_size, buf);
                if (OFFLINE) {
                    log_debug("read_from_disk_no_cache");
                }
            }
        }
        sample_page_read();

        if (OFFLINE) {
            std::string str("read ================ page_no: ");
            str.append(std::to_string(page_no));
            str.append(", pos: ");
            str.append(std::to_string(pos));
            str.append(", compressed_size: ");
            str.append(std::to_string(compressed_size));
            str.append(", decode_len: ");
            str.append(std::to_string(decoded_len));
            str.append(", seg_write_index: ");
            str.append(std::to_string(pos >> SEG_WRITE_MOVE));
            log_debug(str);
        }
    }

    inline char *read_data_from_cache_ignore_keep(uint32_t dst_instance_index, uint32_t seg_index) {
        if ((seg_index >> READ_CACHE_MOVE) == this->seg_write_index && dst_instance_index == this->instance_index) {
            if ((seg_index & READ_CACHE_MOVE) == 1) {
                return data_write_buf + SEG_READ_SIZE;
            } else {
                return data_write_buf;
            }
        }
        auto complex_seg_index = (dst_instance_index << 28) | seg_index;
        auto find = seg_key_map.get(complex_seg_index);
        if (find == -1) {
            local_sample_disk_read();
            auto slot = next_cache_index();
            if (seg_key_array[slot] != -1) {
                seg_key_map.remove(seg_key_array[slot]);
            }
            seg_key_array[slot] = complex_seg_index;
            seg_key_map.put(complex_seg_index, slot);
            auto addr = (cache_buf + (slot << SEG_READ_MOVE));
            auto file_fd = data_fd_array[dst_instance_index];
            pread(file_fd, addr, SEG_READ_SIZE, seg_index << SEG_READ_MOVE);
            if (OFFLINE) {
                std::string str("data_read: seg_index: ");
                str.append(std::to_string(seg_index));
                str.append(", file_index: ");
                str.append(std::to_string(dst_instance_index));
                str.append(", seg_index: ");
                str.append(std::to_string(seg_index));
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
            return (cache_buf + (slot << SEG_READ_MOVE));
        }
    }

    inline char *read_data_from_cache_1(uint32_t dst_instance_index, uint32_t seg_index, int *slot_ret) {
        if ((seg_index >> READ_CACHE_MOVE) == this->seg_write_index && dst_instance_index == this->instance_index) {
            slot_ret[0] = -1;
            if ((seg_index & READ_CACHE_MOVE) == 1) {
                return data_write_buf + SEG_READ_SIZE;
            } else {
                return data_write_buf;
            }
        }
        auto complex_seg_index = (dst_instance_index << 28) | seg_index;
        auto find = seg_key_map.get(complex_seg_index);
        if (find == -1) {
            local_sample_disk_read();
            auto slot = next_cache_index();
            if (seg_key_array[slot] != -1) {
                seg_key_map.remove(seg_key_array[slot]);
            }
            seg_key_array[slot] = complex_seg_index;
            seg_key_map.put(complex_seg_index, slot);
            slot_ret[0] = slot;
            auto addr = (cache_buf + (slot << SEG_READ_MOVE));
            auto file_fd = data_fd_array[dst_instance_index];
            pread(file_fd, addr, SEG_READ_SIZE, seg_index << SEG_READ_MOVE);
            if (OFFLINE) {
                std::string str("data_read: seg_index: ");
                str.append(std::to_string(seg_index));
                str.append(", file_index: ");
                str.append(std::to_string(dst_instance_index));
                str.append(", seg_index: ");
                str.append(std::to_string(seg_index));
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
            slot_ret[0] = slot;
            return (cache_buf + (slot << SEG_READ_MOVE));
        }
    }

    inline char *read_data_from_cache_2(
            uint32_t dst_instance_index, uint32_t seg_index, int keep_slot, char *p_codec_buf, int off, int copy_len) {
        if ((seg_index >> READ_CACHE_MOVE) == this->seg_write_index && dst_instance_index == this->instance_index) {
            if ((seg_index & READ_CACHE_MOVE) == 1) {
                return data_write_buf + SEG_READ_SIZE;
            } else {
                return data_write_buf;
            }
        }
        auto complex_seg_index = (dst_instance_index << 28) | seg_index;
        auto find = seg_key_map.get(complex_seg_index);
        if (find == -1) {
            local_sample_disk_read();
            auto slot = next_cache_index();
            if (seg_key_array[slot] != -1) {
                seg_key_map.remove(seg_key_array[slot]);
            }
            seg_key_array[slot] = complex_seg_index;
            seg_key_map.put(complex_seg_index, slot);
            auto addr = (cache_buf + (slot << SEG_READ_MOVE));
            if (slot == keep_slot) {
                memcpy(p_codec_buf, addr + off, copy_len);
            }
            auto file_fd = data_fd_array[dst_instance_index];
            pread(file_fd, addr, SEG_READ_SIZE, seg_index << SEG_READ_MOVE);
            if (OFFLINE) {
                std::string str("data_read: seg_index: ");
                str.append(std::to_string(seg_index));
                str.append(", file_index: ");
                str.append(std::to_string(dst_instance_index));
                str.append(", seg_index: ");
                str.append(std::to_string(seg_index));
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
            return (cache_buf + (slot << SEG_READ_MOVE));
        }
    }

};

#endif //INTERFACE_INSTANCE_LOW_H
