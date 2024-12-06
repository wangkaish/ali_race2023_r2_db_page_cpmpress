//
// Created by wangkai on 2022/7/13.
//

#ifndef INTERFACE_INSTANCE_PROXY_H
#define INTERFACE_INSTANCE_PROXY_H

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
#include "instance_low.h"
#include "instance_hi2.h"

class InstanceProxy {

public:

    int instance_index;
    InstanceHi2 *hi_instance;
    InstanceLow *low_instance;
    Compressor *compressor = new Compressor;

    void init(int index) {
        if (HI_COMPRESS) {
            hi_instance = new InstanceHi2;
            hi_instance->init(index);
        } else {
            low_instance = new InstanceLow;
            low_instance->init(index);
        }
    }

    inline void idle_flush_meta_data() const {
        if (HI_COMPRESS) {
            hi_instance->idle_flush_meta_data();
        } else {
            low_instance->idle_flush_meta_data();
        }
    }

    inline void copy_meta_index(InstanceProxy *p_instance) const {
        if (HI_COMPRESS) {
            hi_instance->copy_meta_index(p_instance->hi_instance);
        } else {
            low_instance->copy_meta_index(p_instance->low_instance);
        }
    }

    inline void idle_flush_meta_index() const {
        if (HI_COMPRESS) {
            hi_instance->idle_flush_meta_index();
        } else {
            low_instance->idle_flush_meta_index();
        }
    }

    inline void page_write_hi(uint32_t page_no, char *buf) const {
        hi_instance->page_write(compressor, page_no, buf);
    }

    inline void page_write_low(uint32_t page_no, char *buf) const {
        low_instance->page_write(compressor, page_no, buf);
    }

    inline void page_write(uint32_t page_no, char *buf) const {
        if (HI_COMPRESS) {
            hi_instance->page_write(compressor, page_no, buf);
        } else {
            low_instance->page_write(compressor, page_no, buf);
        }
    }

    void page_read_hi(uint32_t page_no, char *buf) const {
        hi_instance->page_read(compressor, page_no, buf);
    }

    void page_read_low(uint32_t page_no, char *buf) const {
        low_instance->page_read(compressor, page_no, buf);
    }

    void page_read(uint32_t page_no, char *buf) const {
        if (HI_COMPRESS) {
            hi_instance->page_read(compressor, page_no, buf);
        } else {
            low_instance->page_read(compressor, page_no, buf);
        }
    }

};

#endif //INTERFACE_INSTANCE_PROXY_H
