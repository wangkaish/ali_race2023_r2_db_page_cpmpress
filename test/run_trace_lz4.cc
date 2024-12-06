// Copyright [2023] Alibaba Cloud All rights reserved

/*
 * Local test to run sample trace
 */

#include "page_engine.h"
#include <iostream>
#include <fstream>
#include <cassert>
#include <cstring>
#include <sstream>
#include "../page_engine/util.h"

int c_batch = 0;
int batch = 2;
const int page_size = 1024 * 16;

char page_buf[page_size];
char trace_buf[page_size];
char c_buf[1024 * 1024];
char c_buf_dst[1024 * 1024];
auto all_batch = batch * page_size;
int all_compressed_size = 0;

LZ4_stream_t *encodeCtx = LZ4_createStream();

// Write 16KB page into disk
void pageWrite(uint32_t page_no, const void *buf) {
//    log_debug("write_page no: ", page_no);
    memcpy(c_buf + c_batch * page_size, buf, page_size);
    c_batch += 1;
    if (c_batch == batch) {
        c_batch = 0;
        LZ4_resetStream_fast(encodeCtx);
        auto compressed_size = LZ4_compress_fast_continue(encodeCtx, c_buf, c_buf_dst, all_batch, 1024 * 1024, 1);
        log_debug("compress: ", all_batch, ", compressed_size: ", compressed_size);
        all_compressed_size += compressed_size;
    }
}

// Read 16KB page from disk
void pageRead(uint32_t page_no, void *buf) {
//    log_debug("read_page no: ", page_no);
}

void run_trace(std::string path) {

    std::ifstream trace_file(path);
    char RW;
    uint32_t page_no;

    std::string line;
    while (std::getline(trace_file, line)) {
        std::stringstream linestream(line);
        if (!(linestream >> RW >> page_no)) break;
        trace_file.read((char *) trace_buf, page_size);

        switch (RW) {
            case 'R': {
                pageRead(page_no, page_buf);
                break;
            }
            case 'W': {
                pageWrite(page_no, trace_buf);
                break;
            }
            default:
                assert(false);
        }
    }
    trace_file.close();
}


int main(int argc, char *argv[]) {

    std::string path = "/home/test/temp/sample-trace/wiki.trace";

    run_trace(path);

    log_debug("all compressed size: ", all_compressed_size);
    std::cout << "Finished trace run!" << std::endl;
    return 0;
}
