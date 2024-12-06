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

class Visitor {
private:
    PageEngine *page_engine;
    static const int page_size = 16384;
    char page_buf[page_size];
    char trace_buf[page_size];

public:
    Visitor() {
        std::string path = "/home/test/temp/trace/";
        RetCode ret = PageEngine::Open(path, &page_engine);
        assert(ret == kSucc);
    }

    ~Visitor() {
        delete page_engine;
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

            memset(trace_buf, 0, 1024 * 14);

            switch (RW) {
                case 'R': {
//          std::cout << "Read Page page_no: " << page_no << std::endl;
                    RetCode ret = page_engine->pageRead(page_no, page_buf);
                    assert(ret == kSucc);
                    if (memcmp(page_buf, trace_buf, page_size) != 0) {
                        log_debug("read_error: ", page_no, ", hash: ", page_hash((uint64_t *) trace_buf));
                        assert(false);
                    }
                    break;
                }
                case 'W': {
//          std::cout << "Write Page page_no: " << page_no << std::endl;
                    RetCode ret = page_engine->pageWrite(page_no, trace_buf);
                    assert(ret == kSucc);
                    break;
                }
                default:
                    assert(false);
            }
        }
        trace_file.close();
    }
};

int main(int argc, char *argv[]) {

    clean_work_dir("/home/test/temp/trace/");

//  std::string path = "/home/test/temp/sample-trace/imdb.trace";
//    std::string path = "/home/test/temp/sample-trace/sysbench.trace";
//    std::string path = "/home/test/temp/sample-trace/tpch.trace";
    std::string path = "/home/test/temp/sample-trace/wiki.trace";

    Visitor visitor = Visitor();

    uint64_t start_time = native_current_milliseconds();

    visitor.run_trace(path);

    auto cost = native_current_milliseconds() - start_time;
    log_debug("cost: ", cost);

    std::cout << "Finished trace run!, path: " << path << std::endl;
    return 0;
}
