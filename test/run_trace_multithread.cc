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
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <vector>
#include <thread>
#include "../page_engine/util.h"

//static const float SHUTDOWN_THD = 0.01;
static const float SHUTDOWN_THD = 0.0005;
static const int THREAD_NUM = INSTANCE_COUNT;
static const int MEM_SET_ZERO_COUNT = 1024 * 8;

typedef enum {
    IO_READ = 0,
    IO_WRITE = 1
} IOType;

struct Pipe {
    int pipefd[2];
};

class Visitor {
private:
    PageEngine *page_engine;
    const int page_size{16384};
    int thread_num;
    int pid;
    std::vector<Pipe> pipes;

public:
    Visitor(int thread) : thread_num(thread), pid(-1) {
        for (int i = 0; i < thread_num; i++) {
            pipes.emplace_back();
            int rc = pipe(pipes.back().pipefd);
            assert(rc != -1);
        }
    }

    ~Visitor() {
    }

    void thread_func(int thread_id) {
        assert(pid == 0);
        assert(page_size == 16384);
        log_debug("Thread: ", thread_id, " start on pipe: ", pipes[thread_id].pipefd[0]);
//        std::cout << "Thread " << thread_id << " start on pipe " << pipes[thread_id].pipefd[0] << std::endl;
        void *page_ptr = malloc(2 * page_size);
        void *page_buf = (void *) (((uint64_t) page_ptr + 16384) & (~0x3fff));

        void *trace_ptr = malloc(2 * page_size);
        void *trace_buf = (void *) (((uint64_t) trace_ptr + 16384) & (~0x3fff));

        while (true) {
            uint8_t cmd;
            uint32_t page_no;
            int bytes;
            RetCode ret;

            bytes = read(pipes[thread_id].pipefd[0], &cmd, 1);
            if (bytes == 0) break;
            assert(bytes == 1);

            bytes = read(pipes[thread_id].pipefd[0], &page_no, 4);
            assert(bytes == 4);

            bytes = read(pipes[thread_id].pipefd[0], trace_buf, page_size);
            assert(bytes == bytes);

            memset(trace_buf, 0, MEM_SET_ZERO_COUNT);

            switch (cmd) {
                case IO_READ:
//          std::cout << "Thread " << thread_id << " Receive CMD: Read Page page_no: " << page_no << std::endl;
                    ret = page_engine->pageRead(page_no, page_buf);
                    assert(ret == kSucc);
                    if (memcmp(page_buf, trace_buf, page_size) != 0) {
                        log_debug("read_error: ", page_no, ", hash: ", page_hash((uint64_t *) trace_buf));
                        assert(false);
                    }
                    break;

                case IO_WRITE:
//          std::cout << "Thread " << thread_id << " Receive CMD: Write Page page_no: " << page_no << std::endl;
                    ret = page_engine->pageWrite(page_no, trace_buf);
                    assert(ret == kSucc);
                    break;
            }
        }

        free(page_ptr);
        free(trace_ptr);

        log_debug("Thread exit: ", thread_id);
//    std::cout << "Thread " << thread_id << " exit" << std::endl;
    }

    void pageRead(uint32_t page_no, void *buf) {
        assert(pid > 0);
        int thread_id = page_no % thread_num;
        uint8_t io_type = IO_READ;

//    std::cout << "Send CMD to thread " << thread_id << ": Read Page page_no: " << page_no << std::endl;
        write(pipes[thread_id].pipefd[1], &io_type, 1);
        write(pipes[thread_id].pipefd[1], &page_no, 4);
        write(pipes[thread_id].pipefd[1], buf, page_size);
    }

    void pageWrite(uint32_t page_no, void *buf) {
        assert(pid > 0);
        int thread_id = page_no % thread_num;
        uint8_t io_type = IO_WRITE;

//    std::cout << "Send CMD to thread " << thread_id << ": Write Page page_no: " << page_no << std::endl;
        write(pipes[thread_id].pipefd[1], &io_type, 1);
        write(pipes[thread_id].pipefd[1], &page_no, 4);
        write(pipes[thread_id].pipefd[1], buf, page_size);
    }

    void run() {

        pid = fork();
        assert(pid >= 0);

        if (pid == 0) {
            std::string path = "/home/test/temp/trace/";
            RetCode ret = PageEngine::Open(path, &page_engine);
            assert(ret == kSucc);

            std::vector<std::thread> threads;

            for (int thread_id = 0; thread_id < thread_num; thread_id++) {
                close(pipes[thread_id].pipefd[1]);
                threads.emplace_back(std::thread(&Visitor::thread_func, this, thread_id));
//                sleep(1);
            }

            for (int thread_id = 0; thread_id < thread_num; thread_id++) {
                threads[thread_id].join();
                close(pipes[thread_id].pipefd[0]);
            }

            log_debug("exit_0_will_call");
            // online judge will not call delete(page_engine)
//       delete(page_engine);
            exit(0);
        } else {
            for (int thread_id = 0; thread_id < thread_num; thread_id++) {
                close(pipes[thread_id].pipefd[0]);
            }
        }
    }

    void shutdown() {
        for (int thread_id = 0; thread_id < thread_num; thread_id++) {
            close(pipes[thread_id].pipefd[1]);
        }

        int status;
        wait(&status);
        if (WIFEXITED(status)) {
            int exitCode = WEXITSTATUS(status);
            printf("Child process exited with code %d\n", exitCode);
            assert(exitCode == 0);
        } else {
            printf("Child process did not exit normally\n");
            assert(false);
        }
    }
};

void run_trace(std::string path) {
    std::ifstream trace_file(path);
    char RW;
    uint32_t page_no;
    const int page_size = 16384;

    Visitor *visitor = new Visitor(THREAD_NUM);
    visitor->run();

    void *trace_buf = malloc(page_size);

    std::string line;
    while (std::getline(trace_file, line)) {
        std::stringstream linestream(line);
        if (!(linestream >> RW >> page_no)) break;
        trace_file.read((char *) trace_buf, page_size);

        switch (RW) {
            case 'R': {
                visitor->pageRead(page_no, trace_buf);
                break;
            }
            case 'W': {
                visitor->pageWrite(page_no, trace_buf);
                break;
            }
            default:
                assert(false);
        }

        if ((float) rand() / RAND_MAX < SHUTDOWN_THD) {
            std::cout << "shutdown" << std::endl;
            visitor->shutdown();
            delete visitor;
            visitor = new Visitor(THREAD_NUM);
            visitor->run();
        }
    }
    trace_file.close();

    delete visitor;
    free(trace_buf);
}

int main(int argc, char *argv[]) {

    clean_work_dir("/home/test/temp/trace/");

//  std::string path = "/home/test/temp/sample-trace/imdb.trace";
//    std::string path = "/home/test/temp/sample-trace/sysbench.trace";
//    std::string path = "/home/test/temp/sample-trace/tpch.trace";
    std::string path = "/home/test/temp/sample-trace/wiki.trace";

    srand(0);

    uint64_t start_time = native_current_milliseconds();

    run_trace(path);

    auto cost = native_current_milliseconds() - start_time;
    log_debug("cost: ", cost);

    std::cout << "Finished trace run!, path: " << path << std::endl;
    return 0;
}
