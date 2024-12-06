// Copyright [2023] Alibaba Cloud All rights reserved
#ifndef PAGE_ENGINE_DUMMY_ENGINE_H_
#define PAGE_ENGINE_DUMMY_ENGINE_H_

#include "def.h"
#include "page_engine.h"
#include "instance_proxy.h"

/*
 * Dummy sample of page engine
 */

InstanceProxy instances[INSTANCE_COUNT];

class DummyEngine : public PageEngine {
public:
    std::string path;

    static RetCode Open(const std::string &path, PageEngine **eptr);

    explicit DummyEngine(const std::string &path);

    ~DummyEngine() override;

    RetCode pageWrite(uint32_t page_no, const void *buf) override;

    RetCode pageRead(uint32_t page_no, void *buf) override;
};

#endif  // PAGE_ENGINE_DUMMY_ENGINE_H_
