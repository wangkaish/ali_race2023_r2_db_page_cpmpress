![plot](./banner.png)
 
# Tianchi x PolarDB: Page Compression Engine

Aiming to implement a page engine that achieves higher data compression rates and page read/write performance.

## Quick Start

Execute following command to build and run local test:

```
$ cmake .
$ make
$ ./bin/random_read_write
```

Download sample trace and tune performance [Sample Trace](https://atomgit.com/polardb_tianchi/page_compression_2023_sample_trace)

## Coding

`page_engine/` dir contains sample code using naive pread/pwrite which can pass the test.

`template/` dir contains blank interface templates that can be used as a codebase for implementing your own data page compression engine.

`test/` dir contains local test scripts for different scenarios, which can be run locally before submitting.

## Tips

- Only `page_engine/` dir is available for online judge
- Remove `-lrt` in `CMakeList.txt` if using MacOS
- We use `du --block-size=1` to calculate compressed data size

## 注意！

- 只有对`page_engine/`目录的修改会在测评中被采用，其他目录仅作为示例和本地测试使用
- 未安装完整开发工具的MacOS会在编译时报错`ld: library not found for -lrt`，只需在`CMakeList.txt`中删除`-lrt`即可, `-laio` 同样处理
- 在线测评采用`du --block-size=1`统计压缩后的文件大小

## 复赛相关

- 复赛中，每条trace约进行10次shutdown
- `test/`中加入了`run_trace_multithread`本地测试，模拟多线程和随机shutdown
- 线上测评环境中对文件系统page cache做了非常严格的限制，具体来说:
  - 设置了`sysctl -w vm.dirty_ratio=0`
  - 循环`sysctl -w vm.drop_caches=3`
  - 因此建议选手使用Direct IO进行持久化数据读写，以获得更好性能
