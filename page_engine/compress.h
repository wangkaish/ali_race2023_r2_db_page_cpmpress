//
// Created by test on 23-8-30.
//

#ifndef PAGE_ENGINE_COMPRESSOR_H
#define PAGE_ENGINE_COMPRESSOR_H

#define ZSTD_STATIC_LINKING_ONLY

#include "def.h"
#include "zstd/zstd.h"
#include "zstd/zdict.h"

class Compressor {

    ZSTD_CCtx *encodeCtx = ZSTD_createCCtx();
    ZSTD_DCtx *decodeCtx = ZSTD_createDCtx();

//    ZSTD_CCtx *encodeCtx = ZSTD_initStaticCCtx(malloc(1024 * 256), 1024 * 256);
//    ZSTD_DCtx *decodeCtx = ZSTD_initStaticDCtx(malloc(1024 * 128), 1024 * 128);

public:

    int compress(char *src_buf, int src_size, char *codec_buf, int level) {
        ZSTD_CCtx_reset(encodeCtx, ZSTD_reset_session_only);
        return ZSTD_compressCCtx(
                encodeCtx, codec_buf, PAGE_SIZE, src_buf, src_size, level);
    }

    int compress32(char *src_buf, int src_size, char *codec_buf, int level) {
        ZSTD_CCtx_reset(encodeCtx, ZSTD_reset_session_only);
        return ZSTD_compressCCtx(
                encodeCtx, codec_buf, PAGE_SIZE * 2, src_buf, src_size, level);
    }

    int decompress(char *compressed_buf, int compressed_size, char *dst_buf) {
        ZSTD_DCtx_reset(decodeCtx, ZSTD_reset_session_only);
        return ZSTD_decompressDCtx(decodeCtx, dst_buf, PAGE_SIZE, compressed_buf, compressed_size);
    }

};


#endif //PAGE_ENGINE_COMPRESSOR_H
