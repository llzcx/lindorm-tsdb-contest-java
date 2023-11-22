package com.alibaba.lindorm.contest.extendstructs.vlog;

import java.nio.ByteBuffer;

public class ByteBufferVo{
        public final ByteBuffer decompressed;
        public final boolean inWriter;

        public ByteBufferVo(ByteBuffer decompressed, boolean inWriter) {
            this.decompressed = decompressed;
            this.inWriter = inWriter;
        }
    }