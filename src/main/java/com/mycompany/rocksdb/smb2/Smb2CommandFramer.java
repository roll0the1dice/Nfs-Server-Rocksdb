package com.mycompany.rocksdb.smb2;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.parsetools.RecordParser;
import org.reactivestreams.Publisher;

import java.util.concurrent.atomic.AtomicReference;

public class Smb2CommandFramer implements FlowableTransformer<Buffer, Buffer> {
    private Smb2ParseState currentState = Smb2ParseState.READING_MARKER;
    private boolean isLastFragment;
    private int expectedFragmentLength;
    private RecordParser parser;

    @Override
    public Publisher<Buffer> apply(Flowable<Buffer> upstream) {
        return Flowable.create(emitter -> {
            // currentState 初始应为 READING_NBSS_HEADER
            parser = RecordParser.newFixed(4); 

            parser.handler(buffer -> {
                try {
                    if (currentState == Smb2ParseState.READING_MARKER) { // 建议更名为 READING_NBSS_HEADER
                        // 1. 验证第一个字节是否为 0x00 (SMB2 over TCP 规范)
                        short type = buffer.getUnsignedByte(0);
                        
                        // 2. 提取 24 位的长度 (第 1, 2, 3 字节)
                        // Vert.x Buffer 提供 getUnsignedMedium 可以直接读取 3 个字节
                        int messageLength = buffer.getUnsignedMedium(1);

                        if (messageLength <= 0) {
                            // 处理心跳或空包
                            parser.fixedSizeMode(4);
                            currentState = Smb2ParseState.READING_MARKER;
                        } else {
                            // 3. 切换到读取消息体模式
                            parser.fixedSizeMode(messageLength);
                            currentState = Smb2ParseState.READING_FRAGMENT_DATA;
                        }
                    } else if (currentState == Smb2ParseState.READING_FRAGMENT_DATA) {
                        // 此时 buffer 包含了完整的 SMB2 消息（不含 4 字节头）
                        // 直接发送给下游 Handler 处理
                        emitter.onNext(buffer.copy()); 

                        // 4. 回到读取下一个包的头部模式
                        parser.fixedSizeMode(4);
                        currentState = Smb2ParseState.READING_MARKER;
                    }
                } catch (Exception e) {
                    emitter.onError(e);
                }
            });

            parser.exceptionHandler(emitter::onError);

            upstream.subscribe(
                    parser::handle,
                    emitter::onError,
                    emitter::onComplete
            );

        }, io.reactivex.BackpressureStrategy.BUFFER);
    }

    private enum Smb2ParseState {
        READING_MARKER,
        READING_FRAGMENT_DATA
    }
}

