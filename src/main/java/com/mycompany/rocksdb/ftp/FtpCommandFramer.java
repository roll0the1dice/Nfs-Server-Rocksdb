package com.mycompany.rocksdb.ftp;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.reactivestreams.Publisher;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.reactivex.core.buffer.Buffer; // 使用 Reactivex Buffer

import java.nio.charset.StandardCharsets;

/**
 * 一个 FlowableTransformer，用于将原始的 TCP 字节流解析成一行一行的 FTP 命令。
 * 它使用 \r\n 作为分隔符，并将每一行转换成一个 String 后向下游发射。
 */
public class FtpCommandFramer implements FlowableTransformer<Buffer, String> {

    @Override
    public Publisher<String> apply(Flowable<Buffer> upstream) {
        return Flowable.create(emitter -> {

            // 使用 newDelimited 来按行解析，这正是 FTP 所需的！
            final RecordParser parser = RecordParser.newDelimited("\r\n");

            parser.handler(lineBuffer -> {
                // RecordParser 给出的 buffer 是一行完整的数据（不含 \r\n）
                String commandLine = lineBuffer.toString(StandardCharsets.UTF_8).trim();

                // 忽略空行
                if (!commandLine.isEmpty()) {
                    System.out.println("FTP Command Received: " + commandLine);
                    emitter.onNext(commandLine); // 向下游发射解析出的命令字符串
                }
            });

            parser.exceptionHandler(emitter::onError);

            // 订阅上游的原始字节流，并将数据喂给 parser
            upstream.subscribe(
                    reactiveBuffer -> parser.handle(reactiveBuffer.getDelegate()), // 同样需要 getDelegate()
                    emitter::onError,
                    emitter::onComplete
            );

        }, io.reactivex.BackpressureStrategy.BUFFER);
    }
}