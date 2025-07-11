package com.mycompany.rocksdb;

import com.mycompany.rocksdb.netserver.MountServer;
import com.mycompany.rocksdb.netserver.Nfsv3Server;
import com.mycompany.rocksdb.netserver.PortMapperServer;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
public class App {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        vertx.deployVerticle(new PortMapperServer());
        vertx.deployVerticle(new MountServer());
        vertx.deployVerticle(new Nfsv3Server());

            SimpleRSocketServer server = new SimpleRSocketServer();

            CloseableChannel channel = RSocketFactory.receive()
                    .frameDecoder(PayloadDecoder.DEFAULT)
                    .acceptor((setup, sendingSocket) -> Mono.just(server))
                    .transport(TcpServerTransport.create("0.0.0.0", 7000))
                    .start()
                    .block();

            log.info("Simple RSocket Server started on 0.0.0.0:7000 (accessible from any IP)");

            // 保持服务器运行
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                log.info("Server interrupted");
            } finally {
                if (channel != null) {
                    channel.dispose();
                }
            }
    }
}
