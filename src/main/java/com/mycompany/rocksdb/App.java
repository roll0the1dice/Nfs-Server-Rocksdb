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

    }
}
