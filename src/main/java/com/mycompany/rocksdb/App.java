package com.mycompany.rocksdb;

import com.mycompany.rocksdb.nfs4.Nfsv4Server;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        // vertx.deployVerticle(new PortMapperServer());
        // vertx.deployVerticle(new MountServer());
        // vertx.deployVerticle(new Nfsv3Server());
        vertx.deployVerticle(new Nfsv4Server());

    }
}
