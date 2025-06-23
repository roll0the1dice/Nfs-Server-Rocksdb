package com.mycompany.rocksdb;

import com.mycompany.rocksdb.netserver.MountServer;
import com.mycompany.rocksdb.netserver.Nfsv3Server;
import com.mycompany.rocksdb.netserver.PortMapperServer;
import io.vertx.core.Vertx;

public class App {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        vertx.deployVerticle(new PortMapperServer());
        vertx.deployVerticle(new MountServer());
        vertx.deployVerticle(new Nfsv3Server());
    }
}
