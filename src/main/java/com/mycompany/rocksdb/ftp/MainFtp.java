package com.mycompany.rocksdb.ftp;

import io.vertx.core.Vertx;

public class MainFtp {
    public static void main(String[] args) throws Exception {
        Vertx vertx = Vertx.vertx();
        FTPServer.EXTERNAL_IP = "172.20.0.185";
        vertx.deployVerticle(new FTPServer());
    }
}
