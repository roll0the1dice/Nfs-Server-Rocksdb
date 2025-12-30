package com.mycompany.rocksdb;

import com.mycompany.rocksdb.netserver.MountServer;
import com.mycompany.rocksdb.netserver.Nfsv3Server;
import com.mycompany.rocksdb.netserver.PortMapperServer;
import com.mycompany.rocksdb.nfs4.Nfsv4Server;
import com.mycompany.rocksdb.smb2.Smb2Server;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {
    public static void main(String[] args) {
        VertxOptions options = new VertxOptions()
        // 设置阻塞检查间隔（单位：毫秒），默认是 2000
        .setBlockedThreadCheckInterval(1000 * 60 * 60); // 设置为 1 小时，基本就没警告了
            Vertx vertx = Vertx.vertx(options);

         vertx.deployVerticle(new PortMapperServer());
         vertx.deployVerticle(new MountServer());
         vertx.deployVerticle(new Nfsv3Server());
        // vertx.deployVerticle(new Nfsv4Server());
        //vertx.deployVerticle(new Smb2Server());
    }
}
    