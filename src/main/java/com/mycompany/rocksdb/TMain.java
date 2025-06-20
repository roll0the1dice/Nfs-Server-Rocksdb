package com.mycompany.rocksdb;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class TMain {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        FileSystem fs = vertx.fileSystem();

        Mono.fromCallable(() -> {
            return "[11,22,33]";
        })
        .map(link -> link.substring(1, link.length() - 1).split(","))
                .flatMapMany(Flux::fromArray)
                .index()
                .map(tuple -> {
                    String ip = "192.168.1.1";
                    String lunName = "fs-SP0-2";
                    String curVnode = tuple.getT2();

                    return Tuples.of(tuple.getT1(), ip, lunName, curVnode);
                })
                .collectMap(reactor.util.function.Tuple2::getT1)
                .map(resMap -> {
                    int m = 0, k = 1;
                    List<Tuple3<String, String, String>> res = new ArrayList<>(k + m);
                    for (long i = 0; i < k + m; i++) {
                        res.add(Tuples.of(resMap.get(i).getT2(), resMap.get(i).getT3(), resMap.get(i).getT4()));
                    }
                    return res;
                })
                .subscribe(objects -> {
                    System.out.println("objects: " + objects);
                });


    }
}
