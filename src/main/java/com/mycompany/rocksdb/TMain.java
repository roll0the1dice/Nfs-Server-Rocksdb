package com.mycompany.rocksdb;


import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import io.lettuce.core.RedisClient;


public class TMain {
    public static void main(String[] args) {
//        Vertx vertx = Vertx.vertx();
//        FileSystem fs = vertx.fileSystem();
//
//        Mono.fromCallable(() -> {
//            return "[11,22,33]";
//        })
//        .map(link -> link.substring(1, link.length() - 1).split(","))
//                .flatMapMany(Flux::fromArray)
//                .index()
//                .map(tuple -> {
//                    String ip = "192.168.1.1";
//                    String lunName = "fs-SP0-2";
//                    String curVnode = tuple.getT2();
//
//                    return Tuples.of(tuple.getT1(), ip, lunName, curVnode);
//                })
//                .collectMap(reactor.util.function.Tuple2::getT1)
//                .map(resMap -> {
//                    int m = 0, k = 1;
//                    List<Tuple3<String, String, String>> res = new ArrayList<>(k + m);
//                    for (long i = 0; i < k + m; i++) {
//                        res.add(Tuples.of(resMap.get(i).getT2(), resMap.get(i).getT3(), resMap.get(i).getT4()));
//                    }
//                    return res;
//                })
//                .subscribe(objects -> {
//                    System.out.println("objects: " + objects);
//                });

        // List<Long> link = Arrays.asList(31312L);
        // saveRedis("4436", "fs-SP0-2", link, "0002");
        System.out.println(Paths.get("/tmp/smb2_share", "/").toString());

    }

    public static void saveRedis(String vnodeId, String lun, List<Long> link, String s_uuid) {
        // 连接到本地 Redis 6号库
        RedisURI redisURI = RedisURI.builder().withHost("localhost")
                .withPort(6379)
                .withPassword("Gw@uUp8tBedfrWDy".toCharArray())
                .withDatabase(6)
                .build();
        RedisClient redisClient = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> commands = connection.sync();
        // 写入数据
        String redisKey = "dataa" + vnodeId;

        commands.hset(redisKey, "v_num", vnodeId);
        commands.hset(redisKey, "lun_name", lun);
        commands.hset(redisKey, "link", link.toString());
        commands.hset(redisKey, "s_uuid", s_uuid);
        commands.hset(redisKey, "take_over", "0");

        // 关闭连接
        connection.close();
        redisClient.shutdown();
    }
}
