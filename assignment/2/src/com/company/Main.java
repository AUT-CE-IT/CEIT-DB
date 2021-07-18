package com.company;

import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Main {

    private static void read(Jedis jedis) throws FileNotFoundException {
        File file = new File("NYSE_20210301.csv");
        Scanner sc = new Scanner(file);
        while (sc.hasNext()) {
            String[] data = sc.next().split(",");
            jedis.set(data[0], data[1]);
        }
    }

    private static void flush(Jedis jedis) {
        jedis.flushAll();
    }

    private static void create(Jedis jedis, String key, String value) {
        if (jedis.exists(key)) {
            System.out.println(false);
            return;
        }

        jedis.set(key, value);
        System.out.println(true);
    }

    private static void fetch(Jedis jedis, String key) {
        if (!jedis.exists(key)) {
            System.out.println(false);
            return;
        }

        System.out.println(true);
        System.out.println(jedis.get(key));
    }

    private static void update(Jedis jedis, String key, String value) {
        if (!jedis.exists(key)) {
            System.out.println(false);
            return;
        }

        jedis.set(key, value);
        System.out.println(true);
    }

    private static void delete(Jedis jedis, String key) {
        if (!jedis.exists(key)) {
            System.out.println(false);
            return;
        }

        jedis.del(key);
        System.out.println(true);
    }

    public static void main(String[] args) {
        try{
            Jedis jedis = new Jedis("localhost");
            flush(jedis);

            //read from file
            read(jedis);

            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String[] input = sc.nextLine().split(" ", -1);

                switch (input[0]) {
                    case "create" -> {
                        if (input.length == 3) {
                            create(jedis, input[1], input[2]);
                        } else {
                            System.err.println("Invalid operation");
                        }
                    }
                    case "fetch" -> {
                        if (input.length == 2) {
                            fetch(jedis, input[1]);
                        } else {
                            System.err.println("Invalid operation");
                        }
                    }
                    case "update" -> {
                        if (input.length == 3) {
                            update(jedis, input[1], input[2]);
                        } else {
                            System.err.println("Invalid operation");
                        }
                    }
                    case "delete" -> {
                        if (input.length == 2) {
                            delete(jedis, input[1]);
                        } else {
                            System.err.println("Invalid operation");
                        }
                    }
                    case "flush" -> {
                        if (input.length == 1) {
                            flush(jedis);
                        } else {
                            System.err.println("Invalid operation");
                        }
                    }
                    default -> System.err.println("Invalid operation");
                }

                System.out.println();
            }

        } catch (Exception e){
            System.err.println(e.toString());
        }
    }
}