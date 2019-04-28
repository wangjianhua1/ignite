package com.demo.server;

import com.demo.service.HelloService;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import java.util.Scanner;

public class IgniteClientDemo {
    public static void main(String[] args) {
        IgniteClientDemo demo = new IgniteClientDemo();
        demo.call();
    }

    private void call() {
        try (Ignite ignite = Ignition.start()) {
            HelloService helloService = ignite.services().serviceProxy(IgniteServerDemo.serviceName, HelloService.class, false);
            Scanner in = new Scanner(System.in);
            while (in.hasNext()) {
                helloService.sayRepeat(in.next());
            }
        }
    }

}
