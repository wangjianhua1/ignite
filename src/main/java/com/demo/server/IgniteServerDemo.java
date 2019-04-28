package com.demo.server;

import com.demo.service.HelloServiceImpl;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class IgniteServerDemo {
    public static String serviceName = "sayHello";

    public static void main(String[] args) {
        IgniteServerDemo demo = new IgniteServerDemo();
        demo.deploy();
    }

    private void deploy() {
        Ignite ignite = Ignition.start();
        ignite.services().deployClusterSingleton(serviceName, new HelloServiceImpl());

    }

}
