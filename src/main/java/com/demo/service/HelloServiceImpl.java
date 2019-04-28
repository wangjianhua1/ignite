package com.demo.service;

import org.apache.ignite.services.ServiceContext;

public class HelloServiceImpl implements HelloService {
    @Override
    public String sayRepeat(String msg) {
        System.out.println("接收字符串：" + msg);
        return msg;
    }

    @Override
    public void cancel(ServiceContext ctx) {

    }

    @Override
    public void init(ServiceContext ctx) throws Exception {

    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {

    }

    public static void main(String[] args) {

    }
}
