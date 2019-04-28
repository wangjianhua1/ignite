package com.demo.service;

import org.apache.ignite.services.Service;

public interface HelloService extends Service {
    //接收客户端信息
    String sayRepeat(String msg);
}
