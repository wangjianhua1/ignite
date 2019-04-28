package com.demo;

import com.demo.entity.City;
import org.apache.ignite.*;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import java.sql.*;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class IgniteDemo {

    public static void main(String[] args) throws SQLException {
        Connection conn = new IgniteDemo().getConn();
////        initTable(conn);
////        initData(conn);
////        selectPerson(conn);
////        queryCacheCity();
////        conn.close();
////        compute();
////        cache();
//        opAbsent();
//
    }

    /**
     * 获取ignite连接，默认10800端口
     *
     * @return
     */
    public Connection getConn() {
        try {
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
            return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 初始化表结构
     */
    @Test
    public void initTable() {
        Connection conn = getConn();
        try (Statement stmt = conn.createStatement()) {
            // 创建基于复制模式的City表
            stmt.executeUpdate("CREATE TABLE City (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"");
            // 创建基于分片模式，备份数为1的Person表
            stmt.executeUpdate("CREATE TABLE Person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id))WITH \"backups=1, affinityKey=city_id\"");
            // 创建City表的索引
            stmt.executeUpdate("CREATE INDEX idx_city_name ON City (name)");
            // 创建Person表的索引
            stmt.executeUpdate("CREATE INDEX idx_person_name ON Person (name)");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化表数据
     */
    @Test
    public void initData() {
        Connection conn = getConn();
        // Populate City table
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO City (id, name) VALUES (?, ?)")) {
            stmt.setLong(1, 1L);
            stmt.setString(2, "Forest Hill");
            stmt.executeUpdate();
            stmt.setLong(1, 2L);
            stmt.setString(2, "Denver");
            stmt.executeUpdate();
            stmt.setLong(1, 3L);
            stmt.setString(2, "St. Petersburg");
            stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Populate Person table
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO Person (id, name, city_id) VALUES (?, ?, ?)")) {
            stmt.setLong(1, 1L);
            stmt.setString(2, "John Doe");
            stmt.setLong(3, 3L);
            stmt.executeUpdate();
            stmt.setLong(1, 2L);
            stmt.setString(2, "Jane Roe");
            stmt.setLong(3, 2L);
            stmt.executeUpdate();
            stmt.setLong(1, 3L);
            stmt.setString(2, "Mary Major");
            stmt.setLong(3, 1L);
            stmt.executeUpdate();
            stmt.setLong(1, 4L);
            stmt.setString(2, "Richard Miles");
            stmt.setLong(3, 2L);
            stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试查询Person表数据
     */
    @Test
    public void selectPerson() {
        Connection conn = getConn();
        // 使用标准的sql获取数据
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT p.name, c.name FROM Person p, City c WHERE p.city_id = c.id")) {
                while (rs.next()) {
                    System.out.println(rs.getString(1) + ", " + rs.getString(2));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 作为数据网格应用
     */
    @Test
    public void cache() {
        try (Ignite ignite = Ignition.start()) {
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCacheName");

            // Store keys in cache (values will end up on different cache nodes).
            for (int i = 0; i < 10; i++)
                cache.put(i, Integer.toString(i));

            for (int i = 0; i < 10; i++)
                System.out.println("Got [key=" + i + ", val=" + cache.get(i) + ']');
        }
    }

    @Test
    public void queryCacheCity() {
        Ignite ignite = Ignition.start();
        IgniteCache<Long, City> cityCache = ignite.cache("SQL_PUBLIC_CITY");
        SqlFieldsQuery query = new SqlFieldsQuery("select p.name,c.name from Person p,City c where p.city_id=c.id");

        FieldsQueryCursor<List<?>> cursor = cityCache.query(query);

        Iterator<List<?>> iterator = cursor.iterator();
        while (iterator.hasNext()) {
            List<?> row = iterator.next();
            System.out.println(row.get(0) + ", " + row.get(1));
        }
    }

    /**
     * 原子操作
     */
    @Test
    public void opAbsent() {
        Ignite ignite = Ignition.start();
        Collection<String> cacheNames = ignite.cacheNames();
        System.out.println(cacheNames);
        IgniteCache<String, Integer> cache = ignite.getOrCreateCache("myCacheName");
        // Put-if-absent which returns previous value.
        Integer oldVal = cache.getAndPutIfAbsent("Hello", 11);

        // Put-if-absent which returns boolean success flag.
        boolean success = cache.putIfAbsent("World", 22);

        // Replace-if-exists operation (opposite of getAndPutIfAbsent), returns previous value.
        oldVal = cache.getAndReplace("Hello", 12);

        // Replace-if-exists operation (opposite of putIfAbsent), returns boolean success flag.
        success = cache.replace("World", 23);

        // Replace-if-matches operation.
        success = cache.replace("World", 2, 22);

        // Remove-if-matches operation.
        success = cache.remove("Hello", 1);
        Integer hello = cache.get("Hello");
        Integer world = cache.get("World");
        System.out.println(hello + "==" + world);

    }

    /**
     * 事物操作
     */
    @Test
    public void transaction() {
        Ignite ignite = Ignition.start();
        IgniteCache<String, Integer> cache = ignite.getOrCreateCache("myCacheName");
        try (Transaction tx = ignite.transactions().txStart()) {
            Integer hello = cache.get("Hello");

            if (hello == 1) {
                cache.put("Hello", 111);
            } else {
                cache.put("World", 22);
            }
            tx.commit();
        }
    }

    /**
     * 分布式锁
     */
    @Test
    public void lock() {
        Ignite ignite = Ignition.start();
        IgniteCache<String, Integer> cache = ignite.getOrCreateCache("myCacheName");
        Lock lock = cache.lock("Hello");
        lock.lock();
        try {
            cache.put("Hello", 1111);
            cache.put("World", 2222);
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void compute() {
        //连接集群
        Ignite ignite = Ignition.start();//启动节点
        Collection<IgniteCallable<Integer>> calls = new ArrayList<>();
        //空格分组并将任务计算分派到各个节点计算
        for (final String word : "Count characters using callable".split(" ")) {
            calls.add(word::length);
        }
        //分派
        Collection<Integer> res = ignite.compute().call(calls);
        //计算结果汇总
        int sum = res.stream().mapToInt(Integer::intValue).sum();
        System.out.println("Total number of characters is :" + sum);

    }


    /**
     * 使用编程方式连接集群
     *
     * @return
     */
    public static Ignite getIgnite() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47501"));
        spi.setIpFinder(ipFinder);
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDiscoverySpi(spi);
        cfg.setClientMode(true);//客户端或服务端开关
        return Ignition.start(cfg);
    }

    public static Ignite CommunicationSpi() {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47501"));
        spi.setSlowClientQueueLimit(1000);//设置队列限制值
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setCommunicationSpi(spi);
        cfg.setClientMode(true);//客户端或服务端开关
        return Ignition.start(cfg);
    }

    /**
     * 服务端节点执行
     * 本地节点执行
     */
    @Test
    public void client_cluster() {
        Ignite ignite = Ignition.start("E:\\apache-ignite-2.7.0-bin\\config\\default-config.xml");
//        Ignite ignite = Ignition.ignite();
        ClusterGroup clusterGroup = ignite.cluster().forClients();
        IgniteCompute compute = ignite.compute(clusterGroup);
        compute.broadcast(() -> System.out.println("hello client"));
    }

    /**
     * 服务端节点执行
     * 只把作业广播到远程节点执行(除了本地节点)
     */
    @Test
    public void server_cluster() {
        Ignite ignite = getIgnite();
//        Ignite ignite = Ignition.ignite();
        IgniteCompute compute = ignite.compute();
        // Local Ignite node.
        IgniteCluster cluster = ignite.cluster();
        ClusterNode localNode = cluster.localNode();
        // Node metrics.
        ClusterMetrics metrics = localNode.metrics();
        // Get some metric values.
        double cpuLoad = metrics.getCurrentCpuLoad();
        long usedHeap = metrics.getHeapMemoryUsed();
        int numberOfCores = metrics.getTotalCpus();
        int activeJobs = metrics.getCurrentActiveJobs();
        compute.broadcast(() -> System.out.println("节点Id: " + ignite.cluster().localNode().id()));
    }


}


