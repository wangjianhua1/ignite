package com.demo;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.Ignition;

import java.sql.*;

public class IgniteDemo {

    public static void main(String[] args) throws SQLException {
        Connection conn = getConn();
//        initTable(conn);
//        initData(conn);
        selectPerson(conn);
        conn.close();
    }

    /**
     * 获取ignite连接，默认10800端口
     * @return
     */
    public static Connection getConn() {
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
     *
     * @param conn
     */
    public static void initTable(Connection conn) {
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
     *
     * @param conn
     */
    public static void initData(Connection conn) {
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
     *
     * @param conn
     */
    public static void selectPerson(Connection conn) {
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


    public static void compute() {
        //连接集群
        Ignite ignite = Ignition.start();
        IgniteCache<Long, City> cityCache = ignite.cache("SQL_PUBLIC_CITY");

    }
    
    public static void cluster(){
        Ignite ignite =Ignition.start();
        IgniteCluster cluster = ignite.cluster();
    }

}

class City {
    private Long id;
    private String name;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

class Person {
    private Long id;
    private String name;
    private Long city_id;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getCity_id() {
        return city_id;
    }

    public void setCity_id(Long city_id) {
        this.city_id = city_id;
    }
}