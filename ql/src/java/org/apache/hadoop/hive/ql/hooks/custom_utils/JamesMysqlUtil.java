package org.apache.hadoop.hive.ql.hooks.custom_utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class JamesMysqlUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JamesMysqlUtil.class);


    // MySQL 8.0 以下版本 - JDBC 驱动名及数据库 URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/developer";

    // MySQL 8.0 以上版本 - JDBC 驱动名及数据库 URL
    //static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    //static final String DB_URL = "jdbc:mysql://localhost:3306/RUNOOB?useSSL=false&serverTimezone=UTC";


    // 数据库的用户名与密码，需要根据自己的设置
    static final String USER = "developer";
    static final String PASS = "developer";

    private static ThreadLocal<DateFormat> threadLocal = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    public static Date parse(String dateStr) throws ParseException {
        return threadLocal.get().parse(dateStr);
    }

    public static String format(Date date) {
        return threadLocal.get().format(date);
    }

    public static void main(String[] args) {
        Date date = new Date();
        String formatDate = format(date);
        System.out.print(String.format("formatDate=%s", formatDate));


        String lineage = "lineage string";
        insertLineageIntoMysql(lineage, formatDate);
    }

    public static void insertLineageIntoMysql(String lineage, String updateTime) {
        if (null == lineage || lineage.isEmpty()) {
            return;
        }

        LOG.info(String.format("%s", lineage));

        Connection conn = null;
        PreparedStatement ps = null;

        try {
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            LOG.info("连接数据库...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            String sql = "INSERT INTO txkd_dc_hive_lineage_log(lineage_str,update_time) VALUES (?,?)";
            ps = conn.prepareStatement(sql);
            ps.setString(1, lineage);
            ps.setString(2, updateTime);

            int resultSet = ps.executeUpdate();
            if (resultSet > 0) {
                //如果插入成功，则打印success
                LOG.info("Insert Successfully !");
            } else {
                //如果插入失败，则打印Failure
                LOG.info("Insert Failed !");
            }

            // 完成后关闭
            ps.close();
            conn.close();
        } catch (SQLException se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        } catch (Exception e) {
            // 处理 Class.forName 错误
            e.printStackTrace();
        } finally {
            // 关闭资源
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException se2) {
            }// 什么都不做
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }
}
