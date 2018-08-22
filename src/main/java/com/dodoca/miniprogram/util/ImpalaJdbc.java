package com.dodoca.miniprogram.util;

import org.apache.log4j.Logger;

import java.sql.*;

public class ImpalaJdbc {


    private static Logger logger = Logger.getLogger(ImpalaJdbc.class);

    private ImpalaJdbc() {}

    static {
        try {
            Class.forName(Constant.JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(),e);
        }
    }

    public static Connection getImpalaConnection() {

        Connection con = null;

        try {
            con = DriverManager.getConnection(Constant.CONNECTION_URL);
        } catch (SQLException e) {
            logger.error(e.getMessage(),e);
        }
        return con;
    }

    /**
     * close jdbc resource
     * @param rs
     * @return
     */
    public static void close(ResultSet rs, PreparedStatement pst, Connection conn){
        try {
            if(rs!=null) {
                rs.close();
            }
            if(pst!=null) {
                pst.close();
            }
            if(conn!=null){
                conn.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(),e);
        }
    }
}
