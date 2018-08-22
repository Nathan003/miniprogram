package com.dodoca.miniprogram.service.impl;

import com.dodoca.miniprogram.service.HomePageTradeService;
import com.dodoca.miniprogram.util.ImpalaJdbc;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

@Service
public class HomePageTradeServiceImpl implements HomePageTradeService {
    @Override
    @Cacheable(value = "HomePageTradeServiceImpl", key = "'getYestodayData' + #merchant_id")
    public HashMap<String, Object> getYestodayData(String merchant_id) throws SQLException {
        String getYestodayData = "select merchant_id,'昨日' as day_type,max(1_pv3) as 1_pv3,max(order_1) as order_1\n" +
                "from \n" +
                "(\n" +
                "select merchant_id, 1_pv3,0 as order_1\n" +
                "from query_result_pv\n" +
                "where merchant_id=" + merchant_id + "\n" +
                "union all \n" +
                "select merchant_id, 0 as 1_pv3, order_1\n" +
                "from query_result_order\n" +
                "where merchant_id=" + merchant_id + "\n" +
                ") a\n" +
                "group by merchant_id;";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int browse_sum = rs.getInt("1_pv3");
            int order_sum = rs.getInt("order_1");
            detailMap.put("browse_sum", browse_sum);
            detailMap.put("order_sum", order_sum);
        }
        ImpalaJdbc.close(rs, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "HomePageTradeServiceImpl", key = "'getSevendaysData' + #merchant_id")
    public HashMap<String, Object> getSevendaysData(String merchant_id) throws SQLException {
        String getSevendaysData = "select merchant_id,'7天' as day_type,max(7_sum_pv3) as 7_sum_pv3,max(order_7) as order_7\n" +
                "from \n" +
                "(\n" +
                "select merchant_id, 7_sum_pv3,0 as order_7\n" +
                "from query_result_pv\n" +
                "where merchant_id=" + merchant_id + "\n" +
                "union all \n" +
                "select merchant_id, 0 as 7_sum_pv3, order_7\n" +
                "from query_result_order\n" +
                "where merchant_id=" + merchant_id + "\n" +
                ") a\n" +
                "group by merchant_id;";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevendaysData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int browse_sum = rs.getInt("7_sum_pv3");
            int order_sum = rs.getInt("order_7");
            detailMap.put("browse_sum", browse_sum);
            detailMap.put("order_sum", order_sum);
        }
        ImpalaJdbc.close(rs, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "HomePageTradeServiceImpl", key = "'getThirtydaysData' + #merchant_id")
    public HashMap<String, Object> getThirtydaysData(String merchant_id) throws SQLException {
        String getThirtydaysData = "select merchant_id,'30天' as day_type,max(30_sum_pv3) as 30_sum_pv3,max(order_30) as order_30\n" +
                "from \n" +
                "(\n" +
                "select merchant_id, 30_sum_pv3,0 as order_30\n" +
                "from query_result_pv\n" +
                "where merchant_id=" + merchant_id + "\n" +
                "union all \n" +
                "select merchant_id, 0 as 30_sum_pv3, order_30\n" +
                "from query_result_order\n" +
                "where merchant_id=" + merchant_id + "\n" +
                ") a\n" +
                "group by merchant_id;";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getThirtydaysData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int browse_sum = rs.getInt("30_sum_pv3");
            int order_sum = rs.getInt("order_30");
            detailMap.put("browse_sum", browse_sum);
            detailMap.put("order_sum", order_sum);
        }
        ImpalaJdbc.close(rs, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "HomePageTradeServiceImpl", key = "'getTradeYestodayChart' + #merchant_id")
    public HashMap<String, Object> getTradeYestodayChart(String merchant_id) throws SQLException {
        String getYestodayChart = "select merchant_id,hour , pv3_hour \n" +
                "from query_result_pv_hour\n" +
                "where  merchant_id=" + merchant_id + " and day_type='1_ago';";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayChart);
        ResultSet rs = pst.executeQuery();
        HashMap<String, Object> chartMap = new HashMap<>();
        chartMap.put("00:00~00:59", "");
        chartMap.put("01:00~01:59", "");
        chartMap.put("02:00~02:59", "");
        chartMap.put("03:00~03:59", "");
        chartMap.put("04:00~04:59", "");
        chartMap.put("05:00~05:59", "");
        chartMap.put("06:00~06:59", "");
        chartMap.put("07:00~07:59", "");
        chartMap.put("08:00~08:59", "");
        chartMap.put("09:00~09:59", "");
        chartMap.put("10:00~10:59", "");
        chartMap.put("11:00~11:59", "");
        chartMap.put("12:00~12:59", "");
        chartMap.put("13:00~13:59", "");
        chartMap.put("14:00~14:59", "");
        chartMap.put("15:00~15:59", "");
        chartMap.put("16:00~16:59", "");
        chartMap.put("17:00~17:59", "");
        chartMap.put("18:00~18:59", "");
        chartMap.put("19:00~19:59", "");
        chartMap.put("20:00~20:59", "");
        chartMap.put("21:00~21:59", "");
        chartMap.put("22:00~22:59", "");
        chartMap.put("23:00~23:59", "");
        while (rs.next()) {
            String hour = rs.getString("hour").replace("：", ":");
            int pv3_hour = rs.getInt("pv3_hour");
            if (hour.length() < 11) {
                String[] split = hour.split("~");
                hour = "0" + split[0] + "~0" + split[1];
            }
            chartMap.put(hour, pv3_hour + "");
        }
        ImpalaJdbc.close(rs, pst, conn);
        return chartMap;
    }

    @Override
    @Cacheable(value = "HomePageTradeServiceImpl", key = "'getTradeTodayChart' + #merchant_id")
    public HashMap<String, Object> getTradeTodayChart(String merchant_id) throws SQLException {
        String getTodayChart = "select merchant_id,hour , pv3_hour \n" +
                "from query_result_pv_hour\n" +
                "where merchant_id=" + merchant_id + " and day_type='2_ago';";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getTodayChart);
        ResultSet rs = pst.executeQuery();
        HashMap<String, Object> chartMap = new HashMap<>();
        chartMap.put("00:00~00:59", "");
        chartMap.put("01:00~01:59", "");
        chartMap.put("02:00~02:59", "");
        chartMap.put("03:00~03:59", "");
        chartMap.put("04:00~04:59", "");
        chartMap.put("05:00~05:59", "");
        chartMap.put("06:00~06:59", "");
        chartMap.put("07:00~07:59", "");
        chartMap.put("08:00~08:59", "");
        chartMap.put("09:00~09:59", "");
        chartMap.put("10:00~10:59", "");
        chartMap.put("11:00~11:59", "");
        chartMap.put("12:00~12:59", "");
        chartMap.put("13:00~13:59", "");
        chartMap.put("14:00~14:59", "");
        chartMap.put("15:00~15:59", "");
        chartMap.put("16:00~16:59", "");
        chartMap.put("17:00~17:59", "");
        chartMap.put("18:00~18:59", "");
        chartMap.put("19:00~19:59", "");
        chartMap.put("20:00~20:59", "");
        chartMap.put("21:00~21:59", "");
        chartMap.put("22:00~22:59", "");
        chartMap.put("23:00~23:59", "");
        while (rs.next()) {
            String hour = rs.getString("hour").replace("：", ":");
            int pv3_hour = rs.getInt("pv3_hour");
            if (hour.length() < 11) {
                String[] split = hour.split("~");
                hour = "0" + split[0] + "~0" + split[1];
            }
            chartMap.put(hour, pv3_hour + "");
        }
        ImpalaJdbc.close(rs, pst, conn);
        return chartMap;
    }

    @Override
    @Cacheable(value = "HomePageTradeServiceImpl", key = "'getTradeSevendaysChart' + #merchant_id")
    public HashMap<String, Object> getTradeSevendaysChart(String merchant_id) throws SQLException {
        String getSevendaysChart = "select merchant_id,`date`,pv3_day\n" +
                "from query_result_pv_day\n" +
                "where merchant_id=" + merchant_id + " and day_type='7_list';";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevendaysChart);
        ResultSet rs = pst.executeQuery();
        HashMap<String, Object> chartMap = new HashMap<>();
        while (rs.next()) {
            String date = rs.getString("date");
            int pv3_day = rs.getInt("pv3_day");
            chartMap.put(date, pv3_day + "");
        }
        ImpalaJdbc.close(rs, pst, conn);
        return chartMap;
    }

    @Override
    @Cacheable(value = "HomePageTradeServiceImpl", key = "'getTradeThirtydaysChart' + #merchant_id")
    public HashMap<String, Object> getTradeThirtydaysChart(String merchant_id) throws SQLException {
        String getThirtydaysChart = "select merchant_id,`date`,pv3_day\n" +
                "from query_result_pv_day\n" +
                "where merchant_id=" + merchant_id + " and day_type='30_list';";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getThirtydaysChart);
        ResultSet rs = pst.executeQuery();
        HashMap<String, Object> chartMap = new HashMap<>();
        while (rs.next()) {
            String date = rs.getString("date");
            int pv3_day = rs.getInt("pv3_day");
            chartMap.put(date, pv3_day + "");
        }
        ImpalaJdbc.close(rs, pst, conn);
        return chartMap;
    }
}
