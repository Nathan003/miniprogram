package com.dodoca.miniprogram.service.impl;

import com.dodoca.miniprogram.service.HomePageTwitterService;
import com.dodoca.miniprogram.util.ImpalaJdbc;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

@Service
public class HomePageTwitterServiceImpl implements HomePageTwitterService {
    @Override
    @Cacheable(value = "getTwitterYestodayData")
    public HashMap<String, Object> getTwitterYestodayData(String merchant_id) throws SQLException {

        String getYestodayData = "select merchant_id,max(partner_new_1) as partner_new_1,max(partner_member_1) as partner_member_1\n" +
                ",max(order_amount_1) as order_amount_1,max(total_comission_1) as total_comission_1,max(order_c1) as order_c1\n" +
                "from \n" +
                "(\n" +
                "select merchant_id,partner_new_1,0 as partner_member_1,0 as order_amount_1,0 as total_comission_1,0 as order_c1\n" +
                "from query_result_partner_new\n" +
                "where merchant_id=" + merchant_id + " \n" +
                "union all \n" +
                "select merchant_id,0 as partner_new_1,partner_member_1,0 as order_amount_1,0 as total_comission_1,0 as order_c1\n" +
                "from query_result_partner_member\n" +
                "where merchant_id=" + merchant_id + " \n" +
                "union all \n" +
                "select merchant_id,0 as partner_new_1,0 as partner_member_1,order_amount_1,total_comission_1,order_c1\n" +
                "from query_result_order_kpi\n" +
                "where merchant_id=" + merchant_id + " \n" +
                ") a\n" +
                "group by merchant_id\n" +
                ";";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int partner_new_1 = rs.getInt("partner_new_1");
            int partner_member_1 = rs.getInt("partner_member_1");
            int order_amount_1 = rs.getInt("order_amount_1");
            int total_comission_1 = rs.getInt("total_comission_1");
            int order_c1 = rs.getInt("order_c1");
            detailMap.put("add_sum", partner_new_1);
            detailMap.put("member_sum", partner_member_1);
            detailMap.put("order_amount", order_amount_1);
            detailMap.put("commission_amount", total_comission_1);
            detailMap.put("order_sum", order_c1);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTwitterSevendaysData")
    public HashMap<String, Object> getTwitterSevendaysData(String merchant_id) throws SQLException {
        String getSevendaysData = "select merchant_id,max(partner_new_7) as partner_new_7,max(partner_member_7) as partner_member_7\n" +
                ",max(order_amount_7) as order_amount_7,max(total_comission_7) as total_comission_7,max(order_c7) as order_c7\n" +
                "from \n" +
                "(\n" +
                "select merchant_id,partner_new_7,0 as partner_member_7,0 as order_amount_7,0 as total_comission_7,0 as order_c7\n" +
                "from query_result_partner_new\n" +
                "where merchant_id=" + merchant_id + " \n" +
                "union all \n" +
                "select merchant_id,0 as partner_new_7,partner_member_7,0 as order_amount_7,0 as total_comission_7,0 as order_c7\n" +
                "from query_result_partner_member\n" +
                "where merchant_id=" + merchant_id + " \n" +
                "union all \n" +
                "select merchant_id,0 as partner_new_7,0 as partner_member_7,order_amount_7,total_comission_7,order_c7\n" +
                "from query_result_order_kpi\n" +
                "where merchant_id=" + merchant_id + " \n" +
                ") a\n" +
                "group by merchant_id;";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevendaysData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int partner_new_7 = rs.getInt("partner_new_7");
            int partner_member_7 = rs.getInt("partner_member_7");
            int order_amount_7 = rs.getInt("order_amount_7");
            int total_comission_7 = rs.getInt("total_comission_7");
            int order_c7 = rs.getInt("order_c7");
            detailMap.put("add_sum", partner_new_7);
            detailMap.put("member_sum", partner_member_7);
            detailMap.put("order_amount", order_amount_7);
            detailMap.put("commission_amount", total_comission_7);
            detailMap.put("order_sum", order_c7);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTwitterThirtydaysData")
    public HashMap<String, Object> getTwitterThirtydaysData(String merchant_id) throws SQLException {
        String getThirtydaysData = "select merchant_id,max(partner_new_30) as partner_new_30,max(partner_member_30) as partner_member_30\n" +
                ",max(order_amount_30) as order_amount_30,max(total_comission_30) as total_comission_30,max(order_c30) as order_c30\n" +
                "from \n" +
                "(\n" +
                "select merchant_id,partner_new_30,0 as partner_member_30,0 as order_amount_30,0 as total_comission_30,0 as order_c30\n" +
                "from query_result_partner_new\n" +
                "where merchant_id=" + merchant_id + " \n" +
                "union all \n" +
                "select merchant_id,0 as partner_new_30,partner_member_30,0 as order_amount_30,0 as total_comission_30,0 as order_c30\n" +
                "from query_result_partner_member\n" +
                "where merchant_id=" + merchant_id + " \n" +
                "union all \n" +
                "select merchant_id,0 as partner_new_30,0 as partner_member_30,order_amount_30,total_comission_30,order_c30\n" +
                "from query_result_order_kpi\n" +
                "where merchant_id=" + merchant_id + " \n" +
                ") a\n" +
                "group by merchant_id;";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getThirtydaysData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int partner_new_30 = rs.getInt("partner_new_30");
            int partner_member_30 = rs.getInt("partner_member_30");
            int order_amount_30 = rs.getInt("order_amount_30");
            int total_comission_30 = rs.getInt("total_comission_30");
            int order_c30 = rs.getInt("order_c30");
            detailMap.put("add_sum", partner_new_30);
            detailMap.put("member_sum", partner_member_30);
            detailMap.put("order_amount", order_amount_30);
            detailMap.put("commission_amount", total_comission_30);
            detailMap.put("order_sum", order_c30);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTwitterYestodayChart")
    public HashMap<String, Object> getTwitterYestodayChart(String merchant_id) throws SQLException {
        String getYestodayChart = "select merchant_id,hour , partner_hour \n" +
                "from query_result_guider_new_hour\n" +
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
            int partner_hour = rs.getInt("partner_hour");
            if (hour.length() < 11) {
                String[] split = hour.split("~");
                hour = "0" + split[0] + "~0" + split[1];
            }
            chartMap.put(hour, partner_hour + "");
        }
        ImpalaJdbc.close(null, pst, conn);
        return chartMap;
    }

    @Override
    @Cacheable(value = "getTwitterTodayChart")
    public HashMap<String, Object> getTwitterTodayChart(String merchant_id) throws SQLException {
        String getTodayChart = "select merchant_id,hour , partner_hour \n" +
                "from query_result_guider_new_hour\n" +
                "where  merchant_id=" + merchant_id + " and day_type='2_ago';";

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
            int partner_hour = rs.getInt("partner_hour");
            if (hour.length() < 11) {
                String[] split = hour.split("~");
                hour = "0" + split[0] + "~0" + split[1];
            }
            chartMap.put(hour, partner_hour + "");
        }
        ImpalaJdbc.close(null, pst, conn);
        return chartMap;
    }

    @Override
    @Cacheable(value = "getTwitterSevendaysChart")
    public HashMap<String, Object> getTwitterSevendaysChart(String merchant_id) throws SQLException {
        String getSevendaysChart = "select merchant_id,`day`,guider_new_day\n" +
                "from query_result_guider_new_day\n" +
                "where merchant_id=" + merchant_id + " and day_type='7_list';";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevendaysChart);
        ResultSet rs = pst.executeQuery();
        HashMap<String, Object> chartMap = new HashMap<>();
        while (rs.next()) {
            String date = rs.getString("day");
            int guider_new_day = rs.getInt("guider_new_day");
            chartMap.put(date, guider_new_day + "");
        }
        ImpalaJdbc.close(null, pst, conn);
        return chartMap;
    }

    @Override
    @Cacheable(value = "getTwitterThirtydaysChart")
    public HashMap<String, Object> getTwitterThirtydaysChart(String merchant_id) throws SQLException {
        String getThirtydaysChart = "select merchant_id,`day`,guider_new_day\n" +
                "from query_result_guider_new_day\n" +
                "where merchant_id=" + merchant_id + " and day_type='30_list';";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getThirtydaysChart);
        ResultSet rs = pst.executeQuery();
        HashMap<String, Object> chartMap = new HashMap<>();
        while (rs.next()) {
            String date = rs.getString("day");
            int guider_new_day = rs.getInt("guider_new_day");
            chartMap.put(date, guider_new_day + "");
        }
        ImpalaJdbc.close(null, pst, conn);
        return chartMap;
    }
}
