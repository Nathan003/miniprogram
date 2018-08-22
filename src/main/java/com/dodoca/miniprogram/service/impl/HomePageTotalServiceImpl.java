package com.dodoca.miniprogram.service.impl;

import com.dodoca.miniprogram.service.HomePageTotalService;
import com.dodoca.miniprogram.util.ImpalaJdbc;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

@Service
public class HomePageTotalServiceImpl implements HomePageTotalService {

    @Override
    @Cacheable(value = "getTotalYestodayTotal")
    public HashMap<String, Object> getTotalYestodayTotal(String merchant_id) throws SQLException {
        String getYestodayTotal = "select merchant_id,'昨日' as type ,max(1_pv) as 1_pv,max(1_pv2) as 1_pv2,max(member_1) as member_1\n" +
                "from \n" +
                "(\n" +
                "select merchant_id,1_pv,1_pv2,0 as member_1\n" +
                "from query_result_pv\n" +
                "where merchant_id=" + merchant_id + "\n" +
                "union ALL \n" +
                "select merchant_id,0 AS 1_pv,0 AS 1_pv2, member_1\n" +
                "from query_result_member\n" +
                "where merchant_id=" + merchant_id + "\n" +
                ") a\n" +
                "group by merchant_id;";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayTotal);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int browse_sum = rs.getInt("1_pv");
            int trans_sum = rs.getInt("1_pv2");
            int member_add = rs.getInt("member_1");
            detailMap.put("browse_sum", browse_sum);
            detailMap.put("trans_sum", trans_sum);
            detailMap.put("member_add", member_add);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalSevendaysTotal")
    public HashMap<String, Object> getTotalSevendaysTotal(String merchant_id) throws SQLException {
        String getSevendaysTotal = "select merchant_id,'7天' as type ,max(7_sum_pv) as 7_sum_pv,max(7_sum_pv2) as 7_sum_pv2,max(member_7) as member_7\n" +
                "from \n" +
                "(\n" +
                "select merchant_id,7_sum_pv,7_sum_pv2,0 as member_7\n" +
                "from query_result_pv\n" +
                "where merchant_id=" + merchant_id + "\n" +
                "union ALL \n" +
                "select merchant_id,0 AS 7_sum_pv,0 AS 7_sum_pv2, member_7\n" +
                "from query_result_member\n" +
                "where merchant_id=" + merchant_id + "\n" +
                ") a\n" +
                "group by merchant_id;";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevendaysTotal);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int browse_sum = rs.getInt("7_sum_pv");
            int trans_sum = rs.getInt("7_sum_pv2");
            int member_add = rs.getInt("member_7");
            detailMap.put("browse_sum", browse_sum);
            detailMap.put("trans_sum", trans_sum);
            detailMap.put("member_add", member_add);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalThirtydaysTotal")
    public HashMap<String, Object> getTotalThirtydaysTotal(String merchant_id) throws SQLException {
        String getThirtydaysTotal = "select merchant_id,'30天' as type ,max(30_sum_pv) as 30_sum_pv,max(30_sum_pv2) as 30_sum_pv2,max(member_30) as member_30\n" +
                "from \n" +
                "(\n" +
                "select merchant_id,30_sum_pv,30_sum_pv2,0 as member_30\n" +
                "from query_result_pv\n" +
                "where merchant_id=" + merchant_id + "\n" +
                "union ALL \n" +
                "select merchant_id,0 AS 30_sum_pv,0 AS 30_sum_pv2, member_30\n" +
                "from query_result_member\n" +
                "where merchant_id=" + merchant_id + "\n" +
                ") a\n" +
                "group by merchant_id;";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getThirtydaysTotal);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int browse_sum = rs.getInt("30_sum_pv");
            int trans_sum = rs.getInt("30_sum_pv2");
            int member_add = rs.getInt("member_30");
            detailMap.put("browse_sum", browse_sum);
            detailMap.put("trans_sum", trans_sum);
            detailMap.put("member_add", member_add);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalHistoryTotal")
    public HashMap<String, Object> getTotalHistoryTotal(String merchant_id) throws SQLException {
        String getHistoryTotal = "select merchant_id,'历史汇总' as type ,max(all_sum_pv) as all_sum_pv,max(all_sum_pv2) as all_sum_pv2,max(member_all) as member_all\n" +
                "from \n" +
                "(\n" +
                "select merchant_id,all_sum_pv,all_sum_pv2,0 as member_all\n" +
                "from query_result_pv\n" +
                "where merchant_id=" + merchant_id + "\n" +
                "union ALL \n" +
                "select merchant_id,0 AS all_sum_pv,0 AS all_sum_pv2, member_all\n" +
                "from query_result_member\n" +
                "where merchant_id=" + merchant_id + "\n" +
                ") a\n" +
                "group by merchant_id;";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getHistoryTotal);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int browse_sum = rs.getInt("all_sum_pv");
            int trans_sum = rs.getInt("all_sum_pv2");
            int member_add = rs.getInt("member_all");
            detailMap.put("browse_sum", browse_sum);
            detailMap.put("trans_sum", trans_sum);
            detailMap.put("member_add", member_add);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalAvgTotal")
    public HashMap<String, Object> getTotalAvgTotal(String merchant_id) throws SQLException {
        String getAvgTotal = "select merchant_id,'每日平均' as type ,max(day_pv_avg) as day_pv_avg,max(day_pv2_avg) as day_pv2_avg,max(new_member_avg) as new_member_avg\n" +
                "from \n" +
                "(\n" +
                "select merchant_id,day_pv_avg,day_pv2_avg,0 as new_member_avg\n" +
                "from query_result_pv\n" +
                "where merchant_id=" + merchant_id + "\n" +
                "union ALL \n" +
                "select merchant_id,0 AS day_pv_avg,0 AS day_pv2_avg, new_member_avg\n" +
                "from query_result_member\n" +
                "where merchant_id=" + merchant_id + "\n" +
                ") a\n" +
                "group by merchant_id;";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getAvgTotal);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int browse_sum = rs.getInt("day_pv_avg");
            int trans_sum = rs.getInt("day_pv2_avg");
            int member_add = rs.getInt("new_member_avg");
            detailMap.put("browse_sum", browse_sum);
            detailMap.put("trans_sum", trans_sum);
            detailMap.put("member_add", member_add);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalMaxTotal")
    public HashMap<String, Object> getTotalMaxTotal(String merchant_id) throws SQLException {
        String getMaxTotal = "select merchant_id,'历史峰值' as type ,max(day_pv) as day_pv,max(day_pv_2) as day_pv_2,max(new_member_max) as new_member_max\n" +
                "from \n" +
                "(\n" +
                "select merchant_id,day_pv,day_pv_2,0 as new_member_max\n" +
                "from query_result_pv\n" +
                "where merchant_id=" + merchant_id + "\n" +
                "union ALL \n" +
                "select merchant_id,0 AS day_pv,0 AS day_pv_2, new_member_max\n" +
                "from query_result_member\n" +
                "where merchant_id=" + merchant_id + "\n" +
                ") a\n" +
                "group by merchant_id;";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getMaxTotal);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            int browse_sum = rs.getInt("day_pv");
            int trans_sum = rs.getInt("day_pv_2");
            int member_add = rs.getInt("new_member_max");
            detailMap.put("browse_sum", browse_sum);
            detailMap.put("trans_sum", trans_sum);
            detailMap.put("member_add", member_add);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalYestodayChart")
    public HashMap<String, Object> getTotalYestodayChart(String merchant_id) throws SQLException {
        String getYestodayChart = "select merchant_id,hour , pv_hour \n" +
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
            int pv_hour = rs.getInt("pv_hour");
            if (hour.length() < 11) {
                String[] split = hour.split("~");
                hour = "0" + split[0] + "~0" + split[1];
            }
            chartMap.put(hour, pv_hour + "");
        }
        ImpalaJdbc.close(null, pst, conn);
        return chartMap;
    }

    @Override
    @Cacheable(value = "getTotalTodayChart")
    public HashMap<String, Object> getTotalTodayChart(String merchant_id) throws SQLException {
        String getTodayChart = "select merchant_id,hour , pv_hour \n" +
                "from query_result_pv_hour\n" +
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
            int pv_hour = rs.getInt("pv_hour");
            if (hour.length() < 11) {
                String[] split = hour.split("~");
                hour = "0" + split[0] + "~0" + split[1];
            }
            chartMap.put(hour, pv_hour + "");
        }
        ImpalaJdbc.close(null, pst, conn);
        return chartMap;
    }

    @Override
    @Cacheable(value = "getTotalSevendaysChart")
    public HashMap<String, Object> getTotalSevendaysChart(String merchant_id) throws SQLException {
        String getSevendaysChart = "select merchant_id,`date`,pv_day\n" +
                "from query_result_pv_day\n" +
                "where merchant_id=" + merchant_id + " and day_type='7_list';";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevendaysChart);
        ResultSet rs = pst.executeQuery();
        HashMap<String, Object> chartMap = new HashMap<>();
        while (rs.next()) {
            String hour = rs.getString("date");
            int pv_day = rs.getInt("pv_day");
            chartMap.put(hour, pv_day + "");
        }
        ImpalaJdbc.close(null, pst, conn);
        return chartMap;
    }

    @Override
    @Cacheable(value = "getTotalThirtydaysChart")
    public HashMap<String, Object> getTotalThirtydaysChart(String merchant_id) throws SQLException {
        String getThirtydaysChart = "select merchant_id,`date`,pv_day\n" +
                "from query_result_pv_day\n" +
                "where merchant_id=" + merchant_id + " and day_type='30_list';";

        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getThirtydaysChart);
        ResultSet rs = pst.executeQuery();
        HashMap<String, Object> chartMap = new HashMap<>();
        while (rs.next()) {
            String hour = rs.getString("date");
            int pv_day = rs.getInt("pv_day");
            chartMap.put(hour, pv_day + "");
        }
        ImpalaJdbc.close(null, pst, conn);
        return chartMap;
    }
}
