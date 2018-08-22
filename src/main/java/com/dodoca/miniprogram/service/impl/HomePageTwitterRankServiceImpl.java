package com.dodoca.miniprogram.service.impl;

import com.dodoca.miniprogram.service.HomePageTwitterRankService;
import com.dodoca.miniprogram.util.ImpalaJdbc;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

@Service
public class HomePageTwitterRankServiceImpl implements HomePageTwitterRankService {
    @Override
    @Cacheable(value = "getTotalTwitterSubData")
    public HashMap<String, Object> getTotalTwitterSubData(String merchant_id) throws SQLException {
        String getTotalTwitterSubData = "select merchant_id,guider_name,guider_count,gd_rank_total\n" +
                "from query_result_guider_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getTotalTwitterSubData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int guider_count = rs.getInt("guider_count");
            int gd_rank_total = rs.getInt("gd_rank_total");
            detailMap.put("name", guider_name);
            detailMap.put("sum", guider_count);
            detailMap.put("rank", gd_rank_total);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalCommissionSubData")
    public HashMap<String, Object> getTotalCommissionSubData(String merchant_id) throws SQLException {
        String getTotalCommissionSubData = "select merchant_id,guider_name,comission_guider_count,cn_guider_rank_total\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getTotalCommissionSubData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int comission_guider_count = rs.getInt("comission_guider_count");
            int cn_guider_rank_total = rs.getInt("cn_guider_rank_total");
            detailMap.put("name", guider_name);
            detailMap.put("sum", comission_guider_count);
            detailMap.put("rank", cn_guider_rank_total);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalOrderSumData")
    public HashMap<String, Object> getTotalOrderSumData(String merchant_id) throws SQLException {
        String getTotalOrderSumData = "select merchant_id,guider_name,order_count_total,order_count_rank_total\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getTotalOrderSumData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int order_count_total = rs.getInt("order_count_total");
            int order_count_rank_total = rs.getInt("order_count_rank_total");
            detailMap.put("name", guider_name);
            detailMap.put("sum", order_count_total);
            detailMap.put("rank", order_count_rank_total);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalOrderAmountData")
    public HashMap<String, Object> getTotalOrderAmountData(String merchant_id) throws SQLException {
        String getTotalOrderAmountData = "select merchant_id,guider_name,order_amount_total,order_amount_total\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getTotalOrderAmountData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int order_amount_total = rs.getInt("order_amount_total");
//            int order_amount_total = rs.getInt("order_amount_total");
            detailMap.put("name", guider_name);
            detailMap.put("sum", order_amount_total);
            detailMap.put("rank", order_amount_total);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalCommissionAmountsData")
    public HashMap<String, Object> getTotalCommissionAmountsData(String merchant_id) throws SQLException {
        return null;
    }

    @Override
    @Cacheable(value = "getYestodayTwitterSubData")
    public HashMap<String, Object> getYestodayTwitterSubData(String merchant_id) throws SQLException {
        String getYestodayTwitterSubData = "select merchant_id,guider_name,guider_count_1,gd_rank_1\n" +
                "from query_result_guider_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayTwitterSubData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int guider_count_1 = rs.getInt("guider_count_1");
            int gd_rank_1 = rs.getInt("gd_rank_1");
            detailMap.put("name", guider_name);
            detailMap.put("sum", guider_count_1);
            detailMap.put("rank", gd_rank_1);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getYestodayCommissionSubData")
    public HashMap<String, Object> getYestodayCommissionSubData(String merchant_id) throws SQLException {
        String getYestodayCommissionSubData = "select merchant_id,guider_name,comission_guider_count_1,cn_guider_rank_1\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayCommissionSubData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int comission_guider_count_1 = rs.getInt("comission_guider_count_1");
            int cn_guider_rank_1 = rs.getInt("cn_guider_rank_1");
            detailMap.put("name", guider_name);
            detailMap.put("sum", comission_guider_count_1);
            detailMap.put("rank", cn_guider_rank_1);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getYestodayOrderSumData")
    public HashMap<String, Object> getYestodayOrderSumData(String merchant_id) throws SQLException {
        String getYestodayOrderSumData = "select merchant_id,guider_name,order_count_1,order_count_rank_1\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayOrderSumData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int order_count_1 = rs.getInt("order_count_1");
            int order_count_rank_1 = rs.getInt("order_count_rank_1");
            detailMap.put("name", guider_name);
            detailMap.put("sum", order_count_1);
            detailMap.put("rank", order_count_rank_1);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getYestodayOrderAmountData")
    public HashMap<String, Object> getYestodayOrderAmountData(String merchant_id) throws SQLException {
        String getYestodayOrderAmountData = "select merchant_id,guider_name,order_amount_1,order_amount_rank_1\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayOrderAmountData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int order_amount_1 = rs.getInt("order_amount_1");
            int order_amount_rank_1 = rs.getInt("order_amount_rank_1");
            detailMap.put("name", guider_name);
            detailMap.put("sum", order_amount_1);
            detailMap.put("rank", order_amount_rank_1);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getYestodayCommissionAmountsData")
    public HashMap<String, Object> getYestodayCommissionAmountsData(String merchant_id) throws SQLException {
        String getYestodayCommissionAmountsData = "select merchant_id,guider_name,comission_1,comission_rank_1\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayCommissionAmountsData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int comission_1 = rs.getInt("comission_1");
            int comission_rank_1 = rs.getInt("comission_rank_1");
            detailMap.put("name", guider_name);
            detailMap.put("sum", comission_1);
            detailMap.put("rank", comission_rank_1);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getSevenTwitterSubData")
    public HashMap<String, Object> getSevenTwitterSubData(String merchant_id) throws SQLException {
        String getSevenTwitterSubData = "select merchant_id,guider_name,guider_count_7,gd_rank_7\n" +
                "from query_result_guider_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevenTwitterSubData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int guider_count_7 = rs.getInt("guider_count_7");
            int gd_rank_7 = rs.getInt("gd_rank_7");
            detailMap.put("name", guider_name);
            detailMap.put("sum", guider_count_7);
            detailMap.put("rank", gd_rank_7);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getSevenCommissionSubData")
    public HashMap<String, Object> getSevenCommissionSubData(String merchant_id) throws SQLException {
        String getSevenCommissionSubData = "select merchant_id,guider_name,comission_guider_count_7,cn_guider_rank_17\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevenCommissionSubData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int comission_guider_count_7 = rs.getInt("comission_guider_count_7");
            int cn_guider_rank_7 = rs.getInt("cn_guider_rank_7");
            detailMap.put("name", guider_name);
            detailMap.put("sum", comission_guider_count_7);
            detailMap.put("rank", cn_guider_rank_7);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getSevenOrderSumData")
    public HashMap<String, Object> getSevenOrderSumData(String merchant_id) throws SQLException {
        String getSevenOrderSumData = "select merchant_id,guider_name,order_count_7,order_count_rank_7\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevenOrderSumData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int order_count_7 = rs.getInt("order_count_7");
            int order_count_rank_7 = rs.getInt("order_count_rank_7");
            detailMap.put("name", guider_name);
            detailMap.put("sum", order_count_7);
            detailMap.put("rank", order_count_rank_7);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getSevenOrderAmountData")
    public HashMap<String, Object> getSevenOrderAmountData(String merchant_id) throws SQLException {
        String getSevenOrderAmountData = "select merchant_id,guider_name,order_amount_7,order_amount_rank_7\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevenOrderAmountData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int order_amount_7 = rs.getInt("order_amount_7");
            int order_amount_rank_7 = rs.getInt("order_amount_rank_7");
            detailMap.put("name", guider_name);
            detailMap.put("sum", order_amount_7);
            detailMap.put("rank", order_amount_rank_7);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getSevenCommissionAmountsData")
    public HashMap<String, Object> getSevenCommissionAmountsData(String merchant_id) throws SQLException {
        String getSevenCommissionAmountsData = "select merchant_id,guider_name,comission_7,comission_rank_7\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevenCommissionAmountsData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int comission_7 = rs.getInt("comission_7");
            int comission_rank_7 = rs.getInt("comission_rank_7");
            detailMap.put("name", guider_name);
            detailMap.put("sum", comission_7);
            detailMap.put("rank", comission_rank_7);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getMonthTwitterSubData")
    public HashMap<String, Object> getMonthTwitterSubData(String merchant_id) throws SQLException {
        String getMonthTwitterSubData = "select merchant_id,guider_name,guider_count_30,gd_rank_30\n" +
                "from query_result_guider_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getMonthTwitterSubData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int guider_count_30 = rs.getInt("guider_count_30");
            int gd_rank_30 = rs.getInt("gd_rank_30");
            detailMap.put("name", guider_name);
            detailMap.put("sum", guider_count_30);
            detailMap.put("rank", gd_rank_30);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getMonthCommissionSubData")
    public HashMap<String, Object> getMonthCommissionSubData(String merchant_id) throws SQLException {
        String getMonthCommissionSubData = "select merchant_id,guider_name,comission_guider_count_30,cn_guider_rank_130\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getMonthCommissionSubData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int comission_guider_count_30 = rs.getInt("comission_guider_count_30");
            int cn_guider_rank_130 = rs.getInt("cn_guider_rank_130");
            detailMap.put("name", guider_name);
            detailMap.put("sum", comission_guider_count_30);
            detailMap.put("rank", cn_guider_rank_130);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getMonthOrderSumData")
    public HashMap<String, Object> getMonthOrderSumData(String merchant_id) throws SQLException {
        String getMonthOrderSumData = "select merchant_id,guider_name,order_count_30,order_count_rank_30\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getMonthOrderSumData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int order_count_30 = rs.getInt("order_count_30");
            int order_count_rank_30 = rs.getInt("order_count_rank_30");
            detailMap.put("name", guider_name);
            detailMap.put("sum", order_count_30);
            detailMap.put("rank", order_count_rank_30);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getMonthOrderAmountData")
    public HashMap<String, Object> getMonthOrderAmountData(String merchant_id) throws SQLException {
        String getMonthOrderAmountData = "select merchant_id,guider_name,order_amount_30,order_amount_rank_30\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getMonthOrderAmountData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int order_amount_30 = rs.getInt("order_amount_30");
            int order_amount_rank_30 = rs.getInt("order_amount_rank_30");
            detailMap.put("name", guider_name);
            detailMap.put("sum", order_amount_30);
            detailMap.put("rank", order_amount_rank_30);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getMonthCommissionAmountsData")
    public HashMap<String, Object> getMonthCommissionAmountsData(String merchant_id) throws SQLException {
        String getMonthCommissionAmountsData = "select merchant_id,guider_name,comission_30,comission_rank_30\n" +
                "from query_result_comission_low_lv_cnt\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getMonthCommissionAmountsData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String guider_name = rs.getString("guider_name");
            int comission_30 = rs.getInt("comission_30");
            int comission_rank_30 = rs.getInt("comission_rank_30");
            detailMap.put("name", guider_name);
            detailMap.put("sum", comission_30);
            detailMap.put("rank", comission_rank_30);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }
}
