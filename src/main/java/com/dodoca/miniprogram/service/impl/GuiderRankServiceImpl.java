package com.dodoca.miniprogram.service.impl;

import com.dodoca.miniprogram.service.GuiderRankService;
import com.dodoca.miniprogram.util.ImpalaJdbc;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class GuiderRankServiceImpl implements GuiderRankService {
    @Override
    @Cacheable(value = "GuiderRankServiceImpl", key = "'getGuiderRank' + #merchant_id + #filter + #order")
    public List<Map<String, Object>> getGuiderRank(String merchant_id, String filter, String order) throws SQLException {
        String getGuiderRank = null;
        if ("history".equals(filter)) {
            getGuiderRank = "select member_id , guider_name,mobile\n" +
                    ",max(guider_count) as guider_count\n" +
                    ",max(comission_guider_count) as comission_guider_count\n" +
                    ",max(order_count_total) as order_count_total\n" +
                    ",max(order_amount_total) as order_amount_total\n" +
                    ",max(comission_total) as comission_total\n" +
                    "from \n" +
                    "(select member_id , guider_name,mobile\n" +
                    ",guider_count,0 as comission_guider_count,0 as order_count_total,0 as order_amount_total,0 as comission_total\n" +
                    "from query_result_guider_low_lv_cnt\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select member_id , guider_name,mobile\n" +
                    ",0 as guider_count, comission_guider_count,0 as order_count_total,0 as order_amount_total,0 as comission_total\n" +
                    "from query_result_comission_low_lv_cnt\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select member_id , guider_name,mobile\n" +
                    ",0 as guider_count, 0 as comission_guider_count,order_count_total, order_amount_total,comission_total\n" +
                    "from query_result_guider_order\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    ")a\n" +
                    "group by member_id , guider_name,mobile\n" +
                    "order by @column desc;";
            if ("guidercount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "guider_count");
            } else if ("comissioncount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "comission_guider_count");
            } else if ("ordercount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "order_count_total");
            } else if ("orderamount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "order_amount_total");
            } else if ("comissionamount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "comission_total");
            }
            List<Map<String, Object>> dataList = new ArrayList<>();
            Connection conn = ImpalaJdbc.getImpalaConnection();
            PreparedStatement pst = conn.prepareStatement(getGuiderRank);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                HashMap<String, Object> detailMap = new HashMap<>();
                int member_id = rs.getInt("member_id");
                String nickname = rs.getString("guider_name");
                String mobile = rs.getString("mobile");
                int twitter_subsum = rs.getInt("guider_count");
                int commission_subsum = rs.getInt("comission_guider_count");
                int order_num = rs.getInt("order_count_total");
                double order_amount = rs.getDouble("order_amount_total");
                double commission_amount = rs.getDouble("comission_total");
                detailMap.put("member_id", member_id);
                detailMap.put("nickname", nickname);
                detailMap.put("mobile", mobile);
                detailMap.put("twitter_subsum", twitter_subsum);
                detailMap.put("commission_subsum", commission_subsum);
                detailMap.put("order_num", order_num);
                detailMap.put("order_amount", order_amount);
                detailMap.put("commission_amount", commission_amount);
                dataList.add(detailMap);
            }
            ImpalaJdbc.close(rs, pst, conn);
            return dataList;
        } else if ("yesterday".equals(filter)) {
            getGuiderRank = "select member_id , guider_name,mobile\n" +
                    ",max(guider_count_1) as guider_count_1\n" +
                    ",max(comission_guider_count_1) as comission_guider_count_1\n" +
                    ",max(order_count_1) as order_count_1\n" +
                    ",max(order_amount_1) as order_amount_1\n" +
                    ",max(comission_1) as comission_1\n" +
                    "from \n" +
                    "(select member_id , guider_name,mobile\n" +
                    ",guider_count_1,0 as comission_guider_count_1,0 as order_count_1,0 as order_amount_1,0 as comission_1\n" +
                    "from query_result_guider_low_lv_cnt\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select member_id , guider_name,mobile\n" +
                    ",0 as guider_count_1, comission_guider_count_1,0 as order_count_1,0 as order_amount_1,0 as comission_1\n" +
                    "from query_result_comission_low_lv_cnt\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select member_id , guider_name,mobile\n" +
                    ",0 as guider_count_1, 0 as comission_guider_count_1,order_count_1, order_amount_1,comission_1\n" +
                    "from query_result_guider_order\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    ")a\n" +
                    "group by member_id , guider_name,mobile\n" +
                    "order by @column desc;";
            if ("guidercount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "guider_count_1");
            } else if ("comissioncount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "comission_guider_count_1");
            } else if ("ordercount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "order_count_1");
            } else if ("orderamount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "order_amount_1");
            } else if ("comissionamount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "comission_1");
            }
            List<Map<String, Object>> dataList = new ArrayList<>();
            Connection conn = ImpalaJdbc.getImpalaConnection();
            PreparedStatement pst = conn.prepareStatement(getGuiderRank);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                HashMap<String, Object> detailMap = new HashMap<>();
                int member_id = rs.getInt("member_id");
                String nickname = rs.getString("guider_name");
                String mobile = rs.getString("mobile");
                int twitter_subsum = rs.getInt("guider_count_1");
                int commission_subsum = rs.getInt("comission_guider_count_1");
                int order_num = rs.getInt("order_count_1");
                double order_amount = rs.getDouble("order_amount_1");
                double commission_amount = rs.getDouble("comission_1");
                detailMap.put("member_id", member_id);
                detailMap.put("nickname", nickname);
                detailMap.put("mobile", mobile);
                detailMap.put("twitter_subsum", twitter_subsum);
                detailMap.put("commission_subsum", commission_subsum);
                detailMap.put("order_num", order_num);
                detailMap.put("order_amount", order_amount);
                detailMap.put("commission_amount", commission_amount);
                dataList.add(detailMap);
            }
            ImpalaJdbc.close(rs, pst, conn);
            return dataList;
        } else if ("sevendays".equals(filter)) {
            getGuiderRank = "select member_id , guider_name,mobile\n" +
                    ",max(guider_count_7) as guider_count_7\n" +
                    ",max(comission_guider_count_7) as comission_guider_count_7\n" +
                    ",max(order_count_7) as order_count_7\n" +
                    ",max(order_amount_7) as order_amount_7\n" +
                    ",max(comission_7) as comission_7\n" +
                    "from \n" +
                    "(select member_id , guider_name,mobile\n" +
                    ",guider_count_7,0 as comission_guider_count_7,0 as order_count_7,0 as order_amount_7,0 as comission_7\n" +
                    "from query_result_guider_low_lv_cnt\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select member_id , guider_name,mobile\n" +
                    ",0 as guider_count_7, comission_guider_count_7,0 as order_count_7,0 as order_amount_7,0 as comission_7\n" +
                    "from query_result_comission_low_lv_cnt\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select member_id , guider_name,mobile\n" +
                    ",0 as guider_count_7, 0 as comission_guider_count_7,order_count_7, order_amount_7,comission_7\n" +
                    "from query_result_guider_order\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    ")a\n" +
                    "group by member_id , guider_name,mobile\n" +
                    "order by @column desc;";
            if ("guidercount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "guider_count_7");
            } else if ("comissioncount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "comission_guider_count_7");
            } else if ("ordercount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "order_count_7");
            } else if ("orderamount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "order_amount_7");
            } else if ("comissionamount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "comission_7");
            }
            List<Map<String, Object>> dataList = new ArrayList<>();
            Connection conn = ImpalaJdbc.getImpalaConnection();
            PreparedStatement pst = conn.prepareStatement(getGuiderRank);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                HashMap<String, Object> detailMap = new HashMap<>();
                int member_id = rs.getInt("member_id");
                String nickname = rs.getString("guider_name");
                String mobile = rs.getString("mobile");
                int twitter_subsum = rs.getInt("guider_count_7");
                int commission_subsum = rs.getInt("comission_guider_count_7");
                int order_num = rs.getInt("order_count_7");
                double order_amount = rs.getDouble("order_amount_7");
                double commission_amount = rs.getDouble("comission_7");
                detailMap.put("member_id", member_id);
                detailMap.put("nickname", nickname);
                detailMap.put("mobile", mobile);
                detailMap.put("twitter_subsum", twitter_subsum);
                detailMap.put("commission_subsum", commission_subsum);
                detailMap.put("order_num", order_num);
                detailMap.put("order_amount", order_amount);
                detailMap.put("commission_amount", commission_amount);
                dataList.add(detailMap);
            }
            ImpalaJdbc.close(rs, pst, conn);
            return dataList;
        } else if ("thirtydays".equals(filter)) {
            getGuiderRank = "select member_id , guider_name,mobile\n" +
                    ",max(guider_count_30) as guider_count_30\n" +
                    ",max(comission_guider_count_30) as comission_guider_count_30\n" +
                    ",max(order_count_30) as order_count_30\n" +
                    ",max(order_amount_30) as order_amount_30\n" +
                    ",max(comission_30) as comission_30\n" +
                    "from \n" +
                    "(select member_id , guider_name,mobile\n" +
                    ",guider_count_30,0 as comission_guider_count_30,0 as order_count_30,0 as order_amount_30,0 as comission_30\n" +
                    "from query_result_guider_low_lv_cnt\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select member_id , guider_name,mobile\n" +
                    ",0 as guider_count_30, comission_guider_count_30,0 as order_count_30,0 as order_amount_30,0 as comission_30\n" +
                    "from query_result_comission_low_lv_cnt\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select member_id , guider_name,mobile\n" +
                    ",0 as guider_count_30, 0 as comission_guider_count_30,order_count_30, order_amount_30,comission_30\n" +
                    "from query_result_guider_order\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    ")a\n" +
                    "group by member_id , guider_name,mobile\n" +
                    "order by @column desc;";
            if ("guidercount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "guider_count_30");
            } else if ("comissioncount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "comission_guider_count_30");
            } else if ("ordercount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "order_count_30");
            } else if ("orderamount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "order_amount_30");
            } else if ("comissionamount".equals(order)) {
                getGuiderRank = getGuiderRank.replace("@column", "comission_30");
            }
            List<Map<String, Object>> dataList = new ArrayList<>();
            Connection conn = ImpalaJdbc.getImpalaConnection();
            PreparedStatement pst = conn.prepareStatement(getGuiderRank);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                HashMap<String, Object> detailMap = new HashMap<>();
                int member_id = rs.getInt("member_id");
                String nickname = rs.getString("guider_name");
                String mobile = rs.getString("mobile");
                int twitter_subsum = rs.getInt("guider_count_30");
                int commission_subsum = rs.getInt("comission_guider_count_30");
                int order_num = rs.getInt("order_count_30");
                double order_amount = rs.getDouble("order_amount_30");
                double commission_amount = rs.getDouble("comission_30");
                detailMap.put("member_id", member_id);
                detailMap.put("nickname", nickname);
                detailMap.put("mobile", mobile);
                detailMap.put("twitter_subsum", twitter_subsum);
                detailMap.put("commission_subsum", commission_subsum);
                detailMap.put("order_num", order_num);
                detailMap.put("order_amount", order_amount);
                detailMap.put("commission_amount", commission_amount);
                dataList.add(detailMap);
            }
            ImpalaJdbc.close(rs, pst, conn);
            return dataList;
        }
        List<Map<String, Object>> dataList = new ArrayList<>();
        HashMap<String, Object> detailMap = new HashMap<>();
        detailMap.put("member_id", null);
        detailMap.put("nickname", null);
        detailMap.put("mobile", null);
        detailMap.put("twitter_subsum", null);
        detailMap.put("commission_subsum", null);
        detailMap.put("order_num", null);
        detailMap.put("order_amount", null);
        detailMap.put("commission_amount", null);
        dataList.add(detailMap);

        return dataList;

    }
}
