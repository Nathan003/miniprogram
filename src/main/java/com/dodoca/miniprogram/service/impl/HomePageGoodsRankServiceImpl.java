package com.dodoca.miniprogram.service.impl;

import com.dodoca.miniprogram.service.HomePageGoodsRankService;
import com.dodoca.miniprogram.util.ImpalaJdbc;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

@Service
public class HomePageGoodsRankServiceImpl implements HomePageGoodsRankService {
    @Override
    @Cacheable(value = "getTotalBrowseData")
    public HashMap<String, Object> getTotalBrowseData(String merchant_id) throws SQLException {

        String getTotalBrowseData = "select merchant_id,goods_name,browse_goods_cnt_total,g_rank_total\n" +
                "from query_result_browse_rank\n" +
                "where merchant_id=" + merchant_id + " ;";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getTotalBrowseData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int browse_goods_cnt_total = rs.getInt("browse_goods_cnt_total");
            int rank = rs.getInt("g_rank_total");
            detailMap.put("goods_name", goods_name);
            detailMap.put("browse_sum", browse_goods_cnt_total);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalBuyData")
    public HashMap<String, Object> getTotalBuyData(String merchant_id) throws SQLException {
        String getTotalBuyData = "select merchant_id,goods_name,order_goods_cnt_total,og_rank_total\n" +
                "from query_result_buy_rank\n" +
                "where merchant_id=" + merchant_id + " ;";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getTotalBuyData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int order_goods_cnt_total = rs.getInt("order_goods_cnt_total");
            int rank = rs.getInt("og_rank_total");
            detailMap.put("goods_name", goods_name);
            detailMap.put("order_sum", order_goods_cnt_total);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getTotalRebuysData")
    public HashMap<String, Object> getTotalRebuysData(String merchant_id) throws SQLException {
        String getTotalRebuysData = "select merchant_id,goods_name,rebuy_percent,rebuy_percent_rank\n" +
                "from query_result_rebuy_percent_rank\n" +
                "where merchant_id=" + merchant_id + " and day_type='total';";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getTotalRebuysData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int rebuy_percent = rs.getInt("rebuy_percent");
            int rank = rs.getInt("rebuy_percent_rank");
            detailMap.put("goods_name", goods_name);
            detailMap.put("rebuy_percent", rebuy_percent);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getYestodayBrowseData")
    public HashMap<String, Object> getYestodayBrowseData(String merchant_id) throws SQLException {
        String getYestodayBrowseData = "select merchant_id,goods_name,browse_goods_cnt_1,g_rank_1\n" +
                "from query_result_browse_rank\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayBrowseData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int browse_goods_cnt_1 = rs.getInt("browse_goods_cnt_1");
            int rank = rs.getInt("g_rank_1");
            detailMap.put("goods_name", goods_name);
            detailMap.put("browse_sum", browse_goods_cnt_1);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getYestodayBuyData")
    public HashMap<String, Object> getYestodayBuyData(String merchant_id) throws SQLException {
        String getYestodayBuyData = "select merchant_id,goods_name,order_goods_cnt_1,og_rank_1\n" +
                "from query_result_buy_rank\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayBuyData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int order_goods_cnt_1 = rs.getInt("order_goods_cnt_1");
            int rank = rs.getInt("og_rank_1");
            detailMap.put("goods_name", goods_name);
            detailMap.put("order_sum", order_goods_cnt_1);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getYestodayRebuysData")
    public HashMap<String, Object> getYestodayRebuysData(String merchant_id) throws SQLException {
        String getYestodayRebuysData = "select merchant_id,goods_name,rebuy_percent,rebuy_percent_rank\n" +
                "from query_result_rebuy_percent_rank\n" +
                "where merchant_id=" + merchant_id + " and day_type='1_ago';";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getYestodayRebuysData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int rebuy_percent = rs.getInt("rebuy_percent");
            int rank = rs.getInt("rebuy_percent_rank");
            detailMap.put("goods_name", goods_name);
            detailMap.put("rebuy_percent", rebuy_percent);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getSevendayBrowseData")
    public HashMap<String, Object> getSevendayBrowseData(String merchant_id) throws SQLException {
        String getSevendayBrowseData = "select merchant_id,goods_name,browse_goods_cnt_7,g_rank_7\n" +
                "from query_result_browse_rank\n" +
                "where merchant_id=" + merchant_id + " ;";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevendayBrowseData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int browse_goods_cnt_7 = rs.getInt("browse_goods_cnt_7");
            int rank = rs.getInt("g_rank_7");
            detailMap.put("goods_name", goods_name);
            detailMap.put("browse_sum", browse_goods_cnt_7);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getSevendayBuyData")
    public HashMap<String, Object> getSevendayBuyData(String merchant_id) throws SQLException {
        String getSevendayBuyData = "select merchant_id,goods_name,order_goods_cnt_7,og_rank_7\n" +
                "from query_result_buy_rank\n" +
                "where merchant_id=" + merchant_id + " and day_type='1_ago';";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevendayBuyData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int order_goods_cnt_7 = rs.getInt("order_goods_cnt_7");
            int rank = rs.getInt("og_rank_7");
            detailMap.put("goods_name", goods_name);
            detailMap.put("order_sum", order_goods_cnt_7);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getSevendayRebuysData")
    public HashMap<String, Object> getSevendayRebuysData(String merchant_id) throws SQLException {
        String getSevendayRebuysData = "select merchant_id,goods_name,rebuy_percent,rebuy_percent_rank\n" +
                "from query_result_rebuy_percent_rank\n" +
                "where merchant_id=" + merchant_id + " and day_type='7_ago';";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getSevendayRebuysData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int rebuy_percent = rs.getInt("rebuy_percent");
            int rank = rs.getInt("rebuy_percent_rank");
            detailMap.put("goods_name", goods_name);
            detailMap.put("rebuy_percent", rebuy_percent);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getThirtydayBrowseData")
    public HashMap<String, Object> getThirtydayBrowseData(String merchant_id) throws SQLException {
        String getThirtydayBrowseData = "select merchant_id,goods_name,browse_goods_cnt_30,g_rank_30\n" +
                "from query_result_browse_rank\n" +
                "where merchant_id=" + merchant_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getThirtydayBrowseData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int browse_goods_cnt_30 = rs.getInt("browse_goods_cnt_30");
            int rank = rs.getInt("g_rank_30");
            detailMap.put("goods_name", goods_name);
            detailMap.put("browse_sum", browse_goods_cnt_30);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getThirtydayBuyData")
    public HashMap<String, Object> getThirtydayBuyData(String merchant_id) throws SQLException {
        String getThirtydayBuyData = "select merchant_id,goods_name,order_goods_cnt_30,og_rank_30\n" +
                "from query_result_buy_rank\n" +
                "where merchant_id=" + merchant_id + " and day_type='1_ago';";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getThirtydayBuyData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int order_goods_cnt_30 = rs.getInt("order_goods_cnt_30");
            int rank = rs.getInt("og_rank_30");
            detailMap.put("goods_name", goods_name);
            detailMap.put("order_sum", order_goods_cnt_30);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }

    @Override
    @Cacheable(value = "getThirtydayRebuysData")
    public HashMap<String, Object> getThirtydayRebuysData(String merchant_id) throws SQLException {
        String getThirtydayRebuysData = "select merchant_id,goods_name,rebuy_percent,rebuy_percent_rank\n" +
                "from query_result_rebuy_percent_rank\n" +
                "where merchant_id=" + merchant_id + " and day_type='30_ago';";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getThirtydayRebuysData);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String goods_name = rs.getString("goods_name");
            int rebuy_percent = rs.getInt("rebuy_percent");
            int rank = rs.getInt("rebuy_percent_rank");
            detailMap.put("goods_name", goods_name);
            detailMap.put("rebuy_percent", rebuy_percent);
            detailMap.put("rank", rank);
        }
        ImpalaJdbc.close(null, pst, conn);
        return detailMap;
    }
}
