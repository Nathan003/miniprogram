package com.dodoca.miniprogram.service.impl;

import com.dodoca.miniprogram.service.GoodsRankService;
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
public class GoodsRankServiceImpl implements GoodsRankService {
    @Override
    @Cacheable(value = "GoodsRankServiceImpl", key = "'getGoodsRankService' + #merchant_id + #filter + #order")
    public List<Map<String, Object>> getGoodsRankService(String merchant_id, String filter, String order) throws SQLException {
        System.err.println("进入service");
        String getGoodsRankService = null;
        if ("history".equals(filter)) {
            getGoodsRankService = "select goods_id,goods_name\n" +
                    ",max(browse_goods_cnt_total) as browse_goods_cnt_total --浏览次数\n" +
                    ",max(goods_relay_cnt_total) as goods_relay_cnt_total   --转发次数\n" +
                    ",max(order_goods_cnt_total) as order_goods_cnt_total   --购买次数\n" +
                    ",max(rebuy_percent_rank) as rebuy_percent_rank         --复购率\n" +
                    "from\n" +
                    "(select  goods_id, goods_name ,browse_goods_cnt_total,0 as goods_relay_cnt_total,0 as order_goods_cnt_total,0 as rebuy_percent_rank\n" +
                    "from query_result_browse_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_total,goods_relay_cnt_total,0 as order_goods_cnt_total,0 as rebuy_percent_rank\n" +
                    "from query_result_goods_relay_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_total,0 as goods_relay_cnt_total,order_goods_cnt_total,0 as rebuy_percent_rank\n" +
                    "from query_result_buy_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_total,0 as goods_relay_cnt_total,0 as order_goods_cnt_total, rebuy_percent_rank\n" +
                    "from query_result_rebuy_percent_rank\n" +
                    "where merchant_id=" + merchant_id + " and day_type='total'\n" +
                    ")a\n" +
                    "group by goods_id,goods_name\n" +
                    "order by @column desc; ";
            if ("browse".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "browse_goods_cnt_total");
            } else if ("goods".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "goods_relay_cnt_total");
            } else if ("order".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "order_goods_cnt_total");
            } else if ("rebuy".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "rebuy_percent_rank");
            }
            System.err.println(getGoodsRankService);
            List<Map<String, Object>> dataList = new ArrayList<>();
            Connection conn = ImpalaJdbc.getImpalaConnection();
            PreparedStatement pst = conn.prepareStatement(getGoodsRankService);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                HashMap<String, Object> detailMap = new HashMap<>();
                int goods_id = rs.getInt("goods_id");
                String title = rs.getString("goods_name");
                int browse_sum = rs.getInt("browse_goods_cnt_total");
                int trans_sum = rs.getInt("goods_relay_cnt_total");
                int buy_sum = rs.getInt("order_goods_cnt_total");
                int rate_lv = rs.getInt("rebuy_percent_rank");
                detailMap.put("goods_id", goods_id);
                detailMap.put("title", title);
                detailMap.put("browse_sum", browse_sum);
                detailMap.put("trans_sum", trans_sum);
                detailMap.put("buy_sum", buy_sum);
                detailMap.put("rate_lv", rate_lv);
                dataList.add(detailMap);
            }
            ImpalaJdbc.close(rs, pst, conn);
            return dataList;
        } else if ("yesterday".equals(filter)) {
            getGoodsRankService = "select goods_id,goods_name\n" +
                    ",max(browse_goods_cnt_1) as browse_goods_cnt_1\n" +
                    ",max(goods_relay_cnt_1) as goods_relay_cnt_1\n" +
                    ",max(order_goods_cnt_1) as order_goods_cnt_1\n" +
                    ",max(rebuy_percent_rank) as rebuy_percent_rank\n" +
                    "from\n" +
                    "(select  goods_id, goods_name ,browse_goods_cnt_1,0 as goods_relay_cnt_1,0 as order_goods_cnt_1,0 as rebuy_percent_rank\n" +
                    "from query_result_browse_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_1,goods_relay_cnt_1,0 as order_goods_cnt_1,0 as rebuy_percent_rank\n" +
                    "from query_result_goods_relay_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_1,0 as goods_relay_cnt_1,order_goods_cnt_1,0 as rebuy_percent_rank\n" +
                    "from query_result_buy_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_1,0 as goods_relay_cnt_1,0 as order_goods_cnt_1\n" +
                    ", rebuy_percent_rank\n" +
                    "from query_result_rebuy_percent_rank\n" +
                    "where merchant_id=" + merchant_id + "  and day_type='1_ago'\n" +
                    ")a\n" +
                    "group by goods_id,goods_name\n" +
                    "order by @column desc ;";
            if ("browse".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "browse_goods_cnt_1");
            } else if ("goods".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "goods_relay_cnt_1");
            } else if ("order".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "order_goods_cnt_1");
            } else if ("rebuy".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "rebuy_percent_rank");
            }
            List<Map<String, Object>> dataList = new ArrayList<>();
            Connection conn = ImpalaJdbc.getImpalaConnection();
            PreparedStatement pst = conn.prepareStatement(getGoodsRankService);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                HashMap<String, Object> detailMap = new HashMap<>();
                int goods_id = rs.getInt("goods_id");
                String title = rs.getString("goods_name");
                int browse_sum = rs.getInt("browse_goods_cnt_1");
                int trans_sum = rs.getInt("goods_relay_cnt_1");
                int buy_sum = rs.getInt("order_goods_cnt_1");
                int rate_lv = rs.getInt("rebuy_percent_rank");
                detailMap.put("goods_id", goods_id);
                detailMap.put("title", title);
                detailMap.put("browse_sum", browse_sum);
                detailMap.put("trans_sum", trans_sum);
                detailMap.put("buy_sum", buy_sum);
                detailMap.put("rate_lv", rate_lv);
                dataList.add(detailMap);
            }
            ImpalaJdbc.close(rs, pst, conn);
            return dataList;
        } else if ("sevendays".equals(filter)) {
            getGoodsRankService = "select goods_id,goods_name\n" +
                    ",max(browse_goods_cnt_7) as browse_goods_cnt_7\n" +
                    ",max(goods_relay_cnt_7) as goods_relay_cnt_7\n" +
                    ",max(order_goods_cnt_7) as order_goods_cnt_7\n" +
                    ",max(rebuy_percent_rank) as rebuy_percent_rank\n" +
                    "from\n" +
                    "(select  goods_id, goods_name ,browse_goods_cnt_7,0 as goods_relay_cnt_7,0 as order_goods_cnt_7,0 as rebuy_percent_rank\n" +
                    "from query_result_browse_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_7,goods_relay_cnt_7,0 as order_goods_cnt_7,0 as rebuy_percent_rank\n" +
                    "from query_result_goods_relay_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_7,0 as goods_relay_cnt_7,order_goods_cnt_7,0 as rebuy_percent_rank\n" +
                    "from query_result_buy_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_7,0 as goods_relay_cnt_7,0 as order_goods_cnt_7\n" +
                    ", rebuy_percent_rank\n" +
                    "from query_result_rebuy_percent_rank\n" +
                    "where merchant_id=" + merchant_id + "  and day_type='7_ago'\n" +
                    ")a\n" +
                    "group by goods_id,goods_name\n" +
                    "order by @column desc;";
            if ("browse".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "browse_goods_cnt_7");
            } else if ("goods".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "goods_relay_cnt_7");
            } else if ("order".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "order_goods_cnt_7");
            } else if ("rebuy".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "rebuy_percent_rank");
            }
            List<Map<String, Object>> dataList = new ArrayList<>();
            Connection conn = ImpalaJdbc.getImpalaConnection();
            PreparedStatement pst = conn.prepareStatement(getGoodsRankService);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                HashMap<String, Object> detailMap = new HashMap<>();
                int goods_id = rs.getInt("goods_id");
                String title = rs.getString("goods_name");
                int browse_sum = rs.getInt("browse_goods_cnt_7");
                int trans_sum = rs.getInt("goods_relay_cnt_7");
                int buy_sum = rs.getInt("order_goods_cnt_7");
                int rate_lv = rs.getInt("rebuy_percent_rank");
                detailMap.put("goods_id", goods_id);
                detailMap.put("title", title);
                detailMap.put("browse_sum", browse_sum);
                detailMap.put("trans_sum", trans_sum);
                detailMap.put("buy_sum", buy_sum);
                detailMap.put("rate_lv", rate_lv);
                dataList.add(detailMap);
            }
            ImpalaJdbc.close(rs, pst, conn);
            return dataList;
        } else if ("thirtydays".equals(filter)) {
            getGoodsRankService = "select goods_id,goods_name\n" +
                    ",max(browse_goods_cnt_30) as browse_goods_cnt_30\n" +
                    ",max(goods_relay_cnt_30) as goods_relay_cnt_30\n" +
                    ",max(order_goods_cnt_30) as order_goods_cnt_30\n" +
                    ",max(rebuy_percent_rank) as rebuy_percent_rank\n" +
                    "from\n" +
                    "(select  goods_id, goods_name ,browse_goods_cnt_30,0 as goods_relay_cnt_30,0 as order_goods_cnt_30,0 as rebuy_percent_rank\n" +
                    "from query_result_browse_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_30,goods_relay_cnt_30,0 as order_goods_cnt_30,0 as rebuy_percent_rank\n" +
                    "from query_result_goods_relay_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_30,0 as goods_relay_cnt_30,order_goods_cnt_30,0 as rebuy_percent_rank\n" +
                    "from query_result_buy_rank\n" +
                    "where merchant_id=" + merchant_id + "\n" +
                    "union all \n" +
                    "select  goods_id, goods_name ,0 as browse_goods_cnt_30,0 as goods_relay_cnt_30,0 as order_goods_cnt_30\n" +
                    ", rebuy_percent_rank\n" +
                    "from query_result_rebuy_percent_rank\n" +
                    "where merchant_id=" + merchant_id + "  and day_type='30_ago'\n" +
                    ")a\n" +
                    "group by goods_id,goods_name\n" +
                    "order by @column desc;";
            if ("browse".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "browse_goods_cnt_30");
            } else if ("goods".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "goods_relay_cnt_30");
            } else if ("order".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "order_goods_cnt_30");
            } else if ("rebuy".equals(order)) {
                getGoodsRankService=getGoodsRankService.replace("@column", "rebuy_percent_rank");
            }
            List<Map<String, Object>> dataList = new ArrayList<>();
            Connection conn = ImpalaJdbc.getImpalaConnection();
            PreparedStatement pst = conn.prepareStatement(getGoodsRankService);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                HashMap<String, Object> detailMap = new HashMap<>();
                int goods_id = rs.getInt("goods_id");
                String title = rs.getString("goods_name");
                int browse_sum = rs.getInt("browse_goods_cnt_30");
                int trans_sum = rs.getInt("goods_relay_cnt_30");
                int buy_sum = rs.getInt("order_goods_cnt_30");
                int rate_lv = rs.getInt("rebuy_percent_rank");
                detailMap.put("goods_id", goods_id);
                detailMap.put("title", title);
                detailMap.put("browse_sum", browse_sum);
                detailMap.put("trans_sum", trans_sum);
                detailMap.put("buy_sum", buy_sum);
                detailMap.put("rate_lv", rate_lv);
                dataList.add(detailMap);
            }
            ImpalaJdbc.close(rs, pst, conn);
            return dataList;
        }
        List<Map<String, Object>> dataList = new ArrayList<>();
        HashMap<String, Object> detailMap = new HashMap<>();
        detailMap.put("goods_id", null);
        detailMap.put("title", null);
        detailMap.put("browse_sum", null);
        detailMap.put("trans_sum", null);
        detailMap.put("buy_sum", null);
        detailMap.put("rate_lv", null);
        dataList.add(detailMap);
        return dataList;

    }
}
