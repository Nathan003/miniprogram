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
    @Cacheable(value = "getGoodsRankService")
    public List<Map<String, Object>> getGoodsRankService(String merchant_id) throws SQLException {

        String getGoodsRankService = "select goods_id,goods_name\n" +
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
                "order by browse_goods_cnt_total desc; ";

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

        ImpalaJdbc.close(null, pst, conn);
        return dataList;
    }
}
