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
    @Cacheable(value = "GuiderRankServiceImpl", key = "'getGuiderRank' + #merchant_id")
    public List<Map<String, Object>> getGuiderRank(String merchant_id) throws SQLException {

        String getGuiderRank = "select member_id , guider_name,mobile\n" +
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
                "order by guider_count desc;";

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
    }
}
