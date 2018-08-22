package com.dodoca.miniprogram.service.impl;

import com.dodoca.miniprogram.service.CommissionSubRankService;
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
public class CommissionSubRankServiceImpl implements CommissionSubRankService {
    @Override
    @Cacheable(value = "CommissionSubRankServiceImpl", key = "'getCommissionSubRank' + #merchant_id + #member_id")
    public List<Map<String, Object>> getCommissionSubRank(String merchant_id, String member_id) throws SQLException {

        String getCommissionSubRank = "select guider_name ,avatar,total_comission_m,total_comission_rank\n" +
                "from query_result_guider_lcm_rank  \n" +
                "where merchant_id=" + merchant_id + " and member_id=" + member_id + ";";

        List<Map<String, Object>> dataList = new ArrayList<>();
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getCommissionSubRank);

        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            HashMap<String, Object> detailMap = new HashMap<>();
            String nick_name = rs.getString("guider_name");
            String avatar = rs.getString("avatar");
            double total_comission = rs.getDouble("total_comission_m");
            int rank = rs.getInt("total_comission_rank");
            detailMap.put("nick_name", nick_name);
            detailMap.put("avatar", avatar);
            detailMap.put("total_comission", total_comission);
            detailMap.put("rank", rank);
            dataList.add(detailMap);
        }
        ImpalaJdbc.close(null, pst, conn);
        return dataList;
    }
}
