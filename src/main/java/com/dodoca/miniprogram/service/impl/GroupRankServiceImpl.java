package com.dodoca.miniprogram.service.impl;

import com.dodoca.miniprogram.service.GroupRankService;
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
public class GroupRankServiceImpl implements GroupRankService {
    @Override
    @Cacheable(value = "GroupRank")
    public List<Map<String, Object>> getGroupRank(String merchant_id, String member_id) throws SQLException {

        String getGroupRank = "select chat_group_id,click_count\n" +
                "from \n" +
                "query_result_chat_group_count\n" +
                "where merchant_id=" + merchant_id + " and member_id=" + member_id + ";";

        List<Map<String, Object>> dataList = new ArrayList<>();
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getGroupRank);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String chat_group_id = rs.getString("chat_group_id");
            int result = rs.getInt("click_count");
            detailMap.put("chat_group_id", chat_group_id);
            detailMap.put("result", result);
        }
        dataList.add(detailMap);
        ImpalaJdbc.close(null, pst, conn);
        return dataList;
    }
}
