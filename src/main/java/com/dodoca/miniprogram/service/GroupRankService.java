package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface GroupRankService {
    List<Map<String, Object>> getGroupRank(String merchant_id, String member_id) throws SQLException;
}
