package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface CommissionSubRankService {
    List<Map<String, Object>> getCommissionSubRank(String merchant_id, String member_id) throws SQLException;
}
