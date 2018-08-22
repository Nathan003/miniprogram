package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface GuiderRankService {
    List<Map<String, Object>> getGuiderRank(String merchant_id) throws SQLException;
}
