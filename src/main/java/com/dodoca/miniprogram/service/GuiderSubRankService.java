package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface GuiderSubRankService {
    List<Map<String, Object>> getGuiderSubRankByGuiderNumber(String merchant_id, String member_id) throws SQLException;
    List<Map<String, Object>> getGuiderSubRankByCommission(String merchant_id, String member_id) throws SQLException;
}
