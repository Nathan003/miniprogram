package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface GoodsRankService {
    List<Map<String, Object>> getGoodsRankService(String merchant_id , String filter, String order) throws SQLException;
}
