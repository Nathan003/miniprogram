package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author : Nathan
 * @date : Created in 14:10 2018/8/22
 */
public interface GoodsBrowseRank {
    List<Map<String, Object>> getGoodsBrowseRank(String merchant_id) throws SQLException;
}
