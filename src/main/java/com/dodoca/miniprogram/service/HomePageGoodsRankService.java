package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.HashMap;

public interface HomePageGoodsRankService {
    HashMap<String,Object> getTotalBrowseData(String merchant_id) throws SQLException;
    HashMap<String,Object> getTotalBuyData(String merchant_id) throws SQLException;
    HashMap<String,Object> getTotalRebuysData(String merchant_id) throws SQLException;
    HashMap<String,Object> getYestodayBrowseData(String merchant_id) throws SQLException;
    HashMap<String,Object> getYestodayBuyData(String merchant_id) throws SQLException;
    HashMap<String,Object> getYestodayRebuysData(String merchant_id) throws SQLException;
    HashMap<String,Object> getSevendayBrowseData(String merchant_id) throws SQLException;
    HashMap<String,Object> getSevendayBuyData(String merchant_id) throws SQLException;
    HashMap<String,Object> getSevendayRebuysData(String merchant_id) throws SQLException;
    HashMap<String,Object> getThirtydayBrowseData(String merchant_id) throws SQLException;
    HashMap<String,Object> getThirtydayBuyData(String merchant_id) throws SQLException;
    HashMap<String,Object> getThirtydayRebuysData(String merchant_id) throws SQLException;
}
