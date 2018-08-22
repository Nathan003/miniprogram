package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.HashMap;

public interface HomePageTradeService {
    HashMap<String,Object> getYestodayData(String merchant_id) throws SQLException;
    HashMap<String,Object> getSevendaysData(String merchant_id) throws SQLException;
    HashMap<String,Object> getThirtydaysData(String merchant_id) throws SQLException;
    HashMap<String,Object> getTradeYestodayChart(String merchant_id) throws SQLException;
    HashMap<String,Object> getTradeTodayChart(String merchant_id) throws SQLException;
    HashMap<String,Object> getTradeSevendaysChart(String merchant_id) throws SQLException;
    HashMap<String,Object> getTradeThirtydaysChart(String merchant_id) throws SQLException;
}
