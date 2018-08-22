package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.HashMap;

public interface HomePageTwitterService {
    HashMap<String,Object> getTwitterYestodayData(String merchant_id) throws SQLException;
    HashMap<String,Object> getTwitterSevendaysData(String merchant_id) throws SQLException;
    HashMap<String,Object> getTwitterThirtydaysData(String merchant_id) throws SQLException;
    HashMap<String,Object> getTwitterYestodayChart(String merchant_id) throws SQLException;
    HashMap<String,Object> getTwitterTodayChart(String merchant_id) throws SQLException;
    HashMap<String,Object> getTwitterSevendaysChart(String merchant_id) throws SQLException;
    HashMap<String,Object> getTwitterThirtydaysChart(String merchant_id) throws SQLException;
}
