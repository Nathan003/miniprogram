package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.HashMap;

public interface HomePageTotalService {
    HashMap<String, Object> getTotalYestodayTotal(String merchant_id) throws SQLException;

    HashMap<String, Object> getTotalSevendaysTotal(String merchant_id) throws SQLException;

    HashMap<String, Object> getTotalThirtydaysTotal(String merchant_id) throws SQLException;

    HashMap<String, Object> getTotalHistoryTotal(String merchant_id) throws SQLException;

    HashMap<String, Object> getTotalAvgTotal(String merchant_id) throws SQLException;

    HashMap<String, Object> getTotalMaxTotal(String merchant_id) throws SQLException;

    HashMap<String, Object> getTotalYestodayChart(String merchant_id) throws SQLException;

    HashMap<String, Object> getTotalTodayChart(String merchant_id) throws SQLException;

    HashMap<String, Object> getTotalSevendaysChart(String merchant_id) throws SQLException;

    HashMap<String, Object> getTotalThirtydaysChart(String merchant_id) throws SQLException;
}
