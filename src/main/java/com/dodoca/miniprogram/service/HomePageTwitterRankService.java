package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.HashMap;

public interface HomePageTwitterRankService {
    HashMap<String,Object> getTotalTwitterSubData(String merchant_id) throws SQLException;
    HashMap<String,Object> getTotalCommissionSubData(String merchant_id) throws SQLException;
    HashMap<String,Object> getTotalOrderSumData(String merchant_id) throws SQLException;
    HashMap<String,Object> getTotalOrderAmountData(String merchant_id) throws SQLException;
    HashMap<String,Object> getTotalCommissionAmountsData(String merchant_id) throws SQLException;
    HashMap<String,Object> getYestodayTwitterSubData(String merchant_id) throws SQLException;
    HashMap<String,Object> getYestodayCommissionSubData(String merchant_id) throws SQLException;
    HashMap<String,Object> getYestodayOrderSumData(String merchant_id) throws SQLException;
    HashMap<String,Object> getYestodayOrderAmountData(String merchant_id) throws SQLException;
    HashMap<String,Object> getYestodayCommissionAmountsData(String merchant_id) throws SQLException;
    HashMap<String,Object> getSevenTwitterSubData(String merchant_id) throws SQLException;
    HashMap<String,Object> getSevenCommissionSubData(String merchant_id) throws SQLException;
    HashMap<String,Object> getSevenOrderSumData(String merchant_id) throws SQLException;
    HashMap<String,Object> getSevenOrderAmountData(String merchant_id) throws SQLException;
    HashMap<String,Object> getSevenCommissionAmountsData(String merchant_id) throws SQLException;
    HashMap<String,Object> getMonthTwitterSubData(String merchant_id) throws SQLException;
    HashMap<String,Object> getMonthCommissionSubData(String merchant_id) throws SQLException;
    HashMap<String,Object> getMonthOrderSumData(String merchant_id) throws SQLException;
    HashMap<String,Object> getMonthOrderAmountData(String merchant_id) throws SQLException;
    HashMap<String,Object> getMonthCommissionAmountsData(String merchant_id) throws SQLException;
}
