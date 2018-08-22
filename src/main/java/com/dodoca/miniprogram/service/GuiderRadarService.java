package com.dodoca.miniprogram.service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface GuiderRadarService {
    double getPartnerVitality(String merchant_id, String member_id) throws SQLException;
    List<Map<String,Object>> getPartnerDetails(String merchant_id, String member_id) throws SQLException;
    double getCommissionVitality(String merchant_id, String member_id) throws SQLException;
    double getOrderVitality(String merchant_id, String member_id) throws SQLException;
    List<Map<String,Object>> getOrderDetails(String merchant_id, String member_id) throws SQLException;
}
