package com.dodoca.miniprogram.service.impl;

import com.dodoca.miniprogram.service.GuiderRadarService;
import com.dodoca.miniprogram.util.ImpalaJdbc;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class GuiderRadarServiceImpl implements GuiderRadarService {
    @Override
    @Cacheable(value = "getPartnerVitality")
    public double getPartnerVitality(String merchant_id, String member_id) throws SQLException {
        String guiderSubaVitality = "select guider_active_per from query_result_guider_lper \n" +
                "where merchant_id=" + merchant_id + " and member_id=" + member_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(guiderSubaVitality);
        ResultSet rs = pst.executeQuery();
        double vitality = 0;
        while (rs.next()) {
            vitality = rs.getDouble("guider_active_per");
        }
        ImpalaJdbc.close(null, pst, conn);
        return vitality;
    }

    @Override
    @Cacheable(value = "getPartnerDetails")
    public List<Map<String, Object>> getPartnerDetails(String merchant_id, String member_id) throws SQLException {
        String getDetails = "select day,guider_new_day from \n" +
                "query_result_pguider_new_day \n" +
                "where merchant_id=" + merchant_id + " and member_id=" + member_id + ";";
        List<Map<String, Object>> detailList = new ArrayList<>();
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getDetails);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String date = rs.getString("day");
            int total = rs.getInt("guider_new_day");
            detailMap.put("date", date);
            detailMap.put("total", total);
        }
        detailList.add(detailMap);
        ImpalaJdbc.close(null, pst, conn);
        return detailList;
    }

    @Override
    @Cacheable(value = "getCommissionVitality")
    public double getCommissionVitality(String merchant_id, String member_id) throws SQLException {
        String getCommissionVitality = "select guider_active_per\n" +
                "from query_result_comission_lper \n" +
                "where merchant_id=" + merchant_id + " and member_id=" + member_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getCommissionVitality);
        ResultSet rs = pst.executeQuery();
        double vitality = 0;
        while (rs.next()) {
            vitality = rs.getDouble("guider_active_per");
        }
        ImpalaJdbc.close(null, pst, conn);
        return vitality;
    }

    @Override
    @Cacheable(value = "getOrderVitality")
    public double getOrderVitality(String merchant_id, String member_id) throws SQLException {

        String getOrderVitality = "select  guider_active_per\n" +
                "from  query_result_order_lper  \n" +
                "where merchant_id=" + merchant_id + " and member_id=" + member_id + ";";
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getOrderVitality);
        ResultSet rs = pst.executeQuery();
        double vitality = 0;
        while (rs.next()) {
            vitality = rs.getDouble("guider_active_per");
        }
        ImpalaJdbc.close(null, pst, conn);
        return vitality;

    }

    @Override
    @Cacheable(value = "getOrderDetails")
    public List<Map<String, Object>> getOrderDetails(String merchant_id, String member_id) throws SQLException {

        String getOrderDetails = "select day,order_amount_day ,order_count_day\n" +
                "from query_result_gorder_kpi_day\n" +
                "where merchant_id=" + merchant_id + " and member_id=" + member_id + ";";

        List<Map<String, Object>> detailList = new ArrayList<>();
        Connection conn = ImpalaJdbc.getImpalaConnection();
        PreparedStatement pst = conn.prepareStatement(getOrderDetails);
        HashMap<String, Object> detailMap = new HashMap<>();
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            String date = rs.getString("day");
            double order_amount = rs.getDouble("order_amount_day");
            int order_number = rs.getInt("order_count_day");
            detailMap.put("date", date);
            detailMap.put("order_amount", order_amount);
            detailMap.put("order_number",order_number);
        }
        detailList.add(detailMap);
        ImpalaJdbc.close(null, pst, conn);
        return detailList;
    }


}
