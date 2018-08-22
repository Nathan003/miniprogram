package com.dodoca.miniprogram.controller;

import com.dodoca.miniprogram.service.GuiderRadarService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class GuiderRadarController {
    @Autowired
    private GuiderRadarService guiderRadarService;

    @ResponseBody
    @RequestMapping(value = "/guider", method = RequestMethod.POST)
    public Map guiderRadar(@RequestBody Map<String, String> data) {
        String merchant_id = data.get("merchant_id");
        String member_id = data.get("member_id");
        HashMap<String, Object> dataMap = new HashMap<>();
        HashMap<String, Object> guiderMap = new HashMap<>();
        double partnerVitality = 0;
        HashMap<String, Object> partnerMap = new HashMap<>();
        HashMap<String, Object> commissionMap = new HashMap<>();
        HashMap<String, Object> orderMap = new HashMap<>();
        try {
            partnerVitality = this.guiderRadarService.getPartnerVitality(merchant_id, member_id);
            List<Map<String, Object>> partnerDetailsList = guiderRadarService.getPartnerDetails(merchant_id, member_id);
            double commissionVitality = guiderRadarService.getCommissionVitality(merchant_id, member_id);
            double orderVitality = guiderRadarService.getOrderVitality(merchant_id, member_id);
            List<Map<String, Object>> orderDetails = guiderRadarService.getOrderDetails(merchant_id, member_id);

            partnerMap.put("vitality", partnerVitality);
            partnerMap.put("details", partnerDetailsList);
            commissionMap.put("vitality", commissionVitality);
            orderMap.put("details", orderDetails);
            orderMap.put("vitality", orderVitality);
            guiderMap.put("partner", partnerMap);
            guiderMap.put("commission", commissionMap);
            guiderMap.put("order", orderMap);
            dataMap.put("data",guiderMap);
        } catch (Exception e) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            e.printStackTrace(new PrintStream(baos));
            String exception = baos.toString();
            System.err.println(exception);
        }
        return dataMap;
    }

}
