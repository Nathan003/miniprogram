package com.dodoca.miniprogram.controller;

import com.dodoca.miniprogram.service.GuiderRankService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class GuiderRankController {

    @Autowired
    private GuiderRankService guiderRankService;

    @ResponseBody
    @RequestMapping(value = "/guiderrank", method = RequestMethod.POST)
    public HashMap<String, Object> getGuiderRank(@RequestBody Map<String, String> data) {
        String merchant_id = data.get("merchant_id");

        HashMap<String, Object> dataMap = new HashMap<>();

        try {
            List<Map<String, Object>> guiderRank = guiderRankService.getGuiderRank(merchant_id);
            dataMap.put("data", guiderRank);
        } catch (Exception e) {
            System.err.println(e.getStackTrace());
        }
        return dataMap;
    }
}
