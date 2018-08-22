package com.dodoca.miniprogram.controller;

import com.dodoca.miniprogram.service.GuiderSubRankService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController(value = "/guidersubrank")
public class GuiderSubRankController {
    @Autowired
    private GuiderSubRankService guiderSubRankService;

    @ResponseBody
    @RequestMapping(value = "/guidersubrank/byguider", method = RequestMethod.POST)
    public HashMap<String,Object> getGuiderRankByGuider(@RequestBody Map<String, String> data){
        String merchant_id = data.get("merchant_id");
        String member_id = data.get("member_id");
        HashMap<String, Object> dataMap = new HashMap<>();
        try {
            List<Map<String, Object>> dataList = guiderSubRankService.getGuiderSubRankByGuiderNumber(merchant_id, member_id);
            dataMap.put("data",dataList);
        }catch (Exception e){
            System.err.println(e.getStackTrace());
        }
        return dataMap;
    }


    @ResponseBody
    @RequestMapping(value = "/guidersubrank/bycommission", method = RequestMethod.POST)
    public HashMap<String,Object> getGuiderRankByCommission(@RequestBody Map<String, String> data){
        String merchant_id = data.get("merchant_id");
        String member_id = data.get("member_id");
        HashMap<String, Object> dataMap = new HashMap<>();
        try {
            List<Map<String, Object>> dataList = guiderSubRankService.getGuiderSubRankByCommission(merchant_id, member_id);
            dataMap.put("data",dataList);
        }catch (Exception e){
            System.err.println(e.getStackTrace());
        }
        return dataMap;
    }
}
