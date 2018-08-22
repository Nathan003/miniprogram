package com.dodoca.miniprogram.controller;

import com.dodoca.miniprogram.service.GroupRankService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class GroupRankController {

    @Autowired
    private GroupRankService groupRankService;

    @ResponseBody
    @RequestMapping(value = "/grouprank", method = RequestMethod.POST)
    public HashMap<String,Object> getGroupRank(@RequestBody Map<String, String> data){
        String merchant_id = data.get("merchant_id");
        String member_id = data.get("member_id");
        HashMap<String, Object> dataMap = new HashMap<>();
        try {
            List<Map<String, Object>> dataList = groupRankService.getGroupRank(merchant_id, member_id);
            dataMap.put("data",dataList);
        }catch (Exception e){
            System.err.println(e.getStackTrace());
        }
        return dataMap;
    }
}
