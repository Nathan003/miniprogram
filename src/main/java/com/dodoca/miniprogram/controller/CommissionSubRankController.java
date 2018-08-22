package com.dodoca.miniprogram.controller;

import com.dodoca.miniprogram.service.CommissionSubRankService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class CommissionSubRankController {

    @Autowired
    private CommissionSubRankService commissionSubRankService;

    @ResponseBody
    @RequestMapping(value = "/commissionrank", method = RequestMethod.POST)
    public HashMap<String,Object> getCommissionRank(@RequestBody Map<String, String> data){
        String merchant_id = data.get("merchant_id");
        String member_id = data.get("member_id");
        HashMap<String, Object> dataMap = new HashMap<>();
        try {
            List<Map<String, Object>> dataList = commissionSubRankService.getCommissionSubRank(merchant_id, member_id);
            dataMap.put("data",dataList);
        }catch (Exception e){
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            e.printStackTrace(new PrintStream(baos));
            String exception = baos.toString();
            System.err.println(exception);
        }
        return dataMap;
    }
}
