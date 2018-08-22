package com.dodoca.miniprogram.controller;

import com.dodoca.miniprogram.service.GoodsRankService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class GoodsRankController {

    @Autowired
    private GoodsRankService goodsRankService;

    @ResponseBody
    @RequestMapping(value = "/goodsrank", method = RequestMethod.POST)
    public HashMap<String, Object> getGoodsRank(@RequestBody Map<String, String> data) {
        String merchant_id = data.get("merchant_id");

        HashMap<String, Object> dataMap = new HashMap<>();

        try {
            List<Map<String, Object>> mapList = this.goodsRankService.getGoodsRankService(merchant_id);
            dataMap.put("data", mapList);
        } catch (Exception e) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            e.printStackTrace(new PrintStream(baos));
            String exception = baos.toString();
            System.err.println(exception);
        }

        return dataMap;
    }

}
