package com.dodoca.miniprogram.controller;

import com.dodoca.miniprogram.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

@RestController
public class HomePageController {

    @Autowired
    private HomePageTotalService homePageTotalService;

    @Autowired
    private HomePageTradeService homePageTradeService;

    @Autowired
    private HomePageGoodsRankService homePageGoodsRankService;

    @Autowired
    private HomePageTwitterService homePageTwitterService;

    @Autowired
    private HomePageTwitterRankService homePageTwitterRankService;


    @ResponseBody
    @RequestMapping(value = "/homepagedata", method = RequestMethod.POST)
    public HashMap<String, Object> getHomePageData(@RequestBody Map<String, String> data) {
        String merchant_id = data.get("merchant_id");
        HashMap<String, Object> dataMap = new HashMap<>();
        HashMap<String, Object> totalMap = new HashMap<>();
        HashMap<String, Object> totaChartlMap = new HashMap<>();
        HashMap<String, Object> tradeMap = new HashMap<>();
        HashMap<String, Object> tradeChartMap = new HashMap<>();
        HashMap<String, Object> goodsRankMap = new HashMap<>();
        HashMap<String, Object> twitterMap = new HashMap<>();
        HashMap<String, Object> twitterChartMap = new HashMap<>();
        HashMap<String, Object> twitterRankMap = new HashMap<>();
        try {

            HashMap<String, Object> yestoday = homePageTotalService.getTotalYestodayTotal(merchant_id);
            HashMap<String, Object> sevendaysTotal = homePageTotalService.getTotalSevendaysTotal(merchant_id);
            HashMap<String, Object> thirtydaysTotal = homePageTotalService.getTotalThirtydaysTotal(merchant_id);
            HashMap<String, Object> historyTotal = homePageTotalService.getTotalHistoryTotal(merchant_id);
            HashMap<String, Object> avgTotal = homePageTotalService.getTotalAvgTotal(merchant_id);
            HashMap<String, Object> maxTotal = homePageTotalService.getTotalMaxTotal(merchant_id);
            HashMap<String, Object> yestodayChart = homePageTotalService.getTotalYestodayChart(merchant_id);
            HashMap<String, Object> todayChart = homePageTotalService.getTotalTodayChart(merchant_id);
            HashMap<String, Object> sevendaysChart = homePageTotalService.getTotalSevendaysChart(merchant_id);
            HashMap<String, Object> thirtydaysChart = homePageTotalService.getTotalThirtydaysChart(merchant_id);
            totalMap.put("active_lv", "");
            totalMap.put("yestoday", yestoday);
            totalMap.put("seven_day", sevendaysTotal);
            totalMap.put("month_day", thirtydaysTotal);
            totalMap.put("history", historyTotal);
            totalMap.put("avg", avgTotal);
            totalMap.put("max", maxTotal);
            totaChartlMap.put("yestoday", yestodayChart);
            totaChartlMap.put("today", todayChart);
            totalMap.put("chart_yestoday_today", totaChartlMap);
            totalMap.put("chart_yestoday", yestodayChart);
            totalMap.put("chart_seven", sevendaysChart);
            totalMap.put("chart_month", thirtydaysChart);


            HashMap<String, Object> yestodayData = homePageTradeService.getYestodayData(merchant_id);
            HashMap<String, Object> sevendaysData = homePageTradeService.getSevendaysData(merchant_id);
            HashMap<String, Object> thirtydaysData = homePageTradeService.getThirtydaysData(merchant_id);
            HashMap<String, Object> yestodayChart1 = homePageTradeService.getTradeYestodayChart(merchant_id);
            HashMap<String, Object> todayChart1 = homePageTradeService.getTradeTodayChart(merchant_id);
            HashMap<String, Object> sevendaysChart1 = homePageTradeService.getTradeSevendaysChart(merchant_id);
            HashMap<String, Object> thirtydaysChart1 = homePageTradeService.getTradeThirtydaysChart(merchant_id);
            tradeMap.put("order_lv", "");
            tradeMap.put("yestoday", yestodayData);
            tradeMap.put("seven_day", sevendaysData);
            tradeMap.put("month_day", thirtydaysData);
            tradeChartMap.put("yestoday", yestodayChart1);
            tradeChartMap.put("today", todayChart1);
            tradeMap.put("chart_yestoday_today", tradeChartMap);
            tradeMap.put("chart_yestoday", yestodayChart1);
            tradeMap.put("chart_seven", sevendaysChart1);
            tradeMap.put("chart_month", thirtydaysChart1);

            HashMap<String, Object> totalBrowseData = homePageGoodsRankService.getTotalBrowseData(merchant_id);
            HashMap<String, Object> totalBuyData = homePageGoodsRankService.getTotalBuyData(merchant_id);
            HashMap<String, Object> totalRebuysData = homePageGoodsRankService.getTotalRebuysData(merchant_id);
            HashMap<String, Object> yestodayBrowseData = homePageGoodsRankService.getYestodayBrowseData(merchant_id);
            HashMap<String, Object> yestodayBuyData = homePageGoodsRankService.getYestodayBuyData(merchant_id);
            HashMap<String, Object> yestodayRebuysData = homePageGoodsRankService.getYestodayRebuysData(merchant_id);
            HashMap<String, Object> sevendayBrowseData = homePageGoodsRankService.getSevendayBrowseData(merchant_id);
            HashMap<String, Object> sevendayBuyData = homePageGoodsRankService.getSevendayBuyData(merchant_id);
            HashMap<String, Object> sevendayRebuysData = homePageGoodsRankService.getSevendayRebuysData(merchant_id);
            HashMap<String, Object> thirtydayBrowseData = homePageGoodsRankService.getThirtydayBrowseData(merchant_id);
            HashMap<String, Object> thirtydayBuyData = homePageGoodsRankService.getThirtydayBuyData(merchant_id);
            HashMap<String, Object> thirtydayRebuysData = homePageGoodsRankService.getThirtydayRebuysData(merchant_id);
            goodsRankMap.put("total_bowse_rank", totalBrowseData);
            goodsRankMap.put("total_buy_rank", totalBuyData);
            goodsRankMap.put("total_rebuy_rank", totalRebuysData);
            goodsRankMap.put("yestoday_bowse_rank", yestodayBrowseData);
            goodsRankMap.put("yestoday_buy_rank", yestodayBuyData);
            goodsRankMap.put("yestoday_rebuy_rank", yestodayRebuysData);
            goodsRankMap.put("seven_bowse_rank", sevendayBrowseData);
            goodsRankMap.put("seven_buy_rank", sevendayBuyData);
            goodsRankMap.put("seven_rebuy_rank", sevendayRebuysData);
            goodsRankMap.put("month_bowse_rank", thirtydayBrowseData);
            goodsRankMap.put("month_buy_rank", thirtydayBuyData);
            goodsRankMap.put("month_rebuy_rank", thirtydayRebuysData);

            HashMap<String, Object> yestodayData1 = homePageTwitterService.getTwitterYestodayData(merchant_id);
            HashMap<String, Object> sevendaysData1 = homePageTwitterService.getTwitterSevendaysData(merchant_id);
            HashMap<String, Object> thirtydaysData1 = homePageTwitterService.getTwitterThirtydaysData(merchant_id);
            HashMap<String, Object> yestodayChart2 = homePageTwitterService.getTwitterYestodayChart(merchant_id);
            HashMap<String, Object> todayChart2 = homePageTwitterService.getTwitterTodayChart(merchant_id);
            HashMap<String, Object> sevendaysChart2 = homePageTwitterService.getTwitterSevendaysChart(merchant_id);
            HashMap<String, Object> thirtydaysChart2 = homePageTwitterService.getTwitterThirtydaysChart(merchant_id);
            twitterMap.put("member_lv", "");
            twitterMap.put("member_act", "");
            twitterMap.put("yestoday", yestodayData1);
            twitterMap.put("seven_day", sevendaysData1);
            twitterMap.put("month_day", thirtydaysData1);
            twitterChartMap.put("yestoday", yestodayChart2);
            twitterChartMap.put("today", todayChart2);
            twitterMap.put("chart_yestoday_today", twitterChartMap);
            twitterMap.put("chart_yestoday", yestodayChart2);
            twitterMap.put("chart_seven", sevendaysChart2);
            twitterMap.put("chart_month", thirtydaysChart2);

            HashMap<String, Object> totalTwitterSubData = homePageTwitterRankService.getTotalTwitterSubData(merchant_id);
            HashMap<String, Object> totalCommissionSubData = homePageTwitterRankService.getTotalCommissionSubData(merchant_id);
            HashMap<String, Object> totalOrderSumData = homePageTwitterRankService.getTotalOrderSumData(merchant_id);
            HashMap<String, Object> totalOrderAmountData = homePageTwitterRankService.getTotalOrderAmountData(merchant_id);
            HashMap<String, Object> totalCommissionAmountsData = homePageTwitterRankService.getTotalCommissionAmountsData(merchant_id);
            HashMap<String, Object> yestodayTwitterSubData = homePageTwitterRankService.getYestodayTwitterSubData(merchant_id);
            HashMap<String, Object> yestodayCommissionSubData = homePageTwitterRankService.getYestodayCommissionSubData(merchant_id);
            HashMap<String, Object> yestodayOrderSumData = homePageTwitterRankService.getYestodayOrderSumData(merchant_id);
            HashMap<String, Object> yestodayOrderAmountData = homePageTwitterRankService.getYestodayOrderAmountData(merchant_id);
            HashMap<String, Object> yestodayCommissionAmountsData = homePageTwitterRankService.getYestodayCommissionAmountsData(merchant_id);
            HashMap<String, Object> sevenTwitterSubData = homePageTwitterRankService.getSevenTwitterSubData(merchant_id);
            HashMap<String, Object> sevenCommissionSubData = homePageTwitterRankService.getSevenCommissionSubData(merchant_id);
            HashMap<String, Object> sevenOrderSumData = homePageTwitterRankService.getSevenOrderSumData(merchant_id);
            HashMap<String, Object> sevenOrderAmountData = homePageTwitterRankService.getSevenOrderAmountData(merchant_id);
            HashMap<String, Object> sevenCommissionAmountsData = homePageTwitterRankService.getSevenCommissionAmountsData(merchant_id);
            HashMap<String, Object> monthTwitterSubData = homePageTwitterRankService.getMonthTwitterSubData(merchant_id);
            HashMap<String, Object> monthCommissionSubData = homePageTwitterRankService.getMonthCommissionSubData(merchant_id);
            HashMap<String, Object> monthOrderSumData = homePageTwitterRankService.getMonthOrderSumData(merchant_id);
            HashMap<String, Object> monthOrderAmountData = homePageTwitterRankService.getMonthOrderAmountData(merchant_id);
            HashMap<String, Object> monthCommissionAmountsData = homePageTwitterRankService.getMonthCommissionAmountsData(merchant_id);
            twitterRankMap.put("total_twitter_sub", totalTwitterSubData);
            twitterRankMap.put("total_commission_sub", totalCommissionSubData);
            twitterRankMap.put("total_order_sum", totalOrderSumData);
            twitterRankMap.put("total_order_amount", totalOrderAmountData);
            twitterRankMap.put("total_commission_amount", totalCommissionAmountsData);
            twitterRankMap.put("yestoday_twitter_sub", yestodayTwitterSubData);
            twitterRankMap.put("yestoday_commission_sub", yestodayCommissionSubData);
            twitterRankMap.put("yestoday_order_sum", yestodayOrderSumData);
            twitterRankMap.put("yestoday_order_amount", yestodayOrderAmountData);
            twitterRankMap.put("yestoday_commission_amount", yestodayCommissionAmountsData);
            twitterRankMap.put("seven_twitter_sub", sevenTwitterSubData);
            twitterRankMap.put("seven_commission_sub", sevenCommissionSubData);
            twitterRankMap.put("seven_order_sum", sevenOrderSumData);
            twitterRankMap.put("seven_order_amount", sevenOrderAmountData);
            twitterRankMap.put("seven_commission_amount", sevenCommissionAmountsData);
            twitterRankMap.put("month_twitter_sub", monthTwitterSubData);
            twitterRankMap.put("month_commission_sub", monthCommissionSubData);
            twitterRankMap.put("month_order_sum", monthOrderSumData);
            twitterRankMap.put("month_order_amount", monthOrderAmountData);
            twitterRankMap.put("month_commission_amount", monthCommissionAmountsData);


            dataMap.put("total", totalMap);
            dataMap.put("trade", tradeMap);
            dataMap.put("goods_rank", goodsRankMap);
            dataMap.put("twitter", twitterMap);
            dataMap.put("twitter_rank", twitterRankMap);
        } catch (Exception e) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            e.printStackTrace(new PrintStream(baos));
            String exception = baos.toString();
            System.err.println(exception);
        }
        return dataMap;
    }

}
