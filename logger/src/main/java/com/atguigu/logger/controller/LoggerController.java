package com.atguigu.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.constant.GmallConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController     // Controller + ResponseBody
/*@Controller*/
public class LoggerController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    /* @RequestMapping(value = "/log", method = RequestMethod.POST) */
    @PostMapping("/log")
    /*@ResponseBody*/
    public String dolog(@RequestParam("log") String logJson){
        // 补时间戳
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts", System.currentTimeMillis());

        // 落盘到logfile log4j
        logger.info(jsonObject.toJSONString());

        // 发送到Kafka
        if("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, jsonObject.toJSONString());
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, jsonObject.toJSONString());
        }
        return "success";
    }
}
