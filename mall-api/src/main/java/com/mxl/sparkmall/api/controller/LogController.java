package com.mxl.sparkmall.api.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
public class LogController {
    private final static Logger logger = LoggerFactory.getLogger(LogController.class);
    //private final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(LogController.class);

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.start}")
    private String topic_start;

    @Value("${kafka.topic.event}")
    private String topic_event;

    @GetMapping("/")
    public String root() {
        //logger.info(topic_event + "," + topic_start);
        return "hello";
    }

    @PostMapping("/log")
    public void log(@RequestBody String log) {
        JSONObject logObj = JSON.parseObject(log);
        logObj.put("ts", System.currentTimeMillis());

        logger.info(logObj.toJSONString());

        String logType = logObj.getString("type");
        if ("event".equals(logType)) {
            kafkaTemplate.send(topic_event, logObj.toJSONString());
        } else {
            String s = "";
            kafkaTemplate.send(topic_start, logObj.toJSONString());
        }
    }
}
