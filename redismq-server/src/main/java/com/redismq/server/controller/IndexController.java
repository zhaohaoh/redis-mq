package com.redismq.server.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class IndexController {
 
    @RequestMapping("/")
    public String index(){
        return "index";
    }
 
    @RequestMapping("/index.html")
    public String indexHtml(){
        return "index";
    }
}