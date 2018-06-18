package com.taotao.sso.controller;

import com.taotao.manager.pojo.User;
import com.taotao.sso.service.UserService;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * @author steven
 * @version 1.0
 * @description com.taotao.sso.controller
 * @date 2018-3-1
 */
@Controller
@RequestMapping("user")
public class UserController {
    private Logger logger = Logger.getLogger(UserController.class);
    @Autowired
    private UserService userService;

    //http://sso.taotao.com/user/check/{param}/{type}
    @RequestMapping("check/{param}/{type}")
    public ResponseEntity<String> check(@PathVariable String param,@PathVariable Integer type,String callback){
        //如果type不在1-3之内，反回参数无效错误码
        if(type < 1 || type > 3){
            //return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(false);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(callback + "(" + false + ")");
        }
        try {
            Boolean flag = userService.check(param,type);
            //返回查询结果
            //return ResponseEntity.ok(flag);
            return ResponseEntity.ok(callback + "(" + flag + ")");
        } catch (Exception e) {
            e.printStackTrace();
        }
        //如果发生异常，返回500状态码
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(callback + "(" + false + ")");
    }

    //http://sso.taotao.com/user/{ticket}
    @RequestMapping("{ticket}")
    public ResponseEntity<User> queryUserByTicket(@PathVariable String ticket){
        try {
            User user = userService.queryUserByTicket(ticket);
            //如果找不到用户，返回404错误码
            if (user == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
            }else{
                return ResponseEntity.ok(user);
            }
        } catch (Exception e){
            logger.error("跟据ticket查询用户发生异常",e);
        }
        //如果发生异常，返回500状态码
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
    }
}
