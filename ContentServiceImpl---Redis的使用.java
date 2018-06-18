package com.taotao.manager.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.taotao.manager.jedis.RedisUtils;
import com.taotao.manager.pojo.Content;
import com.taotao.manager.service.ContentService;
import com.taotao.manager.utils.TaoResult;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author steven
 * @version 1.0
 * @description com.taotao.manager.service.impl
 * @date 2018-2-25
 */
@Service
public class ContentServiceImpl extends BaseServiceImpl<Content> implements ContentService {

    @Override
    public TaoResult<Content> queryContent(Long categoryId, Integer page, Integer rows) {
        TaoResult<Content> result = new TaoResult<>();
        //设置查询条件
        Content where = new Content();
        where.setCategoryId(categoryId);

        //设置分页参数
        PageHelper.startPage(page, rows);

        List<Content> contents = super.queryListByWhere(where);
        result.setRows(contents);
        //查询total
        PageInfo<Content> info = new PageInfo<Content>(contents);
        result.setTotal(info.getTotal());
        return result;
    }

    @Autowired
    private RedisUtils redisUtils;
    @Value("${TAOTAO_PORTAL_AD_KEY}")
    private String TAOTAO_PORTAL_AD_KEY;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String queryContentByCategoryId(Long categoryId) {

        String json = redisUtils.get(TAOTAO_PORTAL_AD_KEY);
        //如果redis找到数据了，直接返回
        if (StringUtils.isNotBlank(json)) {
            return json;
        }

        //查询数据
        Content where = new Content();
        where.setCategoryId(categoryId);
        List<Content> contents = super.queryListByWhere(where);

        //组装数据
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> map = null;
        for (Content content : contents) {
            map = new HashMap<>();
            map.put("srcB", content.getPic());
            map.put("height", 240);
            map.put("alt", "");
            map.put("width", 670);
            map.put("src", content.getPic());
            map.put("widthB", 550);
            map.put("href", content.getUrl());
            map.put("heightB", 240);

            result.add(map);
        }
        try {
            json = mapper.writeValueAsString(result);
            redisUtils.set(TAOTAO_PORTAL_AD_KEY, json, 60 * 60 * 24 * 7);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return json;
    }
}
