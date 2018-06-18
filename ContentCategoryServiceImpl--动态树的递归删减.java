package org.xx.manager.service.impl;

import org.xx.manager.pojo.ContentCategory;
import org.xx.manager.service.ContentCategoryService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;


@Service
public class ContentCategoryServiceImpl extends BaseServiceImpl<ContentCategory> implements ContentCategoryService {

    @Override
    public List<ContentCategory> queryContentCategoryByParentId(Long categoryId) {
        ContentCategory where = new ContentCategory();
        where.setParentId(categoryId);
        List<ContentCategory> categories = super.queryListByWhere(where);
        return categories;
    }

    @Override
    public ContentCategory saveContentCategorySelective(ContentCategory category) {
        category.setStatus(1);
        category.setIsParent(false);
        super.saveSelective(category);

        ContentCategory parent = super.queryById(category.getParentId());
        //如果当前添加节点的父节点为叶子节点时，更父节点状态
        if (!parent.getIsParent()) {
            parent.setIsParent(true);
            super.updateByIdSelective(parent);
        }
        //这里别忘了返回数据
        return category;
    }

    @Override
    public void deleteContentCategory(ContentCategory category) {
        //找出当前节点下所有的子节点
        //要删除的id列表
        List<Object> ids = new ArrayList<>();
        //先添加本身
        ids.add(category.getId());
        //查询所有子节点====>去创建一个递归的方法
        queryIds(category.getId(),ids);

        //删除所有节点
        super.deleteByIds(ids);

        //检查当前要删除的节点的父节点是否还有子节点
        ContentCategory where = new ContentCategory();
        where.setParentId(category.getParentId());
        Integer count = super.queryCountByWhere(where);
        //如果当前父节点没有子节点，更新父节点状态
        if(count < 1){
            ContentCategory parent = new ContentCategory();
            parent.setId(category.getParentId());
            parent.setIsParent(false);
            super.updateByIdSelective(parent);
        }
    }

    /**
     * 查询所有子子节点
     * @param parentId 要搜索的根
     * @param ids 记录查找到的节点
     */
    private void queryIds(Long parentId,List<Object> ids){
        ContentCategory parent = new ContentCategory();
        parent.setParentId(parentId);
        List<ContentCategory> categories = super.queryListByWhere(parent);
        for (ContentCategory category : categories) {
            //如果当前的子节点为父母节点时，递归调用本身
            if (category.getIsParent()) {
                queryIds(category.getId(),ids);
            }
            //记录要删除的节点
            ids.add(category.getId());
        }
    }
}
