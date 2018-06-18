import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.List;

public class BaseServiceImpl<T extends BasePojo> implements BaseService<T> {

    private Class<T> clazz;

    public BaseServiceImpl() {
        // 获取父类的type
        Type type = this.getClass().getGenericSuperclass();

        // 强转为ParameterizedType，可以使用获取泛型类型的方法
        ParameterizedType pType = (ParameterizedType) type;

        // 获取泛型的class
        this.clazz = (Class<T>) pType.getActualTypeArguments()[0];
    }

    @Autowired
    private Mapper<T> mapper;

    @Override
    public T queryById(Long id) {
        return mapper.selectByPrimaryKey(id);
    }

    @Override
    public List<T> queryAll() {
        return mapper.select(null);
    }

    @Override
    public Integer queryCountByWhere(T t) {
        return mapper.selectCount(t);
    }

    @Override
    public List<T> queryListByWhere(T t) {
        return mapper.select(t);
    }

    @Override
    public List<T> queryByPage(Integer page, Integer rows) {
        PageHelper.startPage(page, rows);
        List<T> list = mapper.select(null);
        return list;
    }

    @Override
    public T queryOne(T t) {
        return mapper.selectOne(t);
    }

    @Override
    public void save(T t) {
        if (t.getCreated() == null) {
            t.setCreated(new Date());
            t.setUpdated(t.getCreated());
        }
        mapper.insert(t);
    }

    @Override
    public void saveSelective(T t) {
        if (t.getCreated() == null) {
            t.setCreated(new Date());
            t.setUpdated(t.getCreated());
        }
        mapper.insertSelective(t);
    }

    @Override
    public void updateById(T t) {
        if (t.getUpdated() == null) {
            t.setUpdated(new Date());
        }
        mapper.updateByPrimaryKey(t);
    }

    @Override
    public void updateByIdSelective(T t) {
        if (t.getUpdated() == null) {
            t.setUpdated(new Date());
        }
        mapper.updateByPrimaryKeySelective(t);
    }

    @Override
    public void deleteById(Long id) {
        mapper.deleteByPrimaryKey(id);
    }

    @Override
    public void deleteByIds(List<Object> ids) {
        //组装id列表条件
        Example example = new Example(clazz);
        Example.Criteria criteria = example.createCriteria();
        criteria.andIn("id", ids);
        mapper.deleteByExample(example);
    }
}
