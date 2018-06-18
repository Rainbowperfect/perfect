package cn.itcast.mybatis.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.apache.ibatis.session.SqlSession;

public class DaoMapperProxy implements InvocationHandler{

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		//拿到SqlSession
		SqlSession sqlSession = MyBatisUtils.getSqlSession();
		
		Class clazz = (proxy.getClass().getInterfaces())[0];
		//执行增加
		//int acount= sqlSession.insert("cn.itcast.mybatis.dao.UserDao.addUser", args[0]);
		int acount= sqlSession.insert(clazz.getName()+"."+method.getName(), args[0]);
		
		//提交数据
		sqlSession.commit();
		return acount;
	}

}
