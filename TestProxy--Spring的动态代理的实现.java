package org.person.test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.junit.Test;
import org.springframework.cglib.proxy.Callback;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import org.person.service.UserService;
import org.person.service.impl.ProductService;
import org.person.service.impl.UserServiceImpl;
import org.person.util.Logger;

public class TestProxy {
	
	@Test
	public void testCglibProxy(){
		//1. 创建真实对象
		final ProductService productService = new ProductService();
		
		//2. 创建代理对象 靠enhancer去创建代理
		Enhancer enhancer = new Enhancer();
		
		//设置父类是谁
		enhancer.setSuperclass(ProductService.class);
		
		//设置回调
		enhancer.setCallback(new MethodInterceptor() {
			
			//arg1 方法引用
			//arg2 :方法参数
			@Override
			public Object intercept(Object arg0, Method arg1, Object[] arg2, MethodProxy arg3) throws Throwable {
				System.out.println("~!~!");
				if(arg1.getName().equals("save")){
					Logger.log();
				}
				
				return arg1.invoke(productService, arg2);
			}
		});
		
		//拿到代理对象
		ProductService proxyObj  = (ProductService) enhancer.create();
		
		//使用代理对象调用方法·
		proxyObj.save();
	}
		

	
	@Test
	public void testJDKProxy(){
		
		//1. 创建真实对象
		final UserService userservice = new UserServiceImpl();
		
		//2. 创建代理对象
		UserService  proxyObj = (UserService) Proxy.newProxyInstance(
				userservice.getClass().getClassLoader(),  //加载这个代理类用什么类加载器。 真实类用什么，咱们就用什么
				userservice.getClass().getInterfaces(), //真实类实现了什么接口，代理类也实现这些接口
				new InvocationHandler() {//调用处理器。 就是一会回来调用的函数， 简称回调函数
					
					
					//method 外面的方法引用，也就是外面走的proxyObj.save()，这里的method即使save方法的应用
					//args 参数
					@Override
					public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
						
						System.out.println("~~!~!");
						//userservice.save(); 正向调用 
						
						//扩展功能
						if(method.getName().equals("save")){
							Logger.log();
						}
						
						//反射调用
						return method.invoke(userservice, args);
					}
				});
		
		//3. 使用代理对象调用方法~~
		proxyObj.save();
	}
	
	
	
	
}

