
public class ProductServiceImpl implements ProductService {

	@Override
	public List<Product> findHot() throws Exception {
		ProductDao productDao = new ProductDaoImpl();
		return productDao.findHot();
	}

	@Override
	public List<Product> findNew() throws Exception {
		ProductDao productDao = new ProductDaoImpl();
		return productDao.findNew();
	}

	@Override
	public Product findByPid(String pid) throws Exception {
		// 调用Dao
		ProductDao productDao = new ProductDaoImpl();
		return productDao.findByPid(pid);
	}

	@Override
	public PageBean<Product> findByPage(String cid, int curPage) throws Exception {

		// 0 .创建Dao
		ProductDao productDao = new ProductDaoImpl();

		// 当前类别下的商品总数量
		int count = productDao.selectCount(cid);
		// 一页显示的数量
		int curSize = Constant.PRODUCT_PAGE_SIZE;
		// 总页数
		int sumPage = 0;
		if (count % curSize == 0) {
			sumPage = count / curSize;
		} else {
			sumPage = count / curSize + 1;
		}
		// 商品的List集合
		int b = curSize;
		int a = (curPage - 1) * b;
		List<Product> list = productDao.selectLimit(cid, a, b);

		// 1. 封装PageBean set5次,缺少什么set什么
		PageBean<Product> pageBean = new PageBean<Product>();
		// 1.1 封装当前页码
		pageBean.setCurPage(curPage);
		// 1.2 封装一页显示的数量
		pageBean.setCurSize(curSize);
		// 1.3 封装当前类别下的商品总数量 查(select count(*) ....)
		pageBean.setCount(count);
		// 1.4 封装总页数
		pageBean.setSumPage(sumPage);
		// 1.5 封装商品的List集合 (查 select *..... limit a ,b)
		pageBean.setList(list);

		return pageBean;
	}

	@Override
	public List<Product> findAll() throws Exception {
		// 0 .创建Dao
		ProductDao productDao = new ProductDaoImpl();
		return productDao.findAll();
	}

	@Override
	public void save(Product product) throws Exception {
		// 0 .创建Dao
		ProductDao productDao = new ProductDaoImpl();
		productDao.save(product);

	}

}
