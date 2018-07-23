import org.csource.common.MyException;
import org.csource.fastdfs.*;

import java.io.IOException;

/**
 *获取conf的路径的方法是：System.getProperty("user.dir")，在idea中setting-->application 中设置Working directory 改为$MODULE_DIR$;
 */
public class FastdfsTest {

    public static void main(String[] args) throws IOException, MyException {
//        1、创建tracker.conf配置文件，内容就是tracker服务的地址。
// 配置文件内容：tracker_server=192.168.37.161:22122，然后加载配置文件(ClientGlobal.init方法加载)
		
        ClientGlobal.init(System.getProperty("user.dir") + "/src/main/resources/tracker.conf");
//        2、创建一个TrackerClient对象。直接new一个。
        TrackerClient trackerClient = new TrackerClient();
//        3、使用TrackerClient对象创建连接，getConnection获得一个TrackerServer对象。
        TrackerServer trackerServer = trackerClient.getConnection();
//        4、创建一个StorageServer的引用，值为null，为接下来创建StorageClient使用
        StorageServer storageServer = null;
//        5、创建一个StorageClient对象，直接new一个，需要两个参数TrackerServer对象、StorageServer的引用
        StorageClient storageClient = new StorageClient(trackerServer, storageServer);
//        6、使用StorageClient对象upload_file方法上传图片。
        //参数一：本地文件的路径，参数二：后缀名，参数三：暂时用不到，直接传入null
        String[] uploadFile = storageClient.upload_file("D:\\WebWork\\360wallpaper (3).jpg", "jpg", null);
//        7、返回数组。包含组名和图片的路径，打印结果。
        for (String s : uploadFile) {
            System.out.println(s);
        }

    }
}
