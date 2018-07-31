package com.org.person.manager.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.org.person.manager.utils.PicUploadResult;
import org.csource.fastdfs.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;


@Controller
@RequestMapping("/pic")
public class PicController {

    private String[] type = {"jpg", "jpeg", "gif", "png", "bmp"};
    @Value("${org.person_FASTDFS_SERVER}")
    private String org.person_FASTDFS_SERVER;

    private final ObjectMapper mapper = new ObjectMapper();

    @RequestMapping(value = "/upload",produces = MediaType.TEXT_HTML_VALUE)
    @ResponseBody
    public String upload(MultipartFile uploadFile) {

        PicUploadResult result = new PicUploadResult();

        //验证图片的后缀名
        //验证通过标识
        boolean flag = false;
        //后缀名
        String oldName = uploadFile.getOriginalFilename();
        String extName = oldName.substring(oldName.lastIndexOf(".") + 1);

        for (String s : type) {
            if (s.equals(extName)) {
                flag = true;
                break;
            }
        }
        //验证图片
        if (flag) {

            //读取图片
            try {
                BufferedImage image = ImageIO.read(uploadFile.getInputStream());
                //设置宽度和高度
                result.setWidth("" + image.getWidth());
                result.setHeight("" + image.getHeight());
            } catch (Exception e) {
                result.setError(1);
                try {
                    String json = mapper.writeValueAsString(result);
                    return json;
                } catch (JsonProcessingException e1) {
                    e1.printStackTrace();
                }


            }
            //上传图片
            try {
                // 1、创建tracker.conf配置文件，内容就是tracker服务的地址。配置文件内容：tracker_server=192.168.37.161:22122，然后加载配置文件(ClientGlobal.init方法加载)

                ClientGlobal.init(System.getProperty("user.dir") + "\\src\\main\\resources\\tracker.conf");
                System.out.println(System.getProperty("user.dir"));
                // 2、创建一个TrackerClient对象。直接new一个。
                TrackerClient trackerClient = new TrackerClient();
                //3、使用TrackerClient对象创建连接，getConnection获得一个TrackerServer对象。
                TrackerServer trackerServer = trackerClient.getConnection();
                //4、创建一个StorageServer的引用，值为null，为接下来创建StorageClient使用
                StorageServer storageServer = null;
                // 5、创建一个StorageClient对象，直接new一个，需要两个参数TrackerServer对象、StorageServer的引用
                StorageClient storageClient = new StorageClient(trackerServer, storageServer);
                //6、使用StorageClient对象upload_file方法上传图片。
                String[] urls = storageClient.upload_file(uploadFile.getBytes(), "extName", null);
                //  7、返回数组。包含组名和图片的路径，打印结果。
                String url=org.person_FASTDFS_SERVER +urls[0]+"/"+urls[1];
                //设置
                result.setUrl(url);
                result.setError(0);
            } catch (Exception e) {
                e.printStackTrace();

            }
        }
            String json="";
        try {
           json= mapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return json;
        }

}
