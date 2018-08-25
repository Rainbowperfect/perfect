package com.org.support.contentstorageservice.client.handler.service.commoncontent;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.org.support.cbb.util.download.range.Range;
import com.org.support.contentstorageservice.client.handler.ceph.impl.ContentServiceS3Impl;
import com.org.support.contentstorageservice.client.handler.exception.CSException;
import com.org.support.contentstorageservice.client.handler.util.CSFileUtil;
import com.org.support.contentstorageservice.client.handler.util.CSThreadFactory;
import com.org.support.contentstorageservice.client.handler.util.CSUtil;
import com.org.support.contentstorageservice.client.handler.util.SliceDownloadUtil;
import com.org.support.contentstorageservice.client.handler.wsmanager.CephCSServiceSupport;
import com.org.support.contentstorageservice.client.handler.wsmanager.CephCSWSManager;
import com.org.support.contentstorageservice.common.ContentStorageConstants;
import com.org.support.contentstorageservice.common.FileInfo;
import com.org.support.contentstorageservice.model.Md5Entity;

/**
 * 
 * CS 通用文件处理服务类
 * 
 * @author  z00164918
 * @version
 * @since  [Support/Service]
 */
public class CephCSCommonContentServiceImpl extends CephCSServiceSupport
{
    /**
     * S3接口
     */
	private ContentServiceS3Impl csS3Impl;
    
    /**
     * 异步写入ceph线程数
     */
    private final static int CORE_POOL_SIZE = 10;
    
    /**
     * 日志组件
     */
    private static Logger logger = LoggerFactory.getLogger(CephCSCommonContentServiceImpl.class);
    
    /**
     * 构造方法
     */
    public CephCSCommonContentServiceImpl()
    {
    	this.csS3Impl = new ContentServiceS3Impl();
    }
    
    /**
     * 向 CS 中增加一个文档(将该文档内容从Ceph中的临时目录copy到CS的Ceph正式目录下,Ceph临时目录的文件不删除)
     */
    public boolean addContentByCopy(String contentType, Map<String, Object> contentMap) throws CSException
    {
        logger.info("addContentByCopy. contentType:" + contentType + "  contentMap:" + contentMap);
        
        try
        {
            // 验证新增时参数是否为空
            if (!CSUtil.checkAddContentByCopy(contentType, contentMap))
            {
                throw new IllegalArgumentException(
                        "CS handler error: addContentByCopy failed ! input parameters illegal.");
            }
            
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString();
            String partNo = contentMap.get(ContentStorageConstants.PART_NO).toString();
            String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
            String sourceKey = contentMap.get(ContentStorageConstants.SOURCE_KEY).toString();
            
            //获取临时桶名，此为逻辑桶名
            String srcBucketName = CephCSWSManager.getCSTempLogicBucket();
            //获取正式桶名，此为逻辑桶名
            String desBucketName = CephCSWSManager.getCSRootLogicBucket();
            contentMap.put(ContentStorageConstants.BUCKET_NAME, desBucketName);
            
            ObjectMetadata tmpMetaData = csS3Impl.getObjectMetaData(srcBucketName, sourceKey);
            if (null == tmpMetaData)
            {
                logger.error("CS handler error: getObjectMetaData is null. nodeId=" + nodeId + " ,partNo=" + partNo
                    + " ,contentType=" + contentType + ",srcBucketName=" + srcBucketName + ",sourceKey=" + sourceKey);
                return false;
            }

            String md5 = tmpMetaData.getUserMetaDataOf(ContentStorageConstants.S3_MD5);
            String fileSize = tmpMetaData.getUserMetaDataOf(ContentStorageConstants.S3_FILESIZE);
            
            if (StringUtils.isEmpty(md5) || StringUtils.isEmpty(fileSize))
            {
                throw new CSException("CS handler error: get md5 or fileSize from tmp bucket error !");
            }
            
            // 验证CS内容是否存在
            if (isContentExist(contentType, nodeId, partNo))
            {
                logger.error("CS handler error: addContentByCopy failed ! please delete the node first.nodeId="
                        + nodeId + " ,partNo=" + partNo + " ,contentType=" + contentType);
                return false;
            }
            
            // 生成Ceph存储的文件名
            String cephFilePath = getCephFilePath(fileName, contentType, nodeId);
            
            if (StringUtils.isEmpty(cephFilePath))
            {
            	logger.error("CS handler error: addContentByCopy failed ! cephFilePath is empty.");
                return false;
            }
            
            // 根据MD5和fileSize,检测生成文件内容是否存在
            contentMap.put(ContentStorageConstants.MD5, md5);
            contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
            Md5Entity md5Entity = cephCSWSManager.getMd5FilePath(md5, fileSize);
            
            // 文件在MD5表的文件路径是否存在，不存在就上传，存在就检查ceph中是否还存在
            if (null == md5Entity)
            {
                // ceph中以key来模拟目录，前面需要加分隔符
                /*if (!csS3Impl.copyObjectWithSingleOperation(sourceBucketName, 
                		sourceKey, 
                		destinationBucketName, 
                		cephFilePath))
                {
                    logger.error("CS handler error: addContentByCopy failed ! copyObjectWithSingleOperation error.");
                    return false;
                }*/
            	
            	// 传入主从region，用于写入双写同步信息与实际上传的一致
                contentMap.put(ContentStorageConstants.S3_REGION_MASTER, CephCSWSManager.getMaterRegion());
                contentMap.put(ContentStorageConstants.S3_REGION_SLAVE, CephCSWSManager.getSlaveRegion());
                
                // 上传文件结束后，再下载到本地做md5校验
                if (!CSFileUtil.copyObjectAndCheckMd5(srcBucketName, desBucketName, sourceKey, cephFilePath, csS3Impl))
                {
                    // 首次校验不一致时，重试一次
                    if (!CSFileUtil.copyObjectAndCheckMd5(srcBucketName, desBucketName, sourceKey, cephFilePath, csS3Impl))
                    {
                        logger.error("CS handler error: addContentByCopy failed ! copyObjectAndCheckMd5 failed.");
                        return false;
                    }
                }
                
                // CS 中没有文件实体,保留新生成的文件,并将路径保存到数据库表
                contentMap.put(ContentStorageConstants.FILE_PATH, cephFilePath);
                
                // 向cs_md5_path_info表中保存数据
                if (!cephCSWSManager.addMd5FilePath(contentMap))
                {
                    logger.error("CS handler error: addContentByCopy failed ! addMd5FilePath to DB failed.");
                    return false;
                }
            }
            else
            {
                String md5FilePath = md5Entity.getFilePath();
                String md5BucketName = md5Entity.getBucketName();
                
                // CS中已经存在文件实体,直接将该路径作为新的文件路径
                contentMap.put(ContentStorageConstants.FILE_PATH, md5FilePath);
                contentMap.put(ContentStorageConstants.BUCKET_NAME, md5BucketName);
                
                // md5中文件路径存在的情况下,根据文件的key获取ceph中文件实体是否还存在，不存在则重新上传
                ObjectMetadata metaData = csS3Impl.getObjectMetaData(md5BucketName, md5FilePath);
                if (null == metaData)
                {
                    /*if (!csS3Impl.copyObjectWithSingleOperation(sourceBucketName,
                            sourceKey,
                            md5BucketName,
                            md5FilePath))
                    {
                        logger.error("CS handler error: addContentByCopy failed ! copyObjectWithSingleOperation error.");
                        return false;
                    }*/
                    
                    // 传入主从region，用于写入双写同步信息与实际上传的一致
                    contentMap.put(ContentStorageConstants.S3_REGION_MASTER, CephCSWSManager.getMaterRegion());
                    contentMap.put(ContentStorageConstants.S3_REGION_SLAVE, CephCSWSManager.getSlaveRegion());
                	
                    // 上传文件结束后，再下载到本地做md5校验
                    if (!CSFileUtil.copyObjectAndCheckMd5(srcBucketName,
                        md5BucketName,
                        sourceKey,
                        md5FilePath,
                        csS3Impl))
                    {
                        // 首次校验不一致时，重试一次
                        if (!CSFileUtil.copyObjectAndCheckMd5(srcBucketName,
                            md5BucketName,
                            sourceKey,
                            md5FilePath,
                            csS3Impl))
                        {
                            logger.error("CS handler error: addContentByCopy failed ! metaData is null, copyObjectAndCheckMd5 failed.");
                            return false;
                        }
                    }
                    
                    // 新增同步任务（通过修改或者新增同步表记录实现）
                    if (!cephCSWSManager.addSyncCephTask(contentMap))
                    {
                        logger.error("CS handler error: addContentByCopy failed ! metaData is null, addSyncCephTask failed.");
                        return false;
                    }
                    
                    logger.error("CS handler info: in addContentByCopy(): metaData is null, uploadObjectAndCheckMd5 and addSyncCephTask finished! ");
                }
                else
                {
                    // 判断Ceph上md5和fileSize是否相同，不同也上传
                	if (!StringUtils.equals(md5, metaData.getUserMetaDataOf(ContentStorageConstants.S3_MD5))
                			|| !StringUtils.equals(fileSize, metaData.getUserMetaDataOf(ContentStorageConstants.S3_FILESIZE)))
                    {
                        /*if (!csS3Impl.copyObjectWithSingleOperation(sourceBucketName,
                                sourceKey,
                                md5BucketName,
                                md5FilePath))
                        {
                            logger.error("CS handler error: addContentByCopy failed !");
                            return false;
                        }*/
                	    
                	    // 传入主从region，用于写入双写同步信息与实际上传的一致
                        contentMap.put(ContentStorageConstants.S3_REGION_MASTER, CephCSWSManager.getMaterRegion());
                        contentMap.put(ContentStorageConstants.S3_REGION_SLAVE, CephCSWSManager.getSlaveRegion());
                    	
                        // 上传文件结束后，再下载到本地做md5校验
                        if (!CSFileUtil.copyObjectAndCheckMd5(srcBucketName,
                            md5BucketName,
                            sourceKey,
                            md5FilePath,
                            csS3Impl))
                        {
                            // 首次校验不一致时，重试一次
                            if (!CSFileUtil.copyObjectAndCheckMd5(srcBucketName,
                                md5BucketName,
                                sourceKey,
                                md5FilePath,
                                csS3Impl))
                            {
                                logger.error("CS handler error: addContentByCopy failed ! md5 or fileSize is not equals, copyObjectAndCheckMd5 failed.");
                                return false;
                            }
                        }
                        
                        // 新增同步任务（通过修改或者新增同步表记录实现）
                        if (!cephCSWSManager.addSyncCephTask(contentMap))
                        {
                            logger.error("CS handler error: addContentByCopy failed ! md5 or fileSize is not equals, addSyncCephTask failed.");
                            return false;
                        }
                        
                        logger.error("CS handler info: in addContentByCopy(): md5 or fileSize is not equals, uploadObjectAndCheckMd5 and addSyncCephTask finished! ");
                    }
                }
            }
            
            // 插入cs_basic_info是否成功 失败则直接返回失败
            if (!cephCSWSManager.addContentInfo(contentType, contentMap))
            {
                logger.error("CS handler error: addContentByCopy failed ！ addContentInfo error.");
                return false;
            }
            logger.info("addContentByCopy success ！");
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addContentByCopy failed ！ exception = " + e.toString());
            return false;
        }
        
        return true;
    }
    
    /**
     * 向 CS 中增加一个文档(将该文档内容从Ceph中的临时目录copy到CS的Ceph正式目录下,Ceph临时目录的文件不删除)
     */
    public boolean addContentByMove(String contentType, Map<String, Object> contentMap) throws CSException
    {
        logger.info("addContentByMove. contentType:" + contentType + "contentMap:" + contentMap);
        
        try
        {
            // 先调用拷贝文件方法
            if (!addContentByCopy(contentType, contentMap))
            {
                throw new Exception("CS error: addContentByMove failed !");
            }
            
            String sourceKey = contentMap.get(ContentStorageConstants.SOURCE_KEY).toString();
            String tmpBucketName = CephCSWSManager.getCSTempLogicBucket();
            
            // 拷贝完后,删除Ceph临时桶中的源文件
            if (!csS3Impl.delObject(tmpBucketName, sourceKey))
            {
                logger.error("CS handler error: addContentByMove,delete temp sourceKey failed ,sourceKey=" + sourceKey);
                return false;
            }
            
            logger.info("addContentByMove success !");
            return true;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addContentByMove failed ,exception = " + e.toString());
            return false;
        }
    }
    
    /**
     * 查询内容在CS中是否已经存在
     */
    private boolean isContentExist(String contentType, String nodeId, String partNo) throws CSException
    {
        // 验证 contentType,nodeId 和 partNo 对应内容是否已存在
        Map<String, String> contentInfo = cephCSWSManager.getContentInfo(contentType, nodeId, partNo);
        if (MapUtils.isEmpty(contentInfo))
        {
            return false;
        }
        return true;
    }
    
    /**
     * 向 CS 正式目录增加一个文档(该文档内容通过流的形式传入到CS,其中contentMap必须包含文档内容fileContent,文件名fileName,
     * 文件大小fileSize等)
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public boolean addContentByStream(String contentType, Map<String, Object> contentMap) throws CSException
    {
        logger.info("addContentByStream. contentType=" + contentType + "  contentMap=" + contentMap);
        
        try
        {
            // 验证新增时参数是否为空
            if (!CSUtil.checkAddContentByStream(contentType, contentMap))
            {
                throw new IllegalArgumentException(
                        "CS handler error: checkAddContentByStream failed ! input parameters illegal.");
            }
            
            // 建立文件临时目录,文件的处理放在临时目录里
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString();
            String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
            Object objCephDir = contentMap.get(ContentStorageConstants.SOURCE_TEMP_FILE_DIR);
            String tmpCephDir = "";
            
            if (null == objCephDir)
            {
                tmpCephDir = File.separator + ContentStorageConstants.APPFILE + File.separator;
            }
            else
            {
                tmpCephDir = objCephDir.toString() + File.separator;
            }
            
            String fileExtName = CSUtil.getFileExtName(fileName);
            String cephDir = cephCSWSManager.getCephPath(contentType, nodeId, fileExtName);
            
            tmpCephDir += cephDir;
            
            // 建立临时的文件
            String tmpFileName = CSUtil.getUUID() + "." + fileExtName;
            String tmpFilePath = tmpCephDir + tmpFileName;
            
            // 获取传入的文件输入流
            InputStream is = (InputStream) contentMap.get(ContentStorageConstants.CONTENT_STREAM);
            
            if (!CSFileUtil.writeFileByInputStream(is, tmpCephDir, tmpFileName))
            {
                logger.error("CS handler error: addContentByStream.writeFileByInputStream failed!");
                return false;
            }
            
            contentMap.put(ContentStorageConstants.SOURCE_FILE_PATH, tmpFilePath);
            
            if (!addContentByFile(contentType, contentMap))
            {
                logger.error("CS handler error: addContentByStream ,addContentByFile failed.");
                return false;
            }
            
            // 删除临时文件
            if (!CSFileUtil.delFolder(tmpCephDir))
            {
                logger.error("CS handler error: addContentByStream ,delete created tmp file error !");
                //临时目录清理不成功不作为文件上传失败的标记
                //return false;
            }
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addContentByStream failed! exception = " + e.toString());
            return false;
        }
        
        return true;
    }
    
    /**
     * 向 CS 临时目录增加一个文档(该文档内容通过流的形式传入到CS)
     * 存储到临时桶
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public Map<String, String> addTmpContentByStream(InputStream is) throws CSException
    {
        logger.info("addTmpContentByStream.");
        return addTmpContentByStream(is, null);
    }
    
    /**
     * 向 CS 临时目录增加一个文档(该文档内容通过流的形式传入到CS)
     * 存储到临时桶
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public Map<String, String> addTmpContentByStream(InputStream is, String tmpSourceFileDir) throws CSException
    {
        logger.info("addTmpContentByStream.");
        
        try
        {
            // 判断传入流是否为null
            if (null == is)
            {
                throw new IllegalArgumentException(
                        "CS handler error: addTmpContentByStream failed ! input parameters illegal.");
            }
            
            // 建立临时的目录
            String dateDire = new SimpleDateFormat(ContentStorageConstants.DATE_FORMAT_H).format(new Date());
            String tmpCephDir = "";
            if (StringUtils.isEmpty(tmpSourceFileDir))
            {
                tmpCephDir = File.separator + ContentStorageConstants.APPFILE + File.separator + dateDire
                        + File.separator;
            }
            else
            {
                tmpCephDir = tmpSourceFileDir + File.separator + dateDire + File.separator;
            }
            
            // 建立临时的文件
            String tmpFileName = CSUtil.getUUID();
            String tmpFilePath = tmpCephDir + tmpFileName;
            
            if (!CSFileUtil.writeFileByInputStream(is, tmpCephDir, tmpFileName))
            {
                logger.error("CS handler error: method addTmpContentByStream,"
                        + " CSFileUtil.writeFileByInputStream failed!");
                return null;
            }
            
            Map<String, String> resultMap = addTmpContentByFile(tmpFilePath);
            if (null == resultMap)
            {
                logger.error("CS handler error: addTmpContentByStream failed ! resultMap is null.");
                return null;
            }
            
            if (StringUtils.isEmpty(resultMap.get(ContentStorageConstants.SOURCE_KEY)))
            {
                logger.error("CS handler error: addTmpContentByStream failed ! result SOURCE_KEY is null.");
                return null;
            }
            
            // 删除临时目录
            if (!CSFileUtil.delFolder(tmpCephDir))
            {
                logger.error("CS handler error: addContentByStream ,delete created tmp file failed!");
                //临时目录清理不成功不作为文件上传失败的标记
                //return null;
            }
            
            return resultMap;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addTmpContentByStream failed! exception = " + e.toString());
            return null;
        }
        
    }
    
    /**
     * 向 CS 中增加一个正式文件(该文件内容通过流二进制数组的形式传入到CS,其中contentMap必须包含文件内容contentBytes,文件名fileName,文件大小fileSize等)
     * <功能详细描述>
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public boolean addContentByBytes(String contentType, Map<String, Object> contentMap) throws CSException
    {
        // 对输入进行校验
        byte[] bytes = (byte[]) contentMap.get(ContentStorageConstants.CONTENT_BYTES);
        if (bytes == null || bytes.length <= 0)
        {
            throw new IllegalArgumentException("CS handler error: addContentByBytes failed ! InputStream is null.");
        }
        
        try
        {
            // 将二进制数组转换成文件流
            InputStream is = new ByteArrayInputStream(bytes);
            contentMap.remove(ContentStorageConstants.CONTENT_BYTES);
            contentMap.put(ContentStorageConstants.CONTENT_STREAM, is);
            
            // 调用以流的方式增加文件的方法,将文件添加到CS
            return addContentByStream(contentType, contentMap);
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addContentByBytes failed ! exception = " + e.toString());
            return false;
        }
    }
    
    /**
     * 向 CS 中增加一个临时文件(该文件内容通过流二进制数组的形式传入到CS,其中contentMap必须包含文件内容contentBytes,文件名fileName,文件大小fileSize等)
     * 存入临时桶
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public Map<String, String> addTmpContentByBytes(byte[] bytes) throws CSException
    {
        // 对输入进行校验
        if (bytes == null || bytes.length <= 0)
        {
            throw new IllegalArgumentException("CS handler error: addTmpContentByBytes failed! bytes is null.");
        }
        
        // 将二进制数组转换成文件流
        InputStream is = new ByteArrayInputStream(bytes);
        
        // 调用以流的方式增加文件的方法,将文件添加到CS
        return addTmpContentByStream(is);
    }
    
    /**
     * 向 CS 中增加一个文件(该文件内容通过文件路径的形式传入到CS,其中contentMap必须包含文件内容contentBytes,文件名fileName,文件大小fileSize等)
     * 从本地上传到ceph正式桶
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public boolean addContentByFile(String contentType, Map<String, Object> contentMap) throws CSException
    {
        logger.info("addContentByFile. contentType=" + contentType + "  contentMap=" + contentMap);
        
        try
        {
            // 验证新增时参数是否为空
            if (!CSUtil.checkAddContentByFile(contentType, contentMap))
            {
                throw new IllegalArgumentException("CS handler error: addContentByFile failed! some args are blank !"
                        + " contentType=" + contentType + "  contentMap=" + contentMap);
            }
            
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString();
            String partNo = contentMap.get(ContentStorageConstants.PART_NO).toString();
            String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
            String sourceFilePath = contentMap.get(ContentStorageConstants.SOURCE_FILE_PATH).toString();
            
            // 验证CS内容是否存在
            if (isContentExist(contentType, nodeId, partNo))
            {
                logger.error("CS handler error: addContentByFile failed! The nodeId already exists. nodeId=" + nodeId
                        + " ,partNo=" + partNo + " ,contentType=" + contentType);
                return false;
            }
            
            // 生成Ceph存储的文件名
            String cephFilePath = getCephFilePath(fileName, contentType, nodeId);
            
            // 获取ceph存储 bucketName
            String bucketName = CephCSWSManager.getCSRootLogicBucket();
            contentMap.put(ContentStorageConstants.BUCKET_NAME, bucketName);
            
            // 根据md5和fileSize，检测生成文件内容是否存在
            String md5 = CSFileUtil.getMD5Hex(sourceFilePath);
            String fileSize = CSFileUtil.getFileSize(sourceFilePath);
            if (StringUtils.isEmpty(md5) || StringUtils.isEmpty(fileSize))
            {
                logger.error("CS handler error : the md5 or fileSize is null ! md5 :" + md5 + " ,fileSize :" + fileSize);
                return false;
            }
            
            contentMap.put(ContentStorageConstants.MD5, md5);
            contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
            
            Md5Entity md5Entity = cephCSWSManager.getMd5FilePath(md5, fileSize);
            
            // 文件在MD5表的文件路径是否存在，不存在就上传，存在就检查ceph中是否还存在
            if (null == md5Entity)
            {
                // ceph存储元数据写入md5和文件大小
                ObjectMetadata metaData = new ObjectMetadata();
                Map<String, String> map = new HashMap<String, String>();
                map.put(ContentStorageConstants.S3_MD5, md5);
                map.put(ContentStorageConstants.S3_FILESIZE, fileSize);
                metaData.setUserMetadata(map);
                metaData.setContentLength(Long.parseLong(fileSize));
                
                // 传入主从region，用于写入双写同步信息与实际上传的一致
                contentMap.put(ContentStorageConstants.S3_REGION_MASTER, CephCSWSManager.getMaterRegion());
                contentMap.put(ContentStorageConstants.S3_REGION_SLAVE, CephCSWSManager.getSlaveRegion());
                
                /*if (StringUtils.isEmpty(csS3Impl.uploadObject(sourceFilePath, 
                		bucketName, 
                		cephFilePath, 
                		metaData)))
                {
                    logger.error("CS handler error: addContentByFile failed ! uploadObject failed.");
                    return false;
                }*/
                
                // 上传文件结束后，再下载到本地做md5校验
                if (!CSFileUtil.uploadObjectAndCheckMd5(bucketName, sourceFilePath, cephFilePath, metaData, csS3Impl))
                {
                    // 首次失败后，重试一次
                    if (!CSFileUtil.uploadObjectAndCheckMd5(bucketName, sourceFilePath, cephFilePath, metaData, csS3Impl))
                    {
                        logger.error("CS handler error: addContentByFile failed ! uploadObjectAndCheckMd5 failed.");
                        return false;
                    }
                }
                
                // CS中没有文件实体,保留新生成的文件,并将路径保存到数据库表
                contentMap.put(ContentStorageConstants.FILE_PATH, cephFilePath);
                
                // 向cs_md5_path_info表中保存数据
                if (!cephCSWSManager.addMd5FilePath(contentMap))
                {
                    logger.error("CS handler error: addContentByFile failed ! addMd5FilePath to DB failed.");
                    return false;
                }
            }
            else
            {
                String md5FilePath = md5Entity.getFilePath();
                String md5BucketName = md5Entity.getBucketName();
                
                // CS中已经存在文件实体,直接将该路径作为新的文件路径
                contentMap.put(ContentStorageConstants.FILE_PATH, md5FilePath);
                contentMap.put(ContentStorageConstants.BUCKET_NAME, md5BucketName);
                
                // md5中文件路径存在的情况下,根据文件的key获取ceph中文件实体是否还存在，不存在则重新上传
                ObjectMetadata metaData = csS3Impl.getObjectMetaData(md5BucketName, md5FilePath);
                if (null == metaData)
                {
                    ObjectMetadata newMetaData = new ObjectMetadata();
                    Map<String, String> map = new HashMap<String, String>();
                    map.put(ContentStorageConstants.S3_MD5, md5);
                    map.put(ContentStorageConstants.S3_FILESIZE, fileSize);
                    newMetaData.setUserMetadata(map);
                    newMetaData.setContentLength(Long.parseLong(fileSize));
                    
					/*if (StringUtils.isEmpty(csS3Impl.uploadObject(sourceFilePath, 
							md5BucketName, 
							md5FilePath,
							newMetaData))) 
					{
                        logger.error("CS handler error: addContentByFile failed ! uploadObject failed.");
                        return false;
                    }*/
                    
                    // 传入主从region，用于写入双写同步信息与实际上传的一致
                    contentMap.put(ContentStorageConstants.S3_REGION_MASTER, CephCSWSManager.getMaterRegion());
                    contentMap.put(ContentStorageConstants.S3_REGION_SLAVE, CephCSWSManager.getSlaveRegion());
	                
                    // 上传文件结束后，再下载到本地做md5校验
                    if (!CSFileUtil.uploadObjectAndCheckMd5(md5BucketName, sourceFilePath, md5FilePath, metaData, csS3Impl))
                    {
                        // 首次失败后，重试一次
                        if (!CSFileUtil.uploadObjectAndCheckMd5(md5BucketName, sourceFilePath, md5FilePath, metaData, csS3Impl))
                        {
                            logger.error("CS handler error: addContentByFile failed ! metaData is null, uploadObjectAndCheckMd5 failed.");
                            return false;
                        }
                    }

                    // 新增同步任务（通过修改或者新增同步表记录实现）
                    if (!cephCSWSManager.addSyncCephTask(contentMap))
                    {
                        logger.error("CS handler error: addContentByFile failed ! metaData is null, addSyncCephTask failed.");
                        return false;
                    }
                    
                    logger.error("CS handler info: in addContentByFile(): metaData is null, uploadObjectAndCheckMd5 and addSyncCephTask finished! ");
                }
                else
                {
                    // 判断Ceph上md5和fileSize是否相同,不同也上传
                	if (!StringUtils.equals(md5, metaData.getUserMetaDataOf(ContentStorageConstants.S3_MD5))
                			|| !StringUtils.equals(fileSize, metaData.getUserMetaDataOf(ContentStorageConstants.S3_FILESIZE)))
                    {
                        ObjectMetadata newMetaData = new ObjectMetadata();
                        Map<String, String> map = new HashMap<String, String>();
                        map.put(ContentStorageConstants.S3_MD5, md5);
                        map.put(ContentStorageConstants.S3_FILESIZE, fileSize);
                        newMetaData.setUserMetadata(map);
                        newMetaData.setContentLength(Long.parseLong(fileSize));
                        
                        /*if (StringUtils.isEmpty(csS3Impl.uploadObject(sourceFilePath, 
                        		md5BucketName, 
                        		md5FilePath, 
                        		newMetaData)))
                        {
                            logger.error("CS handler error: addContentByFile failed ! uploadObject failed !");
                            return false;
                        }*/
                        
                        // 传入主从region，用于写入双写同步信息与实际上传的一致
                        contentMap.put(ContentStorageConstants.S3_REGION_MASTER, CephCSWSManager.getMaterRegion());
                        contentMap.put(ContentStorageConstants.S3_REGION_SLAVE, CephCSWSManager.getSlaveRegion());
    	                
                        // 上传文件结束后，再下载到本地做md5校验
                        if (!CSFileUtil.uploadObjectAndCheckMd5(md5BucketName, sourceFilePath, md5FilePath, metaData, csS3Impl))
                        {
                            // 首次失败后，重试一次
                            if (!CSFileUtil.uploadObjectAndCheckMd5(md5BucketName, sourceFilePath, md5FilePath, metaData, csS3Impl))
                            {
                                logger.error("CS handler error: addContentByFile failed ! md5 or fileSize is not equals, uploadObjectAndCheckMd5 failed.");
                                return false;
                            }
                        }

                        // 新增同步任务（通过修改或者新增同步表记录实现）
                        if (!cephCSWSManager.addSyncCephTask(contentMap))
                        {
                            logger.error("CS handler error: addContentByFile failed ! md5 or fileSize is not equals, addSyncCephTask failed.");
                            return false;
                        }
                        
                        logger.error("CS handler info: in addContentByFile(): md5 or fileSize is not equals, uploadObjectAndCheckMd5 and addSyncCephTask finished! ");
                    }
                }
            }
            
            // 写入cs_basic_info是否成功 失败则直接返回失败
            if (!cephCSWSManager.addContentInfo(contentType, contentMap))
            {
                logger.error("CS handler error: addContentByFile failed ! addContentInfo to DB error.");
                return false;
            }
            logger.info("addContentByFile success !");
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addContentByFile failed ! exception = " + e.toString());
            return false;
        }
        
        return true;
    }
    
    /**
     * 获取ceph存储路径
     * @return
     */
    private String getCephFilePath(String fileName, String contentType, String nodeId)
    {
    	//对输入进行检验
        if (StringUtils.isBlank(fileName) || StringUtils.isBlank(contentType) || StringUtils.isBlank(nodeId))
        {
            logger.error("CS handler error: getCephFilePath failed ! some args are empty. fileName:" 
            		+ fileName +", contentType:" + contentType + ", nodeId:" + nodeId);
            return "";
        }
    	
    	try 
    	{
    		// 生成Ceph存储的文件名
            String cephFileName = CSUtil.getUUID();
            // 如果有扩展名,则加上扩展名
            String fileExtName = CSUtil.getFileExtName(fileName);
            
            // 获取Ceph的路径,如果文件有扩展名，则会以文件的类型再分目录
            String cephFileDir = cephCSWSManager.getCephPath(contentType, nodeId, fileExtName);
            String cephFilePath = cephFileDir + cephFileName + "." + fileExtName;
            cephFilePath = CSUtil.winString2Linux(cephFilePath);
            
            return cephFilePath;
		} 
    	catch (Exception e) 
    	{
    		logger.error("CS handler error: getCephFilePath failed ! exception = " + e.toString());
            return "";
		}
    }
    
    /**
     * 上传实体+校验md5
     * @return
     */
    /*private boolean uploadObjectAndCheckMd5(String md5, String fileSize, String bucketName, String srcFilePath, 
    		String desFilePath, String localFileDir, ObjectMetadata metaData)
    {
    	//对输入进行检验	md5和fileSize仅作为最后校验用，传空不会造成异常
        if (StringUtils.isBlank(bucketName) || StringUtils.isBlank(srcFilePath) 
        		|| StringUtils.isBlank(desFilePath) || null == metaData)
        {
            logger.error("CS handler error: uploadObjectAndCheckMd5 failed ! some args are empty. bucketName:" 
            		+ bucketName +", srcFilePath:" + srcFilePath + ", desFilePath:" + desFilePath);
            return false;
        }
    	
    	try 
    	{
    		if (StringUtils.isEmpty(csS3Impl.uploadObject(srcFilePath, 
    				bucketName, 
    				desFilePath,
    				metaData))) 
        	{
                logger.error("CS handler error: uploadObjectAndCheckMd5 failed ! uploadObject failed.");
                return false;
            }
            
            if(!checkUploadObject(md5, fileSize, bucketName, desFilePath, localFileDir))
            {
            	logger.error("CS handler error: uploadObjectAndCheckMd5 failed ! checkUploadObject failed.");
            	return false;
            }
            
            return true;
		} 
    	catch (Exception e) 
    	{
    		logger.error("CS handler error: uploadObjectAndCheckMd5 failed ! exception = " + e.toString());
            return false;
		}
    }*/
    
    /**
     * 向 CS中增加一个临时文件(该文件内容通过文件路径的形式传入到CS,其中contentMap必须包含文件内容contentBytes,文件名fileName,文件大小fileSize等)
     * 文件存入临时桶
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public Map<String, String> addTmpContentByFile(String sourceFilePath) throws CSException
    {
        logger.info("addTmpContentByFile. filePath = " + sourceFilePath);
        try
        {
            // 判断路径是否为空
            if (null == sourceFilePath || StringUtils.isEmpty(sourceFilePath))
            {
                throw new IllegalArgumentException(
                        "CS handler error: addTmpContentByFile failed! sourceFilePath is null or empty.");
            }
            
            // 判断源文件是否存在
            File tmpFile = new File(sourceFilePath);
            if (!tmpFile.exists())
            {
                throw new CSException("CS handler error: addTmpContentByFile failed! source file does not exist.");
            }
            
            // 生成Ceph存储的文件名
            String tmpCephFileName = CSUtil.getUUID();
            
            // 加上文件后缀
            if (sourceFilePath.lastIndexOf(".") > 0)
            {
                tmpCephFileName += sourceFilePath.substring(sourceFilePath.lastIndexOf("."), sourceFilePath.length());
            }
            
            // 加上年月日目录
            String tmpCephFilePath = cephCSWSManager.getTmpCephPath() + tmpCephFileName;
            tmpCephFilePath = CSUtil.winString2Linux(tmpCephFilePath);
            
            // 获取ceph存储临时桶
            String bucketName = CephCSWSManager.getCSTempLogicBucket();
            
            // 获取文件md5及fileSize
            String md5 = CSFileUtil.getMD5Hex(sourceFilePath);
            String fileSize = CSFileUtil.getFileSize(sourceFilePath);
            if (StringUtils.isEmpty(md5) || StringUtils.isEmpty(fileSize))
            {
                logger.error("CS handler error: the md5 or fileSize is null! md5:" + md5 + " ,fileSize:" + fileSize);
                return null;
            }
            
            ObjectMetadata metaData = new ObjectMetadata();
            Map<String, String> map = new HashMap<String, String>();
            map.put(ContentStorageConstants.S3_MD5, md5);
            map.put(ContentStorageConstants.S3_FILESIZE, fileSize);
            metaData.setUserMetadata(map);
            metaData.setContentLength(Long.parseLong(fileSize));
            
            /*if (StringUtils.isEmpty(csS3Impl.uploadObject(
            		sourceFilePath, 
            		bucketName, 
            		tmpCephFilePath, 
            		metaData)))
            {
                logger.error("CS handler error: addTmpContentByFile failed ! uploadObject failed.");
                return null;
            }*/
            
            // 上传文件结束后，再下载到本地做md5校验
            if (!CSFileUtil.uploadObjectAndCheckMd5(bucketName, sourceFilePath, tmpCephFilePath, metaData, csS3Impl))
            {
                // 首次失败后，重试一次
                if (!CSFileUtil.uploadObjectAndCheckMd5(bucketName, sourceFilePath, tmpCephFilePath, metaData, csS3Impl))
                {
                    logger.error("CS handler error: addTmpContentByFile failed ! uploadObjectAndCheckMd5 failed.");
                    return null;
                }
            }
            
            Map<String, String> resultMap = new HashMap<String, String>();
            resultMap.put(ContentStorageConstants.BUCKET_NAME, bucketName);
            resultMap.put(ContentStorageConstants.SOURCE_KEY, tmpCephFilePath);
            resultMap.put(ContentStorageConstants.MD5, md5);
            resultMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
            
            return resultMap;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addTmpContentByFile failed ! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 从CS 中获取一篇文档的全部信息（不读取内容）， 只从数据库表中查询 partNo,filename等信息
     * @param nodeId
     * @return List 中每一项为一个partNo的信息，每个partNo的信息以MAP的形式保存
     * @throws CSException
     */
    
    public List<Map<String, String>> getContentInfos(String contentType, String nodeId) throws CSException
    {
        // 对输入参数进行较验
        // 2017-07-05 因nodeid唯一，不需要contentType，删除StringUtils.isBlank(contentType) || 
        if (StringUtils.isBlank(nodeId))
        {
            throw new IllegalArgumentException("CS handler error: The input contentType or nodeId is null.");
        }
        
        try
        {
            List<Map<String, String>> contentInfos = cephCSWSManager.getContentInfos(contentType.trim(),
                    nodeId.trim(),
                    null,
                    null);
            
            if (CollectionUtils.isEmpty(contentInfos))
            {
                logger.info("CS handler error: getContentInfos failed ! The contentInfos is null.");
                return Collections.emptyList();
            }
            
            return contentInfos;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: getContentInfos failed ! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 查询单个文档元信息
     * @param contentType 文件MID
     * @param nodeId
     * @param partNo
     * @return 文件实体bean
     */
    public Map<String, String> getContentInfo(String contentType, String nodeId, String partNo) throws CSException
    {
        // 2017-07-05 因nodeid不会重复，经刘星、朱玉磊讨论删除对contentType的校验。删除StringUtils.isBlank(contentType) || ，即去掉contentType的校验 
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new CSException("CS handler error: getContentInfo failed! The input param is null. contentType(Optional):"
                    + contentType + " ,nodeId:" + nodeId + " ,partNo" + partNo);
        }
        
        return cephCSWSManager.getContentInfo(contentType, nodeId, partNo);
    }
    
    /**
     * 批量读取多个nodeid的文件信息
     * @param contentType 文件类型
     * @param nodeIds 文件ID,必选
     * @param partNo 文件partNo
     * @return 
     */
    public Map<String, Map<String, String>> getContentInfos(String contentType, List<String> nodeIds, String partNo)
        throws CSException
    {
        // 2017-07-05 因nodeid不会重复，经刘星、朱玉磊讨论删除对contentType的校验。删除StringUtils.isBlank(contentType) || ，即去掉contentType的校验
        if (nodeIds == null || nodeIds.isEmpty() || StringUtils.isBlank(partNo))
        {
            throw new CSException("CS handler error: getContentInfos failed! The input param is null.contentType(Optional):"
                    + contentType + " ,nodeId:" + nodeIds + " ,partNo" + partNo);
        }
        
        try
        {
            Map<String, List<Map<String, String>>> multiNodeContents = cephCSWSManager.getMultiNodesContentInfos(contentType,
                    nodeIds,
                    partNo);
            
            // 对查询结果进行封装，每个nodeid,partNo只对应一个内容，即内层list大小为一，对外直接以单个MAP表示
            Map<String, Map<String, String>> multiNodeContent = new HashMap<String, Map<String, String>>();
            Set<String> nodeIdSet = multiNodeContents.keySet();
            for (String nodeId : nodeIdSet)
            {
                List<Map<String, String>> contentInfos = multiNodeContents.get(nodeId);
                if (contentInfos == null || contentInfos.size() != 1)
                {
                    logger.error("CS handler error: Content info get from data base empty or not unique. contentType = "
                            + contentType + " ,nodeId = " + nodeId + " ,partNo =" + partNo);
                    continue;
                }
                multiNodeContent.put(nodeId, contentInfos.get(0));
            }
            
            return multiNodeContent;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: getContentInfos failed! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 通过多个nodeId查询 cs信息
     * @param contentType	文件类型
     * @param nodeIds	多个nodeId
     * @return
     * @throws CSException
     */
    public Map<String, List<Map<String, String>>> getContentInfos(String contentType, List<String> nodeIds)
        throws CSException
    {
        // 因nodeid唯一，删除contenttype，校验StringUtils.isBlank(contentType) || 
        if (CollectionUtils.isEmpty(nodeIds))
        {
            throw new CSException("CS handler error: getContentInfos failed! The input param is null.");
        }
        
        return cephCSWSManager.getMultiNodesContentInfos(contentType, nodeIds, "");
    }
    
    /**
     * 查询cs信息
     * @param contentType	文件类型
     * @param nodeId	文件nodeId
     * @param partNoPrefix	文件partNo前缀
     * @return
     * @throws CSException
     */
    public List<Map<String, String>> getContentInfosByPartNoType(String contentType, String nodeId, String partNoPrefix)
        throws CSException
    {
        // 因nodeid唯一，删除contenttype，校验StringUtils.isBlank(contentType) || 
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNoPrefix))
        {
            throw new CSException("CS handler error: getContentInfosByPartNoType failed ! The input param is null.");
        }
        
        return cephCSWSManager.getContentInfos(contentType, nodeId, partNoPrefix, null);
    }
    
    /**
     * 根据 nodeId 及 partNo 删除一项中的一个文档
     * @param nodeId
     *            应用ID
     * @param partNo
     *            文档ID
     * @return 返回删除是否成功
     */
    public boolean delContent(String contentType, String nodeId, String partNo)
    {
        logger.info("delContent. contentType:" + contentType + " ,nodeId:" + nodeId + " ,partNo:" + partNo);
        
        // 对输入参数进行非空判断
        if (StringUtils.isBlank(contentType) || StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new IllegalArgumentException(
                    "CS handler error: delContent failed! The input param is null.  contentType:" + contentType
                            + " ,nodeId = " + nodeId + ", partNo = " + partNo);
        }
        
        try
        {
            if (!cephCSWSManager.updateContentToDelete(contentType.trim(), nodeId.trim(), partNo.trim()))
            {
                logger.error("CS handler error: delContent failed! Failed to delete content from DB.");
                return false;
            }
            
            logger.info("delete content succeed!");
            return true;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: delContent failed! exception = " + e.toString());
            return false;
        }
    }
    
    /**
     * 根据 nodeId 删除该 contentId 下的所有对应的partNo的文档
     * @param nodeId
     *            应用ID
     * @return 删除是否成功
     * @throws CSException 
     */
    public boolean delContents(String contentType, String nodeId) throws CSException
    {
        logger.info("delContents. contentType:" + contentType + " nodeId:" + nodeId);
        
        // 对输入参数进行非空判断
        if (StringUtils.isBlank(contentType) || StringUtils.isBlank(nodeId))
        {
            throw new IllegalArgumentException(
                    "CS handler error: delContents failed! The input param is null.the contentType:" + contentType
                            + " nodeId = " + nodeId);
        }
        
        try
        {
            if (!cephCSWSManager.updateContentsToDelete(contentType, nodeId, null))
            {
                logger.error("CS handler error: delContents failed ! error delete all content from DB.");
                return false;
            }
            
            logger.info("delContents success !");
            return true;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: delContents failed ! exception = " + e.toString());
            return false;
        }
    }
    
    /**
     * 根据 nodeId 及 partNos 删除一项中的多个文档
     * @param nodeId
     *            应用ID
     * @param partNos
     *            文档ID集合
     * @return
     */
    public boolean delContents(String contentType, String nodeId, List<String> partNos) throws CSException
    {
        logger.info("in delContents() the contentType: " + contentType + " ,nodeId:" + nodeId + ", partNos:" + partNos);
        
        // 对输入参数进行非空判断
        if (StringUtils.isBlank(contentType) || StringUtils.isBlank(nodeId) || null == partNos || partNos.isEmpty())
        {
            throw new IllegalArgumentException(
                    "CS handler error: delContents failed! The input param is null. contentType: " + contentType
                            + " ,nodeId = " + nodeId + ", partNos = " + partNos);
        }
        
        try
        {
            if (!cephCSWSManager.updateContentsToDelete(contentType.trim(), nodeId.trim(), partNos))
            {
                logger.error("CS handler error: delContents failed to delete contents from DB. nodeId = " + nodeId
                        + " ,partNos = " + partNos);
                return false;
            }
            
            logger.info("Delete contents succeed!");
            return true;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: delContents failed! exception = " + e.toString());
            return false;
        }
    }
    
    /**
     * 向cs中增加一个文件,该文件在cs中已存在.只需要变换其contentType,nodeId,partNo
     * <功能详细描述>
     * @param oldNodeId
     * @param oldPartNo
     * @param newPartNo
     * @return
     * @throws CSException 
     * @see [类、类#方法、类#成员]
     */
    public boolean addContentByExistContent(String oldContentType, String newContentType, String oldNodeId,
        String newNodeId, String oldPartNo, String newPartNo, String fileSource) throws CSException
    {
        logger.info("addContentByExistContent. oldContentType = " + oldContentType + " ,newContentType="
                + newContentType + ", oldNodeId = " + oldNodeId + " ,newNodeId=" + newNodeId + ", oldPartNo = "
                + oldPartNo + ", newPartNo = " + newPartNo + " ,fileSource" + fileSource);
        
        // 对输入参数进行校验
        if (StringUtils.isBlank(oldContentType) || StringUtils.isBlank(newContentType)
                || StringUtils.isBlank(oldNodeId) || StringUtils.isBlank(newNodeId) || StringUtils.isBlank(oldPartNo)
                || StringUtils.isBlank(newPartNo) || StringUtils.isBlank(fileSource))
        {
            throw new IllegalArgumentException(
                    "CS handler error: addContentByExistContent failed! the input parameter illegal.oldContentType = "
                            + oldContentType + " ,newContentType=" + newContentType + ", oldNodeId = " + oldNodeId
                            + " ,newNodeId=" + newNodeId + ", oldPartNo = " + oldPartNo + ", newPartNo = " + newPartNo
                            + " ,fileSource" + fileSource);
        }
        
        try
        {
            // 验证要添加的内容是否已经在CS中存在
            if (isContentExist(newContentType, newNodeId, newPartNo))
            {
                logger.error("CS handler error: addContentByExistContent failed ! "
                        + "The content to add already exists .contentType =" + newContentType + " ,nodeId=" + newNodeId
                        + " ,partNo=" + newPartNo);
                return false;
            }
            
            // 判断旧的文件是否已存在
            Map<String, String> contentInfoMap = cephCSWSManager.getContentInfo(oldContentType, oldNodeId, oldPartNo);
            if (MapUtils.isEmpty(contentInfoMap))
            {
                logger.error("CS handler error: addContentByExistContent failed! The old content does not exist.");
                return false;
            }
            
            // 构造新的ContentInfo
            Map<String, Object> newContentInfo = new HashMap<String, Object>();
            newContentInfo.put(ContentStorageConstants.CONTENT_TYPE, newContentType);
            newContentInfo.put(ContentStorageConstants.BUCKET_NAME,
                    contentInfoMap.get(ContentStorageConstants.BUCKET_NAME));
            newContentInfo.put(ContentStorageConstants.FILE_NAME, 
            		contentInfoMap.get(ContentStorageConstants.FILE_NAME));
            newContentInfo.put(ContentStorageConstants.FILE_PATH, 
            		contentInfoMap.get(ContentStorageConstants.FILE_PATH));
            newContentInfo.put(ContentStorageConstants.FILE_SIZE, 
            		contentInfoMap.get(ContentStorageConstants.FILE_SIZE));
            newContentInfo.put(ContentStorageConstants.NODE_ID, newNodeId);
            newContentInfo.put(ContentStorageConstants.PART_NO, newPartNo);
            newContentInfo.put(ContentStorageConstants.FILE_SOURCE, fileSource);
            
            // 设置文件格式，爬虫文件索引
            newContentInfo.put(ContentStorageConstants.FILE_TYPE, 
            		contentInfoMap.get(ContentStorageConstants.FILE_TYPE));
            
            if (!cephCSWSManager.addContentInfo(newContentType, newContentInfo))
            {
                logger.error("CS handler error: addContentByExistContent failed ! addContentInfo error.");
                return false;
            }
            
            logger.info("addContentByExistContent success !");
            return true;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addContentByExistContent failed ! addContentInfo error.");
            return false;
        }
    }
    
    /**
     * 从临时桶向正式桶批量copy文件
     * <功能详细描述>
     * @param contentType
     * @param contentMapList
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public boolean addMultiContentsByCopy(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        logger.info("addMultiContentsByCopy. contentType:" + contentType + " list:" + contentMapList);
        
        try
        {
            // 检测参数合法性及内容是否存在
            if (!checkAddMultiContentsByCopy(contentType, contentMapList))
            {
                throw new IllegalArgumentException(
                        "CS handler error: addMultiContentsByCopy failed ! the input parameter is illegal.");
            }
            
            // 批量添加md5
            for (int i = 0; i < contentMapList.size(); i++)
            {
                if (!addMultiMD5File(contentType, contentMapList.get(i)))
                {
                    return false;
                }
            }
            
            // 批量添加cs
            if (!cephCSWSManager.addMultiContentInfo(contentType, contentMapList))
            {
                logger.error("CS handler error: addMultiContentsByCopy failed! add addMultiContentInfo to DB error.");
                return false;
            }
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addMultiContentsByCopy failed! exception = " + e.toString());
            return false;
        }
        
        logger.info("addMultiContentsByCopy success !");
        return true;
    }
    
    /**
     * 从临时桶复制文件到正式桶 并写入md5表
     * @return
     * @throws CSException
     */
    private boolean addMultiMD5File(String contentType, Map<String, Object> contentMap) throws CSException
    {
        String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString();
        String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
        String sourceKey = contentMap.get(ContentStorageConstants.SOURCE_KEY).toString();
        
        String sourceBucketName = CephCSWSManager.getCSTempLogicBucket();
        ObjectMetadata tmpMetaData = null;
        
        try
        {
            tmpMetaData = csS3Impl.getObjectMetaData(sourceBucketName, sourceKey);
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addMultiContentsByCopy failed ! getObjectMetaData exception : ", e);
            return false;
        }
        
        if (null == tmpMetaData)
        {
            logger.error("CS handler error: addMultiContentsByCopy failed ! tmpMetaData is null.");
            return false;
        }
        
        String md5 = tmpMetaData.getUserMetaDataOf(ContentStorageConstants.S3_MD5).toString();
        String fileSize = tmpMetaData.getUserMetaDataOf(ContentStorageConstants.S3_FILESIZE).toString();
        
        if (StringUtils.isEmpty(md5) || StringUtils.isEmpty(fileSize))
        {
            logger.error("CS handler error : addMultiContentsByCopy failed !"
                    + " addMultiMD5File error, md5 or fileSize is null !");
            return false;
        }
        
        // 生成Ceph存储的文件名
        String cephFilePath = getCephFilePath(fileName, contentType, nodeId);
        contentMap.put(ContentStorageConstants.FILE_PATH, cephFilePath);
        
        // 获取ceph存储bucketName
        String sourceBucket = CephCSWSManager.getCSTempLogicBucket();
        String destinationBucket = CephCSWSManager.getCSRootLogicBucket();
        contentMap.put(ContentStorageConstants.BUCKET_NAME, destinationBucket);
        
        // 根据MD5和fileSize,检测生成文件内容是否存在
        contentMap.put(ContentStorageConstants.MD5, md5);
        contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
        
        Md5Entity md5Entity = cephCSWSManager.getMd5FilePath(md5, fileSize);
        
        // 文件在MD5表的文件路径是否存在，不存在就上传，存在就检查ceph中是否还存在
        if (null == md5Entity)
        {
            /*if (!csS3Impl.copyObjectWithSingleOperation(
            		sourceBucket, 
            		sourceKey, 
            		destinationBucket, 
            		cephFilePath))
            {
                logger.error("CS handler error: addMultiContentsByCopy failed!"
                        + " copyObjectWithSingleOperation error.");
                return false;
            }*/
        	
        	// 传入主从region，用于写入双写同步信息与实际上传的一致
            contentMap.put(ContentStorageConstants.S3_REGION_MASTER, CephCSWSManager.getMaterRegion());
            contentMap.put(ContentStorageConstants.S3_REGION_SLAVE, CephCSWSManager.getSlaveRegion());
            
            // 上传文件结束后，再下载到本地做md5校验
            if (!CSFileUtil.copyObjectAndCheckMd5(sourceBucket, destinationBucket, sourceKey, cephFilePath, csS3Impl))
            {
                // 首次校验不一致时，重试一次
                if (!CSFileUtil.copyObjectAndCheckMd5(sourceBucket, destinationBucket, sourceKey, cephFilePath, csS3Impl))
                {
                    logger.error("CS handler error: addMultiContentsByCopy failed ! copyObjectAndCheckMd5 failed.");
                    return false;
                }
            }
            
            // Ceph中没有文件实体,保留新生成的文件,并将路径保存到数据库表
            contentMap.put(ContentStorageConstants.FILE_PATH, cephFilePath);
            
            // 向cs_md5_path_info表中保存数据
            if (!cephCSWSManager.addMd5FilePath(contentMap))
            {
                logger.error("CS handler error: addMultiMD5 failed ! addMd5FilePath to DB failed.");
                return false;
            }
        }
        else
        {
            contentMap.put(ContentStorageConstants.FILE_PATH, md5Entity.getFilePath());
            contentMap.put(ContentStorageConstants.BUCKET_NAME, md5Entity.getBucketName());
        }
        
        return true;
    }
    
    /**
     * 以inputStream方式批量添加
     * 
     * @param contentType
     *            文档类型
     * @param contentMapList
     *            相关信息的集合
     * @return true表示成功， false表示失败
     * @author li.xiong
     * @throws CSException
     */
    public boolean addMultiContentsByBytes(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        logger.info("addMultiContentsByBytes. contentType:" + contentType + " list:" + contentMapList);
        
        try
        {
            // 检测参数合法性及内容是否存在
            if (!checkAddMultiContentsByBytes(contentType, contentMapList))
            {
                throw new IllegalArgumentException(
                        "CS handler error: addMultiContentsByBytes failed ! the input parameter is illegal.");
            }
            
            // 批量添加md5
            for (int i = 0; i < contentMapList.size(); i++)
            {
                if (!addMultiMD5Bytes(contentType, contentMapList.get(i)))
                {
                    return false;
                }
            }
            
            // 批量添加cs
            if (!cephCSWSManager.addMultiContentInfo(contentType, contentMapList))
            {
                logger.error("CS handler error: addMultiContentsByBytes. Failed to add addMultiContentInfo to DB.");
                return false;
            }
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addMultiContentsByBytes failed! exception = " + e.toString());
            return false;
        }
        
        logger.info("addMultiContentsByBytes success !");
        return true;
        
    }
    
    /**
     * 通过bytes将文件上传到正式桶 并写入md5表
     * @return
     * @throws CSException
     */
    private boolean addMultiMD5Bytes(String contentType, Map<String, Object> contentMap) throws CSException
    {
        String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString();
        String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
        
        byte[] bytes = (byte[]) contentMap.get(ContentStorageConstants.CONTENT_BYTES);
        
        // 将二进制数组转换成文件流
        InputStream is = new ByteArrayInputStream(bytes);
        
        Object objCephDir = contentMap.get(ContentStorageConstants.SOURCE_TEMP_FILE_DIR);
        String tmpCephDir = "";
        if (null == objCephDir)
        {
            tmpCephDir = File.separator + ContentStorageConstants.APPFILE + File.separator;
        }
        else
        {
            tmpCephDir = objCephDir.toString() + File.separator;
        }
        
        String fileExtName = CSUtil.getFileExtName(fileName);
        String cephDir = cephCSWSManager.getCephPath(contentType, nodeId, fileExtName);
        
        tmpCephDir += cephDir;
        
        // 建立临时文件
        String tmpFileName = CSUtil.getUUID() + "." + fileExtName;
        String tmpFilePath = tmpCephDir + tmpFileName;
        
        if (!CSFileUtil.writeFileByInputStream(is, tmpCephDir, tmpFileName))
        {
            logger.error("CS handler error: addContentByStream.writeFileByInputStream failed!");
            return false;
        }
        
        contentMap.put(ContentStorageConstants.SOURCE_FILE_PATH, tmpFilePath);
        
        // 生成Ceph存储的文件名
        String cephFileName = CSUtil.getUUID();
        
        // 获取Ceph的路径,如果文件有扩展名，则会以文件的类型再分目录
        String cephFileDir = cephCSWSManager.getCephPath(contentType, nodeId, fileExtName);
        String cephFilePath = cephFileDir + cephFileName + "." + fileExtName;
        cephFilePath = CSUtil.winString2Linux(cephFilePath);
        contentMap.put(ContentStorageConstants.FILE_PATH, cephFilePath);
        
        // 获取ceph存储bucketName
        String bucketName = CephCSWSManager.getCSRootLogicBucket();
        contentMap.put(ContentStorageConstants.BUCKET_NAME, bucketName);
        
        // 根据MD5和fileSize,检测生成文件内容是否存在
        String md5 = CSFileUtil.getMD5Hex(tmpFilePath);
        String fileSize = CSFileUtil.getFileSize(tmpFilePath);
        
        if (StringUtils.isEmpty(md5) || StringUtils.isEmpty(fileSize))
        {
            logger.error("CS handler error: the md5 or fileSize is null! md5:" + md5 + " ,fileSize:" + fileSize);
            return false;
        }
        
        contentMap.put(ContentStorageConstants.MD5, md5);
        contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
        
        Md5Entity md5Entity = cephCSWSManager.getMd5FilePath(md5, fileSize);
        
        // 文件在MD5表的文件路径是否存在，不存在就上传，存在就检查ceph中是否还存在
        if (null == md5Entity)
        {
            ObjectMetadata metaData = new ObjectMetadata();
            Map<String, String> userMetadata = new HashMap<String, String>();
            userMetadata.put(ContentStorageConstants.S3_MD5, md5);
            userMetadata.put(ContentStorageConstants.S3_FILESIZE, fileSize);
            metaData.setUserMetadata(userMetadata);
            metaData.setContentLength(Long.parseLong(fileSize));
            
            // 传入主从region，用于写入双写同步信息与实际上传的一致
            contentMap.put(ContentStorageConstants.S3_REGION_MASTER, CephCSWSManager.getMaterRegion());
            contentMap.put(ContentStorageConstants.S3_REGION_SLAVE, CephCSWSManager.getSlaveRegion());
            
            /*if (StringUtils.isEmpty(csS3Impl.uploadObject(
            		tmpFilePath, 
            		bucketName, 
            		cephFilePath, 
            		metaData)))
            {
                logger.error("CS handler error: addMultiContentsByBytes failed ! uploadObject failed.");
                return false;
            }*/
            
            // 上传文件结束后，再下载到本地做md5校验
            if (!CSFileUtil.uploadObjectAndCheckMd5(bucketName, tmpFilePath, cephFilePath, metaData, csS3Impl))
            {
                // 首次失败后，重试一次
                if (!CSFileUtil.uploadObjectAndCheckMd5(bucketName, tmpFilePath, cephFilePath, metaData, csS3Impl))
                {
                    logger.error("CS handler error: addMultiContentsByBytes failed ! uploadObjectAndCheckMd5 failed.");
                    return false;
                }
            }
            
            contentMap.put(ContentStorageConstants.FILE_PATH, cephFilePath);
            
            // 向cs_md5_path_info表中保存数据
            if (!cephCSWSManager.addMd5FilePath(contentMap))
            {
                logger.error("CS handler error: addMultiMD5 failed ! addMd5FilePath to DB failed.");
                return false;
            }
        }
        else
        {
            contentMap.put(ContentStorageConstants.BUCKET_NAME, md5Entity.getBucketName());
            contentMap.put(ContentStorageConstants.FILE_PATH, md5Entity.getFilePath());
        }
        
        // 删除临时文件
        if (!CSFileUtil.delFolder(tmpCephDir))
        {
            logger.error("CS handler error: Delete created tmp file error.");
            //临时目录清理不成功不作为文件上传失败的标记
            //return false;
        }
        
        return true;
    }
    
    public boolean addMultiContentsByCopyAsyn(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        logger.info("addMultiContentsByCopyAsyn. contentType:" + contentType + " list:" + contentMapList);
        
        try
        {
            // 检测参数合法性及内容是否存在
            if (!checkAddMultiContentsByCopyAsyn(contentType, contentMapList))
            {
                throw new IllegalArgumentException(
                        "CS handler error: addMultiContentsByCopyAsyn failed! the input parameter is illegal.");
            }
            
            // 批量添加md5
            for (int i = 0; i < contentMapList.size(); i++)
            {
                if (!addMultiMD5Copy(contentType, contentMapList.get(i)))
                {
                    return false;
                }
            }
            
            // 批量添加cs
            if (!cephCSWSManager.addMultiContentInfo(contentType, contentMapList))
            {
                logger.error("CS handler error: addMultiContentsByCopyAsyn failed! addMultiContentInfo error.");
                return false;
            }
            
            // 异步添加资源文件
            this.getThreadPool().submit(new AsynFileCopy(contentMapList));
            
            logger.info("addMultiContentsByCopyAsyn success !");
            return true;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addMultiContentsByCopyAsyn failed! exception = " + e.toString());
            return false;
        }
    }
    
    /**
     * 异步将临时桶文件复制到正式桶
     * @return
     * @throws CSException
     */
    private boolean addMultiMD5Copy(String contentType, Map<String, Object> contentMap) throws CSException
    {
        String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString();
        String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
        String sourceKey = contentMap.get(ContentStorageConstants.SOURCE_KEY).toString();
        
        String sourceBucketName = CephCSWSManager.getCSTempLogicBucket();
        ObjectMetadata tmpMetaData = null;
        
        try
        {
            tmpMetaData = csS3Impl.getObjectMetaData(sourceBucketName, sourceKey);
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addMultiContentsByCopyAsyn failed ! getObjectMetaData exception ", e);
            return false;
        }
        
        if (null == tmpMetaData)
        {
            logger.error("CS handler error: addMultiContentsByCopyAsyn failed ! tmpMetaData is null.");
            return false;
        }
        
        String md5 = tmpMetaData.getUserMetaDataOf(ContentStorageConstants.S3_MD5);
        String fileSize = tmpMetaData.getUserMetaDataOf(ContentStorageConstants.S3_FILESIZE);
        
        if (StringUtils.isEmpty(md5) || StringUtils.isEmpty(fileSize))
        {
            logger.error("CS handler error: addMultiContentsByCopyAsyn failed !"
                    + " addMultiMD5File error, md5 or fileSize is null");
            return false;
        }
        
        contentMap.put(ContentStorageConstants.MD5, md5);
        contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
        
        // 获取ceph存储bucketName
        String destBucketName = CephCSWSManager.getCSRootLogicBucket();
        contentMap.put(ContentStorageConstants.BUCKET_NAME, destBucketName);
        
        String sourceBuckeName = CephCSWSManager.getCSTempLogicBucket();
        contentMap.put("sourceBucketName", sourceBuckeName);
        
        // 生成Ceph存储的文件名
        String cephFilePath = getCephFilePath(fileName, contentType, nodeId);
        contentMap.put(ContentStorageConstants.FILE_PATH, cephFilePath);
        
        // 根据MD5和fileSize,检测生成文件内容是否存在
        Md5Entity md5Entity = cephCSWSManager.getMd5FilePath(md5, fileSize);
        
        // 文件在MD5表的文件路径是否存在，不存在就上传，存在就检查ceph中是否还存在
        if (null == md5Entity)
        {
        	// 传入主从region，用于写入双写同步信息与实际上传的一致
            contentMap.put(ContentStorageConstants.S3_REGION_MASTER, CephCSWSManager.getMaterRegion());
            contentMap.put(ContentStorageConstants.S3_REGION_SLAVE, CephCSWSManager.getSlaveRegion());
        	
            // 向cs_md5_path_info表中保存数据
            if (!cephCSWSManager.addMd5FilePath(contentMap))
            {
                logger.error("CS handler error: addMultiMD5 failed! addMd5FilePath to DB failed.");
                return false;
            }
        }
        else
        {
            contentMap.put(ContentStorageConstants.SAME_MD5_FILE, ContentStorageConstants.SAME_MD5_FILE);
            contentMap.put(ContentStorageConstants.BUCKET_NAME, md5Entity.getBucketName());
            contentMap.put(ContentStorageConstants.FILE_PATH, md5Entity.getFilePath());
        }
        
        return true;
    }
    
    /**
     * 以路径方式批量添加文件（异步方式）
     * 
     * @param contentType
     * @param contentMapList
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public boolean addMultiContentsAsyn(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        logger.info("addMultiContentsAsyn(). contentType:" + contentType + " list:" + contentMapList);
        
        try
        {
            // 检测参数合法性及内容是否存在
            if (!checkAddMultiContentsAsyn(contentType, contentMapList))
            {
                throw new IllegalArgumentException(
                        "CS handler error: addMultiContentsAsyn failed ! the input parameter is illegal.");
            }
            
            // 批量添加md5
            for (int i = 0; i < contentMapList.size(); i++)
            {
                if (!addMultiMD5(contentType, contentMapList.get(i)))
                {
                    return false;
                }
            }
            
            // 批量添加cs
            if (!cephCSWSManager.addMultiContentInfo(contentType, contentMapList))
            {
                logger.error("CS handler error: addMultiContentsAsyn failed! addMultilContentInfo error.");
                return false;
            }
            
            // 异步添加资源文件
            this.getThreadPool().submit(new AsynFileWriter(contentMapList));
            
            logger.info("addMultiContentsAsyn success !");
            return true;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addMultiContentsAsyn failed! exception = " + e.toString());
            return false;
        }
    }
    
    /**
     * 写md5表数据，不上传实体文件
     * @return
     * @throws CSException
     */
    private boolean addMultiMD5(String contentType, Map<String, Object> contentMap) throws CSException
    {
        String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString();
        String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
        String sourceFilePath = contentMap.get(ContentStorageConstants.SOURCE_FILE_PATH).toString();
        
        File file = new File(sourceFilePath);
        if (!file.exists())
        {
            logger.error("CS handler error: addMultiMD5 failed ! file does not exists: " + sourceFilePath);
            return false;
        }
        
        // 生成Ceph存储的文件名
        String cephFilePath = getCephFilePath(fileName, contentType, nodeId);
        contentMap.put(ContentStorageConstants.FILE_PATH, cephFilePath);
        
        // 获取ceph存储bucketName
        String bucketName = CephCSWSManager.getCSRootLogicBucket();
        contentMap.put(ContentStorageConstants.BUCKET_NAME, bucketName);
        
        // 根据MD5和fileSize,检测生成文件内容是否存在
        String md5 = CSFileUtil.getMD5Hex(sourceFilePath);
        String fileSize = CSFileUtil.getFileSize(sourceFilePath);
        
        if (StringUtils.isEmpty(md5) || StringUtils.isEmpty(fileSize))
        {
            logger.error("CS handler error: the md5 or fileSize is null! md5:" + md5 + " ,fileSize:" + fileSize);
            return false;
        }
        
        contentMap.put(ContentStorageConstants.MD5, md5);
        contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
        
        Md5Entity md5Entity = cephCSWSManager.getMd5FilePath(md5, fileSize);
        
        // 文件在MD5表的文件路径是否存在，不存在就上传，存在就检查ceph中是否还存在
        if (null == md5Entity)
        {
        	// 传入主从region，用于写入双写同步信息与实际上传的一致
            contentMap.put(ContentStorageConstants.S3_REGION_MASTER, CephCSWSManager.getMaterRegion());
            contentMap.put(ContentStorageConstants.S3_REGION_SLAVE, CephCSWSManager.getSlaveRegion());
            
            // 向cs_md5_path_info表中保存数据
            if (!cephCSWSManager.addMd5FilePath(contentMap))
            {
                logger.error("CS handler error: addMultiMD5 failed ! addMd5FilePath to DB failed.");
                return false;
            }
        }
        else
        {
            contentMap.put(ContentStorageConstants.SAME_MD5_FILE, ContentStorageConstants.SAME_MD5_FILE);
            contentMap.put(ContentStorageConstants.BUCKET_NAME, md5Entity.getBucketName());
            contentMap.put(ContentStorageConstants.FILE_PATH, md5Entity.getFilePath());
        }
        
        return true;
    }
    
    /**
     * ceph写入者
     * 
     * @see  [相关类/方法]
     * @since  [产品/模块版本]
     */
    private class AsynFileWriter implements Runnable
    {
        private List<Map<String, Object>> contentMapList;
        
        public AsynFileWriter(List<Map<String, Object>> contentMapList)
        {
            this.contentMapList = contentMapList;
        }
        
        public void run()
        {
            if (CollectionUtils.isEmpty(contentMapList))
            {
                String msg = "CS handler error: addMultiContentsByCopyAsyn failed ! "
                        + "CSFileWriter.run(): contentMapList is null.";
                logger.error(msg);
                return;
            }
            
            Map<String, Object> contentMap = null;
            
            // 写入Ceph
            for (int i = 0; i < contentMapList.size(); i++)
            {
                contentMap = contentMapList.get(i);
                
                try
                {
                    // 如果已经存在相同MD5文件,就不再上传了
                    String sameMD5 = "" + contentMap.get(ContentStorageConstants.SAME_MD5_FILE);
                    if (ContentStorageConstants.SAME_MD5_FILE.equals(sameMD5.trim()))
                    {
                        continue;
                    }
                    
                    String sourceFilePath = contentMap.get(ContentStorageConstants.SOURCE_FILE_PATH).toString();
                    String bucketName = contentMap.get(ContentStorageConstants.BUCKET_NAME).toString();
                    String cephFilePath = contentMap.get(ContentStorageConstants.FILE_PATH).toString();
                    
                    String md5 = contentMap.get(ContentStorageConstants.MD5).toString();
                    String fileSize = contentMap.get(ContentStorageConstants.FILE_SIZE).toString();
                    
					ObjectMetadata metaData = new ObjectMetadata();
					Map<String, String> userMetadata = new HashMap<String, String>();
					userMetadata.put(ContentStorageConstants.S3_MD5, md5);
					userMetadata.put(ContentStorageConstants.S3_FILESIZE, fileSize);
                    metaData.setUserMetadata(userMetadata);
                    metaData.setContentLength(Long.parseLong(fileSize));
                    
                    /*if (StringUtils.isEmpty(csS3Impl.uploadObject(
                    		sourceFilePath, 
                    		bucketName, 
                    		cephFilePath, 
                    		metaData)))
                    {
                        logger.error("CS handler error: addMultiContentsAsyn.AsynFileWriter failed ! uploadObject failed.");
                        continue;
                    }*/
                    
                    // 上传文件结束后，再下载到本地做md5校验
                    if (!CSFileUtil.uploadObjectAndCheckMd5(bucketName, sourceFilePath, cephFilePath, metaData, csS3Impl))
                    {
                        // 首次失败后，重试一次
                        if (!CSFileUtil.uploadObjectAndCheckMd5(bucketName, sourceFilePath, cephFilePath, metaData, csS3Impl))
                        {
                            logger.error("CS handler error: addMultiContentsAsyn.AsynFileWriter failed ! uploadObjectAndCheckMd5 failed.");
                            continue;
                        }
                    }
                }
                catch (Exception e)
                {
                    logger.error("CS handler error: addMultiContentsAsyn failed !"
                            + " CSFileWriter.run() error. exception = " + e.toString());
                }
            }
        }
    }
    
    /**
     * ceph写入者
     * 
     * @see  [相关类/方法]
     * @since  [产品/模块版本]
     */
    private class AsynFileCopy implements Runnable
    {
        private List<Map<String, Object>> contentMapList;
        
        public AsynFileCopy(List<Map<String, Object>> contentMapList)
        {
            this.contentMapList = contentMapList;
        }
        
        public void run()
        {
            if (CollectionUtils.isEmpty(contentMapList))
            {
                String msg = "CS handler error: addMultiContentsByCopyAsyn failed ! "
                        + "AsynFileCopy.run(): contentMapList is null.";
                logger.error(msg);
                return;
            }
            
            Map<String, Object> contentMap = null;
            
            // 写入Ceph
            for (int i = 0; i < contentMapList.size(); i++)
            {
                contentMap = contentMapList.get(i);
                
                try
                {
					if (null != contentMap.get(ContentStorageConstants.SAME_MD5_FILE))
                	{
                		// 如果已经存在相同MD5文件,就不再上传了
                        String sameMD5 = "" + contentMap.get(ContentStorageConstants.SAME_MD5_FILE);
                        if (ContentStorageConstants.SAME_MD5_FILE.equals(sameMD5.trim()))
                        {
                            continue;
                        }
                	}
                    
                    String sourceKey = contentMap.get(ContentStorageConstants.SOURCE_KEY).toString();
                    String sourceBucketName = contentMap.get("sourceBucketName").toString();
                    String bucketName = contentMap.get(ContentStorageConstants.BUCKET_NAME).toString();
                    String filePath = contentMap.get(ContentStorageConstants.FILE_PATH).toString();
                    
                    /*if (!csS3Impl.copyObjectWithSingleOperation(
                    		sourceBucketName, 
                    		sourceKey, 
                    		bucketName, 
                    		filePath))
                    {
                        logger.error("CS handler error: addMultiContentsByCopyAsyn failed!"
                                + " copyObjectWithSingleOperation error.");
                        continue;
                    }*/
                    
                    // 上传文件结束后，再下载到本地做md5校验
                    if (!CSFileUtil.copyObjectAndCheckMd5(sourceBucketName, bucketName, sourceKey, filePath, csS3Impl))
                    {
                        // 首次校验不一致时，重试一次
                        if (!CSFileUtil.copyObjectAndCheckMd5(sourceBucketName, bucketName, sourceKey, filePath, csS3Impl))
                        {
                            logger.error("CS handler error: addMultiContentsByCopyAsyn failed ! copyObjectAndCheckMd5 failed.");
                            continue;
                        }
                    }
                }
                catch (Throwable e)
                {
                    logger.error("CS handler error: addMultiContentsByCopyAsyn failed! CSFileWriter.run() error.");
                }
            }
        }
    }
    
    /**
     * 获取线程池单例.
     * 
     * @return ExecutorService
     */
    private ExecutorService getThreadPool()
    {
        return WriterThreadPool.writerThreadPool;
    }
    
    /**
     * ceph写入者线程池
     * 
     */
    private static class WriterThreadPool
    {
        final static ExecutorService writerThreadPool = Executors.newFixedThreadPool(CORE_POOL_SIZE,
                new CSThreadFactory("CS", "SWFWriter"));
    }
    
    /**
     * 检测参数合法性及内容是否存在
     * <功能详细描述>
     * @param contentType
     * @param contentMapList
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    private boolean checkAddMultiContentsByCopy(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        // 判断List集合是否为空
        if (CollectionUtils.isEmpty(contentMapList))
        {
            logger.error("checkAddMultiContentsByCopy error. contentMapList is empty.");
            return false;
        }
        
        // 检测参数合法性及内容是否存在
        for (int i = 0; i < contentMapList.size(); i++)
        {
            Map<String, Object> contentMap = contentMapList.get(i);
            
            // 检测参数的合法性
            if (!CSUtil.checkAddContentByCopy(contentType, contentMap))
            {
                return false;
            }
            
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString().trim();
            String partNo = contentMap.get(ContentStorageConstants.PART_NO).toString().trim();
            
            // 检测内容是否已经存在
            if (isContentExist(contentType, nodeId, partNo))
            {
                logger.error("checkMultiInputParameters error, the content already exist,contentType =" + contentType
                        + " ,nodeid =" + nodeId + " ,partNo=" + partNo);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 检测参数合法性及内容是否存在
     * <功能详细描述>
     * @param contentType
     * @param contentMapList
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    private boolean checkAddMultiContentsByCopyAsyn(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        // 判断List集合是否为空
    	if (CollectionUtils.isEmpty(contentMapList))
        {
            return false;
        }
        
        // 检测参数合法性及内容是否存在
        for (int j = 0; j < contentMapList.size(); j++)
        {
            Map<String, Object> contentMap = contentMapList.get(j);
            
            // 检测参数的合法性
            if (!CSUtil.checkAddContentByCopy(contentType, contentMap))
            {
                return false;
            }
            
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString().trim();
            String partNo = contentMap.get(ContentStorageConstants.PART_NO).toString().trim();
            
            // 检测内容是否已经存在
            if (isContentExist(contentType, nodeId, partNo))
            {
                logger.error("checkMultiInputParameters error, the content already exist, contentType =" + contentType
                        + " , nodeid =" + nodeId + " , partNo=" + partNo);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 检测参数合法性及内容是否存在
     * <功能详细描述>
     * @param contentType
     * @param contentMapList
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    private boolean checkAddMultiContentsAsyn(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        // 判断List集合是否为空
    	if (CollectionUtils.isEmpty(contentMapList))
        {
            return false;
        }
        
        // 检测参数合法性及内容是否存在
        for (int i = 0; i < contentMapList.size(); i++)
        {
            Map<String, Object> contentMap = contentMapList.get(i);
            
            // 检测参数的合法性
            if (!CSUtil.checkAddContentByFile(contentType, contentMap))
            {
                return false;
            }
            
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString().trim();
            String partNo = contentMap.get(ContentStorageConstants.PART_NO).toString().trim();
            
            // 检测内容是否已经存在
            if (isContentExist(contentType, nodeId, partNo))
            {
                logger.error("checkMultiInputParameters error, the content already exist,contentType =" + contentType
                        + " ,nodeid =" + nodeId + " ,partNo=" + partNo);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 检测参数合法性及内容是否存在
     * <功能详细描述>
     * @param contentType
     * @param contentMapList
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    private boolean checkAddMultiContentsByBytes(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        // 判断List集合是否为空
    	if (CollectionUtils.isEmpty(contentMapList))
        {
            return false;
        }
        
        // 检测参数合法性及内容是否存在
        for (int i = 0; i < contentMapList.size(); i++)
        {
            Map<String, Object> contentMap = contentMapList.get(i);
            
            // 检测参数的合法性
            if (!CSUtil.checkAddContentByBytes(contentType, contentMap))
            {
                return false;
            }
            
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString().trim();
            String partNo = contentMap.get(ContentStorageConstants.PART_NO).toString().trim();
            
            // 检测内容是否已经存在
            if (isContentExist(contentType, nodeId, partNo))
            {
                logger.error("checkMultiInputParameters error, the content already exist,contentType =" + contentType
                        + " ,nodeid =" + nodeId + " ,partNo=" + partNo);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 根据contentType、nodeId 和 partNo 读取文档内容,以文件流的形式输出
     * 
     * @param nodeId文档ID
     * @param partNo文档ID集合
     * @return 文件流
     * @throws CSException
     */
    public InputStream readTmpContentbyStream(String sourceKey) throws CSException, IOException
    {
        if (StringUtils.isBlank(sourceKey))
        {
            throw new IllegalArgumentException(
                    "CS handler error: readTmpContentbyStream failed ! the sourceKey is blank.");
        }
        
        try
        {
            // 获取文件路径
            String bucketName = CephCSWSManager.getCSTempLogicBucket();
            if (StringUtils.isEmpty(bucketName))
            {
                logger.error("CS handler error: readTmpContentbyStream failed ! CsTempPath is null. ");
                return null;
            }
            
            // 读取文件，以文件流形式返回
            InputStream inContent = csS3Impl.getObject(bucketName, sourceKey);
            
            return inContent;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: readTmpContentbyStream failed ! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 根据contentType、nodeId 和 partNo 读取文档内容,以文件流的形式输出
     * 
     * @param nodeId文档ID
     * @param partNo文档ID集合
     * @return 文件流
     * @throws CSException
     */
    public InputStream readContentbyStream(String contentType, String nodeId, String partNo) throws CSException,
        IOException
    {
        // 因nodeid唯一，去掉contentType判断，删除StringUtils.isBlank(contentType) ||  
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new IllegalArgumentException("CS handler error: readContentbyStream failed! "
                    + "The input param contentType ,nodeId or partNo is blank.");
        }
        
        try
        {
            // 从数据库中获取文件元信息
            Map<String, String> contentInfo = cephCSWSManager.getContentInfo(contentType.trim(),
                    nodeId.trim(),
                    partNo.trim());
            
            if (MapUtils.isEmpty(contentInfo))
            {
                logger.error("CS handler error: readContentbyStream failed! The contentInfo is null.");
                return null;
            }
            
            // 获取文件路径
            String bucketName = contentInfo.get(ContentStorageConstants.BUCKET_NAME);
            String filePath = contentInfo.get(ContentStorageConstants.FILE_PATH);
            
            // 读取文件，以文件流形式返回
            InputStream inContent = csS3Impl.getObject(bucketName, filePath);
            
            return inContent;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: readContentbyStream failed! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * （低并发分片下载）根据contentType、nodeId 和 partNo 读取文档内容,以文件流的形式输出
     * 
     * @param nodeId文档ID
     * @param partNo文档ID集合
     * @return 文件流
     * @throws CSException
     */
    public boolean readContentbyStream(String contentType, String nodeId, String partNo, String localFilePath)
        throws CSException, IOException
    {
        // 因nodeid唯一，去掉contentType判断，删除StringUtils.isBlank(contentType) ||  
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new IllegalArgumentException("CS handler error: readContentbyStream failed! "
                    + "The input param contentType ,nodeId or partNo is blank.");
        }
        
        OutputStream os = null;
        try
        {
            // codedex 
            // 获取文件输出句柄
            File file = new File(localFilePath);
            os = FileUtils.openOutputStream(file);
            
            // 从数据库中获取文件元信息
            Map<String, String> contentInfo = cephCSWSManager.getContentInfo(contentType.trim(),
                    nodeId.trim(),
                    partNo.trim());
            
            if (MapUtils.isEmpty(contentInfo))
            {
                logger.error("CS handler error: readContentbyStream failed! The contentInfo is null. nodeId={},partNo={}.",
                        nodeId,
                        partNo);
                return false;
            }
            
            // 获取文件路径
            String bucketName = contentInfo.get(ContentStorageConstants.BUCKET_NAME);
            String filePath = contentInfo.get(ContentStorageConstants.FILE_PATH);
            String filesize = contentInfo.get(ContentStorageConstants.FILE_SIZE);
            
            // 读取文件，以文件流形式返回
            //InputStream inContent = csS3Impl.getObject(bucketName, filePath);
            
            if (StringUtils.isBlank(bucketName) || StringUtils.isBlank(filePath))
            {
                logger.error("CS handler error: readContentbyStream failed! The bucketName={} or filePath={} is null. nodeId={},partNo={}.",
                        bucketName,
                        filePath,
                        nodeId,
                        partNo);
                return false;
            }
            
            //获取s3对象信息
            ObjectMetadata s3Meta = csS3Impl.getObjectMetaData(bucketName, filePath);
            if (null == s3Meta)
            {
                logger.error("CS handler error: readContentbyStream failed! The s3 object metadata is null.The bucketName={} or filePath={} is null. nodeId={},partNo={}.",
                        bucketName,
                        filePath,
                        nodeId,
                        partNo);
                return false;
            }
            
            long contentLength = s3Meta.getContentLength();
            
            if (Long.parseLong(filesize) != contentLength)
            {
                logger.error("CS handler error: readContentbyStream exception! The cs filesize={} is not equals contentLength={}. nodeId={},partNo={}.",
                        filesize,
                        contentLength,
                        nodeId,
                        partNo);
            }
            
            Range range = new Range(0, contentLength - 1);
            //支持低并发场景
            if (!SliceDownloadUtil.startDownloadUnderLowConcurrency(bucketName, filePath, os, range, csS3Impl))
            {
                logger.error("CS handler error: readContentbyStream failed! startDownloadUnderLowConcurrency download failed. nodeId={},partNo={}.contentLength={}",
                        nodeId,
                        partNo,
                        contentLength);
                return false;
            }
            
            //校验文件（甚至可以md5校验）
            if (null == file || !file.exists())
            {
                logger.error("CS handler error: readContentbyStream failed! The file is not exists.The bucketName={} or filePath={} is null. nodeId={},partNo={}.",
                        bucketName,
                        filePath,
                        nodeId,
                        partNo);
                return false;
            }
            long fileLength = file.length();
            if (contentLength != fileLength)
            {
                logger.error("CS handler error: readContentbyStream failed! The fileLength={} is not equals contentLength={}.The bucketName={} or filePath={} is null. nodeId={},partNo={}.",
                        fileLength,
                        contentLength,
                        bucketName,
                        filePath,
                        nodeId,
                        partNo);
                return false;
            }
            
            return true;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: readContentbyStream failed! nodeId={},partNo={}. exception = "
                    + e.toString(),
                    nodeId,
                    partNo);
            return false;
        }
        finally
        {
            IOUtils.closeQuietly(os);
        }
    }
    
    /**
     * 根据contentType、nodeId 和 partNo 读取文档内容,以文件流的形式输出
     * 
     * @param nodeId文档ID
     * @param partNo文档ID集合
     * @return 文件流
     * @throws CSException
     */
    public InputStream readS3ObjectbyStream(String bucketName, String key)
    {
        if (StringUtils.isBlank(bucketName) || StringUtils.isBlank(key))
        {
            throw new IllegalArgumentException("CS handler error: readS3ObjectbyStream failed! "
                    + "The input param bucketName or key is blank.");
        }
        
        try
        {
            // 读取文件，以文件流形式返回
            InputStream inContent = csS3Impl.getObject(bucketName, key);
            
            return inContent;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: readS3ObjectbyStream failed! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 根据contentType、nodeId 和 partNo 读取文档内容,以文件流的形式输出
     * 
     * @param nodeId文档ID
     * @param partNo文档ID集合
     * @return 文件流
     * @throws CSException
     */
    public InputStream readContentbyStream(String contentType, String nodeId, String partNo, int offset, int length)
        throws CSException
    {
        // 因nodeid唯一，去掉contentType判断，删除StringUtils.isBlank(contentType) ||  
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new IllegalArgumentException("CS handler error: readContentbyStream failed! "
                    + "The input parm contentType(Optional),nodeId or partNo is blank.");
        }
        
        try
        {
            // 从数据库中获取文件元信息
            Map<String, String> contentInfo = cephCSWSManager.getContentInfo(contentType.trim(),
                    nodeId.trim(),
                    partNo.trim());
            
            if (MapUtils.isEmpty(contentInfo))
            {
                logger.info("CS handler error: readContentbyStream failed! the contentInfo is null.");
                return null;
            }
            
            // 获取文件路径
            String bucketName = contentInfo.get(ContentStorageConstants.BUCKET_NAME);
            String filePath = contentInfo.get(ContentStorageConstants.FILE_PATH);
            
            // 读取文件，以文件流形式返回
            InputStream content = csS3Impl.getObjectWithRange(bucketName, filePath, offset, length);
            
            return content;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: readContentbyStream failed! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 根据contentType、nodeId 和 partNo 读取文档内容,以文件流的形式输出
     * 
     * @param nodeId文档ID
     * @param partNo文档ID集合
     * @return 文件流
     * @throws CSException
     */
    public InputStream readS3ObjectbyStream(String bucketName, String key, int offset, int length) throws CSException
    {
        if (StringUtils.isBlank(bucketName) || StringUtils.isBlank(key))
        {
            throw new IllegalArgumentException("CS handler error: readS3ObjectbyStream failed! "
                    + "The input parm bucketName or key is blank.");
        }
        
        try
        {
            // 读取文件，以文件流形式返回
            InputStream content = csS3Impl.getObjectWithRange(bucketName, key, offset, length);
            
            return content;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: readS3ObjectbyStream failed! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 根据contentType、 nodeId 和 partNo 读取文档内容,以二进制数组的形式返回
     * 
     * @param nodeId
     *            文档ID
     * @param partNo
     *            文档ID集合
     * @return 二进制数组
     * @throws CSException
     */
    public byte[] readContentbyByte(String contentType, String nodeId, String partNo) throws CSException
    {
      // 因nodeid唯一，去掉contentType判断，删除StringUtils.isBlank(contentType) ||  
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new IllegalArgumentException("CS handler error: readContentbyByte failed! "
                    + "contentType(Optional),nodeId or partNo is blank.");
        }
        
        // 读取文件，返回二进制数组
        byte[] byteContent = null;
        try
        {
            // 从数据库中获取文件元信息
            Map<String, String> contentInfo = cephCSWSManager.getContentInfo(contentType.trim(),
                    nodeId.trim(),
                    partNo.trim());
            
            if (MapUtils.isEmpty(contentInfo))
            {
                logger.info("CS handler error: readContentbyByte failed ! The contentInfo is null.");
                return null;
            }
            
            // 获取文件路径
            String bucketName = contentInfo.get(ContentStorageConstants.BUCKET_NAME);
            String filePath = contentInfo.get(ContentStorageConstants.FILE_PATH);
            
            // 读取文件，以文件流形式返回
            InputStream content = csS3Impl.getObject(bucketName, filePath);
            
            byteContent = IOUtils.toByteArray(content);
        }
        catch (IOException e)
        {
            logger.info("CS handler error: readContentbyByte failed! IOException = " + e.toString());
            return null;
        }
        catch (Exception e)
        {
            logger.info("CS handler error: readContentbyByte failed! exception = " + e.toString());
            return null;
        }
        
        return byteContent;
    }
    
    /**
     * 读取某一contentType、nodeId下的所有文档
     * 
     * @param nodeId文档ID
     * @return List<Map<String, byte[]>>
     * @throws CSException
     */
    public Map<String, byte[]> readContents(String contentType, String nodeId) throws CSException
    {
        // 对输入参数进行非空判断
      //因nodeid唯一，去掉contentType判断，删除StringUtils.isBlank(contentType) ||  
        if (StringUtils.isBlank(nodeId))
        {
            throw new IllegalArgumentException("CS handler error: readContents failed ! "
                    + "contentType(Optional) or nodeId is null.");
        }
        
        try
        {
            // 从数据库中读取nodeId下所有文件的文件元信息
            List<Map<String, String>> list = getContentInfos(contentType.trim(), nodeId.trim());
            if (CollectionUtils.isEmpty(list))
            {
                logger.error("CS handler error: readContents failed ! the data get from db is null.");
                return null;
            }
            
            Map<String, byte[]> mapPath = new HashMap<String, byte[]>();
            String partNo = null;
            byte[] byteContent = null;
            for (Map<String, String> map : list)
            {
                partNo = map.get(ContentStorageConstants.PART_NO);
                byteContent = readContentbyByte(contentType, nodeId, partNo);
                
                mapPath.put(partNo, byteContent);
            }
            
            return mapPath;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: readContents failed! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 获取md5信息	返回结果map：partNo md5
     * @param contentType
     * @param nodeId
     * @return map：partNo md5
     * @throws CSException
     */
    public Map<String, String> getContentsMd5(String contentType, String nodeId) throws CSException
    {
        // 2017-07-05 因nodeid唯一，不需要contentType，删除StringUtils.isBlank(contentType) || 
        if (StringUtils.isBlank(nodeId))
        {
            logger.error("getContentsMd5 failed ! contentType=" + contentType, " ,nodeId=" + nodeId);
            return null;
        }
        
        return cephCSWSManager.getContentsMd5(contentType, nodeId);
    }
    
    /**
     * 获取UUID对应的信息
     * 
     * @param uuid
     * @return
     */
    public Map<String, String> getImageInfo(String uuid)
    {
        if (StringUtils.isBlank(uuid))
        {
            logger.error("getImageInfo failed, input parameter uuid is null.");
            return null;
        }
        return cephCSWSManager.getImageInfo(uuid);
    }
    
    /**
     * 删除临时桶文件
     * @param contentType
     * @param nodeId
     * @return 
     * @throws CSException
     */
    public boolean delTmpContent(String sourceKey) throws CSException
    {
        String bucketName = CephCSWSManager.getCSTempLogicBucket();
        try
        {
            return csS3Impl.delObject(bucketName, sourceKey);
        }
        catch (Exception e)
        {
            logger.error("CS handler error: delTmpContent failed! exception = " + e.toString());
            return false;
        }
    }
    
    /**
     * 列举桶内所有文件
     * @param contentType
     * @param nodeId
     * @return 
     * @throws CSException
     */
    public List<S3ObjectSummary> listBucketObjects(String bucketName, String preFix, int queryCnt) throws CSException
    {
        List<S3ObjectSummary> result = new ArrayList<S3ObjectSummary>();
        try
        {
            ObjectListing listObjectsV2Result = csS3Impl.listBucketObjects(bucketName, preFix, queryCnt);
            
            if (null == listObjectsV2Result)
            {
                logger.error("CS handler error: listBucketObjects failed! listObjectsV2Result is null.");
                return null;
            }
            
            List<S3ObjectSummary> list = listObjectsV2Result.getObjectSummaries();
            
            if (CollectionUtils.isEmpty(list))
            {
                logger.error("CS handler error: listBucketObjects failed! list is empty.");
                return null;
            }
            
            for (S3ObjectSummary obj : list)
            {
                S3ObjectSummary s3Obj = new S3ObjectSummary();
                s3Obj.setBucketName(obj.getBucketName());
                s3Obj.setETag(obj.getETag());
                s3Obj.setKey(obj.getKey());
                s3Obj.setSize(obj.getSize());
                s3Obj.setLastModified(obj.getLastModified());
                s3Obj.setOwner(obj.getOwner());
                
                result.add(s3Obj);
            }
            
            return result;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: listBucketObjects failed! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 返回ceph中实体文件是否存在
     * @param nodeId
     * @param partNo
     * @param mid
     * @throws CSException
     * @author 
     */
    public boolean existFileInCeph(String nodeId, String partNo, String mid) throws CSException
    {
        try
        {
            Map<String, String> contentMap = cephCSWSManager.getContentInfo(mid, nodeId, partNo);
            
            // 如果数据库不存在此记录,直接返回
            if (MapUtils.isEmpty(contentMap))
            {
                logger.error("CS handler error: existFileInCeph, getContentInfo is null!");
                return false;
            }
            
            String bucketName = contentMap.get(ContentStorageConstants.BUCKET_NAME);
            String filePath = contentMap.get(ContentStorageConstants.FILE_PATH);
            
            if (StringUtils.isEmpty(bucketName) || StringUtils.isEmpty(filePath))
            {
                logger.error("CS handler error: existFileInCeph, bucketName or filePath is null!");
                return false;
            }
            
            ObjectMetadata objectMetadata = csS3Impl.getObjectMetaData(bucketName, filePath);
            
            if (null == objectMetadata)
            {
                return false;
            }
            else
            {
                return true;
            }
            
        }
        catch (Exception e)
        {
            logger.error("CS handler error: existFileInCeph failed! exception = " + e.toString());
            return false;
        }
    }
    
    /**
     * 返回ceph中实体文件相关信息
     * @param nodeId
     * @param partNo
     * @param mid
     * @throws CSException
     * @author 
     */
    public FileInfo FileInfoInCeph(String nodeId, String partNo, String mid) throws CSException
    {
        try
        {
            Map<String, String> contentMap = cephCSWSManager.getContentInfo(mid, nodeId, partNo);
            
            // 如果数据库不存在此记录,直接返回
            if (MapUtils.isEmpty(contentMap))
            {
                return null;
            }
            
            String bucketName = contentMap.get(ContentStorageConstants.BUCKET_NAME);
            String filePath = contentMap.get(ContentStorageConstants.FILE_PATH);
            
            if (StringUtils.isEmpty(bucketName) || StringUtils.isEmpty(filePath))
            {
                logger.error("CS handler error: FileInfoInCeph, bucketName or filePath is null!");
                return null;
            }
            
            ObjectMetadata objectMetadata = csS3Impl.getObjectMetaData(bucketName, filePath);
            
            if (null == objectMetadata)
            {
                return null;
            }
            
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String fileLastUpdateTime = sdf.format(objectMetadata.getLastModified());
            String fileLength = String.valueOf(objectMetadata.getContentLength());
            
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFileLastUpdateTime(fileLastUpdateTime);
            fileInfo.setFileLength(fileLength);
            
            return fileInfo;
            
        }
        catch (Exception e)
        {
            logger.error("CS handler error: FileInfoInCeph failed! exception = " + e.toString());
            return null;
        }
    }
}
----------------------B------------
package com.org.support.contentstorageservice.client.handler.service.commoncontent;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.org.support.cbb.util.io.FileUtils;
import com.org.support.contentstorageservice.client.handler.exception.CSException;
import com.org.support.contentstorageservice.client.handler.util.CSFileUtil;
import com.org.support.contentstorageservice.client.handler.util.CSThreadFactory;
import com.org.support.contentstorageservice.client.handler.util.CSUtil;
import com.org.support.contentstorageservice.client.handler.wsmanager.NasCSServiceSupport;
import com.org.support.contentstorageservice.common.ContentStorageConstants;
import com.org.support.contentstorageservice.model.Md5Entity;

/**
 * 
 * CS 通用文件处理服务类
 * 
 * @author  z00181553
 * @version  [V100R001C00, 2012-9-15]
 * @since  [DSDP/CS]
 */
public class NasCSCommonContentServiceImpl extends NasCSServiceSupport
{
    /**
     * 日志组件
     */
    private Logger logger = LoggerFactory.getLogger(NasCSCommonContentServiceImpl.class);
    
    private final static int CORE_POOL_SIZE = 10;
    
    /**
     * contentMap的key, 用于标识是否存在(md5码, size)相同的文件
     */
    private final static String SAME_MD5_FILE = "sameMd5File";
    
    /**
     * 
     * nas写入者线程池
     * 
     * @author  y00205792
     * @version  [版本号, 2013-12-27]
     * @see  [相关类/方法]
     * @since  [产品/模块版本]
     */
    private static class WriterThreadPool
    {
        final static ExecutorService writerThreadPool = Executors.newFixedThreadPool(CORE_POOL_SIZE,
                new CSThreadFactory("CS", "SWFWriter"));
    }
    
    /**
     * 获取线程池单例.
     * 
     * @return ExecutorService
     */
    private ExecutorService getThreadPool()
    {
        return WriterThreadPool.writerThreadPool;
    }
    
    /**
     * 向 CS 中增加一个文档(将该文档内容从上传的临时目录copy到CS的NAS磁盘目录下,源路径下的文件还存在)
     */
    public boolean addContentByCopy(String contentType, Map<String, Object> contentMap) throws CSException
    {
        logger.info("in addContentByCopy() the contentType:" + contentType + " ,contentMap:" + contentMap);
        
        // 验证新增时参数是否为空
        if (!CSUtil.checkAddContentByCopy(contentType, contentMap))
        {
            throw new IllegalArgumentException("add content by copy error,the input parameters illegal.");
        }
        
        // 兼容ceph参数，FILE_PATH为空时，则复制SOURCE_KEY的值
        String filePath = (String) contentMap.get(ContentStorageConstants.FILE_PATH);
        String sourceKey = (String) contentMap.get(ContentStorageConstants.SOURCE_KEY);
        if(StringUtils.isBlank(filePath) && StringUtils.isNotBlank(sourceKey))
        {
            contentMap.put(ContentStorageConstants.FILE_PATH, sourceKey);
        }
        
        // 兼容ceph参数，统一设置bucket为nas
        String bucketName = (String) contentMap.get(ContentStorageConstants.BUCKET_NAME);
        if(StringUtils.isBlank(bucketName))
        {
            contentMap.put(ContentStorageConstants.BUCKET_NAME, "nas");
        }
        
        String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString();
        String partNo = contentMap.get(ContentStorageConstants.PART_NO).toString();
        // 源文件路径
        String sourceFilePath = contentMap.get(ContentStorageConstants.FILE_PATH).toString();
        // 显示文件名
        String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
        
        // 验证内容是否已经在CS中存在
        if (isContentExist(contentType, nodeId, partNo))
        {
            logger.error("The content already exists. please delete the content first.contentType=" + contentType
                    + " ,nodeId=" + nodeId + " ,partNo=" + partNo);
            throw new CSException("csException_0012");
        }
        
        //根据MD5码,验证上传的内容实体是否已经存在
        String md5 = CSFileUtil.getMD5Hex(sourceFilePath);
        String fileSize = CSFileUtil.getFileSize(sourceFilePath);
        contentMap.put(ContentStorageConstants.MD5, md5);
        contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
        Md5Entity md5Entity = nasCSWSManager.getMd5FilePath(md5, fileSize);
        
        String md5FilePath = null;
        if (md5Entity != null)
        {
            md5FilePath = md5Entity.getFilePath();
        }
        
        logger.info("addContentByCopy md5FilePath: " + md5FilePath);
        CSUtil.createNotExistFile(sourceFilePath, md5FilePath);
        
        if (StringUtils.isNotBlank(md5FilePath))
        {//CS中已经存在文件实体,直接将该路径作为新的文件路径
            contentMap.put(ContentStorageConstants.FILE_PATH, md5FilePath);
        }
        else
        {
            //CS中还没有文件实体,向NAS上新增文件,并保存其MD5,FILESIZE,filePath到数据库
            // Nas上存的文件名
            String nasFileName = CSUtil.getUUID();
            
            // 如果目的文件名有扩展名,则加上扩展名
            String fileExtName = CSUtil.getFileExtName(fileName);
            nasFileName += "." + fileExtName;
            
            // 获取NAS的路径,如果文件有扩展名，则会以文件的类型分目录
            String nasDir = nasCSWSManager.getNasPath(contentType, nodeId, fileExtName);
            
            String newFilePath = nasDir + nasFileName;
            // 将nas文件路径存放到map中,进而方便存入数据库
            contentMap.put(ContentStorageConstants.FILE_PATH, newFilePath);
            
            // Copy 文件
            logger.info("addContentByCopy ready to copy from " + sourceFilePath + " to " + nasDir + ", " + nasFileName);
            if (!CSFileUtil.copyFlie(sourceFilePath, nasDir, nasFileName))
            {
                logger.error("CS error at addContentByCopy. failed at copyFile.");
                return false;
            }
            
            //向CS_MD5_PATH_INFO表中保存数据
            if (!nasCSWSManager.addMd5FilePath(contentMap))
            {
                logger.error("Add content by copy error.add md5 and path info to DB failed.");
                return false;
            }
        }
        
        // 插入DB是否成功 失败则直接返回失败
        if (!nasCSWSManager.addContentInfo(contentType, contentMap))
        {
            logger.error("CS error at ContentStorageServiceImpl.addContentByCopy(),oPtionDB failed.Please check the webservice and database is in service.");
            return false;
        }
        logger.info("in addContentByCopy(). Copy content success.");
        return true;
    }
    
    /**
     * 查询内容在CS中是否已经存在
     */
    private boolean isContentExist(String contentType, String nodeId, String partNo) throws CSException
    {
        //验证 contentType,nodeId 和 partNo 对应内容是否已存在
        Map<String, String> contentInfo = nasCSWSManager.getContentInfo(contentType, nodeId, partNo);
        if (contentInfo == null || contentInfo.isEmpty())
        {
            return false;
        }
        return true;
    }
    
    /**
     * 向 CS 中增加一个文档(将该文档内容从上传的临时目录move到CS的NAS磁盘目录下,源路径下的文件被剪切,已不存在)
     * 在linux下,直接采用剪切方法存在问题(不同磁盘分区),故采用先copy后删除源文件的假剪切方法
     * 
     * @param contentType
     *            文档类型
     * @param contentMap
     *            参数集
     * @return 新增是否成功
     * @throws CSException
     */
    public boolean addContentByMove(String contentType, Map<String, Object> contentMap) throws CSException
    {
        logger.info("in addContentByMove() the contentType:" + contentType + "contentMap:" + contentMap);
        String sourceFilePath = (String) contentMap.get(ContentStorageConstants.FILE_PATH);
        String sourceKey = (String) contentMap.get(ContentStorageConstants.SOURCE_KEY);
        if(StringUtils.isBlank(sourceFilePath) && StringUtils.isNotBlank(sourceKey))
        {
            sourceFilePath = sourceKey;
        }
        
        // 先调用拷贝文件方法
        if (!addContentByCopy(contentType, contentMap))
        {
            logger.error("CS error at addContentByMove. failed at addContentByCopy.");
            return false;
        }
        
        // 拷贝完后,删除源文件,实现"剪切"的功能
        // String sourceFilePath = (String) contentMap.get(ContentStorageConstants.FILE_PATH);
        File sourceFile = new File(sourceFilePath);
        
        if (!sourceFile.delete())
        {
            logger.error("CS error at addContentByMove,delete source failed,source file name=" + sourceFilePath);
            return false;
        }
        
        logger.info("in addContentByMove(), move content succeed.");
        return true;
    }
    
    /**
     * 向 CS 中增加一个文档(该文档内容通过流的形式传入到CS,其中contentMap必须包含文档内容fileContent,文件名fileName,
     * 文件大小fileSize等)
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public boolean addContentByStream(String contentType, Map<String, Object> contentMap) throws CSException
    {
        logger.info("in addContentByStream the contentType:" + contentType + "  contentMap:" + contentMap);
        
        // 验证新增时参数是否为空
        if (!CSUtil.checkAddContentByStream(contentType, contentMap))
        {
            throw new IllegalArgumentException("add content by stream error,the input parameters illegal.");
        }
        
        // 兼容ceph参数，统一设置bucket为nas
        String bucketName = (String) contentMap.get(ContentStorageConstants.BUCKET_NAME);
        if(StringUtils.isBlank(bucketName))
        {
            contentMap.put(ContentStorageConstants.BUCKET_NAME, "nas");
        }
        
        String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString();
        String partNo = contentMap.get(ContentStorageConstants.PART_NO).toString();
        String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString(); // 显示文件名
        
        // 验证内容是否存在,是则先删除
        if (isContentExist(contentType, nodeId, partNo))
        {
            logger.info("The content already exists. please delete the content first.nodeId=" + nodeId + " ,partNo="
                    + partNo);
            throw new CSException("csException_0012");
        }
        
        // 文件输入流
        InputStream is = (InputStream) contentMap.get(ContentStorageConstants.CONTENT_STREAM);
        if (is == null)
        {
            logger.error("Add content by stream error.The input stream is null.");
            return false;
        }
        
        // Nas上存的文件名
        String nasFileName = CSUtil.getUUID();
        
        // 如果有扩展名,则加上扩展名
        String fileExtName = CSUtil.getFileExtName(fileName);
        nasFileName += "." + fileExtName;
        
        // 获取NAS的路径,如果文件有扩展名，则会以文件的类型分目录
        String nasDir = nasCSWSManager.getNasPath(contentType, nodeId, fileExtName);
        String filePath = nasDir + nasFileName;
        
        // 写文件是否成功 失败则直接返回失败
        if (!CSFileUtil.writeFileByInputStream(is, nasDir, nasFileName))
        {
            logger.error("CS error at addContent by stream.WriteFileByInputStream failed.");
            return false;
        }
        
        // 根据MD5和fileSize,检测生成文件内容是否存在
        String md5 = CSFileUtil.getMD5Hex(filePath);
        String fileSize = CSFileUtil.getFileSize(filePath);
        contentMap.put(ContentStorageConstants.MD5, md5);
        contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
        Md5Entity md5Entity = nasCSWSManager.getMd5FilePath(md5, fileSize);
        String md5FilePath = null;
        if (md5Entity != null)
        {
            md5FilePath = md5Entity.getFilePath();
        }
        CSUtil.createNotExistFile(filePath, md5FilePath);
        
        if (CSUtil.stringIsEmpty(md5FilePath))
        {
            //CS 中没有文件实体,保留新生成的文件,并将路径保存到数据库表
            contentMap.put(ContentStorageConstants.FILE_PATH, filePath);
            
            //向CS_MD5_PATH_INFO表中保存数据
            if (!nasCSWSManager.addMd5FilePath(contentMap))
            {
                logger.error("Add content by stream error.add md5 and path info to DB failed.");
                return false;
            }
        }
        else
        {
            //CS中已经存在文件实体,直接将该路径作为新的文件路径,并删除新生成的文件
            contentMap.put(ContentStorageConstants.FILE_PATH, md5FilePath);
            File newFile = new File(filePath);
            if (!newFile.delete())
            {
                logger.error("Delete created file error.");
                return false;
            }
        }
        
        // 插入DB是否成功 失败则直接返回失败
        if (!nasCSWSManager.addContentInfo(contentType, contentMap))
        {
            logger.error("CS error at addContentByStream, failed at addContentInfo to DB. please check the webservice and DB is in service.");
            return false;
        }
        logger.info("in addContentByStream, return true.");
        return true;
    }
    
    /**
     * 向 CS 中增加一个文件(该文件内容通过流二进制数组的形式传入到CS,其中contentMap必须包含文件内容contentBytes,文件名fileName,文件大小fileSize等)
     * <功能详细描述>
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public boolean addContentByBytes(String contentType, Map<String, Object> contentMap) throws CSException
    {
        // 对输入进行校验
        byte[] bytes = (byte[]) contentMap.get(ContentStorageConstants.CONTENT_BYTES);
        if (bytes == null || bytes.length <= 0)
        {
            throw new IllegalArgumentException("The input content bytes is null.");
        }
        
        // 将二进制数组转换成文件流
        InputStream is = new ByteArrayInputStream(bytes);
        contentMap.remove(ContentStorageConstants.CONTENT_BYTES);
        contentMap.put(ContentStorageConstants.CONTENT_STREAM, is);
        
        // 调用以流的方式增加文件的方法,将文件添加到CS
        return addContentByStream(contentType, contentMap);
    }
    
    /**
     * 向 CS 中增加一个文件(该文件内容通过文件路径的形式传入到CS,其中contentMap必须包含文件内容contentBytes,文件名fileName,文件大小fileSize等)
     * 从本地上传到ceph正式桶
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public boolean addContentByFile(String contentType, Map<String, Object> contentMap) throws CSException
    {
        logger.info("addContentByFile. contentType=" + contentType + "  contentMap=" + contentMap);
        
        // 验证新增时参数是否为空
        if (!CSUtil.checkAddContentByFile(contentType, contentMap))
        {
            throw new IllegalArgumentException("CS handler error: addContentByFile failed! some args are blank !"
                    + " contentType=" + contentType + "  contentMap=" + contentMap);
        }
        
        String sourceFilePath = contentMap.get(ContentStorageConstants.SOURCE_FILE_PATH).toString();
        
        // 将文件转换成文件流
        try
        {
            InputStream is = new FileInputStream(sourceFilePath);
            contentMap.put(ContentStorageConstants.CONTENT_STREAM, is);
        }
        catch(Exception e)
        {
            throw new IllegalArgumentException("CS handler error: addContentByFile failed! source file may not exist !"
                + " contentType=" + contentType + "  contentMap=" + contentMap);
        }
        
        // 调用以流的方式增加文件的方法,将文件添加到CS
        return addContentByStream(contentType, contentMap);
        
    }
    
    /**
     * 向 CS中增加一个临时文件(该文件内容通过文件路径的形式传入到CS)
     * 文件存入临时根目录
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public Map<String, String> addTmpContentByFile(String sourceFilePath) throws CSException
    {
        logger.info("addTmpContentByFile. filePath = " + sourceFilePath);
        try
        {
            // 判断路径是否为空
            if (null == sourceFilePath || StringUtils.isEmpty(sourceFilePath))
            {
                throw new IllegalArgumentException(
                        "CS handler error: addTmpContentByFile failed! sourceFilePath is null or empty.");
            }
            
            // 判断源文件是否存在
            File tmpFile = new File(sourceFilePath);
            if (!tmpFile.exists())
            {
                throw new CSException("CS handler error: addTmpContentByFile faield! source file does not exist.");
            }
            
            // 生成Nas存储的文件名
            String tmpNasFileName = CSUtil.getUUID();
            
            // 加上文件后缀
            if (sourceFilePath.lastIndexOf(".") > 0)
            {
                tmpNasFileName += sourceFilePath.substring(sourceFilePath.lastIndexOf("."), sourceFilePath.length());
            }
            
            // 加上年月日目录
            String tmpNasDir = nasCSWSManager.getTmpNasPath();
            tmpNasDir = FilenameUtils.separatorsToUnix(tmpNasDir);
            String tmpNasFilePath = tmpNasDir + tmpNasFileName;
            
            // 获取文件md5及fileSize
            String md5 = CSFileUtil.getMD5Hex(sourceFilePath);
            String fileSize = CSFileUtil.getFileSize(sourceFilePath);
            if (StringUtils.isEmpty(md5) || StringUtils.isEmpty(fileSize))
            {
                logger.error("CS handler error: the md5 or fileSize is null! md5:" + md5 + " ,fileSize:" + fileSize);
                return null;
            }
            
            // 写文件是否成功 失败则直接返回失败
            // InputStream is = new FileInputStream(new File(sourceFilePath));
            // if (!CSFileUtil.writeFileByInputStream(is, tmpNasDir, tmpNasFileName))
            
            byte[] contentBytes = org.apache.commons.io.FileUtils.readFileToByteArray(new File(sourceFilePath));
            if(!nasCSWSManager.addTmpContentByFile(tmpNasDir, tmpNasFileName, contentBytes))
            {
                logger.error("CS error at addContent by addTmpContentByFile failed.");
                return null;
            }
            
            Map<String, String> resultMap = new HashMap<String, String>();
            resultMap.put(ContentStorageConstants.SOURCE_KEY, tmpNasFilePath);
            resultMap.put(ContentStorageConstants.MD5, md5);
            resultMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
            
            return resultMap;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: addTmpContentByFile faield ! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 根据 nodeId 删除该 contentId 下的所有对应的partNo的文档
     * @param nodeId
     *            应用ID
     * @return 删除是否成功
     * @throws CSException 
     */
    public boolean delContents(String contentType, String nodeId) throws CSException
    {
        logger.info("in delContents the contentType:" + contentType + " nodeId:" + nodeId);
        
        // 对输入参数进行非空判断
        if (StringUtils.isBlank(contentType) || StringUtils.isBlank(nodeId))
        {
            throw new IllegalArgumentException("The input param is null.the contentType:" + contentType + " nodeId = "
                    + nodeId);
        }
        
        if (!nasCSWSManager.updateContentsToDelete(contentType, nodeId, null))
        {
            logger.error("CS error at delContents. Failed to delete all content from DB.");
            return false;
        }
        
        logger.info("Delete all content succeed!");
        return true;
    }
    
    /**
     * 根据 nodeId 及 partNo 删除一项中的一个文档
     * @param nodeId
     *            应用ID
     * @param partNo
     *            文档ID
     * @return 返回删除是否成功
     */
    public boolean delContent(String contentType, String nodeId, String partNo)
    {
        logger.info("in delContent. contentType:" + contentType + " ,nodeId:" + nodeId + " ,partNo:" + partNo);
        
        // 对输入参数进行非空判断
        if (StringUtils.isBlank(contentType) || StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new IllegalArgumentException("The input param is null.  contentType:" + contentType + " ,nodeId = "
                    + nodeId + ", partNo = " + partNo);
        }
        
        if (!nasCSWSManager.updateContentToDelete(contentType.trim(), nodeId.trim(), partNo.trim()))
        {
            logger.error("CS error at delContent(). Failed to delete content from DB.");
            return false;
        }
        
        logger.info("delete content succeed!");
        return true;
    }
    
    /**
     * 根据 nodeId 及 partNos 删除一项中的多个文档
     * @param nodeId
     *            应用ID
     * @param partNos
     *            文档ID集合
     * @return
     */
    public boolean delContents(String contentType, String nodeId, List<String> partNos) throws CSException
    {
        logger.info("in delContents() the contentType: " + contentType + " ,nodeId:" + nodeId + ", partNos:" + partNos);
        
        // 对输入参数进行非空判断
        if (StringUtils.isBlank(contentType) || StringUtils.isBlank(nodeId) || null == partNos || partNos.isEmpty())
        {
            throw new IllegalArgumentException("The input param is null. contentType: " + contentType + " ,nodeId = "
                    + nodeId + ", partNos = " + partNos);
        }
        
        if (!nasCSWSManager.updateContentsToDelete(contentType.trim(), nodeId.trim(), partNos))
        {
            logger.error("CS error at delContents. Failed to delete contents from DB. nodeId = " + nodeId
                    + " ,partNos = " + partNos);
            return false;
        }
        
        logger.info("Delete contents succeed!");
        return true;
    }
    
    /**
     * 删除临时文件
     */
    public boolean delTmpContent(String sourceKey) throws CSException
    {
        String tmpNasRoot = nasCSWSManager.getTmpNasRoot();
        try
        {
            // 防止误删：不是临时根目录下的文件 或 文件不存在 或 是文件夹
            Path sourcePath = Paths.get(sourceKey);
            if(StringUtils.isBlank(sourceKey) || !sourceKey.startsWith(tmpNasRoot) || !Files.exists(sourcePath) || Files.isDirectory(sourcePath))
            {
                return false;
            }
            
            // 删除单个文件
            FileUtils.deleteFile(sourceKey);
            
            return true;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: delTmpContent failed! exception = " + e.toString());
            return false;
        }
    }
    
    /**
     * 根据contentType、 nodeId 和 partNo 读取文档内容,以二进制数组的形式返回
     * 
     * @param nodeId
     *            文档ID
     * @param partNo
     *            文档ID集合
     * @return 二进制数组
     * @throws CSException
     */
    public byte[] readContentbyByte(String contentType, String nodeId, String partNo) throws CSException
    {
        // 对输入参数进行非空判断
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new IllegalArgumentException("the input parm is null.contentType=" + contentType + " ,nodeId="
                    + nodeId + " ,partNo=" + partNo);
        }
        
        // 从数据库中获取文件元信息
        Map<String, String> contentInfo = nasCSWSManager.getContentInfo(contentType.trim(),
                nodeId.trim(),
                partNo.trim());
        if (contentInfo == null || contentInfo.isEmpty())
        {
            logger.error("CS error at readContentbyByte. The contentInfo is null.");
            return null;
        }
        
        // 获取文件路径
        String filePath = contentInfo.get(ContentStorageConstants.FILE_PATH);
        
        // 读取文件，返回二进制数组
        byte[] byteContent = CSFileUtil.readFileByByte(filePath);
        
        return byteContent;
    }
    
    /**
     * 根据contentType、nodeId 和 partNo 读取文档内容,以文件流的形式输出
     * 
     * @param nodeId文档ID
     * @param partNo文档ID集合
     * @return 文件流
     * @throws CSException
     */
    public InputStream readContentbyStream(String contentType, String nodeId, String partNo) throws CSException,
        IOException
    {
        if (StringUtils.isBlank(contentType) || StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new IllegalArgumentException("The input parm contentType、nodeId or partNo is blank.");
        }
        
        // 从数据库中获取文件元信息
        Map<String, String> contentInfo = nasCSWSManager.getContentInfo(contentType.trim(),
                nodeId.trim(),
                partNo.trim());
        if (contentInfo == null || contentInfo.isEmpty())
        {
            logger.info("CS error at readContentbyStream. The contentInfo is null.");
            return null;
        }
        
        // 获取文件路径
        String filePath = contentInfo.get(ContentStorageConstants.FILE_PATH);
        
        // 读取文件，以文件流形式返回
        InputStream content = CSFileUtil.readFileInput(filePath);
        
        return content;
    }
    
    /**
     * 根据contentType、contentId 和 partNo 读取文档内容,以文件流的形式输出(分段)
     * 
     * @param nodeId文档ID
     * @param partNo文档ID集合
     * @param offset文件的起始偏置
     * @param length读取的文件长度
     * @return 文件流
     * @throws CSException 
     */
    /*
    public InputStream readContentbyStream(String contentType, String nodeId, String partNo, int offset, int length)
        throws CSException
    {
        if (StringUtils.isBlank(contentType) || StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new IllegalArgumentException("The input parm contentType,nodeId or partNo is null.");
        }
        // 从数据库中获取文件元信息
        Map<String, String> contentInfo = nasCSWSManager.getContentInfo(contentType.trim(),
                nodeId.trim(),
                partNo.trim());
        if (contentInfo == null || contentInfo.isEmpty())
        {
            logger.error("CS error at readContentbyStream. The contentInfo is null.");
            return null;
        }
        
        // 获取文件路径
        String filePath = contentInfo.get(ContentStorageConstants.FILE_PATH);
        InputStream is = null;
        if (StringUtils.isBlank(filePath))
        {
            logger.error("CS error at readContentbyStream(). the filepath get from db is null. please check the webservice and DB is in service.");
            return is;
        }
        
        is = readContentbyStreamOffset(filePath, offset, length);
        return is;
    }
    
    private InputStream readContentbyStreamOffset(String filePath, int offset, int length)
    {
        InputStream is = null;
        if (!CSUtil.stringIsEmpty(filePath))
        {
            File file = new File(filePath);
            RandomAccessFile raf = null;
            FileChannel channel = null;
            try
            {
                if (offset >= 0 && length >= 0)
                {
                    // 分段读取大文件
                    raf = new RandomAccessFile(filePath, "r");
                    channel = raf.getChannel();
                    MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
                    
                    byte[] arrResult = new byte[length];
                    for (int i = 0; i < length; i++)
                    {
                        arrResult[i] = buffer.get(i);
                    }
                    // System.gc();
                    is = new ByteArrayInputStream(arrResult);
                }
                else
                {
                    // 全量读取小文件
                    is = new FileInputStream(file);
                }
            }
            catch (FileNotFoundException e)
            {
                logger.error("CS error at readContentbyStreamOffset.");
                return null;
            }
            catch (IOException e)
            {
                logger.error("CS error at readContentbyStreamOffset.");
                return null;
            }
            finally
            {
                IOUtils.closeQuietly(raf);
                IOUtils.closeQuietly(channel);
            }
        }
        return is;
    }
    */
    
    /**
     * 读取某一contentType、nodeId下的所有文档
     * 
     * @param nodeId文档ID
     * @return List<Map<String, byte[]>>
     * @throws CSException
     */
    public Map<String, byte[]> readContents(String contentType, String nodeId) throws CSException
    {
        // 对输入参数进行非空判断
        if (StringUtils.isBlank(contentType) || StringUtils.isBlank(nodeId))
        {
            throw new IllegalArgumentException("The input contentType or nodeId is null.");
        }
        
        // 从数据库中读取nodeId下所有文件的文件元信息
        List<Map<String, String>> list = getContentInfos(contentType.trim(), nodeId.trim());
        if (list == null || list.isEmpty())
        {
            logger.error("CS error at readContents(). the data get from db is null.");
            return null;
        }
        
        Map<String, byte[]> mapPath = new HashMap<String, byte[]>();
        String partNo = null;
        String filePath = null;
        byte[] byteContent = null;
        for (Map<String, String> map : list)
        {
            partNo = map.get(ContentStorageConstants.PART_NO);
            filePath = map.get(ContentStorageConstants.FILE_PATH);
            byteContent = CSFileUtil.readFileByByte(filePath);
            
            mapPath.put(partNo, byteContent);
        }
        return mapPath;
    }
    
    /**
     * 根据sourceKey 读取文档内容,以文件流的形式输出
     * 
     * @param sourceKey 临时文件路径
     * @return 文件流
     */
    public InputStream readTmpContentbyStream(String sourceKey) throws CSException, IOException
    {
        if (StringUtils.isBlank(sourceKey))
        {
            throw new IllegalArgumentException(
                    "CS handler error: readTmpContentbyStream failed ! the sourceKey is blank.");
        }
        
        try
        {
            // 检查临时根目录，读取文件时不校验
            /*String tmpNasRoot = nasCSWSManager.getTmpNasRoot();
            if (!FilenameUtils.separatorsToUnix(sourceKey).startsWith(FilenameUtils.separatorsToUnix(tmpNasRoot)))
            {
                logger.error("CS handler error: readTmpContentbyStream failed ! sourceKey is not temp file. ");
                return null;
            }*/
            
            // 读取文件，以文件流形式返回
            byte[] fileBytes = nasCSWSManager.readTmpContentByStream(sourceKey);
            if(fileBytes == null)
            {
                logger.error("source bytes is null: " + sourceKey);
                return null;
            }
            InputStream content = new ByteArrayInputStream(fileBytes);
            
            return content;
        }
        catch (Exception e)
        {
            logger.error("CS handler error: readTmpContentbyStream failed ! exception = " + e.toString());
            return null;
        }
    }
    
    /**
     * 从CS 中获取一篇文档的全部信息（不读取内容）， 只从数据库表中查询 partNo,filename等信息
     * @param nodeId
     * @return List 中每一项为一个partNo的信息，每个partNo的信息以MAP的形式保存
     * @throws CSException
     */
    
    public List<Map<String, String>> getContentInfos(String contentType, String nodeId) throws CSException
    {
        // 对输入参数进行较验
        if (StringUtils.isBlank(nodeId))
        {
            throw new IllegalArgumentException("The input contentType or nodeId is null.");
        }
        
        List<Map<String, String>> contentInfos = nasCSWSManager.getContentInfos(contentType.trim(),
                nodeId.trim(),
                null,
                null);
        if (contentInfos == null || contentInfos.isEmpty())
        {
            logger.info("CS error at getContentInfo. The contentInfos is null.");
            return Collections.emptyList();
        }
        
        return contentInfos;
    }
    
    /**
     * 检测参数合法性及内容是否存在
     * <功能详细描述>
     * @param contentType
     * @param contentMapList
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    private boolean checkMultiInputParameters(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        // 判断List集合是否为空
        if ((null == contentMapList) || (contentMapList.isEmpty()))
        {
            return false;
        }
        
        // 检测参数合法性及内容是否存在
        for (int i = 0; i < contentMapList.size(); i++)
        {
            Map<String, Object> contentMap = contentMapList.get(i);
            
            // 检测参数的合法性
            if (!CSUtil.checkAndRebuildInputParameters(contentType, contentMap))
            {
                return false;
            }
            
            String nodeId = (String) contentMap.get(ContentStorageConstants.NODE_ID);
            String partNo = (String) contentMap.get(ContentStorageConstants.PART_NO);
            
            //检测内容是否已经存在
            if (isContentExist(contentType, nodeId, partNo))
            {
                logger.error("check multi input parameters error, the content already exist,contentType ="
                        + contentType + " ,nodeid =" + nodeId + " ,partNo=" + partNo);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 检测参数合法性及内容是否存在
     * <功能详细描述>
     * @param contentType
     * @param contentMapList
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    private boolean checkAddMultiContentsByBytes(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        // 判断List集合是否为空
        if ((null == contentMapList) || (contentMapList.isEmpty()))
        {
            return false;
        }
        
        // 检测参数合法性及内容是否存在
        for (int i = 0; i < contentMapList.size(); i++)
        {
            Map<String, Object> contentMap = contentMapList.get(i);
            
            // 检测参数的合法性
            if (!CSUtil.checkAddContentByBytes(contentType, contentMap))
            {
                return false;
            }
            
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString().trim();
            String partNo = contentMap.get(ContentStorageConstants.PART_NO).toString().trim();
            
            // 检测内容是否已经存在
            if (isContentExist(contentType, nodeId, partNo))
            {
                logger.error("checkMultiInputParameters error, the content already exist,contentType =" + contentType
                        + " ,nodeid =" + nodeId + " ,partNo=" + partNo);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 以路径方式批量添加文件
     * <功能详细描述>
     * @param contentType
     * @param contentMapList
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public boolean addMultiContentsByCopy(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        logger.info("in addMultiContentsByCopy() the contentType:" + contentType + " list:" + contentMapList);
        
        // 检测参数合法性及内容是否存在
        if (!checkMultiInputParameters(contentType, contentMapList))
        {
            throw new IllegalArgumentException("CS error at addMultiContentsByCopy,the input parameter is illegal.");
        }
        
        // 操作NAS，只要一条失败返回false;否则操作数据库
        for (int i = 0; i < contentMapList.size(); i++)
        {
            if (!writeSingleFileOfGivenPath(contentType, contentMapList.get(i)))
            {
                return false;
            }
        }
        // 添加失败
        if (!nasCSWSManager.addMultiContentInfo(contentType, contentMapList))
        {
            logger.error("CS error at addMultilContentInfo. Failed to add addMultiContentInfo to DB.");
            return false;
        }
        logger.info("in addMultiContentsByCopy() return true.");
        return true;
    }
    
    /**
     * nas写入者
     * 
     * @author  y00205792
     * @version  [版本号, 2013-12-27]
     * @see  [相关类/方法]
     * @since  [产品/模块版本]
     */
    private class AsynFileWriter implements Runnable
    {
        private List<Map<String, Object>> contentMapList;
        
        public AsynFileWriter(List<Map<String, Object>> contentMapList)
        {
            this.contentMapList = contentMapList;
        }
        
        public void run()
        {
            String newFilePath = "";
            String sourceFilePath = "";
            Map<String, Object> contentMap = null;
            
            if (CollectionUtils.isEmpty(contentMapList))
            {
                String msg = "in CSFileWriter.run(): contentMapList is null";
                logger.error(msg);
                return;
            }
            
            // 写入NAS
            for (int i = 0; i < contentMapList.size(); i++)
            {
                contentMap = contentMapList.get(i);
                
                // 已经存在了(md5,size)相同的文件，就不再写入了
                if (Boolean.parseBoolean("" + contentMap.get(SAME_MD5_FILE)))
                {
                    continue;
                }
                
                try
                {
                    sourceFilePath = contentMap.get(ContentStorageConstants.FILE_PATH).toString();
                    newFilePath = contentMap.get(ContentStorageConstants.FILE_PATH).toString();
                    File file = new File(newFilePath);
                    
                    // copy文件是否成功 失败则直接返回失败
                    if (!CSFileUtil.copyFlie(sourceFilePath, file.getParent() + File.separator, file.getName()))
                    {
                        logger.error("in CSFileWriter.run(): copyFile failed. ");
                    }
                }
                catch (Throwable e)
                {
                    logger.error("in CSFileWriter.run(): write nas failed.");
                }
            }
            
            // 删除原解压目录(拷贝的方式，要删除原目录吗？)
            //            String firstSourceFilePath = ""
            //                    + contentMapList.get(0)
            //                            .get(ContentStorageConstants.SOURCE_FILE_PATH);
            //            File firstSourceFile = new File(firstSourceFilePath);
            //            CSFileUtil.delFolder(firstSourceFile.getParent());
        }
    }
    
    /**
     * 以路径方式批量添加文件（异步方式）
     * 
     * @param contentType
     * @param contentMapList
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public boolean addMultiContentsByCopyAsyn(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        logger.info("in addMultiContentsByCopy() the contentType:" + contentType + " list:" + contentMapList);
        
        // 检测参数合法性及内容是否存在
        if (!checkMultiInputParameters(contentType, contentMapList))
        {
            throw new IllegalArgumentException(
                    "CS error at addMultiContentsByCopy asyn,the input parameter is illegal.");
        }
        
        // 操作NAS，只要一条失败返回false;否则操作数据库
        for (int i = 0; i < contentMapList.size(); i++)
        {
            if (!updateContentMap(contentType, contentMapList.get(i)))
            {
                return false;
            }
        }
        
        // 添加失败
        if (!nasCSWSManager.addMultiContentInfo(contentType, contentMapList))
        {
            logger.error("CS error at addMultilContentInfo. Failed to add addMultiContentInfo to DB.");
            return false;
        }
        
        // 异步添加资源文件
        this.getThreadPool().submit(new AsynFileWriter(contentMapList));
        
        logger.info("in addMultiContentsByCopy() return true.");
        return true;
    }
    
    /** <一句话功能简述>
     * <功能详细描述>
     * @param contentType
     * @param contentMap
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    private boolean writeSingleFileOfGivenPath(String contentType, Map<String, Object> contentMap) throws CSException
    {
        // 显示文件名
        String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
        // 源文件路径
        if (!contentMap.containsKey(ContentStorageConstants.FILE_PATH))
        {
            logger.error("write file error,the input SOURCE_FILE_PATH is null.");
            return false;
        }
        String sourceFilePath = contentMap.get(ContentStorageConstants.FILE_PATH).toString();
        
        //获取MD5码并判断文件是否存在
        String md5 = CSFileUtil.getMD5Hex(sourceFilePath);
        String fileSize = CSFileUtil.getFileSize(sourceFilePath);
        contentMap.put(ContentStorageConstants.MD5, md5);
        contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
        Md5Entity md5Entity = nasCSWSManager.getMd5FilePath(md5, fileSize);
        String md5FilePath = null;
        if (md5Entity != null)
        {
            md5FilePath = md5Entity.getFilePath();
        }
        
        CSUtil.createNotExistFile(sourceFilePath, md5FilePath);
        
        if (CSUtil.stringIsEmpty(md5FilePath))
        {
            // 如果CS中没有文件实体,则创建文件
            // nas文件名
            String nasFileName = CSUtil.getUUID();
            
            // 如果有扩展名,要加上扩展名
            String fileExtName = CSUtil.getFileExtName(fileName);
            nasFileName += "." + fileExtName;
            
            // 获取NAS的路径,如果文件有扩展名，则会以文件的类型分目录
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString().trim();
            String nasDir = nasCSWSManager.getNasPath(contentType, nodeId, fileExtName);
            
            // 将nas路径放入map,方便后续将其插入数据库
            String newFilePath = nasDir + nasFileName;
            contentMap.put(ContentStorageConstants.FILE_PATH, newFilePath);
            
            // copy文件是否成功 失败则直接返回失败
            if (!CSFileUtil.copyFlie(sourceFilePath, nasDir, nasFileName))
            {
                logger.error("copy file error.");
                return false;
            }
            
            //将文件MD5,filesize,filepath保存到cs_md5_path_info表
            if (!nasCSWSManager.addMd5FilePath(contentMap))
            {
                logger.error("Add md5 file path to db error.");
                return false;
            }
        }
        else
        {
            // CS中已经存在文件实体,直接保存路径即可
            contentMap.put(ContentStorageConstants.FILE_PATH, md5FilePath);
        }
        
        return true;
    }
    
    /**
     * 更新contentMap，用于异步写入nas版本
     * 
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    private boolean updateContentMap(String contentType, Map<String, Object> contentMap) throws CSException
    {
        // 显示文件名
        String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
        // 源文件路径
        if (!contentMap.containsKey(ContentStorageConstants.FILE_PATH))
        {
            logger.error("write file error,the input SOURCE_FILE_PATH is null.");
            return false;
        }
        String sourceFilePath = contentMap.get(ContentStorageConstants.FILE_PATH).toString();
        
        //获取MD5码并判断文件是否存在
        String md5 = CSFileUtil.getMD5Hex(sourceFilePath);
        String fileSize = CSFileUtil.getFileSize(sourceFilePath);
        contentMap.put(ContentStorageConstants.MD5, md5);
        contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
        Md5Entity md5Entity = nasCSWSManager.getMd5FilePath(md5, fileSize);
        String md5FilePath = null;
        if (md5Entity != null)
        {
            md5FilePath = md5Entity.getFilePath();
        }
        CSUtil.createNotExistFile(sourceFilePath, md5FilePath);
        
        if (CSUtil.stringIsEmpty(md5FilePath))
        {
            // 如果CS中没有文件实体,则创建文件
            // nas文件名
            String nasFileName = CSUtil.getUUID();
            
            // 如果有扩展名,要加上扩展名
            String fileExtName = CSUtil.getFileExtName(fileName);
            nasFileName += "." + fileExtName;
            
            // 获取NAS的路径,如果文件有扩展名，则会以文件的类型分目录
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString().trim();
            String nasDir = nasCSWSManager.getNasPath(contentType, nodeId, fileExtName);
            
            // 将nas路径放入map,方便后续将其插入数据库
            String newFilePath = nasDir + nasFileName;
            contentMap.put(ContentStorageConstants.FILE_PATH, newFilePath);
            
            //将文件MD5,filesize,filepath保存到cs_md5_path_info表
            if (!nasCSWSManager.addMd5FilePath(contentMap))
            {
                logger.error("Add md5 file path to db error.");
                return false;
            }
            
            contentMap.put(SAME_MD5_FILE, false);
        }
        else
        {
            // CS中已经存在文件实体,直接保存路径即可
            contentMap.put(ContentStorageConstants.FILE_PATH, md5FilePath);
            contentMap.put(SAME_MD5_FILE, true);
        }
        
        return true;
    }
    
    /**
     * 以inputStream方式批量添加
     * 
     * @param contentType
     *            文档类型
     * @param contentMapList
     *            相关信息的集合
     * @return true表示成功， false表示失败
     * @author li.xiong
     * @throws CSException
     */
    public boolean addMultiContentsByBytes(String contentType, List<Map<String, Object>> contentMapList)
        throws CSException
    {
        logger.info("in addMulticontentsByBytes() the contentType:" + contentType + " list:" + contentMapList);
        
        // 检测参数合法性及内容是否存在
        if (!checkAddMultiContentsByBytes(contentType, contentMapList))
        {
            throw new IllegalArgumentException("CS error at addMultiContentsByBytes,the input parameter illegal.");
        }
        
        // 操作NAS，只要一条失败返回false;否则操作数据库
        for (int i = 0; i < contentMapList.size(); i++)
        {
            // 兼容ceph参数，统一设置bucket为nas
            String bucketName = (String) contentMapList.get(i).get(ContentStorageConstants.BUCKET_NAME);
            if(StringUtils.isBlank(bucketName))
            {
                contentMapList.get(i).put(ContentStorageConstants.BUCKET_NAME, "nas");
            }
            
            if (!writeSingleFileOfGivenBytes(contentType, contentMapList.get(i)))
            {
                return false;
            }
        }
        // 添加失败
        if (!nasCSWSManager.addMultiContentInfo(contentType, contentMapList))
        {
            logger.error("CS error at addMultiContentInfo. Failed to add contentInfo to DB.");
            return false;
        }
        logger.info("in addMultiContentsByCopy() return true.");
        return true;
        
    }
    
    /**
     * 以二进制数组形式写单篇内容
     * <功能详细描述>
     * @param contentType
     * @param contentMap
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    private boolean writeSingleFileOfGivenBytes(String contentType, Map<String, Object> contentMap) throws CSException
    {
        // 显示文件名
        String fileName = contentMap.get(ContentStorageConstants.FILE_NAME).toString();
        // 源文件路径
        if (!contentMap.containsKey(ContentStorageConstants.CONTENT_BYTES))
        {
            logger.error("write file error,the input contentMap lacks CONTENT_BYTES.");
            return false;
        }
        byte[] contentBytes = (byte[]) contentMap.get(ContentStorageConstants.CONTENT_BYTES);
        if (contentBytes == null)
        {
            logger.error("write file error,the input CONTENT_BYTES is null.");
            return false;
        }
        
        //获取MD5码并判断文件是否存在
        String md5 = CSFileUtil.getMD5Hex(contentBytes);
        String fileSize = String.valueOf(contentBytes.length);
        contentMap.put(ContentStorageConstants.MD5, md5);
        contentMap.put(ContentStorageConstants.FILE_SIZE, fileSize);
        Md5Entity md5Entity = nasCSWSManager.getMd5FilePath(md5, fileSize);
        String md5FilePath = null;
        if (md5Entity != null)
        {
            md5FilePath = md5Entity.getFilePath();
        }
        CSUtil.createNotExistFile(contentBytes, md5FilePath);
        
        if (CSUtil.stringIsEmpty(md5FilePath))
        {
            // 如果CS中没有文件实体,则创建文件
            // nas文件名
            String nasFileName = CSUtil.getUUID();
            
            // 如果有扩展名,要加上扩展名
            String fileExtName = CSUtil.getFileExtName(fileName);
            nasFileName += "." + fileExtName;
            
            // 获取NAS的路径,如果文件有扩展名，则会以文件的类型分目录
            String nodeId = contentMap.get(ContentStorageConstants.NODE_ID).toString().trim();
            String nasDir = nasCSWSManager.getNasPath(contentType, nodeId, fileExtName);
            
            // 将nas路径放入map,方便后续将其插入数据库
            String newFilePath = nasDir + nasFileName;
            contentMap.put(ContentStorageConstants.FILE_PATH, newFilePath);
            
            InputStream is = new ByteArrayInputStream(contentBytes);
            // 将流写到NAS， 失败则直接返回失败
            if (!CSFileUtil.writeFileByInputStream(is, nasDir, nasFileName))
            {
                logger.error("Write bytes to nas error.");
                return false;
            }
            
            //将文件MD5,filesize,filepath保存到cs_md5_path_info表
            if (!nasCSWSManager.addMd5FilePath(contentMap))
            {
                logger.error("Add md5 file path to database error.");
                return false;
            }
        }
        else
        {
            // CS中已经存在文件实体,直接保存路径即可
            contentMap.put(ContentStorageConstants.FILE_PATH, md5FilePath);
        }
        return true;
    }
    
    /**
     * 向cs中增加一个文件,该文件在cs中已存在.只需要变换其contentType,nodeId,partNo
     * <功能详细描述>
     * @param oldNodeId
     * @param oldPartNo
     * @param newPartNo
     * @return
     * @throws CSException 
     * @see [类、类#方法、类#成员]
     */
    public boolean addContentByExistContent(String oldContentType, String newContentType, String oldNodeId,
        String newNodeId, String oldPartNo, String newPartNo, String fileSource) throws CSException
    {
        logger.info("In addContentByExistContent. oldContentType = " + oldContentType + " ,newContentType="
                + newContentType + ", oldNodeId = " + oldNodeId + " ,newNodeId=" + newNodeId + ", oldPartNo = "
                + oldPartNo + ", newPartNo = " + newPartNo);
        
        // 对输入参数进行校验
        if (StringUtils.isBlank(oldContentType) || StringUtils.isBlank(newContentType)
                || StringUtils.isBlank(oldNodeId) || StringUtils.isBlank(newNodeId) || StringUtils.isBlank(oldPartNo)
                || StringUtils.isBlank(newPartNo))
        {
            throw new IllegalArgumentException("CS error at addContentByExistContent,the input parameter illegal.");
        }
        
        // 验证要添加的内容是否已经在CS中存在
        if (isContentExist(newContentType, newNodeId, newPartNo))
        {
            logger.error("The content to add already exists. please delete the content first.contentType ="
                    + newContentType + " ,nodeId=" + newNodeId + " ,partNo=" + newPartNo);
            throw new CSException("csException_0012");
        }
        
        // 判断旧的文件是否已存在
        Map<String, String> contentInfoMap = nasCSWSManager.getContentInfo(oldContentType, oldNodeId, oldPartNo);
        if (contentInfoMap == null)
        {
            logger.error("CS error at addContentByExistContent. The old content not exists.");
            return false;
        }
        
        // 构造新的ContentInfo
        Map<String, Object> newContentInfo = new HashMap<String, Object>();
        newContentInfo.put(ContentStorageConstants.CONTENT_TYPE, newContentType);
        newContentInfo.put(ContentStorageConstants.FILE_NAME, contentInfoMap.get(ContentStorageConstants.FILE_NAME));
        newContentInfo.put(ContentStorageConstants.FILE_PATH, contentInfoMap.get(ContentStorageConstants.FILE_PATH));
        newContentInfo.put(ContentStorageConstants.FILE_SIZE, contentInfoMap.get(ContentStorageConstants.FILE_SIZE));
        newContentInfo.put(ContentStorageConstants.NODE_ID, newNodeId);
        newContentInfo.put(ContentStorageConstants.PART_NO, newPartNo);
        newContentInfo.put(ContentStorageConstants.FILE_SOURCE, fileSource);
        
        // 设置文件格式，爬虫文件索引
        newContentInfo.put(ContentStorageConstants.FILE_TYPE, contentInfoMap.get(ContentStorageConstants.FILE_TYPE));
        
        if (!nasCSWSManager.addContentInfo(newContentType, newContentInfo))
        {
            logger.error("CS error at addContentByExistContent. Failed to add contentInfo.");
            return false;
        }
        
        logger.info("Succeed in addContentByExistContent.");
        return true;
    }
    
    /**
     * 根据contentType,nodeIds和partNO获取文件大小
     * @param nodeIds
     * @param partNo
     * @return
     * @throws CSException
     * @see [类、类#方法、类#成员]
     */
    public List<Map<String, String>> getContentsFileSize(String contentType, List<String> nodeIds, String partNo)
        throws CSException
    {
        if (nodeIds == null || nodeIds.isEmpty() || StringUtils.isBlank(partNo))
        {
            throw new CSException("The input param is null.");
        }
        
        List<Map<String, String>> fileSizeInfos = new ArrayList<Map<String, String>>();
        
        Map<String, Map<String, String>> multiNodeContent = getContentInfos(contentType, nodeIds, partNo);
        
        Set<String> nodeIdSet = multiNodeContent.keySet();
        for (String nodeId : nodeIdSet)
        {
            Map<String, String> contentInfo = multiNodeContent.get(nodeId);
            if (contentInfo == null)
            {
                throw new CSException("Failed to get contentInfo from data base.");
            }
            Map<String, String> fileSizeInfo = new HashMap<String, String>();
            fileSizeInfo.put(ContentStorageConstants.NODE_ID, contentInfo.get(ContentStorageConstants.NODE_ID));
            fileSizeInfo.put(ContentStorageConstants.FILE_SIZE, contentInfo.get(ContentStorageConstants.FILE_SIZE));
            fileSizeInfos.add(fileSizeInfo);
        }
        
        return fileSizeInfos;
    }
    
    public Map<String, Map<String, String>> getContentInfos(String contentType, List<String> nodeIds, String partNo)
        throws CSException
    {
        if (StringUtils.isBlank(contentType) || nodeIds == null || nodeIds.isEmpty() || StringUtils.isBlank(partNo))
        {
            throw new CSException("The input param is null.");
        }
        
        Map<String, List<Map<String, String>>> multiNodeContents = nasCSWSManager.getMultiNodesContentInfos(contentType,
                nodeIds,
                partNo);
        
        // 对查询结果进行封装，每个nodeid,partNo只对应一个内容，即内层list大小为一，对外直接以单个MAP表示
        Map<String, Map<String, String>> multiNodeContent = new HashMap<String, Map<String, String>>();
        Set<String> nodeIdSet = multiNodeContents.keySet();
        for (String nodeId : nodeIdSet)
        {
            List<Map<String, String>> contentInfos = multiNodeContents.get(nodeId);
            if (contentInfos == null || contentInfos.size() != 1)
            {
                logger.error("Content info get from data base empty or not unique. contentType = " + contentType
                        + " ,nodeId = " + nodeId + " ,partNo =" + partNo);
                continue;
            }
            multiNodeContent.put(nodeId, contentInfos.get(0));
        }
        
        return multiNodeContent;
    }
    
    public Map<String, String> getContentInfo(String contentType, String nodeId, String partNo) throws CSException
    {
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNo))
        {
            throw new CSException("The input param is null.");
        }
        return nasCSWSManager.getContentInfo(contentType, nodeId, partNo);
    }
    
    public Map<String, List<Map<String, String>>> getContentInfos(String contentType, List<String> nodeIds)
        throws CSException
    {
        if (nodeIds == null || nodeIds.isEmpty())
        {
            throw new CSException("The input param is null.");
        }
        
        return nasCSWSManager.getMultiNodesContentInfos(contentType, nodeIds, "");
    }
    
    public List<Map<String, String>> getContentInfosByPartNoType(String contentType, String nodeId, String partNoPrefix)
        throws CSException
    {
        if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(partNoPrefix))
        {
            throw new CSException("The input param is null.");
        }
        return nasCSWSManager.getContentInfos(contentType, nodeId, partNoPrefix, null);
    }
    
    public Map<String, String> getContentsMd5(String contentType, String nodeId) throws CSException
    {
        if (StringUtils.isBlank(nodeId))
        {
            logger.error("In delAsyncPublishContent, input param null,contentType=" + contentType, " ,nodeId=" + nodeId);
            throw new CSException("The input param is null.");
        }
        
        return nasCSWSManager.getContentsMd5(contentType, nodeId);
    }
}

