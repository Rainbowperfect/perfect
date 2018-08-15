1.在t_node_field表中增加文件扩展名字段，所有场景通过判断t_node_field字段来进行浏览处理，字段key为： suffix, value为文件扩展名，例如：pdf类型文件，后缀名为：pdf，以此类推，注意事项：1、后缀名比较不区分大小写，兼容.doc和.DOC的场景；2、通过MarkDown上传的交付件和服务组件通过t_cs_basic_info表中的文件名截取后缀名。
2.历史数据中，nodeType=0或者nodeType=2是IDP文档，刷新后缀名为idp，nodeType=3是WEB在线浏览文档，刷新后缀名为html，这些可通过写mysql语句刷新，nodeType=1的是附件上传根据实际文件名后缀刷新
3.创建文档入口需要修改的地方： 批量导入部分创建node需要根据文件名的扩展名设置suffix
项目文档批量导入部分创建node需要根据文件扩展名设置suffix
markdown上传交付件组件cbb代码需要修改，根据文件扩展名设置suffix
创建任务交付件类型需要创建node，根据文件扩展名设置suffix
创建任务idp类型需要新增创建node，根据文件扩展名设置suffix
4.前端页面，包括：知识库，项目文档，搜索，交付件需要修改通过判断suffix来进行跳转，如果是idp，走原有逻辑跳转到idp页面
5.documentwebsite，projcetwebsite中，/document/docsearch/doc/query/page接口，/project/deliverable/query/page接口，/project/document/getDocumentList接口，需要修改，给前端页面增加返回suffix字段



•	创建IDP写作任务
创建IDP写作任务时，修改TaskExtensionController类的createOrUpdateNode方法，保存文档后缀名到文档扩展属性表。



•	在线写作
在线写作对应的都是IDP文档（nodeType=0），调用接口Node node = contentManagementService.getNodeById(nid);获取文档扩展属性信息（node.getFieldValues()），然后在原有文档扩展属性基础上，添加文件后缀扩展属性suffix=idp，最后调用批量更新文档扩展属性接口contentManagementService.updateNodeFieldBatch(sysuid, nodes);更新文档扩展属性。注意事项：历史数据在线写作文档允许上传附件，这些附件需要下载到本地，然后手动在IDP在线写作页面通过附件上传方式上传。IDP在线写作附件支持格式： txt、iso、html、exe、rm、avi、tmp、mdf、mid、chm.jar 、vsdx、xmind等。单个文件大小不能超过15M。附件最多支持50个。
•	Word导入
Word导入生的文档全部是IDP文档（nodeType=2），刷数据方式通在线写作，将suffix设置为idp，注意事项同在线写作。
•	非Word发布
非Word发布属于附件上传（nodeType=1）,一篇文档对应多个附件的数据，目前生产环境共有7条数据，这部分数据暂时不考虑刷数据，这些数据与文档作者沟通，重新通过批量上传导入。其他单个附件的非Word文档直接通过CS表中保存的附件名截取获取的对应的后缀名，保存到t_node_field表中。通过CS接口contentStorageService.getContentInfo("DT-DOC", nid, "2001");查询文档附件信息，截取返回的附件名字段，获取到文档后缀名，调用接口Node node = contentManagementService.getNodeById(nid);获取文档扩展属性信息（node.getFieldValues()），然后在原有文档扩展属性基础上，添加文件后缀扩展属性，添加文件后缀扩展属性suffix=附件后缀名，最后调用批量更新文档扩展属性接口contentManagementService.updateNodeFieldBatch(sysuid, nodes);更新文档扩展属性。
•	Web在线浏览
Web在线浏览文档（nodeType=3），调用接口Node node = contentManagementService.getNodeById(nid);获取文档扩展属性信息（node.getFieldValues()），然后在原有文档扩展属性基础上，添加文件后缀扩展属性suffix=web，最后调用批量更新文档扩展属性接口contentManagementService.updateNodeFieldBatch(sysuid, nodes);更新文档扩展属性。注意事项：web在线浏览文档的图片和CSS文件保存在资源服务里面，需要在Java代码进行转换处理，这里需要跟html文件区分开。
•	批量导入
知识库批量导入文档（nodeType=1），数据刷新同非Word发布；项目文档批量导入文档（mid=DT-PDOC，fileType不为空），数据刷新方式同非Word发布。注意事项：知识库批量导入文档的mid=DT-DOC，项目文档批量导入文档mid=DT-PDOC。
•	任务交付件
IDP类型交付件（交付件类型type=1），会创建IDP文档，需要通过交付件表中的nodeid字段，调用接口Node node = contentManagementService.getNodeById(nid);获取文档扩展属性信息（node.getFieldValues()），然后在原有文档扩展属性基础上，添加文件后缀扩展属性suffix=idp，最后调用批量更新文档扩展属性接口contentManagementService.updateNodeFieldBatch(sysuid, nodes);更新文档扩展属性。
附件类型交付件（交付件类型type=2），不会生成Node，点击在线浏览的时候，先弹窗显示所有附件，点击单个附件的时候，查询t_cs_basic_info表获取文件后缀，根据后缀名判断在线浏览方式。


创建idp类任务，设置suffix字段	创建idp类任务，要创建node,同时根据文件扩展名设置suffix字段
创建任务交付件类型创建node,设置suffix	创建任务交付件类型创建node,同时根据文件扩展名设置suffix字段
