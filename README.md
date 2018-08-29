完成版
<template>
    <div class = "overview">
        <div class = "doc_write">
            <div class = "doc_write_task">
                <p class = "title_p">我的待办</p>
                <div  class="lable">
                    <span class="lable_document" @click="isdocument=true" v-bind:class="{'active':isdocument}">文档</span>
                    <span class="lable_failuremode" @click="isdocument=false" v-bind:class="{'active':!isdocument}">故障模式</span>
                </div>    
                <div class = "docTaskWait" v-if="isdocument">
                    <div class = "fold"  @click = "showWrite = !showWrite;showApproval=false" v-bind:class="{'rotate':!showWrite}">
                        <img src="../../assets/xdesigner/unfold.png"></img>
                        <p>设计任务</p>
                        <span class = "count">{{total}}</span>
                    </div>
                    <div class="task_table table" v-show="showWrite">
                        <el-table
                            :data="taskList"
                            ref="multipleTable"
                            style="width: 100%"
                            empty-text=" "
                            border
                            max-height="632">
                            <el-table-column
                              label="任务名称">
                              <template slot-scope="scope">
                                 <el-popover trigger="hover" placement="right-end">
                                    <p> {{ scope.row.taskName }}</p>
                                    <div slot="reference" class="name-wrapper">
                                        <!--<span @click="viewDoc(scope.row)">-->
                                        <!--跳转入在线写作页面-->
                                        <span><a  :href="taskUrl + scope.row.taskId" target="_blank">{{ scope.row.taskName }}</a></span>
                                        
                                    </div>
                                </el-popover>
                              </template>
                            </el-table-column>
                            <el-table-column
                                label="所属项目">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ projectNameMap[scope.row.taskId]}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>
                                                <a :href="'/project/detail/' + projectIdMap[scope.row.taskId]">{{projectNameMap[scope.row.taskId]}}</a>
                                            </span>
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column
                                label="交付件名称">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ scope.row.deliverableName}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>
                                                <a :href="deliverableUrl + scope.row.deliverableId + '?status=' + scope.row.deliverableStatus" target="_blank">{{scope.row.deliverableName}}</a>
                                            </span>
                                            
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column
                                label="任务类型">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ scope.row.taskType}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>{{ scope.row.taskType}}</span>
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column
                                label="作者列表">
                                <template slot-scope="scope">  
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ scope.row.ownerCommonName}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>{{ scope.row.ownerCommonName}}</span>
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column
                                label="计划完成时间">
                                <template slot-scope="scope">  
                                    {{ scope.row.finishTime | onlyDay }}
                                </template>
                            </el-table-column>
                           
                        </el-table>
                        <pagination :total="total" :pageItem="pageItem"  @pagechange = "pageChange"  ref = "pag" ></pagination>
                    </div>
                   
                    <!--表单切换的叶签-->
                    <div class="search_div" >
                            <div class="btnBox">
                                <button v-bind:class="[!toggleViewShows?'unfinishTaskBtns':'historyRecordBtns']" @click="historyRecords">历史记录</button>
                                <button v-bind:class="[toggleViewShows?'unfinishTaskBtns':'historyRecordBtns']" @click="currentTasks">当前待办</button>
                            </div>
                    </div>

           <div class="currentTasks" v-if="toggleViewShows">
                    <div class = "fold"  @click = "showApproval = !showApproval;showWrite=false" v-bind:class="{'rotate':!showApproval}">
                        <img src="../../assets/xdesigner/unfold.png"></img>
                        <p>审批任务</p>
                        <span class = "count">{{approvalTotal}}</span>
                    </div>
                    <div class="task_table table" v-show="showApproval">
                        <el-table
                            :data="approvalList"
                            ref="multipleTable"
                            style="width: 100%"
                            empty-text=" "
                            border
                            max-height="632">
                            <el-table-column
                              label="任务编号" width='200'>
                              <template slot-scope="scope" >
                                    <span><a  :href="scope.row.checkUrl" target="_blank">{{scope.row.businessKey}}</a></span> 
                              </template>
                            </el-table-column>
                            <el-table-column
                              label="任务名称">
                              <template slot-scope="scope">
                                 <el-popover trigger="hover" placement="right-end">
                                    <p> {{ scope.row.taskName }}</p>
                                    <div slot="reference" class="name-wrapper">
                                        <span>{{scope.row.taskName}}</span> 
                                    </div>
                                </el-popover>
                              </template>
                            </el-table-column>

                            <el-table-column
                                label="任务状态" width='100'>
                                <template slot-scope="scope">
                                    <span>{{(scope.row.status,scope.row.statusType) | statusType(scope.row.status,scope.row.statusType)}}</span>
                                </template>
                            </el-table-column>

                            <el-table-column
                                label="任务审批类型" width='150'>
                                <template slot-scope="scope">
                                    <span>{{scope.row.taskType}}</span>
                                </template>
                            </el-table-column>

                            <el-table-column
                                label="提交人">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ scope.row.initiator}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>
                                                {{ scope.row.initiator}}
                                            </span>
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column
                                label="提交时间" width='100'>
                                <template slot-scope="scope">
                                    <span>{{ scope.row.initTime | onlyDay}}</span>
                                </template>
                            </el-table-column>
                            <el-table-column
                                label="停留时间" width='100'>
                            <template slot-scope="scope">
                                <el-popover trigger="hover" placement="right-end"><p>{{ scope.row.currentTask.numOfdays | toDay}}</p>
                                    <div slot="reference" class="name-wrapper">
                                        <span>{{ scope.row.currentTask.numOfdays | toDay}}</span>
                                    </div>
                                </el-popover>
                          </template>
                            </el-table-column>  
                        </el-table>
                        <pagination :total="approvalTotal" :pageItem="pageItem"  @pagechange = "approvalPageChange"  ref = "approval" ></pagination>
                    </div>
                </div>
             </div>

               <div class="historyRecord" v-if="!toggleViewShows && isdocument">
                    <div class = "fold"  @click = "showApproval = !showApproval;showWrite=false" v-bind:class="{'rotate':!showApproval}">
                        <img src="../../assets/xdesigner/unfold.png"></img>
                             <p>我的待办历史记录</p>
                             <span class = "count">{{ approvalHistoryTotal }}</span>
                         </div>

                    <div class="task_table table" v-show="showApproval">
                        <el-table
                            :data="approvaHistorylList"
                            ref="multipleTable"
                            style="width: 100%"
                            empty-text=" "
                            border
                            max-height="632">
                            
                         <el-table-column
                              label="任务编号" width='200'>
                              <template slot-scope="scope" >
                                    <span><a  :href="scope.row.checkUrl" target="_blank">{{scope.row.businessKey}}</a></span> 
                              </template>
                            </el-table-column>

                        <el-table-column
                                label="标题">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ scope.row.businessSubject }}</p>
                                        <div slot="reference" class="name-wrapper">
                                            {{ scope.row.businessSubject}}
                                        </div>
                                    </el-popover>
                                </template>
                        </el-table-column>

                        <el-table-column
                            label="任务状态" width='100'>
                            <template slot-scope="scope">
                                <span>{{(scope.row.status,scope.row.statusType) | statusType(scope.row.status,scope.row.statusType)}}</span>
                            </template>
                         </el-table-column>

                        <el-table-column
                            label="任务审批类型" width='150'>
                            <template slot-scope="scope">
                                <span>{{scope.row.taskType}}</span>
                            </template>
                        </el-table-column>

                        <el-table-column
                            label="提交人">
                            <template slot-scope="scope">
                                <el-popover trigger="hover" placement="left">
                                    <p> {{ scope.row.initiator}}</p>
                                    <div slot="reference" class="name-wrapper">
                                        <span>{{ scope.row.initiator}}</span>
                                    </div>
                                </el-popover>
                            </template>
                        </el-table-column>

                        <el-table-column
                            label="提交时间" width='100'>
                            <template slot-scope="scope">
                                <span>{{ scope.row.initTime | onlyDay}}</span>
                            </template>
                        </el-table-column>
                     </el-table>

                    <pagination :total="approvalHistoryTotal" :pageItem="pageItem"  @pagechange = "approvalHistoryPageChange"  ref = "evalutionPages" ></pagination>
                </div>
               </div>
            </div>
           
                <div class = "docTaskWait" v-if="!isdocument">
                    <fmwait> </fmwait>
                </div>    
             </div>

            <div class = "myProject">
                <div class = "line">
                    <p class = "title_p">我的项目</p>
                    <span class = "more"><a href="project/list">更多</a></span>
                </div>
                <!-- <div class = "projectDetail" v-for="project in projectList">
                    <span >
                        <a :href="'/project/detail/' + project.pid">{{project.name}}</a>
                    </span>
                    <span>{{project.description}}</span>
                    <div class = "projectcount">
                        <p>任务数:</p>
                        <p class="number"><u>{{project.taskCount}}</u></p>
                        <p>交付件数:</p>
                        <p class = "number"><u>{{project.fileCount}}</u></p>
                        <p>成员数:</p>
                        <p class = "number"><u>{{project.memberCount}}</u></p>
                    </div>
                    
                    <div class = "line"></div>
                </div> -->
                <div class="task_table table">
                        <el-table
                            :data="projectList"
                            ref="multipleTable"
                            style="width: 100%"
                            border
                            max-height="632">
                            <el-table-column
                              label="项目名称">
                              <template slot-scope="scope">
                                 <el-popover trigger="hover" placement="right-end">
                                    <p> {{ scope.row.name }}</p>
                                    <div slot="reference" class="name-wrapper">
                                       <span>
                                            <a :href="'/project/detail/' + scope.row.pid">{{scope.row.name}}</a>
                                        </span>
                                    </div>
                                </el-popover>
                              </template>
                            </el-table-column>
                            <el-table-column
                                label="摘要">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ scope.row.description}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>{{scope.row.description}}</span>
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column width="100"
                                label="任务数">
                                <template slot-scope="scope">
                                    <a :href="'/project/detail/' + scope.row.pid">{{ scope.row.taskCount}}</a>
                                </template>
                            </el-table-column>
                            <el-table-column width="100"
                                label="交付件数">
                                <template slot-scope="scope">
                                    <a :href="'/project/deliverable/list?projectId=' + scope.row.pid">{{ scope.row.fileCount}}</a>
                                </template>
                            </el-table-column>
                            <el-table-column width="100"
                                label="成员数">
                                <template slot-scope="scope">  
                                    <a :href="'/project/member?projectId=' + scope.row.pid">{{ scope.row.memberCount}}</a>
                                </template>
                            </el-table-column>
                            <el-table-column width="150"
                                label="创建时间">
                                <template slot-scope="scope">  
                                    {{ scope.row.createTime | onlyDay }}
                                </template>
                            </el-table-column>
                           
                        </el-table>
                    </div>
                </div>
            <msgModal ref='msg'></msgModal>     
        </div>
    </div>
</template>

<script type="text/javascript">
import fmwait from '../indexwait/fmWait.vue'   
    export default{
        components:{fmwait},
        data(){
            return {
                taskList:[],
                total:0,
                pageItem:[{text:"5"},{text:"10"},{text:"20"}],
                showWrite:true,
                projectIdMap : {},
                projectNameMap : {},
                projectList : [],
                isdocument:true,
                taskUrl:"/project/task/transit/task/",//任务在线写作页面
                deliverableUrl:"/project/task/transit/deliverable/",//交付件在线浏览页面
                approvalList:[{name:'xxx'}],
                showApproval:false,
                approvalTotal:0,
                toggleViewShows:true,//切换待办任务和历史记录页面,默认显示待办页面
                approvaHistorylList:[],
                approvalHistoryTotal:0,
            }
        },

        created(){
            this.getProjectList();
        },
        methods:{

            viewDoc(task){
                let self = this;
                D.block();
                if(!task.nodeId){
                	window.open('/project/detail/' + self.projectIdMap[task.taskId]);
                    return;
                }
                self.$http.get('/project/taskextension/isIdpDocCreated.json'+new Date().getTime()+'?taskId=' + task.taskId).then(function(res){
                    D.unblock();
                    if(res && res.data && res.data.flag){
                        window.open(res.data.tips);
                    }else{
                        var errorcode = res.data.errorcode;
                        if (errorcode == "dfx.project.task.create.idpdoc.progress" ) {
                            D.showMsg("IDP文档创建中，请稍后再试！");
                        } else if (errorcode == "dfx.project.task.finddoc.failed" ) {
                            D.showMsg("交付件文档信息查询失败，请联系管理员。");
                        } else if(errorcode == "dfx.project.task.create.idpdoc.failed") {
                            D.showMsg("idp文档创建失败，请联系管理员。");
                        //通过任务生成交付件或者链接没有配置默认展开回复
                        } else if(errorcode == "dfx.project.task.deliverableType.url.failed" || errorcode == "dfx.project.task.notfind.deliverabledoc") {
                        	window.open('/project/detail/' + self.projectIdMap[task.taskId]);
                        } else {
                            D.showMsg();
                        }
                    }
                }).catch(function(){
                    D.unblock();
                    D.showMsg();
                })
            },
            pageChange(obj){
                var offset = obj ? obj.offset : 0;
                var limit = obj ? obj.limit : 5;
                this.getMyTask(offset,limit);
            },
            getMyTask(offset,limit){
                let self = this;
                self.$http.get('/myspace/task/query/page.json?limit=' + limit + '&offset=' + offset).then(function(res){
                    if(res && res.data && res.data.head && res.data.head.flag){
                        self.taskList = res.data.body.taskList;
                        self.total = res.data.body.total;
                        self.projectNameMap = res.data.body.projectNameMap;
                        self.projectIdMap = res.data.body.projectIdMap;
                    }
                    else
                    {
                        D.showMsg();
                    }
                }).catch(function(){
                    D.showMsg();
                })
            },
            //获取组件库待办
            approvalPageChange(obj){
                var offset = obj ? obj.offset : 0;
                var limit = obj ? obj.limit : 5;
                this.getCompTaskList(offset,limit);
            },
            getCompTaskList(offset,limit){
                let self = this;
                var project = {"isAllProject" : "false"};
                self.$http.get('/appstore/workFlow/task/page.json?limit=' + limit + '&offset=' + offset).then(res=>{
                    res= res.data;
                    if(res.head.flag){
                        self.approvalList = res.body.processInstances;
                        self.approvalTotal = res.body.count;
                        self.approvalList.forEach(function(item){
                            if(item.processId.match('DFX_Component')){
                                self.$set(item,'checkUrl','/appstore/check/'+item.businessKey+'?instanceId='+item.currentTask.instanceId+'&commonId='+item.commonId);
                                self.$set(item,'statusType','appstore');
                                self.$set(item,'taskType','组件库审批');
                            }
                            if(item.processId.match('DFX_FailureMode')){
                                if(item.status =='14'){
                                    self.$set(item,'checkUrl','/failuremode/review/handle/'+item.commonId+'?role=reviewers');
                                }else{
                                    self.$set(item,'checkUrl','/failuremode/handle/check/'+item.businessKey+'?instanceId='+item.currentTask.instanceId);
                                }
                                self.$set(item,'taskType','故障模式审批');
                            }
                        })
                    }else{
                        D.showMsg();
                    }
                }).catch(function(){
                    D.showMsg();
                });     
            },

            getProjectList(){
                let self = this;
                var project = {"isAllProject" : "false"};
                self.$http.post('/myspace/projectManagement/getProjectList/page.json?limit=5&offset=0&orderBy=timeDesc'
                       ,project
                ).then(res=>{
                    D.unblock();

                    if(res&&res.data&&res.data.head&&res.data.head.flag){
                        self.projectList=res.data.body.projectList;
                    }else{
                        D.showMsg();
                    }
                }).catch(function(){
                    D.unblock();
                    D.showMsg();
                });     
            },
        //当前待办
        currentTasks(){
            var self=this;
            self.toggleViewShows=true;
        },
        //历史记录
        historyRecords(){
            var self=this;
            self.toggleViewShows=false;
        },  
        //获取组件库历史待办
        approvalHistoryPageChange(obj){
                var offset = obj ? obj.offset : 0;
                var limit = obj ? obj.limit : 5;
                this.getApprovalHistoryPageChange(offset,limit);
               
            },
        getApprovalHistoryPageChange(offset,limit){
            let self=this;
            var project = {"isAllProject" : "false"}; 
            this.$http.post('/appstore/workFlow/task/history.json?'+(new Date()).getTime()+"&offset="+offset+"&limit="+limit
            ).then(function(res){
                D.unblock();
                self.approvaHistorylList=res.data.body.processInstances;
                self.approvalHistoryTotal=res.data.body.count;
                //console.log("aaaaaaaa",self.approvalHistoryTotal);
                res= res.data;
                if(res.head.flag){
                        self.approvalList = res.body.processInstances;
                        self.approvalTotal = res.body.count;
                        self.approvalList.forEach(function(item){
                            if(item.processId.match('DFX_Component')){
                                self.$set(item,'checkUrl','/appstore/check/'+item.businessKey+'?instanceId='+item.currentTask.instanceId+'&commonId='+item.commonId);
                                self.$set(item,'statusType','appstore');
                                self.$set(item,'taskType','组件库审批');
                            }
                            if(item.processId.match('DFX_FailureMode')){
                                if(item.status =='14'){
                                    self.$set(item,'checkUrl','/failuremode/review/handle/'+item.commonId+'?role=reviewers');
                                }else{
                                    self.$set(item,'checkUrl','/failuremode/handle/check/'+item.businessKey+'?instanceId='+item.currentTask.instanceId);
                                }
                                self.$set(item,'taskType','故障模式审批');
                            }
                        })

                    }else{
                        D.showMsg();
                    }
            }).catch(function(){
                D.showMsg();
             });
            },
            
        },
    
    }
</script>

<style type="text/css" lang = "stylus" scoped>
        .overview{
            min-height: 500px;
            .doc_write{
                padding: 18px 25px;
            }
            .doc_write_task{
               border: 1px solid #e8e8e8;
               box-shadow: 0 1px 4px 0 rgba(0,0,0,0.20);
            }
            .docTaskWait{
                margin: 10px;
                //border:1px solid #e8e8e8;
            }
            .title_p{
                padding-top: 24px;
                font-size: 18px;
                font-weight: bold;
                padding-left: 15px;
                margin-bottom: 10px;
            }
            .lable{
                margin: 10px;
                padding-top: 15px;
                padding-bottom: 13px;
                border-bottom: 1px solid  #e8e8e8;
                .lable_document{
                    cursor: pointer;
                    margin-left: 15px;
                    padding-bottom: 10px;
                }
                .lable_failuremode{
                    cursor: pointer;
                    margin-left: 30px;
                    padding-bottom: 10px;
                }
            }
            .active{
                border-bottom: 3px  solid #2d2f33;
            }
            .fold{
                height: 40px;
                background-color: #E3E5E6;
                margin: 0px 0px 10px 0px;
                padding: 10px;
                img{
                    float: left;
                    margin-right: 15px;
                    margin-left: 5px;
                    position: relative;
                    top: 5px;
                }
                p{
                    display: inline-block;
                }
                span{
                    color:white;
                    display: inline-block;
                    height: 20px;
                    width: 20px;
                    background-color: #f95f5b;
                    border-radius: 12px;
                    text-align: center;
                    padding-top: 3px;
                    font-size: 12px;}

            }
            .rotate{
               background-color: #E3E5E6;
               margin: 0 0 10px 0;
               padding: 10px;
               img{
                transform: rotate(270deg);
                float: left;
                margin-right: 15px;
                position: relative;
                top: 5px;
                }
                p{
                    display: inline-block;
                }
                span{
                    color: white;
                    display: inline-block;
                    height: 20px;
                    width: 20px;
                    background-color: #f95f5b;
                    border-radius: 12px;
                    text-align: center;
                    padding-top: 3px;
                    font-size: 12px;
                }
            }
            .task_table{
                padding: 5px 10px 10px 10px;
                .operate_td{
                    .span_btn{
                        padding: 0 6px;
                        cursor: pointer;
                        display: inline-block;
                        width: 30px;
                        height: 30px;
                        line-height: 36px;
                        text-align: center;
                        border-radius: 50%;
                        background:fff;

                    }
                    .span_btn:hover{
                        background:rgba(0,0,0,0.1);
                    } 
                    .span_btn:active{
                        border-radius: 50%;
                        background:rgba(0,0,0,0.2);
                    } 
                    .name-wrapper{
                        display: inline-block; 
                        overflow: initial !important;
                    }
                }
                
            }
            .myProject{
                margin-top: 20px;
                border: 1px solid #f0f0f0;
                box-shadow: 0 1px 4px 0 rgba(0,0,0,0.20);
           
            }
            .more{
                float: right;
                padding-top: 24px;
                padding-right: 10px;
                font-size: 14px;
                color: #999;
            }
            .myProject .title_p{
                display: inline-block;
                width: 300px;
                padding-left: 0px;
                padding-top: 0px;
                margin-top: 14px;
            }
            .line{
                border-bottom: 1px solid #e8e8e8;
                margin: 10px 15px;

            }
            .projectDetail{  
                span{
                    padding-left: 15px;
                    padding-right: 15px;
                    display: block;
                    font-size: 12px;
                    margin-bottom: 18px;
                    min-height: 20px;
                }
                a{
                    text-decoration: none;
                    color: #0a9dce;
                    font-size: 14px;
                }
            }
            .projectcount{
                padding-left: 15px;
                font-size: 12px;
                margin-bottom: 4px;
                p{
                    display: inline-block;
                }
            }
            .number{
                padding-left: 14px;
                padding-right: 45px; 
                color: #4c4c4c;
            }
            .historyRecordBtns{
                font-family: PingFangSC-Regular;
                font-size: 12px;
                color: #000;
                letter-spacing: 0;
                text-align: center;
                width:90px;
                height:28px;
                background: #fff;
                border-radius: 0 2px 2px 0;
                float:right;
                border: 1px solid #ccc;
            }
            .search_div{
                height: 30px;
            }
            .unfinishTaskBtns {
                font-family: PingFangSC-Regular;
                font-size: 12px;
                color: #fff;
                letter-spacing: 0;
                text-align: center;
                width: 90px;
                height: 28px;
                background: #3d70b2;
                border-radius: 0 2px 2px 0;
                float: right;
                border: 1px solid #ccc;
            }           
        }
</style>

-------------当前-----------
<template>
    <div class="overview">
        <div class="doc_write">
            <div class="doc_write_task">
                <p class="title_p">我的待办</p>
                <div class="lable">
                    <span class="lable_document" @click="isdocument=true" v-bind:class="{'active':isdocument}">文档</span>
                    <span class="lable_failuremode" @click="isdocument=false" v-bind:class="{'active':!isdocument}">故障模式</span>
                </div>
                <div class="docTaskWait" v-if="isdocument">
                    <div class="fold" @click="showWrite = !showWrite;showApproval=false" v-bind:class="{'rotate':!showWrite}">
                        <img src="../../assets/xdesigner/unfold.png"></img>
                        <p>设计任务</p>
                        <span class="count">{{total}}</span>
                    </div>
                    <div class="task_table table" v-show="showWrite">
                        <el-table :data="taskList" ref="multipleTable" style="width: 100%" empty-text=" " border max-height="632">
                            <el-table-column label="任务名称">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p> {{ scope.row.taskName }}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <!--<span @click="viewDoc(scope.row)">-->
                                            <!--跳转入在线写作页面-->
                                            <span><a  :href="taskUrl + scope.row.taskId" target="_blank">{{ scope.row.taskName }}</a></span>

                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column label="所属项目">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ projectNameMap[scope.row.taskId]}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>
                                                <a :href="'/project/detail/' + projectIdMap[scope.row.taskId]">{{projectNameMap[scope.row.taskId]}}</a>
                                            </span>
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column label="交付件名称">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ scope.row.deliverableName}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>
                                                <a :href="deliverableUrl + scope.row.deliverableId + '?status=' + scope.row.deliverableStatus" target="_blank">{{scope.row.deliverableName}}</a>
                                            </span>

                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column label="任务类型">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ scope.row.taskType}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>{{ scope.row.taskType}}</span>
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column label="作者列表">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ scope.row.ownerCommonName}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>{{ scope.row.ownerCommonName}}</span>
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column label="计划完成时间">
                                <template slot-scope="scope">
                                    {{ scope.row.finishTime | onlyDay }}
                                </template>
                            </el-table-column>

                        </el-table>
                        <pagination :total="total" :pageItem="pageItem" @pagechange="pageChange" ref="pag"></pagination>
                    </div>

                    <!--表单切换的叶签-->
                    <div class="search_div">
                        <div class="btnBox">
                            <button v-bind:class="[!toggleViewShows?'unfinishTaskBtns':'historyRecordBtns']" @click="historyRecords">历史记录</button>
                            <button v-bind:class="[toggleViewShows?'unfinishTaskBtns':'historyRecordBtns']" @click="currentTasks">当前待办</button>
                        </div>
                    </div>
                    <div class="currentTasks" v-if="toggleViewShows">
                    <div class="fold" @click="showApproval = !showApproval;showWrite=false" v-bind:class="{'rotate':!showApproval}">
                        <img src="../../assets/xdesigner/unfold.png"></img>
                        <p>审批任务</p>
                        <span class="count">{{approvalTotal}}</span>
                    </div>
                    <div class="task_table table" v-show="showApproval">
                        <el-table :data="approvalList" ref="multipleTable" style="width: 100%" empty-text=" " border max-height="632">
                            <el-table-column label="任务编号" width='200'>
                                <template slot-scope="scope">
                                    <span><a  :href="scope.row.checkUrl" target="_blank">{{scope.row.businessKey}}</a></span>
                                </template>
                            </el-table-column>
                            <el-table-column label="任务名称">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p> {{ scope.row.businessSubject }}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>{{scope.row.businessSubject}}</span>
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column label="任务状态" width='100'>
                                <template slot-scope="scope">
                                    <span>{{(scope.row.status,scope.row.statusType) | statusType(scope.row.status,scope.row.statusType)}}</span>
                                </template>
                            </el-table-column>
                            <el-table-column label="任务审批类型" width='150'>
                                <template slot-scope="scope">
                                    <span>{{scope.row.taskType}}</span>
                                </template>
                            </el-table-column>
                            <el-table-column label="提交人">
                                <template slot-scope="scope">
                                    <el-popover trigger="hover" placement="right-end">
                                        <p>{{ scope.row.initiator}}</p>
                                        <div slot="reference" class="name-wrapper">
                                            <span>
                                                {{ scope.row.initiator}}
                                            </span>
                                        </div>
                                    </el-popover>
                                </template>
                            </el-table-column>
                            <el-table-column label="提交时间" width='100'>
                                <template slot-scope="scope">
                                    <span>{{ scope.row.initTime | onlyDay}}</span>
                                </template>
                            </el-table-column>
                            <el-table-column label="停留时间" width='100'>
                                <template slot-scope="scope">
                                    <span>{{scope.row.currentTask && scope.row.currentTask.numOfdays?scope.row.currentTask.numOfdays:0}}天</span>
                                </template>
                            </el-table-column>
                        </el-table>
                        <pagination :total="approvalTotal" :pageItem="pageItem" @pagechange="approvalPageChange" ref="approval"></pagination>
                    </div>
                    </div>

                 <div class="historyRecord" v-if="!toggleViewShows && isdocument">
                    <div class="fold" @click="showApproval = !showApproval;showWrite=false" v-bind:class="{'rotate':!showApproval}">
                        <img src="../../assets/xdesigner/unfold.png"></img>
                        <p>我的待办历史记录</p>
                        <span class="count">22</span>
                    </div>
                    <div class="CompotentHistoryVue" >
                        <CpHistory></CpHistory>
                    </div>
                </div>
                
            </div>
            
                <div class="docTaskWait" v-if="!isdocument">
                    <fmwait> </fmwait>
                </div>
            </div>
            <div class="myProject">
                <div class="line">
                    <p class="title_p">我的项目</p>
                    <span class="more"><a href="project/list">更多</a></span>
                </div>
                <!-- <div class = "projectDetail" v-for="project in projectList">
                    <span >
                        <a :href="'/project/detail/' + project.pid">{{project.name}}</a>
                    </span>
                    <span>{{project.description}}</span>
                    <div class = "projectcount">
                        <p>任务数:</p>
                        <p class="number"><u>{{project.taskCount}}</u></p>
                        <p>交付件数:</p>
                        <p class = "number"><u>{{project.fileCount}}</u></p>
                        <p>成员数:</p>
                        <p class = "number"><u>{{project.memberCount}}</u></p>
                    </div>
                    
                    <div class = "line"></div>
                </div> -->
                <div class="task_table table">
                    <el-table :data="projectList" ref="multipleTable" style="width: 100%" border max-height="632">
                        <el-table-column label="项目名称">
                            <template slot-scope="scope">
                                <el-popover trigger="hover" placement="right-end">
                                    <p> {{ scope.row.name }}</p>
                                    <div slot="reference" class="name-wrapper">
                                        <span>
                                            <a :href="'/project/detail/' + scope.row.pid">{{scope.row.name}}</a>
                                        </span>
                                    </div>
                                </el-popover>
                            </template>
                        </el-table-column>
                        <el-table-column label="摘要">
                            <template slot-scope="scope">
                                <el-popover trigger="hover" placement="right-end">
                                    <p>{{ scope.row.description}}</p>
                                    <div slot="reference" class="name-wrapper">
                                        <span>{{scope.row.description}}</span>
                                    </div>
                                </el-popover>
                            </template>
                        </el-table-column>
                        <el-table-column width="100" label="任务数">
                            <template slot-scope="scope">
                                <a :href="'/project/detail/' + scope.row.pid">{{ scope.row.taskCount}}</a>
                            </template>
                        </el-table-column>
                        <el-table-column width="100" label="交付件数">
                            <template slot-scope="scope">
                                <a :href="'/project/deliverable/list?projectId=' + scope.row.pid">{{ scope.row.fileCount}}</a>
                            </template>
                        </el-table-column>
                        <el-table-column width="100" label="成员数">
                            <template slot-scope="scope">
                                <a :href="'/project/member?projectId=' + scope.row.pid">{{ scope.row.memberCount}}</a>
                            </template>
                        </el-table-column>
                        <el-table-column width="150" label="创建时间">
                            <template slot-scope="scope">
                                {{ scope.row.createTime | onlyDay }}
                            </template>
                        </el-table-column>
                    </el-table>
                </div>
            </div>
            <msgModal ref='msg'></msgModal>
        </div>
    </div>
</template>

<script type="text/javascript">
    import fmwait from '../indexwait/fmWait.vue'
    import CpHistory from './CpHistory.vue'
    export default {
        components: { fmwait, CpHistory },
        data() {
            return {
                taskList: [],
                total: 0,
                pageItem: [{ text: "5" }, { text: "10" }, { text: "20" }],
                showWrite: true,
                projectIdMap: {},
                projectNameMap: {},
                projectList: [],
                isdocument: true,
                taskUrl: "/project/task/transit/task/",//任务在线写作页面
                deliverableUrl: "/project/task/transit/deliverable/",//交付件在线浏览页面
                approvalList: [{ name: 'xxx' }],
                toggleViewShows:true,//切换待办任务和历史记录页面,默认显示待办页面
                showApproval: false,
                approvalTotal: 1,
                approvalHistoryTotal: [],
            }
        },

        created() {
            this.getProjectList();
        },
        methods: {

            viewDoc(task) {
                let self = this;
                D.block();
                if (!task.nodeId) {
                    window.open('/project/detail/' + self.projectIdMap[task.taskId]);
                    return;
                }
                self.$http.get('/project/taskextension/isIdpDocCreated.json' + new Date().getTime() + '?taskId=' + task.taskId).then(function (res) {
                    D.unblock();
                    if (res && res.data && res.data.flag) {
                        window.open(res.data.tips);
                    } else {
                        var errorcode = res.data.errorcode;
                        if (errorcode == "dfx.project.task.create.idpdoc.progress") {
                            D.showMsg("IDP文档创建中，请稍后再试！");
                        } else if (errorcode == "dfx.project.task.finddoc.failed") {
                            D.showMsg("交付件文档信息查询失败，请联系管理员。");
                        } else if (errorcode == "dfx.project.task.create.idpdoc.failed") {
                            D.showMsg("idp文档创建失败，请联系管理员。");
                            //通过任务生成交付件或者链接没有配置默认展开回复
                        } else if (errorcode == "dfx.project.task.deliverableType.url.failed" || errorcode == "dfx.project.task.notfind.deliverabledoc") {
                            window.open('/project/detail/' + self.projectIdMap[task.taskId]);
                        } else {
                            D.showMsg();
                        }
                    }
                }).catch(function () {
                    D.unblock();
                    D.showMsg();
                })
            },
            pageChange(obj) {
                var offset = obj ? obj.offset : 0;
                var limit = obj ? obj.limit : 5;
                this.getMyTask(offset, limit);
            },
            getMyTask(offset, limit) {
                let self = this;
                self.$http.get('/myspace/task/query/page.json?limit=' + limit + '&offset=' + offset).then(function (res) {
                    if (res && res.data && res.data.head && res.data.head.flag) {
                        self.taskList = res.data.body.taskList;
                        self.total = res.data.body.total;
                        self.projectNameMap = res.data.body.projectNameMap;
                        self.projectIdMap = res.data.body.projectIdMap;
                    }
                    else {
                        D.showMsg();
                    }
                }).catch(function () {
                    D.showMsg();
                })
            },
            //获取组件库待办
            approvalPageChange(obj) {
                var offset = obj ? obj.offset : 0;
                var limit = obj ? obj.limit : 5;
                this.getCompTaskList(offset, limit);
            },
            getCompTaskList(offset, limit) {
                let self = this;
                var project = { "isAllProject": "false" };
                self.$http.get('/appstore/workFlow/task/page.json?limit=' + limit + '&offset=' + offset).then(res => {
                    res = res.data;
                    if (res.head.flag) {
                        self.approvalList = res.body.processInstances;
                        self.approvalTotal = res.body.count;
                        self.approvalList.forEach(function (item) {
                            if (item.processId.match('DFX_Component')) {
                                self.$set(item, 'checkUrl', '/appstore/check/' + item.businessKey + '?instanceId=' + item.currentTask.instanceId + '&commonId=' + item.commonId + '&type=' + item.processId.split(":")[0]);
                                self.$set(item, 'statusType', 'appstore');
                                self.$set(item, 'taskType', '组件库审批');
                            }
                            if (item.processId.match('DFX_FailureMode')) {
                                if (item.status == '14') {
                                    self.$set(item, 'checkUrl', '/failuremode/review/handle/' + item.commonId + '?role=reviewers');
                                } else {
                                    self.$set(item, 'checkUrl', '/failuremode/handle/check/' + item.businessKey + '?instanceId=' + item.currentTask.instanceId);
                                }
                                self.$set(item, 'taskType', '故障模式审批');
                            }
                            if (item.processId.match('DFX_FMDelete')) {
                                self.$set(item, 'checkUrl', '/failuremode/handle/delete/approval/' + item.businessKey + '/' + item.currentTask.instanceId);
                                self.$set(item, 'taskType', '故障模式审批');
                                self.$set(item, 'status', '待删除');
                            }
                        })

                    } else {
                        D.showMsg();
                    }
                }).catch(function () {
                    D.showMsg();
                });
            },


            getProjectList() {
                let self = this;
                var project = { "isAllProject": "false" };
                self.$http.post('/myspace/projectManagement/getProjectList/page.json?limit=5&offset=0&orderBy=timeDesc'
                    , project
                ).then(res => {
                    D.unblock();

                    if (res && res.data && res.data.head && res.data.head.flag) {
                        self.projectList = res.data.body.projectList;
                    } else {
                        D.showMsg();
                    }
                }).catch(function () {
                    D.unblock();
                    D.showMsg();
                });
            },
        //当前待办
        currentTasks(){
            var self=this;
            self.toggleViewShows=true;
        },
        //历史记录
        historyRecords(){
            var self=this;
            self.toggleViewShows=false;
         },  
        },

    }

</script>

<style type="text/css" lang="stylus" scoped>
    .overview {
        min-height: 500px;
        .doc_write {
            padding: 18px 25px;
        }
        .doc_write_task {
            border: 1px solid #e8e8e8;
            box-shadow: 0 1px 4px 0 rgba(0, 0, 0, 0.20);
        }
        .docTaskWait {
            margin: 10px;
            //border:1px solid #e8e8e8;
        }
        .title_p {
            padding-top: 24px;
            font-size: 18px;
            font-weight: bold;
            padding-left: 15px;
            margin-bottom: 10px;
        }
        .lable {
            margin: 10px;
            padding-top: 15px;
            padding-bottom: 13px;
            border-bottom: 1px solid #e8e8e8;
            .lable_document {
                cursor: pointer;
                margin-left: 15px;
                padding-bottom: 10px;
            }
            .lable_failuremode {
                cursor: pointer;
                margin-left: 30px;
                padding-bottom: 10px;
            }
        }
        .active {
            border-bottom: 3px solid #2d2f33;
        }
        .fold {
            height: 40px;
            background-color: #E3E5E6;
            margin: 0px 0px 10px 0px;
            padding: 10px;
            img {
                float: left;
                margin-right: 15px;
                margin-left: 5px;
                position: relative;
                top: 5px;
            }
            p {
                display: inline-block;
            }
            span {
                color: white;
                display: inline-block;
                height: 20px;
                width: 20px;
                background-color: #f95f5b;
                border-radius: 12px;
                text-align: center;
                padding-top: 3px;
                font-size: 12px;
            }
        }
        .rotate {
            background-color: #E3E5E6;
            margin: 0 0 10px 0;
            padding: 10px;
            img {
                transform: rotate(270deg);
                float: left;
                margin-right: 15px;
                position: relative;
                top: 5px;
            }
            p {
                display: inline-block;
            }
            span {
                color: white;
                display: inline-block;
                height: 20px;
                width: 20px;
                background-color: #f95f5b;
                border-radius: 12px;
                text-align: center;
                padding-top: 3px;
                font-size: 12px;
            }
        }
        .task_table {
            padding: 5px 10px 10px 10px;
            .operate_td {
                .span_btn {
                    padding: 0 6px;
                    cursor: pointer;
                    display: inline-block;
                    width: 30px;
                    height: 30px;
                    line-height: 36px;
                    text-align: center;
                    border-radius: 50%;
                    background: fff;
                }
                .span_btn:hover {
                    background: rgba(0, 0, 0, 0.1);
                }
                .span_btn:active {
                    border-radius: 50%;
                    background: rgba(0, 0, 0, 0.2);
                }
                .name-wrapper {
                    display: inline-block;
                    overflow: initial !important;
                }
            }
        }
        .myProject {
            margin-top: 20px;
            border: 1px solid #f0f0f0;
            box-shadow: 0 1px 4px 0 rgba(0, 0, 0, 0.20);
        }
        .more {
            float: right;
            padding-top: 24px;
            padding-right: 10px;
            font-size: 14px;
            color: #999;
        }
        .myProject .title_p {
            display: inline-block;
            width: 300px;
            padding-left: 0px;
            padding-top: 0px;
            margin-top: 14px;
        }
        .line {
            border-bottom: 1px solid #e8e8e8;
            margin: 10px 15px;
        }
        .projectDetail {
            span {
                padding-left: 15px;
                padding-right: 15px;
                display: block;
                font-size: 12px;
                margin-bottom: 18px;
                min-height: 20px;
            }
            a {
                text-decoration: none;
                color: #0a9dce;
                font-size: 14px;
            }
        }
        .projectcount {
            padding-left: 15px;
            font-size: 12px;
            margin-bottom: 4px;
            p {
                display: inline-block;
            }
        }
        .number {
            padding-left: 14px;
            padding-right: 45px;
            color: #4c4c4c;
        }
        .historyRecordBtns {
            font-family: PingFangSC-Regular;
            font-size: 12px;
            color: #000;
            letter-spacing: 0;
            text-align: center;
            width: 90px;
            height: 28px;
            background: #fff;
            border-radius: 0 2px 2px 0;
            float: right;
            border: 1px solid #ccc;
        }
        .search_div {
            height: 30px;
        }
        .unfinishTaskBtns {
            font-family: PingFangSC-Regular;
            font-size: 12px;
            color: #fff;
            letter-spacing: 0;
            text-align: center;
            width: 90px;
            height: 28px;
            background: #3d70b2;
            border-radius: 0 2px 2px 0;
            float: right;
            border: 1px solid #ccc;
        }
    }
</style>
------------历史----------
<template>
    <div class="CompotentHistoryVue">
        
        <div class="task_table table" v-show="showApproval">
            <el-table :data="approvaHistorylList" ref="multipleTable" style="width: 100%" empty-text=" " border max-height="632">

                <el-table-column label="任务编号" width='200'>
                    <template slot-scope="scope">
                        <span><a  :href="scope.row.checkUrl" target="_blank">{{scope.row.businessKey}}</a></span>
                    </template>
                </el-table-column>

                <el-table-column label="标题">
                    <template slot-scope="scope">
                        <el-popover trigger="hover" placement="right-end">
                            <p>{{ scope.row.businessSubject }}</p>
                            <div slot="reference" class="name-wrapper">
                                {{ scope.row.businessSubject}}
                            </div>
                        </el-popover>
                    </template>
                </el-table-column>

                <el-table-column label="任务状态" width='100'>
                    <template slot-scope="scope">
                        <span>{{(scope.row.status,scope.row.statusType) | statusType(scope.row.status,scope.row.statusType)}}</span>
                    </template>
                </el-table-column>

                <el-table-column label="任务审批类型" width='150'>
                    <template slot-scope="scope">
                        <span>{{scope.row.taskType}}</span>
                    </template>
                </el-table-column>

                <el-table-column label="提交人">
                    <template slot-scope="scope">
                        <el-popover trigger="hover" placement="left">
                            <p> {{ scope.row.initiator}}</p>
                            <div slot="reference" class="name-wrapper">
                                <span>{{ scope.row.initiator}}</span>
                            </div>
                        </el-popover>
                    </template>
                </el-table-column>

                <el-table-column label="提交时间" width='160'>
                    <template slot-scope="scope">
                        <span>{{ scope.row.initTime | onlyDay}}</span>
                    </template>
                </el-table-column>
            </el-table>
             <pagination :total="approvalHistoryTotal" :pageItem="pageItem"  @pagechange = "approvalHistoryPageChange"  ref = "evalutionPages" ></pagination>
        </div>
    </div>
</template>


<script>
    export default {

        data() {
            return {
                showApproval: true,
                approvaHistorylList: [],
                approvalHistoryTotal: 0,
                approvalTotal: 0,
                approvaHistorylList: [],
                pageItem: [{ text: "5" }, { text: "10" }, { text: "20" }],
            }
        },
        methods: {
            //获取组件库历史待办
            approvalHistoryPageChange(obj) {
                var offset = obj ? obj.offset : 0;
                var limit = obj ? obj.limit : 5;
                this.getApprovalHistoryPageChange(offset, limit);

            },
            getApprovalHistoryPageChange(offset, limit) {
                let self = this;
                var project = { "isAllProject": "false" };
                this.$http.post('/appstore/workFlow/task/history.json?' + (new Date()).getTime() + "&offset=" + offset + "&limit=" + limit
                ).then(function (res) {
                    D.unblock();
                    self.approvaHistorylList = res.data.body.processInstances;
                    self.approvalHistoryTotal = res.data.body.count;
                    //console.log("aaaaaaaa",self.approvalHistoryTotal);
                    res = res.data;
                    if (res.head.flag) {
                        self.approvalList = res.body.processInstances;
                        self.approvalTotal = res.body.count;
                        self.approvalList.forEach(function (item) {
                            if (item.processId.match('DFX_Component')) {
                                self.$set(item, 'checkUrl', '/appstore/check/' + item.businessKey + '?instanceId=' + item.currentTask.instanceId + '&commonId=' + item.commonId);
                                self.$set(item, 'statusType', 'appstore');
                                self.$set(item, 'taskType', '组件库审批');
                            }
                            if (item.processId.match('DFX_FailureMode')) {
                                if (item.status == '14') {
                                    self.$set(item, 'checkUrl', '/failuremode/review/handle/' + item.commonId + '?role=reviewers');
                                } else {
                                    self.$set(item, 'checkUrl', '/failuremode/handle/check/' + item.businessKey + '?instanceId=' + item.currentTask.instanceId);
                                }
                                self.$set(item, 'taskType', '故障模式审批');
                            }
                        })

                    } else {
                        D.showMsg();
                    }
                }).catch(function () {
                    D.showMsg();
                });
            },
        },
    }

</script>

<style>
    .CompotentHistoryVue {
        .task_table {
            padding: 5px 10px 10px 10px;
            .operate_td {
                .span_btn {
                    padding: 0 6px;
                    cursor: pointer;
                    display: inline-block;
                    width: 30px;
                    height: 30px;
                    line-height: 36px;
                    text-align: center;
                    border-radius: 50%;
                    background: fff;
                }
                .span_btn:hover {
                    background: rgba(0, 0, 0, 0.1);
                }
                .span_btn:active {
                    border-radius: 50%;
                    background: rgba(0, 0, 0, 0.2);
                }
                .name-wrapper {
                    display: inline-block;
                    overflow: initial !important;
                }
            }
        }
    }
</style>
