<template>
  <!-- 虚拟队列的弹窗 -->
  <el-dialog
    class="queue-dialog"
    v-model="dialogFormVisible"
    style="width: 70%; height: 50%"
  >
    <!-- 虚拟队列表格 -->
    <el-table :data="vQueueData">
      <el-table-column
        v-for="item in vQueueHeader"
        :key="item.prop"
        :label="item.label"
        :prop="item.prop"
      />
      <el-table-column fixed="right" width="300px" label="操作">
        <template #default="scope">
          <el-button
            size="small"
            @click="openMessagePageDiaLog(scope.$index, scope.row)"
            >队列消息</el-button
          >
          <el-button
            size="small"
            @click="openSendMessageDialog(scope.$index, scope.row)"
            >发送消息</el-button
          >
          <el-button
            type="danger"
            size="small"
            @click="clickPullMessage(scope.$index, scope.row)"
            >拉取消息</el-button
          >
        </template>
      </el-table-column>
    </el-table>
    <template #footer> </template>
    <!-- 虚拟队列内部发送消息的弹窗 -->
    <el-dialog
      v-model="sendMessageDialog"
      width="70%"
      title="发送消息"
      append-to-body
      @close="closeSendMessageDialog()"
    >
      <el-form
        ref="sendMessageFormRef"
        :model="sendMessageForm"
        label-width="120px"
      >
        <el-form-item label="虚拟队列" prop="queue" required>
          <el-input v-model="sendMessageForm.queue" />
        </el-form-item>
        <el-form-item label="标签" prop="tag">
          <el-input v-model="sendMessageForm.tag" />
        </el-form-item>
        <el-form-item label="消息体" prop="body" required>
          <!-- 最小尺寸4 -->
          <el-input
            v-model="sendMessageForm.body"
            type="textarea"
            :autosize="{ minRows: 4 }"
          />
        </el-form-item>
        <el-form-item
          v-show="queueQueryPage.delayState"
          label="消费时间"
          prop="consumeTime"
        >
          <div class="block">
            <el-date-picker
              v-model="sendMessageForm.consumeTime"
              type="datetime"
              placeholder="请选择消费时间"
            />
          </div>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="onSubmit">发送</el-button>
        </el-form-item>
      </el-form>
    </el-dialog>
    <!-- 消息列表弹窗 -->
    <el-dialog
      v-model="messageListDiaLog"
      width="70%"
      title="消息列表"
      append-to-body
    >
      <!-- 消息列表 -->
      <el-table :data="messageList">
        <el-table-column
          v-for="item in messageHeader"
          :key="item.prop"
          :label="item.label"
          :prop="item.prop"
        />
        <el-table-column fixed="right" width="200px" label="操作">
          <template #default="scope">
            <el-button
              size="small"
              color="#009688"
              @click="openMessageDetail(scope.$index, scope.row)"
              >查看</el-button
            >
            <el-button
              type="danger"
              size="small"
              @click="clickMessageDelete(scope.$index, scope.row)"
              >删除</el-button
            >
          </template>
        </el-table-column>
      </el-table>
      <el-pagination
        background
        layout="prev, pager, next"
        :total="messageQueryPage.total"
        :current-page="messageQueryPage.page"
        @update:current-page="updatePage"
      />

      <el-dialog
        v-model="messageDetailDiaLog"
        width="80%"
        title="消息详情"
        append-to-body
      >
        <el-input
          v-model="messageBody"
          :autosize="{ minRows: 4, maxRows: 20 }"
          type="textarea"
          placeholder="Please input"
          disabled
        />
      </el-dialog>
    </el-dialog>
  </el-dialog>

  <!-- 标签页 -->
  <el-tabs type="card" class="demo-tabs" @tab-click="handleTabClick">
    <el-tab-pane label="普通队列" name="queue"></el-tab-pane>
    <el-tab-pane label="延时队列" name="delayQueue"></el-tab-pane>
  </el-tabs>
  <!-- 队列表格 -->
  <div class="queue-table">
    <el-table
      :data="queueTableData"
      style="width: 100%"
      height="700px"
      border
      :header-cell-style="{
        background: '#fff !important',
        color: '#000 !important',
        'text-align': 'center',
        'font-size': '15px',
        'font-weight': '700',
      }"
      :row-cell-style="{
        'text-align': 'center',
      }"
      :cell-style="{
        'text-align': 'center',
      }"
    >
      <!-- <el-table-column type="selection" width="55" /> -->
      <el-table-column
        v-for="item in queueTableHeader"
        :key="item.prop"
        :label="item.label"
        :prop="item.prop"
      />
      <!-- 队列操作 -->
      <el-table-column fixed="right" width="200px" label="操作">
        <template #default="scope">
          <el-button size="default" @click="openDetail(scope.$index, scope.row)"
            >详情</el-button
          >
          <el-button
            type="danger"
            size="default"
            @click="clickQueueDelete(scope.$index, scope.row)"
            >删除</el-button
          >
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      background
      layout="prev, pager, next"
      :total="queueQueryPage.total"
      :current-page="queueQueryPage.page"
      @update:current-page="updateQueuePage"
    />
  </div>
</template>

<style>
.el-table .cell {
  color: #000 !important;
}

.el-dialog__headerbtn {
  color: #f15f00;
}
</style>
<script lang="ts" setup>
import "../../assets/scss/common.scss";
import { ElMessageBox } from "element-plus";
import { getCurrentInstance, onMounted, reactive, ref } from "vue";
import "element-plus/theme-chalk/el-message-box.css";
import type { TabsPaneContext } from "element-plus";
import elMessage from "../../util/message";

const { proxy } = getCurrentInstance() as any;

let dialogFormVisible = ref(false);

let action = ref("add");

interface Queue {
  /**
   * 队列名称
   */
  queueName: string;

  /**
   * 消费失败重试次数
   */
  retryMax: number;

  /**
   * 重试时间间隔
   */
  retryInterval: string;

  /**
   * ack模式
   */
  ackMode: string;

  /**
   * 消费者 此处的消费对应的是每个队列的消费者。假如有5个队列。就会有5个线程池。分别最小消费者是1. 会被注解redis监听替换
   */
  concurrency: number;

  /**
   * 最大消费者
   */
  maxConcurrency: number;

  /**
   * 是否延时队列
   */
  delayState: boolean;

  /**
   * 虚拟队列数量
   */
  virtual: number;

  /**
   * 队列最大尺寸
   */
  queueMaxSize: number;
}

interface VQueue {
  /**
   * 队列名称
   */
  queueName: string;
  /**
   * 实时消息条数
   */
  size: number;
}
interface Message {
  id: number;
  /**
   * 队列名称
   */
  key: string;

  tag: string;

  consumeTime: string;
}

const queueTableData = ref<Queue[]>([]);
const vQueueData = ref<VQueue[]>([]);

const messageList = ref([
  {
    id: "",
  },
  {
    key: "",
  },
  {
    tag: "",
  },
  {
    consumeTime: "",
  },
]);
const messageHeader = reactive([
  {
    prop: "id",
    label: "消息id",
  },
  {
    prop: "key",
    label: "消息键",
  },
  {
    prop: "tag",
    label: "标签",
  },
  {
    prop: "consumeTime",
    label: "消费时间",
  },
]);

const vQueueHeader = reactive([
  {
    prop: "queueName",
    label: "虚拟队列名称",
  },
  {
    prop: "size",
    label: "实时消息数量",
  },
]);

const queueTableHeader = reactive([
  {
    prop: "queueName",
    label: "队列名称",
  },
  {
    prop: "retryMax",
    label: "消费失败重试次数",
  },
  {
    prop: "retryInterval",
    label: "重试时间间隔",
  },
  {
    prop: "ackMode",
    label: "ack模式",
  },
  {
    prop: "concurrency",
    label: "并发消费者",
  },
  {
    prop: "maxConcurrency",
    label: "最大并发消费者",
  },
  {
    prop: "virtual",
    label: "虚拟队列数量",
  },
  {
    prop: "queueMaxSize",
    label: "队列容量",
  },
]);

const sendMessageForm = reactive({
  queue: "",
  tag: "",
  body: "",
  consumeTime: 0,
});

const messageBody = ref();
const sendMessageDialog = ref(false);
const messageListDiaLog = ref(false);
const messageDetailDiaLog = ref(false);

const queue = ref<Queue[]>([]);
const delayQueue = ref<Queue[]>([]);

const onSubmit = async () => {
  const messageRequestBody = {
    ...sendMessageForm,
  };
  messageRequestBody.consumeTime = new Date(
    messageRequestBody.consumeTime
  ).getTime();
  let res;
  if (queueQueryPage.delayState) {
    res = await sendTimingMessage(messageRequestBody);
  } else {
    res = await sendMessage(messageRequestBody);
  }
  console.log("sdasd " + res);
  if (res === false) {
    elMessage.error("消息体格式错误,请输入普通字符串或正确的json");
  } else {
    elMessage.success();
    proxy.$refs.sendMessageFormRef.resetFields();
    sendMessageDialog.value = false;
  }
};

// 普通队列和延时队列的标签
const handleTabClick = (tab: TabsPaneContext, event: Event) => {
  if (tab.props.name == "queue") {
    queueQueryPage.delayState = false;
  } else {
    queueQueryPage.delayState = true;
  }
  getQueueList(queueQueryPage);
};

//关闭发送表单前的回调
const closeSendMessageDialog = () => {
  proxy.$refs.sendMessageFormRef.resetFields();
};

const updateQueuePage = (page) => {
  queueQueryPage.page = page;
  getQueueList(queueQueryPage);
};

const updatePage = (page) => {
  messageQueryPage.page = page;
  messagePage(messageQueryPage);
};

const openMessageDetail = (index, row) => {
  messageBody.value = row.body;
  messageDetailDiaLog.value = true;
};

// 打开详情窗口
const openDetail = (index, row) => {
  dialogFormVisible.value = true;
  console.log(row);
  getVQueueDetail({ queueName: row.queueName, virtual: row.virtual });
};

const getVQueueDetail = async (queue) => {
  let res = await proxy.$api.mq.vQueueList(queue);
  vQueueData.value = res;
  console.log(res);
};
const openSendMessageDialog = (index, row) => {
  sendMessageDialog.value = true;
  sendMessageForm.queue = row.queueName;
};

const openMessagePageDiaLog = (index, row) => {
  messageListDiaLog.value = true;
  messageQueryPage.virtualQueueName = row.queueName;
  messagePage(messageQueryPage);
};

// 发送消息
const sendMessage = async (message) => {
  return await proxy.$api.mq.sendMessage(message);
};

// 发送定时消息
const sendTimingMessage = async (message) => {
  return await proxy.$api.mq.sendTimingMessage(message);
};

// 分页获取消息
const messagePage = async (messageQueryPage) => {
  let res = await proxy.$api.mq.pageMessage(messageQueryPage);
  messageList.value = res.list;
  messageQueryPage.total = res.total;
};

const deleteMessage = async (message) => {
  let res = await proxy.$api.mq.deleteMessage(message);
  messagePage(messageQueryPage);
};
const publishPullMessage = async (vQueue) => {
  const param = {
    vQueue: vQueue,
  };
  let res = await proxy.$api.mq.publishPullMessage(param);
  elMessage.success();
};
const deleteQueue = async (queueName) => {
  const param = {
    queue: queueName,
  };
  let res = await proxy.$api.mq.deleteQueue(param);
  getQueueList(queueQueryPage);
};

const clickPullMessage = async (index, row) => {
  publishPullMessage(row.queueName);
};
const clickQueueDelete = async (index, row) => {
  ElMessageBox.confirm("你确定删除消息队列吗?", {
    confirmButtonText: "确定",
    cancelButtonText: "取消",
  })
    .then(() => {
      deleteQueue(row.queueName);
      elMessage.success();
    })
    .catch(() => {});
};

const messageQueryPage = reactive({
  page: 1,
  size: 10,
  total: 0,
  virtualQueueName: "",
});

const queueQueryPage = reactive({
  page: 1,
  size: 10,
  delayState: false,
  total: 0,
});

const clickMessageDelete = (index, row) => {
  ElMessageBox.confirm("你确定删除消息吗?", {
    confirmButtonText: "确定",
    cancelButtonText: "取消",
  })
    .then(() => {
      deleteMessage(row);
      elMessage.success();
    })
    .catch(() => {});
};

onMounted(() => {
  getQueueList(queueQueryPage);
});

// 获取队列分页
const getQueueList = async (params) => {
  let res = await proxy.$api.mq.queueList(params);

  console.log(params);
  res.list.forEach((r) => {
    r.ackMode = r.ackMode == "maual" ? "手动" : "自动";
    r.retryInterval = r.retryInterval + "ms";
  });

  console.log(res.total);
  queueQueryPage.total = res.total;
  queueTableData.value = res.list;
  console.log("abc" + queueTableData.value);
};
</script>
