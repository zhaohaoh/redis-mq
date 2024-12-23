<!-- <template>暂未开放</template> -->
<template>
  <!-- 表格 -->
  <div class="queue-table">
    <el-table
      :data="tableData"
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
        v-for="item in tableHeader"
        :key="item.prop"
        :label="item.label"
        :prop="item.prop"
      />
      <el-table-column fixed="right" width="150px">
        <template #default="scope">
          <el-button
            size="small"
            color="#009688"
            @click="openMessageDetail(scope.$index, scope.row)"
            >查看</el-button
          >
        </template>
      </el-table-column>
    </el-table>
  </div>
  <el-dialog
    v-model="messageDetailDiaLog"
    width="80%"
    title="消息详情"
    append-to-body
  >
    <json-viewer
      :value="messageBody"
      :expand-depth="5"
      copyable
      boxed
      sort
      class="w-100%"
    ></json-viewer>
  </el-dialog>
</template>

<style>
.el-table .cell {
  color: #000 !important;
}
</style>
<script lang="ts" setup>
import "../../assets/scss/common.scss";
import { getCurrentInstance, onMounted, reactive, ref } from "vue";
import "element-plus/theme-chalk/el-message-box.css";

const { proxy } = getCurrentInstance() as any;

const messageDetailDiaLog = ref(false);

let messageBody = ref({});

const openMessageDetail = (index, row) => {
  messageBody.value = JSON.parse(row.body);
  messageDetailDiaLog.value = true;
};

const tableHeader = reactive([
  {
    prop: "id",
    label: "消息id",
  },
  {
    prop: "queue",
    label: "队列",
  },
  {
    prop: "virtualQueueName",
    label: "虚拟队列",
  },
  {
    prop: "key",
    label: "消息key",
  },
  {
    prop: "tag",
    label: "消息标签",
  },
  {
    prop: "createTime",
    label: "创建时间",
  },
  {
    prop: "updateTime",
    label: "更新时间",
  },

  {
    prop: "offset",
    label: "偏移量",
  },
  {
    prop: "executeScope",
    label: "执行的分数",
  },
  {
    prop: "status",
    label: "消费状态",
  },
]);

let tableData = ref([
  {
    id: "",
  },
  {
    queue: "",
  },
  {
    virtualQueueName: "",
  },
  {
    key: "",
  },
  {
    tag: "",
  },
  {
    createTime: "",
  },
  {
    updateTime: "",
  },
  {
    offset: 1,
  },
  {
    executeScope: 1,
  },
  {
    status: 1,
  },
]);

const pageInfo = reactive({
  page: 1,
  size: 10,
  total: 0,
  keyword: "",
  beginTime: null,
  endTime: null,
});

onMounted(() => {
  historyMessagePage(pageInfo);
});

// 获取消费者分页
const historyMessagePage = async (data) => {
  let res = await proxy.$api.mq.historyMessagePage(data);
  console.log("返回的结果：" + res.list);
  tableData.value = res.list;

  // pageInfo.total = parseInt(res.total);
};
</script>
