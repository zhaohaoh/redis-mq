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
      <!-- <el-table-column fixed="right" width="150px">
        <template #default="scope">
          <el-button size="small" @click="openEdit(scope.$index, scope.row)"
            >修改</el-button
          >
          <el-button
            type="danger"
            size="small"
            @click="handleDelete(scope.$index, scope.row)"
            >删除</el-button
          >
        </template>
      </el-table-column> -->
    </el-table>
  </div>
</template>

<style>
.el-table .cell {
  color: #000 !important;
}
</style>
<script lang="ts" setup>
import "../../assets/scss/common.scss";
import { ElMessageBox } from "element-plus";
import { getCurrentInstance, onMounted, reactive, ref } from "vue";
import "element-plus/theme-chalk/el-message-box.css";

import elMessage from "../../util/message";

const { proxy } = getCurrentInstance() as any;

// 这里如果用reactive会有问题。reactive好像只能用在对象上
let roleList = ref([
  {
    roleCode: "",
    roleName: "",
    id: null,
  },
]);
let tableData = ref([
  {
    clientId: "",
    applicationName: "",
  },
]);

let dialogFormVisible = ref(false);

let action = ref("add");

const formUser = reactive({
  username: "",
  email: "",
  phone: "",
  nickName: "",
  roleIds: [],
});
const tableHeader = reactive([
  {
    prop: "applicationName",
    label: "应用",
  },
  {
    prop: "clientId",
    label: "消费客户端",
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
  getConsumerList(pageInfo);
});

// 获取消费者分页
const getConsumerList = async (data) => {
  let res = await proxy.$api.mq.consumerList();
  tableData.value = res;
  //客户端数据赋值 ["abac","qweqwe"] 放到对象{"client"}
  // res.forEach((item, index) => {
  //   tableData.value[index] = { client: item };
  // });
  console.log(tableData);

  // pageInfo.total = parseInt(res.total);
};
</script>
