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
      <el-table-column fixed="right" width="150px" label="操作">
        <template #default="scope">
          <el-button
            type="danger"
            size="small"
            @click="handleDelete(scope.$index, scope.row)"
            >删除</el-button
          >
        </template>
      </el-table-column>
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

let tableData = ref([
  {
    groupId: "",
  },
]);

const tableHeader = reactive([
  {
    prop: "groupId",
    label: "消费者组",
  },
]);

const pageInfo = reactive({
  page: 1,
  size: 10,
  total: 0,
});

const handleDelete = (index, row) => {
  ElMessageBox.confirm(
    "<div  >将同时删除组内所有消息 可能导致redis耗时!<div/><div>是否确认删除？</div>",
    {
      confirmButtonText: "确定",
      cancelButtonText: "取消",
      dangerouslyUseHTMLString: true,
    }
  )
    .then(() => {
      consumerGroupDelete(row);
      elMessage.success();
    })
    .catch(() => {});
};
const consumerGroupDelete = async (group) => {
  // console.log("aa" + group.groupId);
  let res = await proxy.$api.mq.consumerGroupDelete(group.groupId);
  getConsumerGroupList();
};

onMounted(() => {
  getConsumerGroupList();
});

// 获取消费者分页
const getConsumerGroupList = async () => {
  let res = await proxy.$api.mq.consumerGroupList();
  tableData.value = res;
  //客户端数据赋值 ["abac","qweqwe"] 放到对象{"client"}
  // res.forEach((item, index) => {
  //   tableData.value[index] = { client: item };
  // });
  console.log(tableData);

  // pageInfo.total = parseInt(res.total);
};
</script>
