<template>
  <div class="common-layout">
    <!-- 定义容器 响应式开发 -->
    <el-container>
      <el-header class="redismqHeader"><ComponentHeader /></el-header>
      <el-main><RouterView></RouterView></el-main>
    </el-container>
  </div>
</template>
<script>
import { defineComponent } from "vue";
import ComponentHeader from "../components/ComponentHead.vue";
export default defineComponent({
  components: {
    ComponentHeader,
  },
});
</script>

<style>
.el-container {
  height: 100%;
}
.common-layout {
  height: 100%;
}
.el-aside menu {
  height: 100%;
}
</style>
