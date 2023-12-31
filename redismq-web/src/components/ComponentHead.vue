<template>
  <div class="title fisrtHeader">
    <span style="font-size: 20px">RedisMQ控制台</span>
  </div>
  <div class="l-content">
    <el-menu
      mode="horizontal"
      class="el-menu-class"
      @select="handleSelect"
      router
    >
      <el-menu-item index="/consumer">消费者管理</el-menu-item>
      <el-menu-item index="/queue">队列管理</el-menu-item>
      <el-menu-item index="/historyMessage">历史消息管理</el-menu-item>
    </el-menu>
  </div>

  <div class="r-content">
    <span class="switchGroup">
      <a href="https://gitee.com/hzh727172424/redis-mq">Gitee</a>
    </span>
    <el-dropdown>
      <span class="el-dropdown-link">
        <img :src="getImageUrl('hzh')" alt="" class="avatar" />
      </span>
      <!-- <template #dropdown>
        <el-dropdown-menu>
          <el-dropdown-item @click="openUserCenter">个人中心</el-dropdown-item>
          <el-dropdown-item @click="logout">退出登录</el-dropdown-item>
        </el-dropdown-menu>
      </template> -->
    </el-dropdown>
  </div>
</template>
<script>
import { useStore } from "vuex";
import { useRouter } from "vue-router";
import { computed } from "@vue/reactivity";

export default {
  setup() {
    let router = useRouter();
    // 调用mutations的方法改变菜单状态
    let store = useStore();
    let updateMenu = () => {
      store.commit("user/updateCollapse");
    };

    //获取图片地址
    function getImageUrl(name) {
      return new URL(`../images/${name}.jpg`, import.meta.url).href;
    }

    const currentMenu = computed(() => {
      return store.state.user.currentMenu;
    });
    //跳转用户个人中心
    const openUserCenter = () => {
      router.push("/userCenter");
    };

    const logout = () => {
      store.commit("user/removeUserInfo");
      router.push("/login");
    };

    return { getImageUrl, updateMenu, openUserCenter, logout, currentMenu };
  },
};
</script>
<style>
.redismqHeader {
  display: flex;
  justify-content: space-between;
  width: 100%;
  align-items: center;
}

.redismqHeader .title span {
  text-align: center;
  /* 文字居中对齐的方法 */
  line-height: 60px;
  height: 60px;
}

.title {
  display: flex; /* 添加display: flex; */
  align-items: center; /* 使得子元素在交叉轴（默认为垂直方向）上居中 */
  justify-content: center; /* 使得子元素在主轴（默认为水平方向）上居中 */
  height: 60px; /* 给.title设置一个高度 */
  /* 让这个title和文字一样大小 */
  min-width: fit-content;
}
.l-content {
  width: 80%;
  align-items: center;
}
.r-content {
  display: flex;
  width: 120px;
  height: 60px;
}
.r-content .switchGroup {
  display: flex;
  /* 文字居中对齐的方法 */
  line-height: 60px;
  height: 60px;
  width: 80px;
}
.r-content span {
  display: flex;
  justify-content: center;
  align-items: center; /* 垂直居中 */
}

.r-content .avatar {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  /* 鼠标移入展示小手指 关键技术啊 */
  cursor: pointer !important;
}

.fisrtHeader {
  height: 60px;
}

.el-menu-item {
  width: 200px;
  height: 60px !important;
}
.el-menu--horizontal > .el-menu-item.is-active {
  border-bottom: 0px;
}

/* 鼠标移入改变颜色 */
.el-breadcrumb span:hover {
  background-color: #f9bc60;
}
</style>
