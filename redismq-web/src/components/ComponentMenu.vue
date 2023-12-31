<template>
  <!-- :collapse-transition="false" 关闭伸缩的动画 -->
  <el-menu
    class="el-menu-vertical-demo"
    background-color="#3F3F3F"
    text-color="#fff"
    :collapse="!$store.state.user.isCollapse"
    :collapse-transition="false"
  >
    <!-- 不拥有子菜单的 -->
    <el-menu-item
      :index="item.path"
      v-for="item in noChild"
      :key="item.path"
      @click="openMenu(item)"
    >
      <!-- vue3整合element动态图标 -->
      <component class="icons" :is="item.icon"></component>
      <template #title>{{ item.label }}</template>
    </el-menu-item>
    <!-- 拥有子菜单 -->
    <el-sub-menu :index="item.path" v-for="item in hasChild" :key="item.path">
      <template #title>
        <component class="icons" :is="item.icon"></component>
        <span>{{ item.label }}</span>
      </template>
      <el-menu-item-group
        :index="subItem.path"
        v-for="subItem in item.children"
        :key="subItem.path"
        @click="openMenu(subItem)"
      >
        <el-menu-item :index="subItem.path">
          <component class="icons" :is="item.icon"></component
          >{{ subItem.label }}</el-menu-item
        >
      </el-menu-item-group>
    </el-sub-menu>
  </el-menu>
</template>

<style>
.el-menu-vertical-demo:not(.el-menu--collapse) {
  width: 200px;
  min-height: 400px;
  background-color: #3f3f3f;
}
.icons {
  width: 28px;
  height: 28px;
}
.el-menu-item {
  width: 200px;
}
main {
  width: 100%;
}
.el-menu {
  border-right: none;
}
</style>

<script>
import { onMounted, ref, getCurrentInstance } from "vue";
import { useRouter } from "vue-router";
import { useStore } from "vuex";
export default {
  setup() {
    // export的时候是{}则必须用{}  否则就得去掉{}
    const router = useRouter();
    const store = useStore();
    const { proxy } = getCurrentInstance();
    let noChild = ref([]);
    let hasChild = ref([]);
    // 点击菜单跳转路由
    const openMenu = (item) => {
      store.commit("user/setCurrentMenu", item);
      router.push(item.path);
    };

    // const getList = async () => {
    //   let a = await proxy.$api.user.userMenuList();
    //   return a;
    // };

    // 页面加载完执行添加菜单
    onMounted(() => {
      let menu = store.state.user.menu;
      noChild.value = menu.filter((item) => item.type === "MENU");
      console.log(noChild);
      hasChild.value = menu.filter((item) => item.type === "CATALOG");
      console.log(hasChild);
    });

    return {
      noChild,
      hasChild,
      openMenu,
    };
  },
};
</script>
