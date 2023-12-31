<template>
  <el-form :model="loginForm" class="login-container">
    <h3>系统登录</h3>
    <el-form-item>
      <el-input
        type="input"
        placeholder="请输入账号"
        v-model="loginForm.username"
      >
      </el-input>
    </el-form-item>
    <el-form-item>
      <el-input
        type="password"
        placeholder="请输入密码"
        v-model="loginForm.password"
      >
      </el-input>
    </el-form-item>
    <el-form-item>
      <el-button type="primary" @click="login"> 登录 </el-button>
    </el-form-item>
  </el-form>
</template>
<script>
import { reactive } from "vue";
import { getCurrentInstance } from "vue";
import { useStore } from "vuex";
import { useRouter } from "vue-router";
export default {
  setup() {
    const loginForm = reactive({
      username: "hzh",
      password: "123456",
      grant_type: "username",
    });
    const { proxy } = getCurrentInstance();
    const store = useStore();
    const router = useRouter();

    const login = async () => {
      console.log(111);
      let res = await proxy.$api.user.login(loginForm);
      // store.commit("setMenu", res.menu);
      //用户信息持久化到cookie
      store.commit("user/setUserInfo", res);
      //菜单信息初始化
      store.commit("user/initMenu", router);
      // 路由到首页
      router.push({ name: "home" });
    };
    return {
      loginForm,
      login,
    };
  },
};
</script>
<style lang="scss" scoped>
.login-container {
  width: 350px;
  background-color: #fff;
  border: 1px solid #eaeaea;
  border-radius: 15px;
  padding: 35px 35px 15px 35px;
  box-shadow: 0 0 25px #cacaca;
  margin: 180px auto;
  h3 {
    text-align: center;
    margin-bottom: 20px;
    color: #505450;
  }
  :deep(.el-form-item__content) {
    justify-content: center;
  }
}
</style>
