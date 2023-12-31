<template>
  <el-row class="home" :gutter="20">
    <el-col :span="24" style="margin-top: 20px">
      <!-- 用户信息 -->
      <el-card shadow="hover">
        <div class="userInfo">
          <img class="userImg" src="../../images/hzh.jpg" />
          <div>
            <p class="username">Admin</p>
            <p class="role">超级管理员</p>
          </div>
        </div>
      </el-card>
      <!-- 用户信息 -->
      <el-card class="user" shadow="hover" style="margin-top: 20px">
        <div>
          用户名<el-input
            id="username"
            name="aaa"
            v-model="user.userInfo.username"
            placeholder=""
            clearable
          />
          邮箱<el-input
            v-model="user.userInfo.email"
            placeholder=""
            clearable
          />
          手机号<el-input
            v-model="user.userInfo.phone"
            placeholder=""
            clearable
          />
          昵称<el-input
            v-model="user.userInfo.nickName"
            placeholder=""
            clearable
          />
          部门<el-input
            v-model="user.userInfo.avatar"
            placeholder=""
            clearable
          />
          注册时间<el-input disabled :placeholder="user.userInfo.createTime" />
          最近更新时间<el-input
            disabled
            :placeholder="user.userInfo.updateTIme"
          />
          <el-button @click="updateUser" type="primary">保存</el-button>
        </div>
      </el-card>
    </el-col>
  </el-row>
</template>

<script lang="ts" setup>
import { ref, getCurrentInstance, reactive, onMounted } from "vue";
const { proxy } = getCurrentInstance() as any;

let user = reactive({
  userInfo: {
    username: "",
    email: "",
    phone: "",
    nickName: "",
    createTime: "",
    updateTIme: "",
    avatar: "",
  },
});

// const getUserInfo = async () => {
//   user = await proxy.$api.user.getUserInfo();
//   console.log(user);
// };
// onMounted(() => {
//   getUserInfo();
// });

onMounted(() => {
  const getUserInfo = async (id) => {
    let res = await proxy.$api.user.getUserInfo(id);

    user.userInfo = res;
  };
  const params = {
    id: 1,
  };
  getUserInfo(params);
});

const updateUser = () => {
  proxy.$api.user.updateUser(user.userInfo);
};
</script>
<style>
.user .el-input {
  margin: 20px 0;
}
.home .userInfo {
  display: flex;
  justify-content: left;
  align-items: center;
}
.userImg {
  border-radius: 50%;
  width: 150px;
  height: 150px;
  margin-right: 40px;
}
</style>
