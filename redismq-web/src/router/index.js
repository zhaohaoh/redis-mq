import { createRouter, createWebHashHistory } from "vue-router";
// vue路由管理
const routes = [
  {
    path: "/",
    name: "main",
    redirect: "/queue",
    component: () => import("../views/Main.vue"),
    //子路由,其他路由在页面中动态添加
    children: [
      {
        path: "/queue",
        name: "queue",
        component: () => import("../views/mq/Queue.vue"),
      },
      {
        path: "/consumer/group",
        name: "consumerGroup",
        component: () => import("../views/mq/ConsumerGroup.vue"),
      },
      {
        path: "/consumer",
        name: "consumer",
        component: () => import("../views/mq/Consumer.vue"),
      },
      {
        path: "/historyMessage",
        name: "historyMessage",
        component: () => import("../views/mq/HistoryMessage.vue"),
      },
    ],
    // children: [],
  },
  // 登录的路由
  {
    path: "/login",
    name: "login",
    component: () => import("../views/Login.vue"),
  },
];

const router = createRouter({
  // 使用hash路由
  history: createWebHashHistory(),
  routes,
});

export default router;
