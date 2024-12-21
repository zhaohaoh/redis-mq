// 用户模块的接口
import api from "./request.js";

const mq = {
  //消费者列表
  consumerList(params) {
    return api.get({ url: "/consumer/list", data: params });
  },
  //队列
  queueList(params) {
    return api.get({ url: "/queue/page", data: params });
  },
  //队列
  vQueueList(params) {
    return api.get({ url: "/queue/vQueueList", data: params });
  },
  //发送消息
  sendMessage(params) {
    return api.post({ url: "/queue/sendMessage", data: params });
  },

  //发送定时消息
  sendTimingMessage(params) {
    return api.post({ url: "/queue/sendTimingMessage", data: params });
  },
  //删除消息
  deleteMessage(params) {
    return api.post({ url: "/message/deleteMessage", data: params });
  },
  //根据队列分页查询消息
  pageMessage(params) {
    return api.post({ url: "/message/page", data: params });
  },

  //根据队列拉取消息
  publishPullMessage(params) {
    return api.postParam({ url: "/queue/publishPullMessage", data: params });
  },
  //删除队列
  deleteQueue(params) {
    return api.deleteParam({ url: "/queue/deleteQueue", data: params });
  },

  //历史消息分页查询
  historyMessagePage(params) {
    return api.post({ url: "/history/message/page", data: params });
  },

  //消费者组列表
  consumerGroupList(params) {
    return api.get({ url: "/consumer/group/list", data: params });
  },

  //消费者组删除
  consumerGroupDelete(params) {
    return api.post({ url: "/consumer/group/delete", data: params });
  },
};

export default mq;
