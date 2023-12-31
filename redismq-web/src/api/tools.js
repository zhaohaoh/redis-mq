// 用户模块的接口
import api from "./request.js";

const tools = {
  getCodeGeneratorPage() {
    return api.get({ url: "/codeGeneratorConfig/page" });
  },
  getCodeGeneratorConfig() {
    return api.get({ url: "/codeGeneratorConfig/getById" });
  },
  addCodeGeneratorConfig() {
    return api.get({ url: "/codeGeneratorConfig/add" });
  },
  updateCodeGeneratorConfig() {
    return api.get({ url: "/codeGeneratorConfig/updateCodeGeneratorConfig" });
  },
  deleteCodeGeneratorConfig() {
    return api.get({ url: "/codeGeneratorConfig/delete" });
  },
  excuteCodeGenerator(id) {
    return api.post({ url: "/codeGeneratorConfig/excute", data: id });
  },
};

export default tools;
