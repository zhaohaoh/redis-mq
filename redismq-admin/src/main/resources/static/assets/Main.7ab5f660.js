/* empty css             *//* empty css               *//* empty css                */import{_ as m,u as C,a as x,c as $,o as h,b as f,d as n,e,w as t,f as i,F as w,E as M,g as E,h as y,i as H,r as p,j as z,k as U,l as b}from"./index.4cd85549.js";const j="/assets/hzh.32bd6f0c.jpg";const I={setup(){let s=C(),a=x(),_=()=>{a.commit("user/updateCollapse")};function r(c){return new URL(Object.assign({"../images/hzh.jpg":j})[`../images/${c}.jpg`],self.location).href}const l=$(()=>a.state.user.currentMenu);return{getImageUrl:r,updateMenu:_,openUserCenter:()=>{s.push("/userCenter")},logout:()=>{a.commit("user/removeUserInfo"),s.push("/login")},currentMenu:l}}},R=n("div",{class:"title fisrtHeader"},[n("span",{style:{"font-size":"20px"}},"RedisMQ\u63A7\u5236\u53F0")],-1),V={class:"l-content"},k={class:"r-content"},S=n("span",{class:"switchGroup"},[n("a",{href:"https://gitee.com/hzh727172424/redis-mq"},"Gitee")],-1),q={class:"el-dropdown-link"},B=["src"];function G(s,a,_,r,l,d){const o=M,c=E,u=y;return h(),f(w,null,[R,n("div",V,[e(c,{mode:"horizontal",class:"el-menu-class",onSelect:s.handleSelect,router:""},{default:t(()=>[e(o,{index:"/consumer"},{default:t(()=>[i("\u6D88\u8D39\u8005\u7BA1\u7406")]),_:1}),e(o,{index:"/queue"},{default:t(()=>[i("\u961F\u5217\u7BA1\u7406")]),_:1}),e(o,{index:"/historyMessage"},{default:t(()=>[i("\u5386\u53F2\u6D88\u606F\u7BA1\u7406")]),_:1})]),_:1},8,["onSelect"])]),n("div",k,[S,e(u,null,{default:t(()=>[n("span",q,[n("img",{src:r.getImageUrl("hzh"),alt:"",class:"avatar"},null,8,B)])]),_:1})])],64)}const N=m(I,[["render",G]]);const F=H({components:{ComponentHeader:N}}),D={class:"common-layout"};function L(s,a,_,r,l,d){const o=p("ComponentHeader"),c=z,u=p("RouterView"),g=U,v=b;return h(),f("div",D,[e(v,null,{default:t(()=>[e(c,{class:"redismqHeader"},{default:t(()=>[e(o)]),_:1}),e(g,null,{default:t(()=>[e(u)]),_:1})]),_:1})])}const J=m(F,[["render",L]]);export{J as default};
