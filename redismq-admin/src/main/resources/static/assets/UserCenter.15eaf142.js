/* empty css             *//* empty css               *//* empty css              */import{i as I,p as V,q as h,o as U,O as g,w as t,e as o,d as r,f as n,C as a,R as v,L as C,J as B,S as E,T as b,G as F}from"./index.4cd85549.js";const w="/assets/hzh.32bd6f0c.jpg",x=r("div",{class:"userInfo"},[r("img",{class:"userImg",src:w}),r("div",null,[r("p",{class:"username"},"Admin"),r("p",{class:"role"},"\u8D85\u7EA7\u7BA1\u7406\u5458")])],-1),D=I({__name:"UserCenter",setup(y){const{proxy:p}=F();let e=V({userInfo:{username:"",email:"",phone:"",nickName:"",createTime:"",updateTIme:"",avatar:""}});h(()=>{(async d=>{let u=await p.$api.user.getUserInfo(d);e.userInfo=u})({id:1})});const m=()=>{p.$api.user.updateUser(e.userInfo)};return(c,l)=>{const d=v,u=C,i=B,f=E,_=b;return U(),g(_,{class:"home",gutter:20},{default:t(()=>[o(f,{span:24,style:{"margin-top":"20px"}},{default:t(()=>[o(d,{shadow:"hover"},{default:t(()=>[x]),_:1}),o(d,{class:"user",shadow:"hover",style:{"margin-top":"20px"}},{default:t(()=>[r("div",null,[n(" \u7528\u6237\u540D"),o(u,{id:"username",name:"aaa",modelValue:a(e).userInfo.username,"onUpdate:modelValue":l[0]||(l[0]=s=>a(e).userInfo.username=s),placeholder:"",clearable:""},null,8,["modelValue"]),n(" \u90AE\u7BB1"),o(u,{modelValue:a(e).userInfo.email,"onUpdate:modelValue":l[1]||(l[1]=s=>a(e).userInfo.email=s),placeholder:"",clearable:""},null,8,["modelValue"]),n(" \u624B\u673A\u53F7"),o(u,{modelValue:a(e).userInfo.phone,"onUpdate:modelValue":l[2]||(l[2]=s=>a(e).userInfo.phone=s),placeholder:"",clearable:""},null,8,["modelValue"]),n(" \u6635\u79F0"),o(u,{modelValue:a(e).userInfo.nickName,"onUpdate:modelValue":l[3]||(l[3]=s=>a(e).userInfo.nickName=s),placeholder:"",clearable:""},null,8,["modelValue"]),n(" \u90E8\u95E8"),o(u,{modelValue:a(e).userInfo.avatar,"onUpdate:modelValue":l[4]||(l[4]=s=>a(e).userInfo.avatar=s),placeholder:"",clearable:""},null,8,["modelValue"]),n(" \u6CE8\u518C\u65F6\u95F4"),o(u,{disabled:"",placeholder:a(e).userInfo.createTime},null,8,["placeholder"]),n(" \u6700\u8FD1\u66F4\u65B0\u65F6\u95F4"),o(u,{disabled:"",placeholder:a(e).userInfo.updateTIme},null,8,["placeholder"]),o(i,{onClick:m,type:"primary"},{default:t(()=>[n("\u4FDD\u5B58")]),_:1})])]),_:1})]),_:1})]),_:1})}}});export{D as default};
