/* empty css             *//* empty css                     *//* empty css                *//* empty css                       */import{i as d,n as e,p as l,q as _,o as n,b as r,e as b,w as f,F as g,z as x,C as y,G as h,K as C,O as k,I as w}from"./index.4cd85549.js";const v={class:"queue-table"},z=d({__name:"Consumer",setup(E){const{proxy:s}=h();e([{roleCode:"",roleName:"",id:null}]);let t=e([{clientId:"",applicationName:""}]);e(!1),e("add"),l({username:"",email:"",phone:"",nickName:"",roleIds:[]});const c=l([{prop:"applicationName",label:"\u5E94\u7528"},{prop:"clientId",label:"\u6D88\u8D39\u5BA2\u6237\u7AEF"}]);l({page:1,size:10,total:0,keyword:"",beginTime:null,endTime:null}),_(()=>{p()});const p=async i=>{let o=await s.$api.mq.consumerList();t.value=o,console.log(t)};return(i,o)=>{const u=w,m=C;return n(),r("div",v,[b(m,{data:y(t),style:{width:"100%"},height:"700px",border:"","header-cell-style":{background:"#fff !important",color:"#000 !important","text-align":"center","font-size":"15px","font-weight":"700"},"row-cell-style":{"text-align":"center"},"cell-style":{"text-align":"center"}},{default:f(()=>[(n(!0),r(g,null,x(c,a=>(n(),k(u,{key:a.prop,label:a.label,prop:a.prop},null,8,["label","prop"]))),128))]),_:1},8,["data"])])}}});export{z as default};
