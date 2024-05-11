FROM openjdk:8u222-jdk
MAINTAINER hzh
#时区问题
RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone
#用docker-compose.yml入口位置改变,添加app.jar进容器
ADD ./*.jar app.jar

ENV JAVA_AGENT=""
ENV PARAMS=""
ENV JAVA_OPTS=""

#5个日志文件 一个20M轮流
ENTRYPOINT java -jar ${JAVA_OPTS} ${JAVA_AGENT} ${PARAMS} app.jar