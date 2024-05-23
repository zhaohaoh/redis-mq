package com.redismq.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

/**
 * spring关闭监听器
 *
 * @author hmj
 *
 */
@Component
public class SpringStartupListener implements ApplicationListener<ContextRefreshedEvent> {

	private static final Logger logger = LoggerFactory
			.getLogger(SpringStartupListener.class);
   
	@Override
	public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        logger.info("redismq-server starter success");
	}

}
