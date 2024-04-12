package com.redismq.samples.consumer;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * <p>
 * 制图产出文件
 * </p>
 *
 * @author yanlc
 * @since 2024-03-26
 */
@Getter
@Setter
public class MakeMappingFile   {

    private static final long serialVersionUID = 1L;
 
    private Long id;

    /**
     * 名称
     */
    private String name;

    /**
     * 编码
     */
    private String code;

    /**
     * 图片或者文档路径
     */
    private String url;

    /**
     * 缩略图
     */
    private String thumbnail;

    /**
     * 文件类型 doc/img
     */
    private String fileType;

    /**
     * 对应数据的id
     */
    private Long dataId;

    /**
     * 所属模块
     */
    private String module;

    /**
     * 图的配置内容
     */
    private String config;

    /**
     * 状态 0正在制图 1制图完成 -1制图失败
     */
    private Integer status;

    /**
     * 删除标记
     */
    private Boolean isDeleted;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 更新时间
     */
    private Date updateTime;


}
