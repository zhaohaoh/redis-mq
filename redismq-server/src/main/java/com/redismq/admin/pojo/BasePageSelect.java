package com.redismq.admin.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class BasePageSelect  {
    private Integer size = 10;

    private Integer page = 1;

    private Long beginTime;

    private Long endTime;

    private String keyword;

    private String sort;

    public BasePageSelect(Integer size, Integer page) {
        this.size = size;
        this.page = page;
    }
}
