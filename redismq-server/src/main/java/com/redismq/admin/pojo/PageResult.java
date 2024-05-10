package com.redismq.admin.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageResult<T> {
    private int code;
    private String message;
    private Boolean success;
    private List<T> list = Collections.emptyList();
    private long total = 0;
    private long page = 1;
    private long size = 10;

    public PageResult(int code, String message, Boolean success) {
        this.code = code;
        this.message = message;
        this.success = success;
    }
    public PageResult(long page,long size) {
        PageResult<T> pageResult = new PageResult<T>(200, "操作成功", true);
        pageResult.setPage(page);
        pageResult.setSize(size);
    }

    //定义分页的消息
    public static <T> PageResult<T> success(long total,List<T> list) {
        PageResult<T> pageResult = new PageResult<T>(200, "操作成功", true);
        pageResult.setList(list);
        pageResult.setTotal(total);
        return pageResult;
    }


    @SuppressWarnings("unchecked")
    public <R> PageResult<R> convert(Function<? super T, ? extends R> mapper) {
        List<R> collect = this.getList().stream().map(mapper).collect(toList());
        PageResult<R> result = (PageResult<R>) this;
        result.setList(collect);
        return result;
    }
}
