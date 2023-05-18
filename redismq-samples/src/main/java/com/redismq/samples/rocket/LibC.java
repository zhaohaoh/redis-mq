/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redismq.samples.rocket;

import com.sun.jna.*;

public interface LibC extends Library {
    LibC INSTANCE = (LibC) Native.loadLibrary(Platform.isWindows() ? "msvcrt" : "c", LibC.class);

    int MADV_WILLNEED = 3;
    int MADV_DONTNEED = 4;

    int MCL_CURRENT = 1;
    int MCL_FUTURE = 2;
    int MCL_ONFAULT = 4;

    /* sync memory asynchronously */
    int MS_ASYNC = 0x0001;
    /* invalidate mappings & caches */
    int MS_INVALIDATE = 0x0002;
    /* synchronous memory sync */
    int MS_SYNC = 0x0004;
    //锁定内存空间不被swap
    int mlock(Pointer var1, NativeLong var2);
    //释放
    int munlock(Pointer var1, NativeLong var2);

    /**
     * c语言的指令，预读文件数据到内核缓存中。因为mmap只有发生缺页中断才读取
     */
    int madvise(Pointer var1, NativeLong var2, int var3);
    //对指定的数组参数赋值，len是赋值数组的中元素的个数，第二个参数是value
    Pointer memset(Pointer p, int v, long len);
    //锁定全部内存空间  flags可取两个值：MCL_CURRENT,MCL_FUTURE
    //MCL_CURRENT: 表示对所有已经映射到进程地址空间的页上锁
    //MCL_FUTURE:  表示对所有将来映射到进程地空间的页都上锁。
    int mlockall(int flags);
    //把在该内存段的某个部分或者整段中的修改写回到被映射的文件中（或者从被映射文件里读出）。
    int msync(Pointer p, NativeLong length, int flags);
}
