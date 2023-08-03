## give DuckDB a TopDown Optimizer
我们要做的是给原本是基于BottomUp优化器的DuckDB添加一个新的TopDown优化器。
在直接fork了DuckDB的基础上，zhaojy20给main/src/optimizer和main/src/include/optimizer分别添加了cascade目录，在此目录内将包含所有被用于实现TopDown优化器的源码或头文件。
目前准备基于Orca的源码From scratch地完成所有代码的编写。
Orca源码已经复制到了main/Orca中。

## 2023/4/24
目前准备先将Orca完全整合进Cascade目录，之后通过adapter将DuckDB的数据结构与Orca的数据结构相互转换，从而完成DuckDB与TopDown优化器部分的连接。

## 2023/8/3
目前初步整合完毕，可以完成"SELECT * FROM foo;"命令的执行。后续任务为完成statistics的连接，并补充其他操作符的规则。
