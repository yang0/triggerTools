# triggerTools
执行： python index.py  

用来生成sql脚本的小工具：  
1 生成从A表到B表的全量数据拷贝sql: insert into select...    
2 生成insert, update, delete触发器  

SQL执行后目标db可以和源db可以完全保持一致   


mysql连接参数，及要同步的table名称自己看代码改