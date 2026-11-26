# 腾讯云 CPU 候选取件复评

2026-11-26 在上海 CVM A 复用阶段 1 的真实 KindExec 输出，以 result_ref 找到 records.py 的 SHA-256 和大小，校验 Blob 原始字节。两个独立的新任务分别将取回的文件与 eval/fixtures/csv-first-row/verify.py 的 CHECKER、CASES 交给新 KindExec Pod，随后再次读取并校验输出文件。评分依据为四个布尔值。两次 checker 进程均正常退出。

correct.json 与 incorrect.json 包含输入、输出摘要、任务 ID、判定和工具运行时间。pods.txt 为 Kubernetes watch 输出，TASK 标签为空，不能单凭此列关联任务。两个执行 Pod 有不同 UID。输入为参考候选，未调用模型。

复跑使用同一生产端数据目录（含 inspect.json 和 data/blobs）、独立评分 Gateway 数据目录及绑定在回环 18181 的 CPU Gateway。在评分工作目录运行 sudo python3 regrade.py /home/ubuntu/agentbox-cvm-acceptance /path/to/fixtures/csv-first-row/verify.py 。脚本验证两个原始结果和每次评分后的 SHA-256。
