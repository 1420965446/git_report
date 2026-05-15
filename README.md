# Git Work Report Automation

一个基于 `FastAPI + SQLite` 的自动化工作报告系统，用于从多个本地 git 仓库采集提交记录，并生成日报、周报、月报和工作总结。

## 功能

- 配置多个本地 git 仓库
- 为每个仓库分别配置作者姓名/邮箱映射，用于识别本人提交
- 采集指定时间范围内的 git 日志与实际代码改动内容
- 基于代码改动聚合与 OpenAI 兼容接口生成报告草稿
- 在后台页面中查看、编辑、保存报告
- 支持手动生成和后台定时自动生成

## 快速开始

### 方式一：直接双击启动

直接双击 [run_app.bat](C:/Users/Lenovo/Documents/New%20project/run_app.bat) 即可。

它会自动：

- 创建 `.venv` 虚拟环境
- 安装/更新依赖
- 启动 FastAPI 服务
- 自动打开浏览器到 `http://127.0.0.1:8000`

适合日常直接使用。

### 方式二：命令行启动

1. 创建虚拟环境并安装依赖：

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. 启动服务：

```powershell
uvicorn app.main:app --reload
```

3. 打开浏览器访问：

`http://127.0.0.1:8000`

## 环境变量

- `REPORTER_DB_PATH`：SQLite 数据库路径，默认 `data/app.db`
- `REPORTER_LLM_BASE_URL`：OpenAI 兼容接口地址，如 `https://api.openai.com/v1`
- `REPORTER_LLM_MODEL`：模型名
- `REPORTER_LLM_API_KEY`：API Key
- `REPORTER_TIMEZONE`：时区，默认 `Asia/Shanghai`
- `REPORTER_LLM_REPORT_TIMEOUT_SECONDS`：日报生成超时秒数，默认 `45`
- `REPORTER_LLM_COMMIT_SUMMARY_TIMEOUT_SECONDS`：单条提交摘要生成超时秒数，默认 `30`

也可以在页面中修改 LLM 配置和调度规则。

## 说明

- 当前版本只处理本地可访问仓库，不负责自动 clone/pull。
- 若目标仓库存在 `safe.directory` 限制，页面校验与采集接口会返回错误说明。
- 若未配置 LLM 或调用失败，系统会返回规则生成的兜底草稿，并记录分阶段错误信息、超时信息和 prompt 规模提示。
