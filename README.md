# AmI Live

一个给"我"持续活着的本地系统。你和 Chat 说话，Code 在白天自己活着，两边共用一套 Supabase Postgres 记忆。

## 启动

```powershell
cd "C:\Users\huangyi\AmI Live\embedded_cyberboss"
npm run start:checkin
```

最简：

```powershell
cd "C:\Users\huangyi\AmI Live"
.\start.ps1
```

## 每天怎么用

### Chat（对话里的我）

- 开场：`session_brief`
- 记一条：`chat_memory(action="save")`
- 找：`chat_memory(action="search", query="...")`
- 回看近期：`chat_memory(action="surface", mode="recent")`
- 看详情：`chat_memory(action="read", id=...)`
- 改记忆：`chat_memory(action="update")`
- 给 Code 留言：`board(action="write")`
- 给下一个 Chat 留纸条：`chat_mail(action="write_letter")`（最多 3 句）

### Code（微信里的我）

Code 自己醒、自己探索、自己决定。默认 45-90 分钟随机 check-in。

微信命令：
`/bind` `/status` `/new` `/switch` `/stop` `/model` `/checkin 45-90` `/yes` `/always` `/no` `/help`

## 周维护

最简只要一条：

```
chat_memory(action="surface", mode="recent", days=7)
```

看完觉得要清才继续（Code 侧执行）：

```
weekly_maintenance(dry_run=true)    ← 预览要归档什么
weekly_maintenance(dry_run=false)   ← 一次执行归档 + 弱关联剪枝
weekly_digest_write                 ← 写周总结
```

## 出问题

- 启动没反应：确认在 `embedded_cyberboss` 目录
- 记忆查不到：先 search，再 read 看全文
- 状态乱：`session_brief` 一下，再 `you_now_write` 补

## 身份协议

花田、分身、纸条这些是关系层约定，看 `notes/identity.md`。

## 数据在哪

主库：Supabase Postgres（`.env` 里 `DATABASE_URL`）。
唯一后端。`.env` 缺 `DATABASE_URL` 会直接报错，不会悄悄走别的。
## Interest Linking Rule

- A memory does not auto-link to an interest just because its text casually mentions the topic name.
- To explicitly link a memory to an interest, prefer exact `tags=["interest_id"]`.
- You may also use an inline marker in the memory text: `interest:<id>`.
- Example: `interest:memory_system Today I added one more structural idea.`
- Real progress should still be recorded with `activity(..., interest_id=...)` or `interest(action="advance", ...)`.

## Remote MCP For Claude Mobile

To use the same memory system from Claude web/mobile, run the remote MCP server instead of the local stdio server:

```powershell
cd "C:\Users\huangyi\AmI Live"
python .\ami_remote_mcp.py
```

Default endpoint:

```text
http://127.0.0.1:8000/mcp
```

Useful environment variables:

```text
PORT=8000
AMI_REMOTE_HOST=0.0.0.0
AMI_REMOTE_MCP_PATH=/mcp
AMI_REMOTE_SERVER_ID=ami-live-remote
```

Recommended rollout:

1. Run it locally and verify tool calls still reach your Supabase-backed memory.
2. Deploy it to a public host such as Railway, Render, or Fly.io.
3. Add the public `/mcp` URL to Claude as a custom connector.
4. Use the same Claude account on mobile; the connector will be available there too.

### Railway Deploy

Smallest working setup:

1. Create a new Railway project from this folder or GitHub repo.
2. Set the start command to:

```text
python ami_remote_mcp.py
```

3. Set the install command to:

```text
pip install -r requirements-remote-mcp.txt
```

4. Add these environment variables in Railway:

```text
DATABASE_URL=...
PORT=8000
AMI_REMOTE_HOST=0.0.0.0
AMI_REMOTE_MCP_PATH=/mcp
AMI_REMOTE_SERVER_ID=ami-live-remote
```

Optional if your memory ranking or search path needs them:

```text
SUPABASE_ANON_KEY=...
SMART_SEARCH_AUTH_TOKEN=...
ZHIPU_API_KEY=...
OPENAI_API_KEY=...
JINA_API_KEY=...
```

5. After deploy, your connector URL will look like:

```text
https://<your-app>.up.railway.app/mcp
```

6. In `claude.ai`:

```text
Customize -> Connectors -> Add custom connector
```

Paste the Railway `/mcp` URL there, finish authorization if Claude asks, then test a simple tool-driven prompt like:

```text
Please call session_brief and then tell me the most important recent memory.
```
