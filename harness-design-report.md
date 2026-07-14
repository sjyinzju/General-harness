# Agent Harness 系统 —— 最终架构规划报告

> **版本**: v3.0 Final
> **日期**: 2026-07-14
> **定位**: 本地 Agent 操作系统 —— 避免多次输入、目标导向、长时运行、总体规划、可交付生产级代码

---

## 目录

1. [愿景与第一性原理](#一愿景与第一性原理)
2. [架构总览](#二架构总览)
3. [完整工作流：从用户输入到交付](#三完整工作流从用户输入到交付)
4. [Phase 1 — CLARIFYING：需求澄清与目标锁定](#四phase-1--clarifying需求澄清与目标锁定)
5. [Phase 2 — PLANNING：架构设计与执行计划](#五phase-2--planning架构设计与执行计划)
6. [Phase 3 — USER APPROVAL：用户审批与迭代修改](#六phase-3--user-approval用户审批与迭代修改)
7. [Phase 4 — EXECUTING：自动流水线执行](#七phase-4--executing自动流水线执行)
8. [实时仪表板：Live Dashboard](#八实时仪表板live-dashboard)
9. [Worktree 隔离策略](#九worktree-隔离策略)
10. [状态机完整设计](#十状态机完整设计)
11. [Agent 发现与能力矩阵](#十一agent-发现与能力矩阵)
12. [Agent 路由与分派策略](#十二agent-路由与分派策略)
13. [Tools、Hooks 与构成要素](#十三toolshooks-与构成要素)
14. [CLI 工具设计](#十四cli-工具设计)
15. [MVP 范围与实现路线图](#十五mvp-范围与实现路线图)
16. [核心设计原则](#十六核心设计原则)

---

## 一、愿景与第一性原理

### 1.1 要解决的问题

当前多 Agent 协作的**核心痛点**：

| 痛点 | 现状 | Harness 的解决方式 |
|------|------|-------------------|
| 重复描述需求 | 每开一个 Agent 窗口就要重新说一遍 | **说一次**：总 Agent 理解后自动分派 |
| 手动分工 | 人要自己判断"这个给 Claude，那个给 Codex" | **自动路由**：根据能力矩阵 + 历史评分 |
| 上下文过载 | 单一 Agent 塞入所有细节 → 幻觉 | **上下文隔离**：总 Agent 只保留 DAG + 状态摘要 |
| 偏离目标 | 长流程中模型"忘记"最初要做什么 | **Goal Contract + 状态外置 + 偏离检测** |
| 无验收标准 | Agent 说"做好了"但实际不可用 | **证据驱动的完成声明 + 独立验证门** |

### 1.2 设计哲学

```
          确定性软件系统 (80%)               LLM 管理能力 (20%)
     ┌─────────────────────────┐    ┌──────────────────────────┐
     │ • 状态机转换规则          │    │ • 需求理解与澄清提问       │
     │ • DAG 依赖计算            │    │ • 架构设计与任务拆解        │
     │ • 子进程生命周期管理       │    │ • 代码实现                 │
     │ • Git worktree 隔离       │    │ • 代码审查与语义理解        │
     │ • 权限与预算硬限制         │    │ • 调试与根因分析           │
     │ • 确定性验收（测试/lint）  │    │ • 模糊决策（仅在歧义时）    │
     │ • 日志与审计追踪           │    │ • 重规划建议               │
     │ • 实时仪表板渲染           │    │                           │
     └─────────────────────────┘    └──────────────────────────┘
```

**一句话**：不是"再造一个更强的总 Agent"，而是**确定性的本地编排运行时 + 管理 LLM + 可插拔 Agent Adapter**。

---

## 二、架构总览

```
                         ┌──────────────────────────┐
                         │     用户（唯一入口）        │
                         │   harness run "需求描述"   │
                         │   harness status          │
                         │   harness approve         │
                         │   harness cancel          │
                         │   中途输入意见（文本框）     │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │   Harness Daemon（守护进程）│
                         │   - 独立 TypeScript 进程   │
                         │   - SQLite 状态持久化      │
                         │   - 用户配置 API/URL       │
                         └────────────┬─────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          ▼                           ▼                           ▼
┌─────────────────┐   ┌─────────────────────────┐   ┌─────────────────┐
│  Orchestrator   │   │   调度器与状态机           │   │  权限与策略引擎  │
│  (管理 LLM)      │   │   - DAG 依赖解析          │   │  - 预算硬限制    │
│  - 需求澄清      │   │   - Worktree 生命周期     │   │  - 文件范围控制  │
│  - 任务拆解      │   │   - Checkpoint 管理       │   │  - 命令白名单    │
│  - 异常决策      │   │   - 并行调度              │   │  - 审批门        │
│  - 重规划        │   │   - 合并编排              │   │  - 密钥不存储    │
└────────┬────────┘   └────────────┬────────────┘   └────────┬────────┘
         │                         │                          │
         └─────────────────────────┼──────────────────────────┘
                                   ▼
                         ┌──────────────────────────┐
                         │   Agent Adapter Registry  │
                         │   （可插拔适配器层）        │
                         └────────────┬─────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          ▼                           ▼                           ▼
┌─────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐
│  Claude Adapter  │   │   Codex Adapter      │   │  ACP / CLI Adapter  │
│  - Agent SDK     │   │   - SDK              │   │  - stdio JSON-RPC   │
│  - stream-json   │   │   - app-server       │   │  - JSONL            │
│  - claude -p     │   │   - exec --json      │   │  - PTY（最后手段）   │
└────────┬────────┘   └────────┬────────────┘   └──────────┬──────────┘
         │                     │                            │
         ▼                     ▼                            ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────────┐
│  Claude Code     │   │  Codex CLI       │   │  Gemini / Aider     │
│  + DeepSeek V4   │   │  + GPT-4         │   │  / 自定义 CLI        │
│  + GLM-5.2       │   │                  │   │                     │
│  + Sonnet/Opus   │   │                  │   │                     │
└─────────────────┘   └─────────────────┘   └─────────────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │  Git Worktrees（隔离施工） │
                         │  .harness/worktrees/      │
                         │    TASK-014-auth-callback/ │
                         │    TASK-018-dashboard/     │
                         │    ...                     │
                         │                           │
                         │  最终合并 → integration    │
                         │           → main          │
                         └──────────────────────────┘
```

---

## 三、完整工作流：从用户输入到交付

```
用户输入: harness run "做一个有用户登录功能的博客系统"
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 1: CLARIFYING（需求澄清）                                   │
│                                                                  │
│ Orchestrator 提出澄清问题，与用户对话：                            │
│   • 技术栈偏好？（React/Vue、Express/FastAPI...）                 │
│   • 数据库选择？（SQLite/PostgreSQL...）                           │
│   • 认证方式？（JWT/OAuth/Session...）                             │
│   • 部署目标？（Vercel/Docker/VPS...）                             │
│   • 时间与质量约束？                                               │
│   • 是否有现有代码库？                                             │
│   ...直到目标清晰                                                 │
│                                                                  │
│ 输出物: Goal Contract（不可变目标契约）                             │
└──────────────────────────────┬──────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 2: PLANNING（架构设计与执行计划）                            │
│                                                                  │
│ 1. Explorer Agent 扫描现有代码库（如有）                           │
│ 2. Architect Agent 设计系统架构 + 拆解任务 DAG                     │
│    - 每个任务标注：负责的 Agent + LLM                             │
│ 3. Planner Agent 生成完整执行计划                                  │
│                                                                  │
│ 输出物: 执行计划报告（即下文 Phase 2 详述的报告）                    │
└──────────────────────────────┬──────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 3: USER APPROVAL（用户审批）                                │
│                                                                  │
│ 向用户展示完整执行计划报告                                         │
│ 用户可以：                                                        │
│   • 输入修改意见 → Orchestrator 修改计划 → 重新展示                 │
│   • 输入 "approve" 或 "同意" → 开始自动执行                        │
│   • 输入 "cancel" → 取消                                          │
│                                                                  │
│ 循环直到用户批准                                                   │
└──────────────────────────────┬──────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 4: EXECUTING（自动流水线执行）+ 实时仪表板                   │
│                                                                  │
│ 屏幕实时显示：                                                     │
│   ┌──────────────────────────────────────────────────────────┐  │
│   │ 🎯 目标：构建博客系统（登录+发布+评论）                      │  │
│   │ 📊 进度：▓▓▓▓▓▓▓░░░░░░░ 45% (5/11 tasks)                 │  │
│   │                                                          │  │
│   │ ✅ TASK-001 项目脚手架           [completed]  Claude+Sonn  │  │
│   │ ✅ TASK-002 数据库模型           [completed]  Claude+Sonn  │  │
│   │ ✅ TASK-003 用户认证API          [completed]  Codex+GPT4   │  │
│   │ ✅ TASK-004 博客CRUD后端         [completed]  Codex+GPT4   │  │
│   │ 🔵 TASK-005 登录前端页面         [running]    Claude+GLM   │  │
│   │ 🟡 TASK-006 博客列表前端         [reviewing]  Claude+Sonn  │  │
│   │ ⬜ TASK-007 评论系统后端         [pending]                  │  │
│   │ ⬜ TASK-008 评论前端组件         [pending]                  │  │
│   │ ...                                                       │  │
│   │                                                          │  │
│   │ 当前活跃 Agent:                                            │  │
│   │   🔵 Claude Code + GLM-5.2    → 正在处理 TASK-005         │  │
│   │   🟡 Claude Code + Sonnet     → 正在审查 TASK-006         │  │
│   │                                                          │  │
│   │ 💬 输入意见 (输入后回车)：                                   │  │
│   │ > _                                                      │  │
│   └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│ 用户可随时在底部输入框输入意见 → Orchestrator 纳入考量             │
│                                                                  │
│ 每个任务完成后：                                                  │
│   → Reviewer 验证 → 通过则 Integrated → 失败则 Debugger 修复      │
│                                                                  │
│ 所有任务完成后：                                                  │
│   → Integrator 最终合并 → 全量测试 → DONE                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 四、Phase 1 — CLARIFYING：需求澄清与目标锁定

### 4.1 为什么必须在 Plan 之前有澄清阶段

用户说"做一个博客系统"时，有无数种理解方式。如果不问清楚，整个 DAG 建立在错误假设上，后期修改成本极高。

### 4.2 澄清策略

Orchestrator 会按以下类别系统性地提问：

```yaml
clarification_categories:
  - category: "功能范围"
    questions:
      - "博客系统需要哪些核心功能？例如：用户注册登录、文章发布编辑、评论、标签分类、搜索？"
      - "是否需要后台管理面板？"
      - "是否需要多用户支持？还是单人博客？"

  - category: "技术栈"
    questions:
      - "前端框架偏好？(React / Vue / Next.js / 无偏好由我选择)"
      - "后端框架偏好？(Express / FastAPI / Go / 无偏好)"
      - "数据库偏好？(PostgreSQL / MySQL / SQLite / 无偏好)"
      - "是否有现成的代码库需要基于其开发？"

  - category: "认证与安全"
    questions:
      - "用户认证方式？(JWT / Session / OAuth 第三方登录)"
      - "是否需要邮箱验证？"

  - category: "部署与运维"
    questions:
      - "部署目标？(Docker / VPS / Vercel / 不确定)"
      - "是否需要 CI/CD 配置？"

  - category: "质量约束"
    questions:
      - "是否需要单元测试/集成测试？覆盖率要求？"
      - "时间紧迫度？(快速原型 / 正常开发 / 生产级质量)"
      - "预算上限？（美元，如不设限请输入 0）"
```

### 4.3 输出物：Goal Contract

澄清完成后，输出不可变的目标契约：

```json
{
  "projectId": "proj-2026-0714-001",
  "createdAt": "2026-07-14T10:30:00Z",
  "version": 1,
  "objective": "构建一个支持用户注册登录、文章CRUD、评论、标签分类的个人博客系统",
  "techStack": {
    "frontend": "Next.js 14 + TypeScript + Tailwind CSS",
    "backend": "Next.js API Routes (全栈)",
    "database": "PostgreSQL (通过 Prisma ORM)",
    "auth": "NextAuth.js (JWT + OAuth)",
    "deployment": "Docker + docker-compose"
  },
  "deliverables": [
    "可运行的全栈博客应用",
    "用户注册/登录系统",
    "文章发布/编辑/删除功能",
    "评论系统",
    "标签分类与搜索",
    "Docker 部署配置",
    "README 与部署文档"
  ],
  "acceptance": [
    "docker-compose up 一键启动",
    "所有 API 端点有基本的错误处理",
    "单元测试覆盖率 > 60%",
    "前端响应式设计（手机+桌面）",
    "无明文密钥提交"
  ],
  "constraints": [
    "单任务最大预算 $4",
    "不允许 Agent 直接修改 main 分支",
    "高风险命令必须审批",
    "总预算上限 $50"
  ],
  "non_goals": [
    "不包含多语言国际化",
    "不包含实时通知/WebSocket",
    "不包含管理后台"
  ]
}
```

---

## 五、Phase 2 — PLANNING：架构设计与执行计划

### 5.1 Planning 阶段步骤

```
Step 1: Explorer Agent（只读）扫描现有代码库状态
        ├─ git status, 目录结构
        ├─ 现有依赖和技术栈
        └─ 输出: 代码库现状报告

Step 2: Architect Agent 设计系统架构
        ├─ 组件/模块划分
        ├─ 数据模型设计
        ├─ API 路由设计
        ├─ 依赖关系分析
        └─ 输出: 架构设计文档 + ADR

Step 3: Task Decomposer 拆解任务 DAG
        ├─ 每个任务：明确目标、输入、输出、验收条件
        ├─ 依赖关系图
        ├─ 并行可能性分析
        └─ 为每个任务分配 Agent + LLM 组合
```

### 5.2 执行计划报告（用户审批前展示）

这是最关键的用户界面。在用户批准前，Harness 必须生成一份**清晰、完整、可审核的报告**：

```markdown
================================================================================
                    📋 Harness 执行计划报告
================================================================================

项目: 博客系统（用户登录+发布+评论）
计划版本: v1
生成时间: 2026-07-14 10:35:00
预计总耗时: ~15-25 分钟
预计总成本: ~$18-28

================================================================================
                    🎯 一、目标理解
================================================================================

根据您的需求，我理解的系统目标如下：

  构建一个全栈个人博客系统，支持：
  • 用户注册和登录（邮箱+密码，支持 GitHub OAuth）
  • 文章的创建、编辑、删除和列表展示
  • 文章评论功能
  • 标签分类和关键词搜索
  • Docker 一键部署

技术栈：Next.js 14 + TypeScript + Tailwind CSS + Prisma + PostgreSQL
认证：NextAuth.js (Credentials + GitHub OAuth)

================================================================================
                    🤖 二、Agent 与 LLM 分配总览
================================================================================

本次执行将使用以下 Agent 和 LLM 组合：

┌──────────────────┬─────────────────────────┬──────────────────────────┐
│ 角色              │ Agent + LLM              │ 原因                      │
├──────────────────┼─────────────────────────┼──────────────────────────┤
│ 需求澄清/编排     │ 本 Harness Orchestrator  │ 管理 LLM，保持全局视角     │
│                  │ + Claude Sonnet          │                          │
├──────────────────┼─────────────────────────┼──────────────────────────┤
│ 架构设计          │ Claude Code + Opus       │ 深度推理，复杂架构设计     │
├──────────────────┼─────────────────────────┼──────────────────────────┤
│ 后端代码实现      │ Codex + GPT-4            │ 强工程化能力，沙盒执行     │
├──────────────────┼─────────────────────────┼──────────────────────────┤
│ 前端页面实现      │ Claude Code + GLM-5.2    │ 前端生态好，快速迭代       │
├──────────────────┼─────────────────────────┼──────────────────────────┤
│ 代码审查          │ Claude Code + Sonnet     │ 平衡速度与审查质量         │
├──────────────────┼─────────────────────────┼──────────────────────────┤
│ 端到端测试        │ Codex + GPT-4            │ 沙盒运行测试，安全隔离     │
├──────────────────┼─────────────────────────┼──────────────────────────┤
│ 最终集成          │ Claude Code + Opus       │ 最高质量保证               │
└──────────────────┴─────────────────────────┴──────────────────────────┘

================================================================================
                    📐 三、架构设计
================================================================================

项目结构：
  blog/
  ├── prisma/
  │   └── schema.prisma          # 数据模型
  ├── src/
  │   ├── app/
  │   │   ├── api/
  │   │   │   ├── auth/          # 认证 API
  │   │   │   ├── posts/         # 文章 API
  │   │   │   └── comments/      # 评论 API
  │   │   ├── login/             # 登录页面
  │   │   ├── register/          # 注册页面
  │   │   ├── posts/             # 文章页面
  │   │   └── layout.tsx         # 根布局
  │   ├── components/
  │   │   ├── ui/                # 通用 UI 组件
  │   │   ├── auth/              # 认证组件
  │   │   ├── posts/             # 文章组件
  │   │   └── comments/          # 评论组件
  │   └── lib/
  │       ├── auth.ts            # 认证工具
  │       ├── db.ts              # 数据库连接
  │       └── utils.ts           # 通用工具
  ├── docker-compose.yml
  ├── Dockerfile
  └── README.md

数据模型：
  User (id, email, password_hash, name, avatar, created_at)
  Post (id, title, slug, content, author_id, tags, published_at, updated_at)
  Comment (id, content, post_id, author_id, created_at)
  Tag (id, name, slug)

================================================================================
                    📋 四、任务清单与依赖关系（DAG）
================================================================================

Phase A: 项目基础设施（并行）
┌──────────────────────────────────────────────────────────────┐
│ TASK-A1  项目脚手架 + 依赖安装           [Claude + Sonnet]    │
│          Next.js init, Prisma, Tailwind, NextAuth 安装        │
│          验收: npm run dev 可启动                              │
├──────────────────────────────────────────────────────────────┤
│ TASK-A2  数据库 Schema 设计               [Claude + Opus]     │
│          Prisma schema, 关联关系, 索引                        │
│          验收: npx prisma migrate dev 成功                     │
├──────────────────────────────────────────────────────────────┤
│ TASK-A3  Docker 配置                      [Codex + GPT-4]     │
│          docker-compose.yml, Dockerfile, .env.example         │
│          验收: docker-compose up 数据库可启动                   │
└──────────────────────────────────────────────────────────────┘
                        ↓ (A1, A2, A3 都完成后)
                        
Phase B: 后端核心（并行）
┌──────────────────────────────────────────────────────────────┐
│ TASK-B1  用户认证系统 (注册+登录+Session)   [Codex + GPT-4]   │
│          依赖: A1, A2                                        │
│          API: /api/auth/register, /api/auth/login            │
│          NextAuth 配置, 密码哈希, JWT                         │
│          验收: POST /api/auth/register 创建用户成功            │
├──────────────────────────────────────────────────────────────┤
│ TASK-B2  文章 CRUD API                     [Codex + GPT-4]   │
│          依赖: A1, A2                                        │
│          API: /api/posts CRUD, slug 生成, 权限                │
│          验收: CRUD 测试全部通过                               │
├──────────────────────────────────────────────────────────────┤
│ TASK-B3  评论 API                          [Codex + GPT-4]   │
│          依赖: A1, A2, B1（需要认证）                         │
│          API: /api/posts/[id]/comments                        │
│          验收: 创建评论 + 关联文章测试通过                      │
└──────────────────────────────────────────────────────────────┘
                        ↓ (B1, B2, B3 都完成后)

Phase C: 前端页面（部分并行）
┌──────────────────────────────────────────────────────────────┐
│ TASK-C1  全局布局 + UI 组件库              [Claude + GLM-5.2] │
│          依赖: A1                                            │
│          Tailwind 配置, Layout, 导航栏, 基础组件              │
│          验收: 页面布局在桌面+手机端正常渲染                    │
├──────────────────────────────────────────────────────────────┤
│ TASK-C2  登录/注册页面                     [Claude + GLM-5.2] │
│          依赖: C1, B1                                        │
│          表单验证, 错误提示, OAuth 按钮                        │
│          验收: 可成功登录并跳转                                │
├──────────────────────────────────────────────────────────────┤
│ TASK-C3  文章列表 + 详情页面               [Claude + GLM-5.2] │
│          依赖: C1, B2                                        │
│          文章卡片, 分页, Markdown 渲染, 标签过滤              │
│          验收: 页面正确渲染文章列表和内容                       │
├──────────────────────────────────────────────────────────────┤
│ TASK-C4  评论组件                         [Claude + GLM-5.2] │
│          依赖: C1, B3                                        │
│          评论列表, 发表评论表单, 登录状态感知                  │
│          验收: 登录后可发表评论，评论实时显示                   │
└──────────────────────────────────────────────────────────────┘
                        ↓ (C1-C4 都完成后)

Phase D: 集成与交付
┌──────────────────────────────────────────────────────────────┐
│ TASK-D1  集成审查 + 端到端测试             [Claude + Opus]    │
│          所有 worktree 合并到 integration 分支                 │
│          全量测试, lint, 构建检查                             │
│          验收: 所有测试通过, 无构建错误                         │
├──────────────────────────────────────────────────────────────┤
│ TASK-D2  README + 部署文档                 [Codex + GPT-4]    │
│          依赖: D1                                            │
│          验收: 按文档操作可成功部署                             │
└──────────────────────────────────────────────────────────────┘

总计: 12 个任务，预计 4 个阶段
最长路径: A1→B1→C2→D1→D2（5 步）

================================================================================
                    💰 五、预估成本
================================================================================

Claude Opus:     ~$6  (架构设计 + 最终审查)
Claude Sonnet:   ~$5  (脚手架 + 审查 × 多次)
Claude GLM-5.2:  ~$4  (前端页面 × 4)
Codex GPT-4:     ~$8  (后端 API × 3 + Docker + 文档)

总计预估:        ~$23 (硬上限 $50)

================================================================================
                    🔒 六、安全约束
================================================================================

• 所有写入 Agent 将在独立 Git worktree 中工作
• 不会直接修改 main 分支
• API Key 不会被存储或传输
• 高风险命令（如 rm -rf）需要审批
• 每个任务有 maxTurns 和预算硬限制
• 失败的修复循环最多尝试 3 次

================================================================================
                    
请输入:
  "approve" 或 "同意"  → 开始自动执行
  具体修改意见          → 我将根据您的意见重新生成计划
  "cancel" 或 "取消"   → 取消本次执行

>
```

---

## 六、Phase 3 — USER APPROVAL：用户审批与迭代修改

### 6.1 审批门逻辑

```
展示计划报告
      │
      ▼
  ┌──────────────────────┐
  │ 等待用户输入           │
  └──────┬───────────────┘
         │
    ┌────┴──────────────┐
    ▼                   ▼
"approve"/"同意"      其他输入（修改意见）
    │                   │
    ▼                   ▼
进入 EXECUTING    ┌─────────────────┐
                  │ Orchestrator     │
                  │ 解析修改意见      │
                  │ 调整计划          │
                  │ 重新生成报告      │
                  └────────┬────────┘
                           │
                           ▼
                    展示新计划报告
                    （再次等待审批）
```

### 6.2 修改意见示例

用户可以输入：

- "用 FastAPI 替代 Next.js 后端" → Orchestrator 重新做技术选型，回溯到 PLANNING
- "不需要评论系统" → 移除相关任务，重新计算 DAG
- "TASK-B1 和 B2 我觉得可以合并" → 调整任务拆分
- "为什么前端用 GLM-5.2？换 Sonnet" → 调整 Agent 分配

每次修改后，Orchestrator 生成**新版本的计划报告**（Plan Version 递增），并高亮变更部分。

---

## 七、Phase 4 — EXECUTING：自动流水线执行

### 7.1 执行循环

```
while (DAG 中有未完成的任务) {

    // 1. 找到所有依赖已满足的 PENDING 任务
    readyTasks = getReadyTasks(dag)

    if (readyTasks.length == 0 && 有 RUNNING 任务) {
        // 等待正在运行的任务完成
        continue
    }

    if (readyTasks.length == 0 && 无 RUNNING 任务 && 有 FAILED 任务) {
        // 所有可执行的任务都完成了，但有失败的
        handleFailedTasks()
        continue
    }

    // 2. 分析是否可以并行
    parallelGroups = computeParallelGroups(readyTasks)
    //   思路：文件重叠度低的、在各自独立 worktree 的、不修改共享模块的 → 可并行
    //        文件重叠度高的、同一模块的 → 串行

    // 3. 为每个并行组创建 worktree
    for (group of parallelGroups) {
        createWorktree(group.task.id)
    }

    // 4. 并行分派任务
    parallel(groups.map(group =>
        dispatchToAgent({
            task: group.task,
            agent: group.assigned_agent,
            model: group.assigned_model,
            worktree: group.worktree,
            goalContract: project.goalContract
        })
    ))

    // 5. 每个任务完成后 → SUBMITTED → 触发 REVIEWING
    for (completedTask of newlyCompleted) {
        review = dispatchToAgent({
            task: completedTask,
            agent: "reviewer",
            model: "sonnet"
        })

        if (review.verdict == "PASS") {
            completedTask.status = "VERIFIED"
            // 清理 worktree（或保留给 Integrator 合并后清理）
        } else {
            completedTask.status = "FAILED_RETRYABLE"
            // 进入 REPAIRING 循环
            repairLoop(completedTask)
        }
    }

    // 6. 更新仪表板
    updateDashboard()
}

// 所有任务完成 → 最终集成
integrateAndDeliver()
```

### 7.2 Repair Loop

```
task.status = FAILED_RETRYABLE
retryCount = 0

while (retryCount < MAX_RETRIES) {
    debugger = dispatchToAgent({
        task: task,
        issues: review.issues,
        agent: "debugger",
        model: "sonnet"  // 第一次用原模型，第二次可能换模型
    })

    reReview = dispatchToAgent({
        task: task,
        agent: "reviewer",
        model: "sonnet"
    })

    if (reReview.verdict == "PASS") {
        task.status = "VERIFIED"
        break
    }

    retryCount++

    // 相同错误指纹检测
    if (sameErrorFingerprint(lastError, currentError)) {
        // 换个 Agent/Model 试试
        task.assigned_agent = swapAgent(task.assigned_agent)
    }
}

if (retryCount >= MAX_RETRIES) {
    task.status = "FAILED_TERMINAL"
    // 总 Agent 决定：重规划 / 请求用户介入
    orchestratorDecide(task)
}
```

### 7.3 中途用户输入处理

执行过程中，用户可以在仪表板的文本框中随时输入意见。输入会被：

```
用户输入 → 解析
    │
    ├── 如果是简单问题（"现在做到哪了？"）
    │   → Orchestrator 直接回答，不中断执行
    │
    ├── 如果是范围修改（"再加一个点赞功能"）
    │   → 暂停受影响的任务
    │   → 更新 Goal Contract（版本递增）
    │   → 重新规划受影响部分
    │   → 恢复执行
    │
    ├── 如果是紧急指令（"立即停止" / "取消"）
    │   → 中止所有子进程
    │   → 保存当前状态
    │   → 清理 worktree
    │
    └── 如果是质量反馈（"前端太丑了，换个设计"）
        → 标记相关任务为 SUPERSEDED
        → 创建新任务
        → 新任务完成后替换旧产出
```

---

## 八、实时仪表板：Live Dashboard

### 8.1 仪表板布局

执行阶段，终端显示实时更新的仪表板：

```
┌──────────────────────────────────────────────────────────────────────┐
│  🔧 Harness Run: proj-2026-0714-001                                   │
│  🎯 目标: 构建博客系统（登录+发布+评论）                                  │
│  📊 整体进度: ████████████░░░░░░░░ 55%  [6/11]                         │
│  ⏱️ 运行时间: 12m 34s                                                  │
│  💰 已花费: $12.50 / $50 预算                                          │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ✅ TASK-A1  项目脚手架           [COMPLETED]  Claude + Sonnet   2m   │
│  ✅ TASK-A2  数据库 Schema        [COMPLETED]  Claude + Opus     3m   │
│  ✅ TASK-A3  Docker 配置          [COMPLETED]  Codex + GPT-4     2m   │
│  ✅ TASK-B1  用户认证系统          [VERIFIED]   Codex + GPT-4     4m   │
│  ✅ TASK-B2  文章 CRUD API        [VERIFIED]   Codex + GPT-4     3m   │
│  ✅ TASK-B3  评论 API             [VERIFIED]   Codex + GPT-4     2m   │
│  🔵 TASK-C1  全局布局+UI组件      [RUNNING]    Claude + GLM-5.2   现在 │
│  🟡 TASK-C2  登录/注册页面        [REVIEWING]  Claude + Sonnet   审查中│
│  ⬜ TASK-C3  文章列表+详情        [PENDING]     Claude + GLM-5.2  等待 │
│  ⬜ TASK-C4  评论组件             [PENDING]     Claude + GLM-5.2  等待 │
│  ⬜ TASK-D1  集成审查             [BLOCKED]     等待 Phase C 完成       │
│  ⬜ TASK-D2  README+文档         [BLOCKED]     等待 D1                  │
│                                                                      │
├──────────────────────────────────────────────────────────────────────┤
│  🤖 当前活跃 Agent:                                                    │
│                                                                      │
│  🔵 [Claude Code + GLM-5.2]                                          │
│     ├─ 状态: RUNNING                                                  │
│     ├─ 任务: TASK-C1 全局布局+UI组件                                    │
│     ├─ Worktree: .harness/worktrees/TASK-C1-global-layout-ui/         │
│     └─ 最近操作: Writing src/components/ui/Button.tsx                  │
│                                                                      │
│  🟡 [Claude Code + Sonnet]                                           │
│     ├─ 状态: REVIEWING                                                │
│     ├─ 任务: TASK-C2 登录/注册页面（审查中）                              │
│     └─ 发现: 2 个问题待修复（表单验证 + 响应式）                          │
│                                                                      │
│  空闲 Agent: Codex + GPT-4 (等待前端任务完成后进入集成阶段)               │
│                                                                      │
├──────────────────────────────────────────────────────────────────────┤
│  ⚠️ 风险提示: TASK-C2 审查发现 2 个问题，可能需要进入修复循环             │
├──────────────────────────────────────────────────────────────────────┤
│  💬 输入意见（输入后回车，留空不发送）：                                   │
│  > _                                                                  │
└──────────────────────────────────────────────────────────────────────┘
```

### 8.2 状态颜色编码

| 颜色 | 状态 | 含义 |
|------|------|------|
| 🟢 绿色 | COMPLETED / VERIFIED | 已完成并通过验证 |
| 🔵 蓝色 | RUNNING | Agent 正在工作中 |
| 🟡 黄色 | REVIEWING | 正在被审查 |
| 🟠 橙色 | REPAIRING | 修复循环中 |
| 🔴 红色 | FAILED_TERMINAL | 失败，需要人工介入 |
| ⚪ 灰色 | PENDING | 等待依赖完成 |
| ⬜ 白色 | BLOCKED | 被阻塞 |
| 🟣 紫色 | INTEGRATING | 正在合并中 |

### 8.3 仪表板刷新

- 每个 Agent 状态变化 → 即时刷新
- 每个任务完成/审查/失败 → 即时刷新
- 空闲时每 5 秒心跳刷新
- 用户输入意见 → 即时处理并刷新

---

## 九、Worktree 隔离策略

### 9.1 核心原则：Worktree 绑定任务，不绑定 Agent

**推荐命名规范**：`TASK-{编号}-{任务简短描述}-{agent}-{llm}`

```
.harness/worktrees/
├── TASK-A1-project-scaffold-claude-sonnet/
├── TASK-A2-db-schema-claude-opus/
├── TASK-A3-docker-config-codex-gpt4/
├── TASK-B1-auth-system-codex-gpt4/
├── TASK-B2-post-crud-codex-gpt4/
├── TASK-B3-comment-api-codex-gpt4/
├── TASK-C1-global-layout-ui-claude-glm52/
├── TASK-C2-login-register-page-claude-glm52/
├── TASK-C3-post-list-detail-claude-glm52/
├── TASK-C4-comment-component-claude-glm52/
├── TASK-D1-integration-review-claude-opus/
└── TASK-D2-readme-docs-codex-gpt4/
```

命名包含 `{agent}-{llm}` 的好处是：仪表板中做文件名过滤、历史分析时一目了然。如果中途换 Agent（比如 Codex 失败换 Claude 接手），worktree 名称不变，只是在状态数据库里更新 `assigned_profile` 字段。

### 9.2 Worktree 不是项目架构

**关键理解**：`worktrees/TASK-B1-auth-system-codex-gpt4/` 内部包含**完整的项目副本**：

```
worktrees/TASK-B1-auth-system-codex-gpt4/
├── prisma/
│   └── schema.prisma
├── src/
│   ├── app/
│   │   ├── api/auth/    ← 这个任务只改这部分
│   │   ├── login/
│   │   └── ...
│   ├── components/
│   └── lib/
├── package.json
└── ...
```

Worktree 是**物理隔离的工作环境**，不是项目的逻辑子目录。Agent 在 worktree 中拥有完整的项目上下文，通过 `scope.allowedPaths` 限制修改范围。

最终合并流程：

```
TASK-A1 branch ──┐
TASK-A2 branch ──┤
TASK-B1 branch ──┼──→ integration/current ──→ 全量验证 ──→ main
TASK-B2 branch ──┤
TASK-C1 branch ──┘
```

合并后，`worktrees/` 目录可以全部删除。它们只是**施工现场**，不是最终建筑结构。

### 9.3 动态 Worktree 粒度

不强制"一任务一 worktree"。Scheduler 根据分析选择：

```yaml
小任务（bug fix / 单接口 / 单组测试）:
  粒度: "一任务一 worktree"
  适合: 独立修改、低耦合

大工作流（连续多个相关任务）:
  粒度: "一工作流一 worktree"
  示例: worktrees/workstream-authentication/
        内部串行执行 TASK-B1, TASK-C2，避免频繁合并
  适合: 任务紧密耦合，串行更高效

高冲突区域（共享模块修改）:
  粒度: "串行共用一 worktree"
  示例: 两个任务都要改 package.json + 数据库 schema
        在同一个 worktree 中按依赖顺序串行执行
  适合: 避免合并冲突
```

### 9.4 动态范围申请

Agent 不严格局限于初始 scope。如果发现需要修改共享模块：

```json
{
  "type": "scope_expansion_request",
  "task_id": "TASK-B1",
  "path": "packages/shared/types/user.ts",
  "reason": "认证系统需要新增 provider 字段到 User 类型",
  "impact": ["TASK-B2（文章 API 也使用 User 类型）"]
}
```

Orchestrator 可以：
- 允许当前任务扩展范围
- 创建一个新的共享模块任务
- 调整依赖关系
- 暂停可能冲突的另一个任务

**既隔离，又不僵化。**

### 9.5 架构变更流程

当执行中发现初始架构需要改变时（例如从单体→packages 结构），触发明确流程：

```
ARCHITECTURE_CHANGE_PROPOSED
        ↓
影响分析（哪些任务受影响）
        ↓
暂停受影响任务
        ↓
更新 ADR 和任务 DAG（版本化 Plan Revision）
        ↓
先执行架构迁移任务
        ↓
其他 worktree rebase 到新基线
        ↓
恢复执行
```

---

## 十、状态机完整设计

### 10.1 项目级状态

```
CREATED → CLARIFYING → GOAL_LOCKED → ENV_DISCOVERY
                                         ↓
                                    PLANNING
                                         ↓
                                    PLAN_REVIEW ──→ NEEDS_USER_APPROVAL
                                         ↓                │
                                    SCHEDULING ←──────────┘（用户批准后）
                                         ↓
                                    EXECUTING
                                         ↓
                                    INTEGRATING
                                         ↓
                                    VERIFYING
                                      ├── PASS → DELIVERING → DONE
                                      ├── FIXABLE → REPAIRING → EXECUTING
                                      ├── REPLAN → PLAN_REVISION
                                      └── BLOCKED（等待人工）
```

### 10.2 任务级状态

```
PENDING → READY → LEASED → RUNNING → SUBMITTED
                                          │
                                          ▼
                                      REVIEWING
                                        ├── PASS → VERIFIED → MERGED
                                        ├── FAIL → FAILED_RETRYABLE → REPAIRING → RUNNING
                                        ├── FAIL → FAILED_TERMINAL → BLOCKED
                                        └── SUPERSEDED（被取代）
```

### 10.3 状态转换规则（确定性）

**任务进入 VERIFIED 的必要条件（ALL 必须满足）**：

```
✅ Agent 提交了符合 outputSchema 的结果
✅ 有可定位的 Git commit hash
✅ 所有 acceptanceChecks 退出码为 0
✅ 没有未解决的 blocker
✅ 独立 Reviewer Agent 给出 PASS 结论
✅ 没有检测到密钥泄露
```

**LLM 只能建议状态转换，真正的状态变更由确定性规则执行。**

---

## 十一、Agent 发现与能力矩阵

### 11.1 Runtime Profile

```json
{
  "id": "claude-sonnet-profile-1",
  "agent": "claude-code",
  "agentVersion": "2.0.0",
  "adapter": "stream-json",
  "provider": "anthropic",
  "model": "claude-sonnet-5",
  "authState": "verified",
  "workspaceModes": ["read", "write", "shell"],
  "structuredOutput": true,
  "streaming": true,
  "resumeSession": true,
  "maxConcurrency": 3,
  "status": "AVAILABLE"
}
```

### 11.2 发现流程

```
1. 扫描 PATH 中可执行程序
2. 调用 detect() / getVersion() / checkAuthentication()
3. 读取配置文件引用（不读密钥值）
4. 在临时仓库运行微型探测任务
5. 状态分级：DETECTED → CONFIGURED → AUTHENTICATED → PROBED → AVAILABLE
6. 结果写入 agents_registry.json，定时刷新
```

---

## 十二、Agent 路由与分派策略

### 12.1 两阶段路由

**第一阶段：硬过滤**

```
Agent AVAILABLE？
├─ 是否有写权限？（写入任务需要）
├─ 是否支持目标语言/框架？
├─ 是否支持结构化输出？
├─ 是否能运行 shell？
├─ 是否满足成本限制？
└─ 是否拥有需要的 MCP 工具？
```

**第二阶段：评分**

```
score =
    task_affinity
  + historical_success_rate * 0.3
  + verification_pass_rate * 0.2
  + reliability * 0.15
  - estimated_cost * 0.2
  - estimated_latency * 0.1
  - recent_failure_penalty * 0.05
```

### 12.2 Agent ↔ LLM 初始适配矩阵

| 任务类型 | 推荐组合 | 原因 |
|---------|---------|------|
| 需求澄清（Orchestrator） | Claude Code + Sonnet | 推理平衡，成本适中 |
| 架构设计 | Claude Code + Opus | 深度推理，长上下文 |
| 后端/API 实现 | Codex + GPT-4 | 工程化能力，沙盒安全 |
| 前端页面/组件 | Claude Code + GLM-5.2 / Sonnet | 前端生态，快速迭代 |
| 代码审查 | Claude Code + Sonnet | 平衡速度与质量 |
| 调试修复 | Claude Code + Sonnet | 快速修复循环 |
| 测试编写 | Codex + GPT-4 | 沙盒运行测试 |
| 文档生成 | 任意 + Sonnet / Haiku | 轻量任务 |
| 最终集成审查 | Claude Code + Opus | 最高质量 |

**此矩阵会被 Harness 自动收集的历史数据持续修正，不形成静态偏见。**

---

## 十三、Tools、Hooks 与构成要素

### 13.1 Orchestrator 使用的核心 Tools

| 工具 | 用途 |
|------|------|
| `AgentDiscovery` | 扫描本地 Agent 能力，生成 Runtime Profile |
| `AgentDispatch` | 分派任务给指定 Agent+LLM 组合 |
| `StateManager` | 读写项目/任务状态、DAG、检查点 |
| `GoalContractManager` | 创建和版本化 Goal Contract |
| `WorktreeManager` | 创建/合并/清理 Git worktree |
| `CheckpointManager` | 保存和恢复完整运行状态 |
| `BudgetTracker` | 硬性预算跟踪和超限中断 |
| `UserApproval` | 展示报告/问题，等待用户输入 |
| `UserFeedback` | 执行中接收用户的实时反馈 |
| `VerificationOrchestrator` | 编排 Reviewer → Debugger 循环 |

### 13.2 子 Agent 的 Tools（按角色最小授予）

```yaml
explorer: [read, glob, grep, lsp, web_search]
architect: [read, glob, grep, web_search, web_fetch]
implementer: [read, write, edit, bash]
tester: [read, write, bash]
reviewer: [read, glob, grep, bash, lsp]
debugger: [read, write, edit, bash]
integrator: [read, bash(git)]
```

**子 Agent 绝不授予**：`AgentDispatch`、`StateManager`、`GoalContractManager`

### 13.3 Harness 统一 Hooks

```yaml
# 项目生命周期
on_project_created: [init_git_repo, create_harness_dir_structure]

# 任务调度
before_dispatch:
  - verify_workspace_lease
  - auto_commit_if_dirty        # 防止子 Agent 改崩代码无法回滚
  - snapshot_dependency_state

# 工具调用安全拦截
before_tool_call:
  - check_file_in_scope
  - check_command_whitelist
  - check_budget_remaining
after_tool_call: [log_tool_usage, incremental_cost_update]

# 文件变更
after_file_change: [auto_format, auto_lint]
before_commit: [run_unit_tests, check_for_secrets]

# 任务完成
after_task_submitted: [validate_output_schema, compute_artifact_hash]

# 验证
before_verify: [collect_artifacts]
after_verify: [update_verification_status, log_evidence]

# 合并
before_merge: [check_conflicts, run_integration_tests]
after_merge: [cleanup_worktree, release_workspace_lease]

# 异常
on_retry: [check_retry_limits, maybe_swap_agent]
on_stall: [send_health_check, escalate]
on_budget_exceeded: [force_terminate, notify_orchestrator]
on_approval_required: [pause_dag, notify_user]
on_cancel: [terminate_all, cleanup_worktrees, save_final_state]
```

---

## 十四、CLI 工具设计

### 14.1 安装与配置

```bash
# 安装
npm install -g agent-harness

# 首次运行 → 配置向导
harness setup

# 配置向导会引导用户设置：
#   1. Orchestrator 使用的 LLM API
#      - Provider: anthropic / openai / deepseek / zhipu / custom
#      - API Key: ****
#      - API Base URL（可选，用于代理或兼容 API）
#      - Model: sonnet / opus / deepseek-v4 / glm-5.2
#
#   2. 预算默认值
#      - 单任务最大预算: $4
#      - 项目总预算上限: $50
#
#   3. 安全策略
#      - 是否允许 shell 命令？(推荐: 是，但高风险命令需审批)
#      - 是否自动 git commit？(推荐: 是，每个任务自动 commit)
```

配置文件 `~/.harness/config.json`：

```json
{
  "orchestrator": {
    "provider": "anthropic",
    "apiKey": "***",
    "baseUrl": "https://api.anthropic.com",
    "model": "sonnet"
  },
  "budget": {
    "maxPerTask": 4,
    "maxPerProject": 50
  },
  "security": {
    "allowShell": true,
    "requireApprovalForDangerousCommands": true,
    "autoCommit": true
  },
  "agentDiscovery": {
    "scanInterval": 3600,
    "additionalPaths": []
  }
}
```

### 14.2 命令列表

```bash
harness setup              # 配置向导
harness run "需求描述"      # 启动新项目（进入 CLARIFYING）
harness status             # 查看当前项目状态
harness approve            # 批准当前计划
harness feedback "意见"    # 在审批阶段提供修改意见 / 执行中提供实时反馈
harness cancel             # 取消当前运行
harness resume [run_id]    # 恢复之前的运行
harness list               # 列出历史运行
harness agents             # 列出已发现的 Agent 及其能力
harness dashboard          # 打开实时仪表板（如果之前关闭了）
```

---

## 十五、MVP 范围与实现路线图

### 15.1 MVP 范围

```
✅ 入口: harness run / status / approve / cancel / feedback
✅ Agent: Claude Code (stream-json) + Codex (app-server JSON-RPC)
✅ 流程: clarify → plan → user_approval → execute → review → repair → deliver
✅ 存储: SQLite + append-only event log
✅ 安全: 不存储密钥、worktree 隔离、命令白名单、审批门
✅ 仪表板: 终端 TUI 实时显示（Blessed/Ink）
✅ 平台: Windows + macOS + Linux
```

### 15.2 路线图

```
Phase 1: 核心基础设施（3-4 周）
├── Harness CLI 骨架（TypeScript）
├── 配置向导（harness setup）
├── SQLite 数据模型
├── Agent 发现系统（扫描 + 探测）
├── Claude Code stream-json Adapter
├── Codex app-server JSON-RPC Adapter
├── Agent Adapter 统一接口
├── 状态机核心（项目级 + 任务级双层）
├── Goal Contract 管理
└── 文件系统结构 (.harness/)

Phase 2: 用户交互与审批（2-3 周）
├── CLARIFYING 澄清流程（Orchestrator 系统提示 + 提问逻辑）
├── PLANNING 规划流程（Explorer + Architect + Decomposer）
├── 执行计划报告生成（完整格式）
├── USER APPROVAL 审批门（展示+等待+修改→重新规划循环）
├── 终端 TUI 仪表板（Blessed/Ink）
│   ├── 任务列表 + 状态颜色编码
│   ├── 活跃 Agent 面板
│   ├── 进度条
│   └── 用户反馈输入框
└── 中途用户输入处理

Phase 3: 执行引擎（3-4 周）
├── DAG 依赖解析与调度器
├── Worktree 管理器（创建/合并/清理/粒度选择）
├── 结构化任务信封（输入+输出 Schema）
├── Checkpoint 系统（保存/恢复/断点续传）
├── Agent 分派与并行调度
├── Review → Repair 循环（maxRetries + 错误指纹检测）
├── 动态范围申请机制
├── 架构变更流程
├── 预算追踪与硬限制
└── Harness 统一 Hooks 系统（核心 Hook）

Phase 4: 集成与交付（2-3 周）
├── Integrator 合并流程
├── 端到端验证门
├── 跨 Agent 可观测性（OpenTelemetry）
├── 安全策略引擎（文件范围+命令白名单+密钥扫描）
├── 偏离检测（周期性 Goal alignment 检查）
├── Agent 历史评分系统（持续学习路由优化）
├── 错误恢复与降级策略
└── 完整文档

Phase 5: 扩展（未来）
├── ACP Adapter
├── Gemini CLI Adapter
├── MCP Server 暴露
├── Web UI / Tauri 桌面界面
├── Temporal 持久工作流
├── 团队协作支持
└── 插件市场
```

---

## 十六、核心设计原则

### 这个 Harness 应当做

```
✅ 锁定目标，维护 Goal Contract
✅ 执行前澄清所有模糊需求
✅ 生成完整的执行计划报告供用户审批
✅ 审批前允许用户反复修改直到满意
✅ 实时展示执行状态（目标、进度、Agent 活动、成本）
✅ 允许用户在执行中随时输入反馈
✅ 维护任务 DAG 和依赖关系
✅ 按任务隔离 Worktree（不按 Agent 品牌）
✅ 选择最优 Agent+LLM 组合（硬过滤+评分+学习）
✅ 收集可验证的证据（commit/hash/exit code/test output）
✅ 状态转换由确定性规则执行
✅ 支持断点续传和进程崩溃恢复
```

### 这个 Harness 不应当做

```
❌ 自己保存所有上下文
❌ 跳过澄清直接规划
❌ 绕过用户审批直接执行
❌ 仅根据自然语言判断任务完成
❌ 让多个 Agent 共享同一个可写目录
❌ 把密钥读入 Harness 数据库
❌ 无限执行和无限 Debug
❌ 形成静态的 Agent 品牌偏见
```

### 一句话

> 这不是"再造一个更强的总 Agent"，而是**确定性的本地编排运行时 + 管理 LLM + 可插拔 Agent Adapter + 完整的用户交互审批流程 + 实时仪表板**。它让用户说一次需求 → 提问澄清 → 展示计划 → 等待批准 → 自动流水线执行 → 交付可验证的生产级代码。
