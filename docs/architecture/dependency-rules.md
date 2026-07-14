# Dependency Rules — Agent Harness

> **文档类型**: 架构规范
> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 概述

本文档定义了 Agent Harness 代码库中各层之间的依赖规则。这些规则通过架构设计强制执行，并应在 CI 中通过 lint 规则验证。

---

## 2. 核心规则

### 规则 1：严格分层依赖

```
依赖只能从上向下：

cli ──────────────┐
local-api ────────┤
                  ▼
infrastructure ───┐
                  ▼
application ──────┐
                  ▼
domain (contracts + fsm + policies)
```

**具体约束**：
- `domain/` **禁止**引用任何其他层
- `application/` **只能**引用 `domain/`
- `infrastructure/` **只能**引用 `application/` 和 `domain/`
- `cli/` **只能**引用 `infrastructure/` 和 `local-api/`
- `local-api/` **只能**引用 `application/`
- `testing-kit/` **只能**引用 `domain/contracts/`

### 规则 2：Contract 优先

- 所有跨层通信**必须**通过 `domain/contracts/` 中定义的接口
- 具体实现类**不得**被其他层直接 import
- 使用 interface + 依赖注入，而不是 concrete class import

**示例**：
```typescript
// ✅ 正确：application 使用 domain 接口
import { AgentAdapter } from '../../domain/contracts/AgentAdapter.js';
class ProcessManager {
  constructor(private adapter: AgentAdapter) {}
}

// ❌ 错误：application 直接引用 infrastructure 实现
import { CodexSdkAdapter } from '../../infrastructure/adapters/codex/CodexSdkAdapter.js';
```

### 规则 3：禁止循环依赖

- 任何两个模块之间**禁止**相互 import
- 如果 A import B，则 B 不得 import A（直接或传递）
- 使用依赖倒转（DIP）打破循环

### 规则 4：Domain 零依赖

`domain/` 目录只能依赖：
- TypeScript 标准类型
- Zod（仅用于 Schema 定义和运行时校验）
- Node.js 标准库中的纯类型（如 `Buffer`、`Error`）

**禁止**依赖：
- 文件系统（`fs`, `path`）
- 网络（`http`, `net`）
- 数据库驱动
- 子进程管理
- 任何第三方服务 SDK

### 规则 5：避免字符串分发

**禁止**通过 agent name 字符串做 if-else：

```typescript
// ❌ 禁止
if (profile.agentKind === "claude-code") {
  // 特殊处理
} else if (profile.agentKind === "codex") {
  // 特殊处理
}

// ✅ 正确：通过 RuntimeProfile + AgentAdapter 接口多态
const adapter = this.adapterRegistry.getAdapter(profile.adapterKind);
await adapter.sendTask(session, envelope);
```

唯一的例外：`AgentDiscoveryService` 和 Adapter 工厂方法中使用 `adapterKind` 做路由（因为这是工厂的固有职责）。

### 规则 6：无隐式全局状态

- 所有模块**不得**依赖隐式全局变量或单例
- 需要共享状态时，通过显式的依赖注入传入
- SQLite 连接、配置对象等通过构造函数注入

### 规则 7：Schema 边界

- 所有跨模块/跨进程的数据**必须**经过 Zod Schema 校验
- 从 Agent 子进程接收的数据**必须**在校验后才进入 domain 模型
- 从 SQLite 读取的数据**必须**在校验后才返回给调用方

---

## 3. enforcement（执行）

### ESLint Rules

```json
{
  "rules": {
    "import/no-cycle": "error",
    "import/no-self-import": "error",
    "no-restricted-imports": [
      "error",
      {
        "patterns": [
          {
            "group": ["../../../infrastructure/*"],
            "message": "domain/ 不得引用 infrastructure/"
          }
        ]
      }
    ]
  }
}
```

### Project References 检查

未来如果引入 TypeScript Project References，通过 `tsc --build` 自动验证分层。

### Code Review Checklist

- [ ] 新 import 是否跨越了禁止的层级？
- [ ] 是否通过字符串判断 Agent 类型？
- [ ] 跨模块数据是否经过 Schema 校验？
- [ ] 是否有隐式的全局状态依赖？
- [ ] import 图是否引入了循环？

---

## 4. 例外情况

| 例外 | 条件 | 审批 |
|------|------|------|
| `testing-kit/` 引用具体 Adapter | 仅用于集成测试 fixture | ADR 记录 |
| `infrastructure/adapters/` 内部共享工具类 | 同一类 Adapter 之间（如同在 `codex/` 内） | 代码审查 |
| CLI 中硬编码 Adapter 注册 | Foundation Release 期间 | 后续版本改为配置驱动 |

所有例外必须在 ADR 中记录，并注明移除条件。
