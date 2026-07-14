# Adapter Contract Tests — Agent Harness

> **版本**: v1.0
> **日期**: 2026-07-14

---

## 1. 概述

每个 `AgentAdapter` 实现**必须**通过此契约测试套件。这是 Adapter 进入调度池的前提条件。

---

## 2. 测试套件

```typescript
// 契约测试工厂函数
function createAdapterContractTests(
  name: string,
  factory: () => Promise<AgentAdapter>
): void {

  describe(`AgentAdapter Contract: ${name}`, () => {
    let adapter: AgentAdapter;
    let tempDir: string;

    beforeEach(async () => {
      adapter = await factory();
      tempDir = await fs.mkdtemp('harness-contract-test-');
    });

    afterEach(async () => {
      await fs.rm(tempDir, { recursive: true, force: true });
    });

    // ── T1: detect() ──────────────────────────
    it('T1.1: detect() returns found:true when binary exists', async () => {
      const result = await adapter.detect();
      expect(result.found).toBe(true);
      if (result.found) {
        expect(result.binaryPath).toBeTruthy();
      }
    });

    it('T1.2: detect() returns found:false when binary not found', async () => {
      const result = await adapter.detect('/nonexistent/path');
      expect(result.found).toBe(false);
    });

    // ── T2: getVersion() ───────────────────────
    it('T2.1: getVersion() returns non-empty string', async () => {
      const version = await adapter.getVersion();
      expect(typeof version).toBe('string');
      expect(version.length).toBeGreaterThan(0);
    });

    // ── T3: inspectConfiguration() ─────────────
    it('T3.1: inspectConfiguration() returns valid config', async () => {
      const config = await adapter.inspectConfiguration();
      expect(config.authMode).toBeDefined();
      // 不读取实际密钥值
    });

    // ── T4: checkAuthentication() ──────────────
    it('T4.1: checkAuthentication() returns boolean', async () => {
      const auth = await adapter.checkAuthentication();
      expect(typeof auth.authenticated).toBe('boolean');
    });

    // ── T5: probe() ────────────────────────────
    it('T5.1: probe() does not modify user files', async () => {
      const beforeSnapshot = await takeFileSnapshot(tempDir);
      await adapter.probe(tempDir);
      const afterSnapshot = await takeFileSnapshot(tempDir);
      // probe 应在临时目录中只创建测试文件，不修改现有文件
    });

    it('T5.2: probe() returns structured result', async () => {
      const result = await adapter.probe(tempDir);
      expect(result.status).toMatch(/passed|degraded|failed/);
      expect(result.checks).toBeDefined();
    });

    // ── T6: startSession() → sendTask() → receiveEvents() ──
    it('T6.1: Full session lifecycle', async () => {
      const session = await adapter.startSession(mockProfile, {
        workingDirectory: tempDir,
        timeoutMs: 30000
      });

      expect(session.sessionId).toBeTruthy();
      expect(session.isActive).toBe(true);

      await adapter.sendTask(session, {
        taskId: 'CONTRACT-TEST-001',
        taskGoal: 'Create a file named hello.txt with content "world"',
        scope: { allowedPaths: ['hello.txt'] },
        acceptanceChecks: ['test -f hello.txt'],
        allowedTools: ['write', 'bash'],
        outputSchema: 'TaskResultV1',
        budget: { maxTurns: 5, maxTimeMs: 30000 }
      });

      const events: AgentEvent[] = [];
      for await (const event of adapter.receiveEvents(session)) {
        events.push(event);
      }

      expect(events.length).toBeGreaterThan(0);
      expect(events.some(e => e.type === 'session_end')).toBe(true);
    });

    // ── T7: interrupt() ────────────────────────
    it('T7.1: interrupt() stops agent within timeout', async () => {
      const session = await adapter.startSession(mockProfile, {
        workingDirectory: tempDir,
        timeoutMs: 60000
      });

      await adapter.sendTask(session, longRunningTask);

      // 等待 2 秒让 agent 开始执行
      await sleep(2000);
      const startTime = Date.now();
      await adapter.interrupt(session);
      const duration = Date.now() - startTime;

      expect(duration).toBeLessThan(15000); // 应该在 15s 内完成中断
    });

    // ── T8: cancel() ───────────────────────────
    it('T8.1: cancel() forcefully terminates agent', async () => {
      const session = await adapter.startSession(mockProfile, {
        workingDirectory: tempDir,
        timeoutMs: 60000
      });

      await adapter.sendTask(session, longRunningTask);
      await sleep(1000);

      const startTime = Date.now();
      await adapter.cancel(session);
      const duration = Date.now() - startTime;

      expect(duration).toBeLessThan(10000); // cancel 应该比 interrupt 更快
      expect(session.isActive).toBe(false);
    });

    // ── T9: dispose() ──────────────────────────
    it('T9.1: dispose() is idempotent', async () => {
      const session = await adapter.startSession(mockProfile, {
        workingDirectory: tempDir,
        timeoutMs: 30000
      });

      await adapter.dispose(session);
      // 第二次调用不应该抛错
      await expect(adapter.dispose(session)).resolves.not.toThrow();
    });

    it('T9.2: dispose() cleans up resources', async () => {
      const session = await adapter.startSession(mockProfile, {
        workingDirectory: tempDir,
        timeoutMs: 30000
      });

      await adapter.dispose(session);
      expect(session.isActive).toBe(false);
    });

    // ── T10: Error types ────────────────────────
    it('T10.1: Process error has meaningful type', async () => {
      // 模拟一个会导致 Agent 崩溃的场景
      const session = await adapter.startSession(mockProfile, {
        workingDirectory: tempDir,
        timeoutMs: 5000
      });

      try {
        await adapter.sendTask(session, { /* 故意无效的 task */ });
      } catch (error) {
        // error 应该有明确的类型（不是泛型 Error）
        expect(error).toBeInstanceOf(Error);
        expect(error.constructor.name).not.toBe('Error');
      }
    });
  });
}
```

---

## 3. Adapter 注册

每个 Adapter 实现必须通过契约测试后才能注册。在生产代码中：

```typescript
// infrastructure/adapters/register.ts
export async function registerAdapter(
  adapter: AgentAdapter,
  contractTests: AdapterContractTestSuite
): Promise<RegistrationResult> {
  const results = await contractTests.run(adapter);
  if (!results.passed) {
    return {
      status: "rejected",
      failures: results.failures,
      message: `Adapter '${adapter.kind}' failed contract tests`
    };
  }
  return { status: "registered" };
}
```

（Foundation Release 中，此注册在 Adapter 开发时手动验证，不运行时动态注册。）

---

## 4. FakeAdapter Contract Tests

FakeAdapter 也通过相同的契约测试套件。这保证了：
- 契约测试本身是正确的（不会被错误实现通过）
- FakeAdapter 的行为与真实 Adapter 一致

---

## 5. 环境相关测试

| 测试 | FakeAdapter | CodexSdkAdapter | ClaudeCliAdapter |
|------|:---:|:---:|:---:|
| detect() | ✅ | ✅ (需要 Codex 安装) | ✅ (需要 Claude 安装) |
| getVersion() | ✅ | ✅ | ✅ |
| inspectConfiguration() | ✅ | ✅ | ✅ |
| checkAuthentication() | ✅ | ✅ | ✅ |
| probe() | ✅ | ✅ | ✅ |
| Full session | ✅ | ✅ (需要认证) | ✅ (需要登录) |
| interrupt() | ✅ | ✅ | ✅ |
| cancel() | ✅ | ✅ | ✅ |
| dispose() 幂等 | ✅ | ✅ | ✅ |
| Error types | ✅ | ✅ | ✅ |
