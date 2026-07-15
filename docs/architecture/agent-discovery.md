# Agent Discovery & RuntimeProfile Model

> **版本**: v1.0
> **日期**: 2026-07-15

---

## 1. AgentDefinition vs RuntimeProfile

```
AgentDefinition:     执行引擎 (Claude CLI, Codex CLI)
  ├─ executable_path:  "C:\...\npm\claude.ps1"
  ├─ version:          "2.1.210"
  └─ profiles:         [profile-1, profile-2, ...]

RuntimeProfile:      executable + provider + model + auth + capabilities
  ├─ agent_definition_id: "claude-code-abc123"
  ├─ provider:            "deepseek" (or null = default)
  ├─ model:               "deepseek-v4-pro" (or null = default)
  └─ capabilities:        { execute: SUPPORTED, ... }
```

同一 AgentDefinition 可以有多个 RuntimeProfile：
- `claude-native` — 默认 provider/model
- `claude-deepseek` — model=deepseek-v4-pro
- `claude-glm` — wrapper script, provider=zhipu
- `claude-company-gateway` — custom base_url

---

## 2. Discovery Sources

| Source | Example | Detection |
|--------|---------|-----------|
| PATH standard | `claude`, `codex`, `gemini` | `which` / `where` |
| Common install dirs | `~/.local/bin`, npm global, scoop | Recursive scan |
| Wrapper (safe pattern) | `claude-glm`, `claude-*`, `*-claude` | Name matches `claude-*` or `*-claude` |
| Environment | Inherited at Harness startup | `ANTHROPIC_MODEL`, etc. |
| User registered | Explicit `harness config add-agent` | User-provided path |
| Sidecar manifest | `.harness/profile-glm.json` (no keys) | JSON file with provider/model refs |
| Built-in template | Provider templates (not auto-enabled) | Internal catalog |

**Safety rule**: 不扫描并执行 PATH 中所有脚本。Wrapper 仅在名称匹配安全模式、用户显式注册、或存在 sidecar manifest 时才 Probe。

---

## 3. Two-Phase Discovery

### Passive Discovery (免费)

No model invocation. No API cost.

```
1. Executable found?         → which/where
2. Version check             → <agent> --version
3. Config parseable?         → Read config file for provider/model refs (NOT keys)
4. Auth check                → <agent> auth status / login status
5. Wrapper inspection        → --version output; compare with base agent

Output: AgentDefinition + PassiveProbeResult
```

### Active Validation (收费, 需用户批准)

Runs a minimal smoke test in a temp git repo.

```
1. Execute:     <agent> exec --json "Create a file named probe.txt with content 'ok'"
2. Stream:      Verify JSONL output on stdout
3. Result:      Verify turn.completed or result event
4. Cancel:      Verify process responds to SIGTERM
5. Exit:        Verify exit_code reflects success/failure

Output: RuntimeProfile.execution_status = SmokeTestPassed
       + ActiveValidationResult
```

---

## 4. Provider Identification Priority

1. **User declared** — explicit in RuntimeProfile
2. **Known endpoint exact match** — base_url matches known provider
3. **Sidecar manifest** — profile JSON declares provider
4. **Probe metadata** — Agent reports model/provider in result
5. **Fallback** — `custom_anthropic_compatible` / `custom_openai_compatible` / `custom_unknown`

---

## 5. Credential Safety

```
✅ Record: credential presence (auth_mode + credential_ref)
✅ Record: source label ("ANTHROPIC_API_KEY", "login_session")
❌ NEVER read auth.json
❌ NEVER read API key values
❌ NEVER persist keys to database
❌ NEVER auto-modify config, env vars, or login state
```

---

## 6. harness discover Output

```json
{
  "agents": [
    {
      "id": "claude-code-abc123",
      "agent_kind": "claude-code",
      "executable_path": "C:\\Users\\...\\npm\\claude.ps1",
      "version": "2.1.210",
      "wrapper": false,
      "profiles": [
        {
          "id": "claude-default",
          "provider": "anthropic",
          "model": null,
          "auth": "authenticated (api_key)",
          "core_status": "AVAILABLE",
          "execution_status": "UNTESTED"
        }
      ]
    },
    {
      "id": "claude-code-glm-xyz",
      "agent_kind": "claude-code",
      "executable_path": "C:\\Users\\...\\bin\\claude-glm.cmd",
      "version": "2.1.210",
      "wrapper": true,
      "wraps": "claude-code",
      "profiles": [
        {
          "id": "claude-glm-default",
          "provider": "custom_anthropic_compatible",
          "model": null,
          "auth": "unknown",
          "core_status": "AVAILABLE",
          "execution_status": "UNTESTED"
        }
      ]
    },
    {
      "id": "codex-def456",
      "agent_kind": "codex",
      "executable_path": "C:\\Users\\...\\npm\\codex.ps1",
      "version": "0.144.4",
      "profiles": [
        {
          "id": "codex-default",
          "provider": "openai",
          "auth": "authenticated (login_session)",
          "core_status": "AVAILABLE",
          "optional_integrations": [
            {"name":"codex_apps","status":"DEGRADED_STARTUP_TIMEOUT","required":false}
          ]
        }
      ]
    }
  ]
}
```

---

## 7. harness doctor Output

Diagnostic report:

```
PASS   claude 2.1.210 — executable found, auth OK
PASS   claude-glm 2.1.210 — wrapper script, version confirmed
PASS   codex 0.144.4 — executable found, auth OK
WARN   codex MCP "codex_apps" — startup timeout (non-blocking)
INFO   gemini 0.6.1 — found but not yet probed
INFO   3 agents discovered, 4 profiles available
INFO   Active validation: NOT YET RUN (requires user approval)
```

---

## 8. Wrapper Commands

Wrapper (如 `claude-glm`) 是不透明启动入口：
- 不解析其中的 API Key
- 通过 `--version`、headless smoke test、stream-json 和 exit code 判断身份
- 实际执行收费 Probe 前获取用户授权

Wrapper 可能通过环境变量、命令行参数或包装脚本传递不同的 model/provider 配置。Harness 只观察行为，不解析内部实现。
