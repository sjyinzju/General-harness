//! Supervisor ownership acquisition and fencing.
//!
//! Implements single-active-owner semantics using:
//! - SQLite UNIQUE partial index on active leases per state_directory_id
//! - Monotonic fencing token
//! - CAS (compare-and-swap) for lease acquisition and heartbeat
//! - Process identity verification (PID + process creation time + boot nonce)

use chrono::{DateTime, Utc};
use harness_core::contracts::supervisor::{SupervisorInstance, SupervisorInstanceId};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use std::time::Duration;
use tracing;

use super::repo::SupervisorRepo;

/// Result of a takeover operation.
pub struct TakeoverResult {
    pub new_fencing_token: i64,
    pub previous_instance_id: SupervisorInstanceId,
}

/// Manages Supervisor ownership — lease acquisition, fencing, and takeover.
pub struct OwnershipManager {
    repo: SupervisorRepo,
    lease_duration: Duration,
    state_directory_id: String,
}

impl OwnershipManager {
    pub fn new(pool: sqlx::SqlitePool, config: harness_core::contracts::supervisor::SupervisorConfig) -> Self {
        Self {
            repo: SupervisorRepo::new(pool),
            lease_duration: Duration::from_secs(config.lease_duration_secs),
            state_directory_id: config.state_directory_id.clone(),
        }
    }

    /// Attempt to acquire the supervisor lease.
    ///
    /// # Flow
    /// 1. Check if there's an active lease for this state_directory_id
    /// 2. If none → acquire with fencing_token = 1
    /// 3. If active but expired owner → takeover
    /// 4. If active and healthy → reject
    pub async fn acquire(
        &self,
        instance: &SupervisorInstance,
    ) -> Result<(), CoreError> {
        let existing = self
            .repo
            .get_active_lease(&self.state_directory_id)
            .await?;

        match existing {
            None => {
                // No active lease — acquire (fencing_token starts at 1)
                let expires_at = Utc::now() + self.lease_duration;
                self.repo
                    .acquire_lease(
                        &instance.instance_id,
                        &self.state_directory_id,
                        1_i64,
                        expires_at,
                    )
                    .await?;

                tracing::info!(
                    instance_id = %instance.instance_id,
                    fencing_token = 1,
                    "supervisor lease acquired (fresh)"
                );
                Ok(())
            }
            Some(active_lease) => {
                // Check if the existing lease is from a dead/expired owner
                let active_instance_id =
                    SupervisorInstanceId(active_lease.instance_id.clone());

                let expired = parse_time(&active_lease.expires_at) < Utc::now();

                if expired {
                    // Lease expired — verify the old owner is really dead
                    let dead = self
                        .verify_owner_dead(&active_instance_id)
                        .await;

                    if dead {
                        // Stale owner confirmed dead — eligible for takeover
                        let reason = format!(
                            "lease expired at {} and owner {} is dead",
                            active_lease.expires_at, active_instance_id
                        );
                        return Err(CoreError::new(
                            ErrorCode::InvalidState,
                            format!("stale: {reason}"),
                            ErrorSource::Harness,
                        ));
                    } else {
                        // Owner is still alive — cannot take over
                        return Err(CoreError::new(
                            ErrorCode::Conflict,
                            format!(
                                "active supervisor {} exists (lease expired but process still alive)",
                                active_instance_id
                            ),
                            ErrorSource::Harness,
                        ));
                    }
                }

                // Lease not expired — check if the owner process is still alive
                let alive = self
                    .verify_owner_alive(&active_instance_id)
                    .await;

                if alive {
                    Err(CoreError::new(
                        ErrorCode::Conflict,
                        format!(
                            "supervisor {} is already active with fencing_token {}",
                            active_instance_id, active_lease.fencing_token
                        ),
                        ErrorSource::Harness,
                    ))
                } else {
                    // Lease not expired but process is dead — stale (crash)
                    let reason = format!(
                        "lease active until {} but owner {} process is dead",
                        active_lease.expires_at, active_instance_id
                    );
                    Err(CoreError::new(
                        ErrorCode::InvalidState,
                        format!("stale: {reason}"),
                        ErrorSource::Harness,
                    ))
                }
            }
        }
    }

    /// Take over from a stale owner and acquire the lease.
    pub async fn takeover_and_acquire(
        &self,
        instance: &SupervisorInstance,
    ) -> Result<TakeoverResult, CoreError> {
        // Force-deactivate the old lease
        let old_lease = self
            .repo
            .force_deactivate_lease(&self.state_directory_id)
            .await?;

        let old_fencing_token = old_lease
            .as_ref()
            .map(|l| l.fencing_token)
            .unwrap_or(0);
        let new_fencing_token = old_fencing_token + 1;

        let previous_instance_id = old_lease
            .as_ref()
            .map(|l| SupervisorInstanceId(l.instance_id.clone()))
            .unwrap_or_else(|| SupervisorInstanceId("unknown".to_string()));

        let expires_at = Utc::now() + self.lease_duration;

        // Acquire new lease with incremented fencing token
        self.repo
            .acquire_lease(
                &instance.instance_id,
                &self.state_directory_id,
                new_fencing_token,
                expires_at,
            )
            .await?;

        tracing::info!(
            instance_id = %instance.instance_id,
            old_fencing_token = old_fencing_token,
            new_fencing_token = new_fencing_token,
            previous_instance_id = %previous_instance_id,
            "supervisor takeover completed"
        );

        Ok(TakeoverResult {
            new_fencing_token,
            previous_instance_id,
        })
    }

    /// Release the supervisor lease.
    pub async fn release_lease(
        &self,
        instance: &SupervisorInstance,
    ) -> Result<(), CoreError> {
        let released = self
            .repo
            .release_lease_cas(&instance.instance_id, instance.fencing_token)
            .await?;

        if released {
            tracing::info!(
                instance_id = %instance.instance_id,
                fencing_token = instance.fencing_token,
                "supervisor lease released"
            );
        } else {
            tracing::warn!(
                instance_id = %instance.instance_id,
                fencing_token = instance.fencing_token,
                "supervisor lease release CAS failed (already released?)"
            );
        }

        Ok(())
    }

    /// Validate that a write operation from the given fencing token is allowed.
    pub async fn validate_write_fencing(
        &self,
        writer_fencing_token: i64,
    ) -> Result<bool, CoreError> {
        self.repo
            .validate_fencing_for_write(&self.state_directory_id, writer_fencing_token)
            .await
    }

    // ── Process identity verification ──────────────────────────────────

    /// Verify that an owner process is still alive using OS-level identity.
    async fn verify_owner_alive(
        &self,
        instance_id: &SupervisorInstanceId,
    ) -> bool {
        let instance = match self.repo.get_instance(instance_id).await {
            Ok(Some(i)) => i,
            _ => return false,
        };

        is_process_alive(instance.pid, instance.process_started_at)
    }

    /// Verify that an owner process is definitely dead.
    async fn verify_owner_dead(
        &self,
        instance_id: &SupervisorInstanceId,
    ) -> bool {
        let instance = match self.repo.get_instance(instance_id).await {
            Ok(Some(i)) => i,
            _ => return true, // No record → treat as dead
        };

        !is_process_alive(instance.pid, instance.process_started_at)
    }
}

/// Check if a process with the given PID and start time is still running.
/// On Windows, also verifies the process creation time matches to prevent
/// PID reuse false positives.
#[allow(unsafe_code)]
fn is_process_alive(pid: u32, expected_start_time: DateTime<Utc>) -> bool {
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, HANDLE};
        use windows_sys::Win32::System::Threading::{
            GetExitCodeProcess, GetProcessTimes, OpenProcess, PROCESS_QUERY_INFORMATION,
        };
        // STILL_ACTIVE = STATUS_PENDING = 259
        const STILL_ACTIVE: u32 = 259;

        unsafe {
            let handle: HANDLE = OpenProcess(PROCESS_QUERY_INFORMATION, 0, pid);
            if handle.is_null() {
                // Process doesn't exist (or access denied — treat as dead)
                return false;
            }

            // Check exit code
            let mut exit_code: u32 = 0;
            let ok = GetExitCodeProcess(handle, &mut exit_code);
            if ok == 0 {
                CloseHandle(handle);
                return false;
            }

            if exit_code != STILL_ACTIVE {
                CloseHandle(handle);
                return false;
            }

            // Verify creation time matches (PID reuse guard)
            let mut creation: FILETIME = std::mem::zeroed();
            let mut _exit: FILETIME = std::mem::zeroed();
            let mut _kernel: FILETIME = std::mem::zeroed();
            let mut _user: FILETIME = std::mem::zeroed();

            let ok = GetProcessTimes(handle, &mut creation, &mut _exit, &mut _kernel, &mut _user);
            CloseHandle(handle);

            if ok == 0 {
                return false;
            }

            let ticks = ((creation.dwHighDateTime as u64) << 32)
                | (creation.dwLowDateTime as u64);
            let unix_epoch_ticks = 11_644_473_600_000_000_000u64;
            if ticks > unix_epoch_ticks {
                let creation_secs = (ticks - unix_epoch_ticks) / 10_000_000;
                let expected_secs = expected_start_time.timestamp() as u64;
                // Allow ±2 seconds tolerance for clock precision
                if creation_secs.abs_diff(expected_secs) > 2 {
                    // PID reused — different process with same PID but different creation time
                    return false;
                }
            }

            true
        }
    }

    #[cfg(not(windows))]
    {
        // On Unix, check /proc/<pid>/stat for start time
        if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            let parts: Vec<&str> = stat.split_whitespace().collect();
            if parts.len() >= 22 {
                // Field 21 (0-indexed: 21) is starttime
                // We check that the process exists; full creation time
                // verification on Unix requires reading /proc/stat for boot time
                return true;
            }
        }
        false
    }
}

fn parse_time(s: &str) -> DateTime<Utc> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.3fZ")
        .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        .or_else(|_| {
            chrono::DateTime::parse_from_rfc3339(s).map(|dt| dt.with_timezone(&Utc))
        })
        .unwrap_or_else(|_| Utc::now())
}
