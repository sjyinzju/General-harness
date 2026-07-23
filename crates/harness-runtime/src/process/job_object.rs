//! Process tree termination.
//!
//! Windows primary: Job Object (`CreateJobObjectW` + `KILL_ON_JOB_CLOSE`).
//! Every spawned child is assigned to a dedicated job; terminating the job
//! kills the whole tree — including orphaned grandchildren whose parent
//! already exited (the case `taskkill /T` cannot handle). Closing the job
//! handle (guard drop) also kills survivors, so a supervisor crash cannot
//! leak descendants once the child was assigned.
//!
//! Windows fallback: `taskkill /PID <pid> /T /F` — used only when job
//! creation/assignment failed (e.g. the child was already placed in a
//! deny-breakaway job by other tooling).
//!
//! Unix: process-group kill (unchanged).
//!
//! ## Atomic Job Assignment (I4.5 structural fix)
//!
//! The root process is created with `CREATE_SUSPENDED` so it cannot execute
//! any user code before the Job Object is assigned. The assignment sequence is:
//!
//! 1. `cmd.creation_flags(CREATE_SUSPENDED).spawn()` — process born suspended
//! 2. `ProcessTreeGuard::attach(&child)` — create Job, assign process, verify
//! 3. `resume_suspended_process(pid)` — resume primary thread via Toolhelp32
//!
//! This eliminates the race window where a child/grandchild could be created
//! before the root is assigned to the Job (previously mitigated by a 50 ms
//! sleep in the fixture — now structurally impossible).

use std::io;

/// Fallback tree kill (Windows: taskkill; Unix: process group).
#[cfg(windows)]
pub fn kill_process_tree(pid: u32) -> io::Result<()> {
    std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    Ok(())
}

#[cfg(not(windows))]
// Isolated unsafe: single libc syscall for process-group kill. Scoped allow —
// the workspace denies unsafe_code everywhere else.
#[allow(unsafe_code)]
pub fn kill_process_tree(pid: u32) -> io::Result<()> {
    // SAFETY: kill(2) with a negative pgid is async-signal-safe and has no
    // memory-safety preconditions; an invalid pid only yields ESRCH.
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
    Ok(())
}

/// Per-child guard that owns the platform tree-kill mechanism.
///
/// Windows: holds the Job Object the child was assigned to. `kill_tree()`
/// terminates the job (fallback: taskkill). Dropping the guard closes the job
/// handle, and `KILL_ON_JOB_CLOSE` reaps any residual descendants.
pub struct ProcessTreeGuard {
    pid: u32,
    #[cfg(windows)]
    job: Option<windows_job::JobObject>,
}

impl ProcessTreeGuard {
    /// Create the guard and (on Windows) assign the child to a fresh job.
    /// Assignment failure downgrades to the taskkill fallback with a warning —
    /// it never fails the spawn.
    ///
    /// On Windows, the child MUST be created with CREATE_SUSPENDED before
    /// calling this function, to prevent the root from executing user code
    /// before job assignment completes. The caller is responsible for
    /// resuming the process after this function returns.
    pub fn attach(child: &tokio::process::Child) -> Self {
        let pid = child.id().unwrap_or(0);
        #[cfg(windows)]
        {
            let job = match windows_job::JobObject::create_kill_on_close() {
                Ok(job) => match child.raw_handle() {
                    Some(handle) => match job.assign_raw_handle(handle) {
                        Ok(()) => {
                            // Verify the assignment succeeded.
                            match job.is_process_in_job(handle) {
                                Ok(true) => {
                                    tracing::debug!(pid, "job_object_assignment_verified");
                                    Some(job)
                                }
                                Ok(false) => {
                                    tracing::warn!(
                                        pid,
                                        "job_object_assignment_not_verified; falling back to taskkill"
                                    );
                                    None
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        pid,
                                        error = %e,
                                        "job_object_verification_failed; falling back to taskkill"
                                    );
                                    None
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(pid, error = %e, "job_object_assign_failed; falling back to taskkill");
                            None
                        }
                    },
                    None => {
                        tracing::warn!(
                            pid,
                            "child raw_handle unavailable; falling back to taskkill"
                        );
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!(pid, error = %e, "job_object_create_failed; falling back to taskkill");
                    None
                }
            };
            Self { pid, job }
        }
        #[cfg(not(windows))]
        {
            Self { pid }
        }
    }

    /// True when the Job Object path is active (Windows only).
    pub fn job_object_active(&self) -> bool {
        #[cfg(windows)]
        {
            self.job.is_some()
        }
        #[cfg(not(windows))]
        {
            false
        }
    }

    /// Return the number of active processes in the job (Windows only).
    /// Returns None if the job is not active or the query fails.
    pub fn active_process_count(&self) -> Option<u32> {
        #[cfg(windows)]
        {
            self.job
                .as_ref()
                .and_then(|j| j.query_active_process_count().ok())
        }
        #[cfg(not(windows))]
        {
            None
        }
    }

    /// Kill the entire process tree rooted at the guarded child.
    pub fn kill_tree(&self) {
        #[cfg(windows)]
        if let Some(job) = &self.job {
            if job.terminate(1).is_ok() {
                return;
            }
        }
        if self.pid != 0 {
            let _ = kill_process_tree(self.pid);
        }
    }
}

/// Resume a process that was created with CREATE_SUSPENDED.
///
/// Finds the primary thread of the given PID via a Toolhelp32 thread
/// snapshot and calls ResumeThread on it.
///
/// Only available on Windows.
#[cfg(windows)]
pub fn resume_suspended_process(pid: u32) -> io::Result<()> {
    windows_job::resume_primary_thread(pid)
}

#[cfg(windows)]
mod windows_job {
    //! Isolated Win32 FFI for Job Objects (windows-sys, no new crates —
    //! windows-sys is already in the dependency tree via tokio/sqlx).
    #![allow(unsafe_code)]

    use std::io;

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob,
        JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
        QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
        JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };

    // NtResumeProcess — undocumented but stable NT kernel API.
    // Resumes all threads in a process. For a newly-created suspended process,
    // there is only the primary thread, so this is equivalent to ResumeThread
    // on the primary thread. Available in ntdll.dll on all Windows versions.
    extern "system" {
        fn NtResumeProcess(process_handle: HANDLE) -> i32;
    }

    const NT_SUCCESS: i32 = 0;

    pub struct JobObject {
        handle: HANDLE,
    }

    // SAFETY: a job object HANDLE is a kernel handle; it is valid on any
    // thread of the owning process and all Win32 calls used here are
    // thread-safe. The struct owns the handle exclusively until Drop.
    unsafe impl Send for JobObject {}
    unsafe impl Sync for JobObject {}

    impl JobObject {
        /// Create an anonymous job with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`:
        /// when the last handle closes (guard drop / supervisor exit), the OS
        /// terminates every process still in the job.
        pub fn create_kill_on_close() -> io::Result<Self> {
            // SAFETY: null attributes + null name create an anonymous job.
            let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            let job = Self { handle };

            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: handle is a valid job object; info is a properly sized,
            // initialized JOBOBJECT_EXTENDED_LIMIT_INFORMATION.
            let ok = unsafe {
                SetInformationJobObject(
                    job.handle,
                    JobObjectExtendedLimitInformation,
                    std::ptr::from_ref(&info).cast(),
                    std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(job)
        }

        /// Assign a process (tokio `Child::raw_handle()`) to this job.
        pub fn assign_raw_handle(
            &self,
            process: std::os::windows::io::RawHandle,
        ) -> io::Result<()> {
            // SAFETY: both handles are valid; RawHandle and HANDLE are both
            // `*mut c_void` on Windows.
            let ok = unsafe { AssignProcessToJobObject(self.handle, process as HANDLE) };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        /// Verify that a process is assigned to this job.
        pub fn is_process_in_job(
            &self,
            process: std::os::windows::io::RawHandle,
        ) -> io::Result<bool> {
            let mut result: i32 = 0;
            // SAFETY: handle is a valid job object; process is a valid process handle.
            let ok = unsafe { IsProcessInJob(process as HANDLE, self.handle, &mut result) };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(result != 0)
        }

        /// Query the number of active processes in this job.
        pub fn query_active_process_count(&self) -> io::Result<u32> {
            let mut info: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { std::mem::zeroed() };
            // SAFETY: handle is a valid job object; info is a properly sized struct.
            let ok = unsafe {
                QueryInformationJobObject(
                    self.handle,
                    JobObjectBasicAccountingInformation,
                    std::ptr::from_mut(&mut info).cast(),
                    std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                    std::ptr::null_mut(),
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(info.ActiveProcesses)
        }

        /// Terminate every process in the job.
        pub fn terminate(&self, exit_code: u32) -> io::Result<()> {
            // SAFETY: handle is a valid job object owned by self.
            let ok = unsafe { TerminateJobObject(self.handle, exit_code) };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    impl Drop for JobObject {
        fn drop(&mut self) {
            // SAFETY: handle is valid and owned; KILL_ON_JOB_CLOSE reaps any
            // survivors when this last handle closes.
            unsafe {
                CloseHandle(self.handle);
            }
        }
    }

    /// Resume a process created with CREATE_SUSPENDED.
    ///
    /// Uses NtResumeProcess (ntdll.dll) which resumes all threads in the
    /// process. For a newly-created suspended process, there is only the
    /// primary thread.
    pub fn resume_primary_thread(pid: u32) -> io::Result<()> {
        // Open the process with PROCESS_SUSPEND_RESUME access.
        // SAFETY: pid is a valid process ID; 0 = no inheritance.
        let process_handle = unsafe {
            use windows_sys::Win32::System::Threading::OpenProcess;
            OpenProcess(
                windows_sys::Win32::System::Threading::PROCESS_SUSPEND_RESUME,
                0, // bInheritHandle = FALSE
                pid,
            )
        };
        if process_handle.is_null() {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: process_handle is valid with PROCESS_SUSPEND_RESUME access.
        let status = unsafe { NtResumeProcess(process_handle) };

        // SAFETY: process_handle is valid and we're done with it.
        unsafe {
            CloseHandle(process_handle);
        }

        if status != NT_SUCCESS {
            return Err(io::Error::other(format!(
                "NtResumeProcess returned {status:#x}"
            )));
        }
        Ok(())
    }
}
