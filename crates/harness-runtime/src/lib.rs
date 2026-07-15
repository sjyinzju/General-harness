//! harness-runtime: Persistence, scheduler, process manager, workspace, verification.
//! Depends on harness-core, rusqlite, tokio, git2.

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(add(2, 2), 4);
    }
}
