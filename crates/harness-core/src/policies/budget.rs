/// Budget policy — candidate, simple for Foundation.
#[derive(Debug, Clone)]
pub struct BudgetPolicy {
    pub max_turns: u32,
    pub max_time_ms: u64,
    pub max_cost_cents: Option<u32>,
}

impl BudgetPolicy {
    pub fn is_exceeded(&self, turns_used: u32, time_ms: u64, _cost_cents: u32) -> bool {
        if turns_used > self.max_turns {
            return true;
        }
        if time_ms > self.max_time_ms {
            return true;
        }
        false
    }
}
