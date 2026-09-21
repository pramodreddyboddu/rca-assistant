"""RCA agent package: deterministic diagnosis engine plus human approval gate,
with the Jev reasoning layer (TypeSafe System One) on by default when a key
is configured. Jev advises only; it never overrides approvals or audit."""

from .approvals import ApprovalGate
from .jev import JevAdvisory, JevReasoner
from .plans import (PlanResult, PlanStep, RemediationPlan, StepOutcome,
                    VerifySpec, check_verify, execute_approved_step, run_plan)
from .rca import Citation, Diagnosis, Hypothesis, RCAEngine
from .remediation import plan_for

__all__ = ["RCAEngine", "ApprovalGate", "Diagnosis", "Hypothesis", "Citation",
           "RemediationPlan", "PlanStep", "VerifySpec", "PlanResult",
           "StepOutcome", "run_plan", "execute_approved_step", "check_verify",
           "plan_for", "JevReasoner", "JevAdvisory"]
