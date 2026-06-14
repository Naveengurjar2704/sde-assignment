# """
# PostCallCircuitBreaker -- proportional backpressure replacing binary freeze.

# OLD behaviour (broken):
#   - At >= 90% RPM: freeze ALL dialling for agent_id for 1800 seconds
#   - Measured RPM (requests/min) not TPM (tokens/min)
#   - Binary: 89% = full speed, 90% = complete stop for 30 minutes

# NEW behaviour:
#   - Reports a utilisation ratio [0.0, 1.0+] based on TOKENS/min
#   - Dialler uses the ratio to compute a dispatch delay (proportional slowdown)
#   - No agent-level freeze; backpressure is gradual and platform-wide
#   - check_capacity() now returns (allowed: bool, utilisation: float)
#     so callers can apply proportional delay without a hard stop

# The LLMRateLimiter (llm_scheduler.py) is the GATE for LLM calls.
# This circuit breaker is the SIGNAL for the dialler to slow new call dispatch.
# Both work together: the scheduler queues post-call LLM work; the breaker
# tells the dialler to produce less work when the queue is already deep.
# """

# import logging
# import time
# from dataclasses import dataclass, field
# from typing import Dict, Optional, Tuple

# from src.config import settings
# from src.utils.redis_client import redis_client

# logger = logging.getLogger(__name__)

# # We measure TPM (tokens/minute) now, not RPM.
# # Keys are written by LLMRateLimiter._reserve_tokens() and record_actual_usage().
# GLOBAL_TPM_KEY = "llm:tpm:global"


# @dataclass
# class CircuitState:
#     agent_id: str
#     is_open: bool = False
#     opened_at: Optional[float] = None
#     freeze_until: Optional[float] = None
#     consecutive_failures: int = 0


# class PostCallCircuitBreaker:
#     """
#     Provides a utilisation signal to the dialler.

#     Instead of binary freeze, the dialler should read utilisation and apply
#     a proportional inter-call delay:
#         delay_ms = base_delay_ms * utilisation_ratio

#     At 0% utilisation: no added delay.
#     At 80% utilisation: 80% of base_delay_ms added.
#     At 100%+: max_delay_ms applied.

#     check_capacity() still returns a boolean for backward compatibility
#     (True = allowed, False = at or over 100% -- extremely rare with the
#     LLMRateLimiter in front), but the recommended pattern is to read
#     get_utilisation() and slow down proportionally.
#     """

#     def __init__(self) -> None:
#         self._states: Dict[str, CircuitState] = {}

#     async def check_capacity(self, agent_id: str) -> bool:
#         """
#         Returns True if the system has available LLM capacity.
#         Returns False ONLY when we are at or above 100% global token budget --
#         i.e., the LLMRateLimiter is actively blocking every request.

#         Callers should prefer get_utilisation() for proportional control.
#         """
#         utilisation = await self.get_utilisation()
#         if utilisation >= 1.0:
#             logger.warning(
#                 "circuit_breaker_at_capacity",
#                 extra={
#                     "agent_id": agent_id,
#                     "utilisation": round(utilisation, 3),
#                     "note": "LLMRateLimiter will queue requests automatically",
#                 },
#             )
#             return False
#         return True

#     async def get_utilisation(self) -> float:
#         """
#         Returns global TPM utilisation ratio [0.0, 1.0+].

#         0.5 = 50% of token budget consumed this minute.
#         1.0 = fully saturated (LLMRateLimiter will block new requests).
#         >1.0 = burst slightly over limit (harmless; Redis TTL will reset in <60s).
#         """
#         current_tpm = int(await redis_client.get(GLOBAL_TPM_KEY) or 0)
#         max_tpm = settings.LLM_TOKENS_PER_MINUTE
#         return current_tpm / max_tpm if max_tpm > 0 else 0.0

#     # ── Legacy interface kept for backward compatibility ───────────────────────

#     async def record_postcall_start(self) -> None:
#         """
#         Deprecated: LLMRateLimiter._reserve_tokens() handles this now.
#         Kept as a no-op so existing callers don't break.
#         """
#         pass

#     async def record_postcall_end(self) -> None:
#         """
#         Deprecated: kept as a no-op for backward compatibility.
#         """
#         pass


# circuit_breaker = PostCallCircuitBreaker()
"""THIS FILE IS NOW NOT NEEDED ANYMORE WE HAVE REPLACED THIS FILE WITH THE llm_scheduler.py"""