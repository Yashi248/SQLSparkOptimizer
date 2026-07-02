"""
Cost routing — send each stage to the right-sized model and log what it costs.

This is the project's cost thesis made concrete: mechanical stages (translate,
analyze, optimize) are deterministic and run on the LOCAL tier at $0; the one
genuine reasoning stage (explain *why* the fixes help) routes to a SMART tier.
Every call is recorded in a ledger so we can publish cost-per-stage.

The router is graceful: SMART resolves to an OpenAI-compatible endpoint (NVIDIA
NIM via NVIDIA_API_KEY, or any OpenAI-compatible base URL), else Gemini, else
local Ollama, else a deterministic template — so the pipeline runs with zero LLM
setup and lights up when you add a key. The routing decision is logged either way.

Config (env):
  NVIDIA_API_KEY   -> uses https://integrate.api.nvidia.com/v1 (free tier)
  OPENAI_API_KEY + OPENAI_BASE_URL -> any OpenAI-compatible endpoint
  LLM_MODEL        -> model id (default meta/llama-3.3-70b-instruct)
"""
from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from enum import Enum


class Tier(str, Enum):
    LOCAL = "local"   # deterministic or local model -> $0
    SMART = "smart"   # hosted reasoning model -> costs tokens


# USD per 1M tokens: (input, output). Used to estimate cost even on free tiers,
# so the "what this would cost at scale" story is real.
PRICING = {
    "gemini-1.5-flash": (0.075, 0.30),
    "ollama": (0.0, 0.0),
    "template": (0.0, 0.0),
    "deterministic": (0.0, 0.0),
}


@dataclass
class StageCost:
    stage: str
    tier: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    est_cost_usd: float


def _cost(model: str, pt: int, ct: int) -> float:
    cin, cout = PRICING.get(model, (0.0, 0.0))
    return round(pt / 1e6 * cin + ct / 1e6 * cout, 6)


class ModelRouter:
    def __init__(self):
        self.ledger: list[StageCost] = []
        self._openai_model = os.environ.get("LLM_MODEL", "meta/llama-3.3-70b-instruct")
        self._openai = self._init_openai()
        self._gemini = self._init_gemini()
        self._ollama_model = os.environ.get("OLLAMA_MODEL", "llama3.2")
        self._ollama_up = self._ollama_reachable()

    @property
    def llm_available(self) -> bool:
        """True if any real model (not just the template) can be reached — used to
        decide whether LLM escalation is worth attempting."""
        return self._openai is not None or self._gemini is not None or self._ollama_up

    # --- public API --------------------------------------------------------- #
    def record_local(self, stage: str, model: str = "deterministic") -> StageCost:
        """Record a mechanical stage as local/$0 (it routes here by design)."""
        sc = StageCost(stage, Tier.LOCAL, model, 0, 0, 0.0)
        self.ledger.append(sc)
        return sc

    def reason(self, stage: str, prompt: str, fallback_text: str) -> tuple[str, StageCost]:
        """Route a REASONING task to the smart tier. Tries Gemini, then local
        Ollama, then a deterministic template. ANY backend failure (no key, model
        not pulled, server hiccup) degrades to the next option — the pipeline must
        never crash because a model is unavailable. Always records the decision."""
        if self._openai is not None:
            try:
                text, pt, ct = self._call_openai(prompt)
                sc = StageCost(stage, Tier.SMART, self._openai_model, pt, ct,
                               _cost(self._openai_model, pt, ct))
                self.ledger.append(sc)
                return text, sc
            except Exception:  # noqa: BLE001 - fall through to next tier
                pass
        if self._gemini is not None:
            try:
                text, pt, ct = self._call_gemini(prompt)
                sc = StageCost(stage, Tier.SMART, "gemini-1.5-flash", pt, ct,
                               _cost("gemini-1.5-flash", pt, ct))
                self.ledger.append(sc)
                return text, sc
            except Exception:  # noqa: BLE001 - fall through to next tier
                pass
        if self._ollama_up:
            try:
                text, pt, ct = self._call_ollama(prompt)
                sc = StageCost(stage, Tier.LOCAL, f"ollama:{self._ollama_model}",
                               pt, ct, 0.0)
                self.ledger.append(sc)
                return text, sc
            except Exception:  # noqa: BLE001 - e.g. model not pulled -> template
                pass
        sc = StageCost(stage, Tier.LOCAL, "template", 0, 0, 0.0)
        self.ledger.append(sc)
        return fallback_text, sc

    def summary(self) -> dict:
        total = round(sum(s.est_cost_usd for s in self.ledger), 6)
        tokens = sum(s.prompt_tokens + s.completion_tokens for s in self.ledger)
        return {"total_cost_usd": total, "total_tokens": tokens,
                "stages": len(self.ledger)}

    # --- backends ----------------------------------------------------------- #
    def _init_openai(self):
        """OpenAI-compatible client. NVIDIA NIM (free) or any OpenAI endpoint."""
        nvidia = os.environ.get("NVIDIA_API_KEY")
        key = nvidia or os.environ.get("OPENAI_API_KEY")
        if not key:
            return None
        base_url = os.environ.get("OPENAI_BASE_URL")
        if nvidia and not base_url:
            base_url = "https://integrate.api.nvidia.com/v1"
        try:
            from openai import OpenAI
            return OpenAI(api_key=key, base_url=base_url)
        except Exception:  # noqa: BLE001 - package missing / bad config
            return None

    def _call_openai(self, prompt: str) -> tuple[str, int, int]:
        r = self._openai.chat.completions.create(
            model=self._openai_model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1, max_tokens=900,
        )
        u = r.usage
        return (r.choices[0].message.content or "",
                getattr(u, "prompt_tokens", 0) if u else 0,
                getattr(u, "completion_tokens", 0) if u else 0)

    def _init_gemini(self):
        key = os.environ.get("GEMINI_API_KEY")
        if not key:
            return None
        try:
            import google.generativeai as genai
            genai.configure(api_key=key)
            return genai.GenerativeModel("gemini-1.5-flash")
        except Exception:  # noqa: BLE001
            return None

    def _call_gemini(self, prompt: str) -> tuple[str, int, int]:
        resp = self._gemini.generate_content(prompt)
        u = getattr(resp, "usage_metadata", None)
        pt = getattr(u, "prompt_token_count", 0) if u else 0
        ct = getattr(u, "candidates_token_count", 0) if u else 0
        return resp.text, pt, ct

    def _ollama_reachable(self, timeout: float = 0.5) -> bool:
        try:
            with socket.create_connection(("localhost", 11434), timeout=timeout):
                return True
        except OSError:
            return False

    def _call_ollama(self, prompt: str) -> tuple[str, int, int]:
        import ollama
        r = ollama.generate(model=self._ollama_model, prompt=prompt)
        return (r.get("response", ""),
                r.get("prompt_eval_count", 0), r.get("eval_count", 0))
