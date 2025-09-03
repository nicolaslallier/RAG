"""
Local text-generation using Hugging Face transformers.

Audience: Solution Architects & Developers
- Purpose: Provide optional on-box generation for /ask using a configurable model.
- Notes:
  - Defaults to a smaller instruct model if MODEL_ID is not set
  - Lazily loads the model/pipeline on first use to avoid startup cost
  - Device and dtype are chosen automatically by transformers (CPU/GPU if available)
"""

import os
import logging
from typing import List, Optional


logger = logging.getLogger(__name__)

_PIPE = None  # lazy-initialized transformers pipeline


def build_prompt(context_chunks: List[str], question: str) -> str:
    """Build a French instruction-style prompt with context blocks.

    context_chunks: list like ["[p.3] ...", "[p.4] ...", ...]
    """
    context = "\n\n".join(context_chunks)
    prompt = (
        "Tu es un expert BBQ. Réponds précisément à la question\n"
        "en t'appuyant UNIQUEMENT sur le contexte. Cite les pages entre [ ].\n\n"
        f"Contexte:\n{context}\n\n"
        f"Question: {question}\n"
        "Réponse:\n"
    )
    return prompt


def _ensure_pipeline(model_id: Optional[str] = None):
    global _PIPE
    if _PIPE is not None:
        return _PIPE

    try:
        from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline  # type: ignore
    except Exception as exc:  # pragma: no cover
        logger.error("transformers not installed: %s", exc)
        raise

    resolved_model = model_id or os.getenv("MODEL_ID") or "Qwen/Qwen2.5-3B-Instruct"
    logger.info("Loading HF model: %s", resolved_model)

    tok = AutoTokenizer.from_pretrained(resolved_model)
    model = AutoModelForCausalLM.from_pretrained(
        resolved_model,
        device_map="auto",
        torch_dtype="auto",
    )
    _PIPE = pipeline(
        "text-generation",
        model=model,
        tokenizer=tok,
        device_map="auto",
    )
    return _PIPE


def generate_answer(context_chunks: List[str], question: str, *, model_id: Optional[str] = None) -> str:
    """Generate an answer given context and a question using a local HF pipeline."""
    pipe = _ensure_pipeline(model_id)
    prompt = build_prompt(context_chunks, question)
    out = pipe(
        prompt,
        max_new_tokens=int(os.getenv("GEN_MAX_NEW_TOKENS", "400")),
        temperature=float(os.getenv("GEN_TEMPERATURE", "0.2")),
        do_sample=False,
        truncation=True,
    )
    return out[0]["generated_text"]
