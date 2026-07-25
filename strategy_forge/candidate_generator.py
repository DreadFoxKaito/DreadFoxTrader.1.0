from __future__ import annotations

import itertools
import random
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Optional

from .strategy_templates import StrategyCandidate, StrategyTemplate, TEMPLATES, build_candidate, get_template


@dataclass
class CandidateGenerator:
    templates: dict[str, StrategyTemplate] | None = None
    seed: Optional[int] = None

    def __post_init__(self) -> None:
        if self.templates is None:
            self.templates = TEMPLATES
        self.random = random.Random(self.seed)

    def _sample_value(self, param: Any) -> Any:
        if param.kind == "choice":
            return self.random.choice(list(param.choices))
        if param.kind == "int":
            lo = int(param.min_value)
            hi = int(param.max_value)
            return self.random.randint(lo, hi)
        lo = float(param.min_value)
        hi = float(param.max_value)
        step = float(param.step or 0.0)
        if step > 0:
            slots = int(round((hi - lo) / step))
            return round(lo + (self.random.randint(0, max(0, slots)) * step), 6)
        return round(self.random.uniform(lo, hi), 6)

    def random_parameters(self, template: StrategyTemplate, *, max_attempts: int = 1000) -> dict[str, Any]:
        for _ in range(max_attempts):
            params = {name: self._sample_value(param) for name, param in template.parameter_space.items()}
            if template.validate(params):
                return params
        raise ValueError(f"unable to generate valid parameters for {template.name}")

    def random_candidates(
        self,
        *,
        template_names: Iterable[str],
        symbols: list[str],
        timeframe: str,
        trials: int,
    ) -> Iterator[StrategyCandidate]:
        names = [str(name).strip().lower() for name in template_names]
        if not names:
            names = ["ema_rsi_atr_trend"]
        for _ in range(max(0, int(trials))):
            template = get_template(self.random.choice(names))
            params = self.random_parameters(template)
            yield build_candidate(template, params, symbols, timeframe)

    def grid_candidates(
        self,
        *,
        template_name: str,
        symbols: list[str],
        timeframe: str,
        limit: Optional[int] = None,
    ) -> Iterator[StrategyCandidate]:
        template = get_template(template_name)
        keys = list(template.parameter_space.keys())
        grids = [template.parameter_space[key].grid_values() for key in keys]
        emitted = 0
        for combo in itertools.product(*grids):
            params = dict(zip(keys, combo))
            if not template.validate(params):
                continue
            yield build_candidate(template, params, symbols, timeframe)
            emitted += 1
            if limit is not None and emitted >= int(limit):
                break


class OptunaStyleSearch:
    """Small Optuna wrapper with deterministic random fallback.

    The fallback keeps the same outward API and perturbs the strongest observed
    parameter sets, which is enough for long-running CLI searches when Optuna is
    not installed.
    """

    def __init__(self, *, seed: Optional[int] = None) -> None:
        self.seed = seed
        self.generator = CandidateGenerator(seed=seed)

    def suggest_candidates(
        self,
        *,
        template_name: str,
        symbols: list[str],
        timeframe: str,
        trials: int,
        objective: Optional[Any] = None,
    ) -> Iterator[StrategyCandidate]:
        try:
            import optuna  # type: ignore
        except Exception:
            yield from self._fallback_candidates(
                template_name=template_name,
                symbols=symbols,
                timeframe=timeframe,
                trials=trials,
                objective=objective,
            )
            return

        template = get_template(template_name)

        def _suggest(trial: Any) -> dict[str, Any]:
            params: dict[str, Any] = {}
            for key, param in template.parameter_space.items():
                if param.kind == "choice":
                    params[key] = trial.suggest_categorical(key, list(param.choices))
                elif param.kind == "int":
                    params[key] = trial.suggest_int(key, int(param.min_value), int(param.max_value))
                else:
                    params[key] = trial.suggest_float(
                        key,
                        float(param.min_value),
                        float(param.max_value),
                        step=float(param.step) if param.step else None,
                    )
            return params

        sampler = optuna.samplers.TPESampler(seed=self.seed)
        study = optuna.create_study(direction="maximize", sampler=sampler)
        produced: list[StrategyCandidate] = []

        def _objective(trial: Any) -> float:
            params = _suggest(trial)
            if not template.validate(params):
                raise optuna.TrialPruned()
            candidate = build_candidate(template, params, symbols, timeframe)
            produced.append(candidate)
            if objective is None:
                return 0.0
            score = float(objective(candidate))
            if score < -0.5:
                raise optuna.TrialPruned()
            return score

        study.optimize(_objective, n_trials=max(0, int(trials)), show_progress_bar=False)
        yield from produced

    def _fallback_candidates(
        self,
        *,
        template_name: str,
        symbols: list[str],
        timeframe: str,
        trials: int,
        objective: Optional[Any] = None,
    ) -> Iterator[StrategyCandidate]:
        template = get_template(template_name)
        best: list[tuple[float, dict[str, Any]]] = []
        rng = self.generator.random
        for idx in range(max(0, int(trials))):
            if best and idx > 10 and rng.random() < 0.45:
                base = dict(rng.choice(best)[1])
                params = self._perturb(template, base)
                if not template.validate(params):
                    params = self.generator.random_parameters(template)
            else:
                params = self.generator.random_parameters(template)
            candidate = build_candidate(template, params, symbols, timeframe)
            score = 0.0
            if objective is not None:
                score = float(objective(candidate))
                best.append((score, params))
                best.sort(key=lambda item: item[0], reverse=True)
                best = best[:12]
            yield candidate

    def _perturb(self, template: StrategyTemplate, params: dict[str, Any]) -> dict[str, Any]:
        out = dict(params)
        keys = list(template.parameter_space.keys())
        for key in self.generator.random.sample(keys, k=max(1, min(3, len(keys)))):
            spec = template.parameter_space[key]
            if spec.kind == "choice":
                out[key] = self.generator._sample_value(spec)
                continue
            span = float(spec.max_value) - float(spec.min_value)
            step = float(spec.step or (1 if spec.kind == "int" else span / 20.0))
            delta = self.generator.random.choice([-2, -1, 1, 2]) * step
            value = float(out[key]) + delta
            value = max(float(spec.min_value), min(float(spec.max_value), value))
            out[key] = int(round(value)) if spec.kind == "int" else round(value, 6)
        return out


def generate_candidates(
    *,
    mode: str,
    template_names: Iterable[str],
    symbols: list[str],
    timeframe: str,
    trials: int,
    seed: Optional[int] = None,
) -> Iterator[StrategyCandidate]:
    mode_key = str(mode or "random").strip().lower()
    names = list(template_names)
    if mode_key == "grid":
        if len(names) != 1:
            raise ValueError("grid search requires exactly one template")
        yield from CandidateGenerator(seed=seed).grid_candidates(
            template_name=names[0],
            symbols=symbols,
            timeframe=timeframe,
            limit=trials,
        )
    elif mode_key == "optuna":
        if len(names) != 1:
            raise ValueError("optuna-style search requires exactly one template")
        yield from OptunaStyleSearch(seed=seed).suggest_candidates(
            template_name=names[0],
            symbols=symbols,
            timeframe=timeframe,
            trials=trials,
        )
    else:
        yield from CandidateGenerator(seed=seed).random_candidates(
            template_names=names,
            symbols=symbols,
            timeframe=timeframe,
            trials=trials,
        )
