from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from monitor_app.unimmr_alerts import UniMmrTrigger


_BAND_LABELS = {
    "safe": "\u5b89\u5168\u533a\u95f4",
    "warning": "\u9884\u8b66\u533a\u95f4",
    "danger": "\u5371\u9669\u533a\u95f4",
}

_TEXT = {
    "test_message": "UniMMR \u544a\u8b66\u6d4b\u8bd5\u6d88\u606f",
    "warning_entry": "\u8fdb\u5165 UniMMR \u9884\u8b66\u533a\u95f4",
    "warning_step_drop": "UniMMR \u518d\u4e0b\u964d 0.1 \u6863",
    "warning_repeat": "UniMMR \u9884\u8b66\u533a\u95f4 12 \u5c0f\u65f6\u7eed\u62a5",
    "danger_entry": "\u8fdb\u5165 UniMMR \u5371\u9669\u533a\u95f4",
    "danger_step_drop": "UniMMR \u518d\u4e0b\u964d 0.05 \u6863",
    "danger_repeat": "UniMMR \u5371\u9669\u533a\u95f4 5 \u5206\u949f\u7eed\u62a5",
    "title": "UniMMR \u544a\u8b66\u901a\u77e5",
    "danger_section": "\u5371\u9669\u533a\u95f4\uff1a",
    "warning_section": "\u9884\u8b66\u533a\u95f4\uff1a",
    "recovery_section": "\u6062\u590d\u901a\u77e5\uff1a",
}


def default_unimmr_test_message() -> str:
    return _TEXT["test_message"]


def format_unimmr_reason_text(
    reason_code: str,
    *,
    from_band: str | None = None,
    to_band: str | None = None,
) -> str:
    if reason_code in _TEXT:
        return _TEXT[reason_code]
    if reason_code == "recovery":
        from_label = _BAND_LABELS.get(str(from_band or ""), str(from_band or ""))
        to_label = _BAND_LABELS.get(str(to_band or ""), str(to_band or ""))
        return f"UniMMR \u5df2\u4ece{from_label}\u6062\u590d\u5230{to_label}"
    return reason_code


def format_unimmr_telegram_message(triggers: list["UniMmrTrigger"]) -> str:
    danger_lines: list[str] = []
    warning_lines: list[str] = []
    recovery_lines: list[str] = []
    for trigger in triggers:
        line = _format_trigger_line(trigger)
        if trigger.reason_code == "recovery":
            recovery_lines.append(line)
        elif trigger.band == "danger":
            danger_lines.append(line)
        else:
            warning_lines.append(line)

    lines = [_TEXT["title"]]
    if danger_lines:
        lines.append(_TEXT["danger_section"])
        lines.extend(danger_lines)
    if warning_lines:
        lines.append(_TEXT["warning_section"])
        lines.extend(warning_lines)
    if recovery_lines:
        lines.append(_TEXT["recovery_section"])
        lines.extend(recovery_lines)
    return "\n".join(lines)


def _format_trigger_line(trigger: "UniMmrTrigger") -> str:
    value = _format_decimal(trigger.value)
    return (
        f"- {trigger.main_account_name}/{trigger.child_account_name}"
        f" ({trigger.account_id}) UniMMR={value}\uff0c{trigger.reason_text}"
    )


def _format_decimal(value: Decimal) -> str:
    normalized = format(value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized
