from __future__ import annotations


def refresh_completed_message() -> str:
    return "\u5237\u65b0\u5b8c\u6210"


def all_accounts_healthy_message() -> str:
    return "\u6240\u6709\u8d26\u53f7\u72b6\u6001\u6b63\u5e38"


def no_accounts_available_message() -> str:
    return "\u6682\u65e0\u53ef\u7528\u8d26\u53f7"


def all_accounts_failed_message() -> str:
    return "\u6240\u6709\u8d26\u53f7\u5237\u65b0\u5931\u8d25"


def some_accounts_failed_message() -> str:
    return "\u90e8\u5206\u8d26\u53f7\u5237\u65b0\u5931\u8d25"


def refresh_timeout_message() -> str:
    return "\u5237\u65b0\u8d85\u65f6\uff0c\u5df2\u4fdd\u7559\u5f53\u524d\u6570\u636e"


def refresh_failed_message(detail: str) -> str:
    return f"\u5237\u65b0\u5931\u8d25\uff0c\u5df2\u4fdd\u7559\u5f53\u524d\u6570\u636e\uff1a{detail}"


def auto_refresh_timeout_message() -> str:
    return "\u81ea\u52a8\u5237\u65b0\u8d85\u65f6\uff0c\u5df2\u4fdd\u7559\u5f53\u524d\u6570\u636e"


def auto_refresh_failed_message(detail: str) -> str:
    return f"\u81ea\u52a8\u5237\u65b0\u5931\u8d25\uff0c\u5df2\u4fdd\u7559\u5f53\u524d\u6570\u636e\uff1a{detail}"


def excel_import_refresh_success_message() -> str:
    return "Excel \u5bfc\u5165\u6210\u529f\uff0c\u6570\u636e\u5df2\u5237\u65b0"


def excel_import_refresh_failed_message() -> str:
    return "Excel \u5bfc\u5165\u6210\u529f\uff0c\u4f46\u5237\u65b0\u5931\u8d25"


def excel_import_settings_success_message() -> str:
    return "Excel \u5bfc\u5165\u6210\u529f\uff0c\u654f\u611f\u914d\u7f6e\u5df2\u66f4\u65b0"


def excel_import_security_notice() -> str:
    return "\u654f\u611f\u4fe1\u606f\u5df2\u8f6c\u5165\u52a0\u5bc6\u4ed3\u5e93\uff0c\u8bf7\u5220\u9664\u672c\u5730 Excel \u660e\u6587\u6587\u4ef6"


def waiting_for_monitor_connection_message() -> str:
    return "\u7b49\u5f85\u76d1\u63a7\u8fde\u63a5"


def monitor_accounts_reloaded_message() -> str:
    return "\u76d1\u63a7\u8d26\u53f7\u5df2\u91cd\u65b0\u52a0\u8f7d"


def monitoring_disabled_message() -> str:
    return "\u76d1\u63a7\u5df2\u7981\u7528"


def account_snapshot_updated_message() -> str:
    return "\u8d26\u53f7\u5feb\u7167\u5df2\u66f4\u65b0"


def profit_summary_labels() -> dict[str, str]:
    return {
        "today": "\u4eca\u65e5\u6536\u76ca\u4e28\u6536\u76ca\u7387",
        "week": "\u672c\u5468\u6536\u76ca\u4e28\u6536\u76ca\u7387",
        "month": "\u672c\u6708\u6536\u76ca\u4e28\u6536\u76ca\u7387",
        "year": "\u5e74\u5ea6\u6536\u76ca\u4e28\u6536\u76ca\u7387",
        "all": "\u5168\u90e8\u6536\u76ca\u4e28\u6536\u76ca\u7387",
    }
