from __future__ import annotations


def login_page_texts() -> dict[str, str]:
    return {
        "title": "认证登录",
        "description": "请输入游客密码或管理员密码。非白名单地址在登录前只能访问此页面。",
        "password_label": "访问密码",
        "password_placeholder": "请输入密码",
        "submit_button": "进入系统",
        "not_initialized_description": "认证未初始化，请先完成 access_control.json 配置。",
        "not_initialized_error": "认证未初始化",
        "password_required_error": "请输入访问密码",
        "auth_failed_error": "认证失败",
        "request_failed_error": "登录请求失败，请稍后重试",
        "session_failed_error": "无法获取认证状态，请稍后重试",
    }
