# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

from k8s.healthz import healthz_bp
from k8s.readyz import readyz_bp
from api.error_handlers import not_found

def register_blueprints(app):
    """Регистрация всех blueprint'ов в приложении"""
    app.register_blueprint(healthz_bp)
    app.register_blueprint(readyz_bp)

def register_error_handlers(app):
    """Регистрация обработчиков ошибок"""
    app.register_error_handler(404, not_found)