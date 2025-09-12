# SPDX-License-Identifier: AGPL-3.0-only WITH LICENSE-ADDITIONAL
# Copyright (C) 2025 Петунин Лев Михайлович

from flask import jsonify

def not_found(error):
    response = jsonify({
        "status": False,
        "code": 404,
        "body": {
            "message": "Not Found"
        }
    })
    response.status_code = 404
    return response