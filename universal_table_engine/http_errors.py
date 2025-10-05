from __future__ import annotations

from fastapi import HTTPException, status

from .models import ErrorResponse


def http_error(
    status_code: int,
    error_code: str,
    message: str,
    *,
    hint: str | None = None,
) -> HTTPException:
    detail = ErrorResponse(error_code=error_code, message=message, hint=hint).model_dump()
    return HTTPException(status_code=status_code, detail=detail)


def bad_request(error_code: str, message: str, *, hint: str | None = None) -> HTTPException:
    return http_error(status.HTTP_400_BAD_REQUEST, error_code, message, hint=hint)


def unauthorized(error_code: str, message: str, *, hint: str | None = None) -> HTTPException:
    return http_error(status.HTTP_401_UNAUTHORIZED, error_code, message, hint=hint)


def forbidden(error_code: str, message: str, *, hint: str | None = None) -> HTTPException:
    return http_error(status.HTTP_403_FORBIDDEN, error_code, message, hint=hint)


def not_found(error_code: str, message: str, *, hint: str | None = None) -> HTTPException:
    return http_error(status.HTTP_404_NOT_FOUND, error_code, message, hint=hint)


def conflict(error_code: str, message: str, *, hint: str | None = None) -> HTTPException:
    return http_error(status.HTTP_409_CONFLICT, error_code, message, hint=hint)


__all__ = [
    "http_error",
    "bad_request",
    "unauthorized",
    "forbidden",
    "not_found",
    "conflict",
]
