"""Reusable Streamlit payment details fields.

This module centralizes the payment select options and the renderer used by the
seller app so the same payment block can be imported by other Streamlit apps.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, Sequence

import streamlit as st

PAYMENT_METHOD_OPTIONS: tuple[str, ...] = (
    "Transferencia",
    "Efectivo",
    "Depósito en Efectivo",
    "Tarjeta de Débito",
    "Tarjeta de Crédito",
    "Cheque",
)
LINK_PAYMENT_OPTION = "Link de Pago"
CARD_PAYMENT_METHODS: tuple[str, ...] = ("Tarjeta de Débito", "Tarjeta de Crédito")
TERMINAL_OPTIONS: tuple[str, ...] = (
    "BANORTE",
    "AFIRME",
    "VELPAY",
    "CLIP",
    "PAYPAL",
    "BBVA",
    "CONEKTA",
    "MERCADO PAGO",
)
BANK_DESTINATION_OPTIONS: tuple[str, ...] = (
    "BANORTE",
    "BANAMEX",
    "AFIRME",
    "BANCOMER OP",
    "BANCOMER CURSOS",
)


@dataclass(frozen=True)
class PaymentDetails:
    """Values captured by the reusable payment details component."""

    fecha_pago: Optional[date]
    forma_pago: str
    terminal: str
    banco_destino: str
    monto_pago: float
    referencia_pago: str


def build_payment_method_options(include_link_pago: bool = False) -> list[str]:
    """Return payment method select options with optional link payment support."""
    options = list(PAYMENT_METHOD_OPTIONS)
    if include_link_pago and LINK_PAYMENT_OPTION not in options:
        options.append(LINK_PAYMENT_OPTION)
    return options


def _safe_index(options: Sequence[str], value: Optional[str]) -> int:
    """Return a safe Streamlit selectbox index for a possibly missing value."""
    if value in options:
        return list(options).index(value)  # type: ignore[arg-type]
    return 0


def render_payment_details_fields(
    *,
    include_fecha: bool = True,
    include_monto: bool = True,
    include_link_pago: bool = False,
    key_prefix: str = "",
    fecha_value: Optional[date] = None,
    forma_pago_value: Optional[str] = None,
    terminal_value: Optional[str] = None,
    banco_destino_value: Optional[str] = None,
    monto_pago_value: float = 0.0,
    referencia_pago_value: str = "",
    label_suffix: str = "",
) -> PaymentDetails:
    """Render reusable payment details inputs and return captured values.

    ``key_prefix`` lets another app reuse this component without colliding with
    existing Streamlit session-state keys. An empty prefix preserves the legacy
    keys used by ``app_v.py``.
    """
    key = lambda name: f"{key_prefix}{name}" if key_prefix else name
    suffix = f" {label_suffix}" if label_suffix else ""
    payment_method_options = build_payment_method_options(include_link_pago)

    rendered_fecha_pago = fecha_value
    rendered_forma_pago = forma_pago_value or payment_method_options[0]
    rendered_terminal = terminal_value or ""
    rendered_banco_destino = banco_destino_value or ""
    rendered_monto_pago = monto_pago_value
    rendered_referencia_pago = referencia_pago_value

    col1, col2, col3 = st.columns(3)
    with col1:
        if include_fecha:
            rendered_fecha_pago = st.date_input(
                f"📅 Fecha del Pago{suffix}",
                value=fecha_value or date.today(),
                key=key("fecha_pago_input"),
            )
        else:
            rendered_forma_pago = st.selectbox(
                f"💳 Forma de Pago{suffix}",
                payment_method_options,
                index=_safe_index(payment_method_options, forma_pago_value),
                key=key("forma_pago_input"),
            )
    with col2:
        if include_fecha:
            rendered_forma_pago = st.selectbox(
                f"💳 Forma de Pago{suffix}",
                payment_method_options,
                index=_safe_index(payment_method_options, forma_pago_value),
                key=key("forma_pago_input"),
            )
        else:
            if rendered_forma_pago in CARD_PAYMENT_METHODS:
                rendered_terminal = st.selectbox(
                    f"🏧 Terminal{suffix}",
                    TERMINAL_OPTIONS,
                    index=_safe_index(TERMINAL_OPTIONS, terminal_value),
                    key=key("terminal_input"),
                )
                rendered_banco_destino = ""
            else:
                rendered_banco_destino = st.selectbox(
                    f"🏦 Banco Destino{suffix}",
                    BANK_DESTINATION_OPTIONS,
                    index=_safe_index(BANK_DESTINATION_OPTIONS, banco_destino_value),
                    key=key("banco_destino_input"),
                )
                rendered_terminal = ""
    with col3:
        if include_monto:
            rendered_monto_pago = st.number_input(
                f"💲 Monto del Pago{suffix}",
                min_value=0.0,
                value=float(monto_pago_value or 0.0),
                format="%.2f",
                key=key("monto_pago_input"),
            )
        else:
            rendered_referencia_pago = st.text_input(
                f"🔢 Referencia (opcional){suffix}",
                value=referencia_pago_value,
                key=key("referencia_pago_input"),
            )

    if include_monto:
        col4, col5 = st.columns(2)
        with col4:
            if rendered_forma_pago in CARD_PAYMENT_METHODS:
                rendered_terminal = st.selectbox(
                    f"🏧 Terminal{suffix}",
                    TERMINAL_OPTIONS,
                    index=_safe_index(TERMINAL_OPTIONS, terminal_value),
                    key=key("terminal_input"),
                )
                rendered_banco_destino = ""
            else:
                rendered_banco_destino = st.selectbox(
                    f"🏦 Banco Destino{suffix}",
                    BANK_DESTINATION_OPTIONS,
                    index=_safe_index(BANK_DESTINATION_OPTIONS, banco_destino_value),
                    key=key("banco_destino_input"),
                )
                rendered_terminal = ""
        with col5:
            rendered_referencia_pago = st.text_input(
                f"🔢 Referencia (opcional){suffix}",
                value=referencia_pago_value,
                key=key("referencia_pago_input"),
            )

    return PaymentDetails(
        fecha_pago=rendered_fecha_pago,
        forma_pago=rendered_forma_pago,
        terminal=rendered_terminal,
        banco_destino=rendered_banco_destino,
        monto_pago=float(rendered_monto_pago or 0.0),
        referencia_pago=rendered_referencia_pago,
    )
