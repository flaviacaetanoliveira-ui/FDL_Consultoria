"""Descoberta de pastas Shopee sob cliente_root ou ``Empresa/Vendas_Shopee`` (ex.: Esquilo)."""

from __future__ import annotations

from pathlib import Path

from etapa3_conciliacao_vendas_liberacoes_validas import _discover_shopee_dirs


def test_shopee_vendas_sob_pasta_empresa(tmp_path: Path) -> None:
    root = tmp_path / "Cliente_4"
    esperado = root / "Esquilo" / "Vendas_Shopee"
    esperado.mkdir(parents=True)
    got = _discover_shopee_dirs(
        root,
        ("Vendas_Shopee", "Vendas Shopee"),
        ("vendas", "shopee"),
    )
    assert got == [esperado]


def test_shopee_vendas_na_raiz_cliente(tmp_path: Path) -> None:
    root = tmp_path / "Cliente_4"
    esperado = root / "Vendas_Shopee"
    esperado.mkdir(parents=True)
    got = _discover_shopee_dirs(
        root,
        ("Vendas_Shopee", "Vendas Shopee"),
        ("vendas", "shopee"),
    )
    assert got == [esperado]


def test_shopee_liberacoes_sob_pasta_empresa(tmp_path: Path) -> None:
    root = tmp_path / "Cliente_4"
    esperado = root / "Esquilo" / "Liberações_Shopee"
    esperado.mkdir(parents=True)
    got = _discover_shopee_dirs(
        root,
        ("Liberações_Shopee", "Liberacoes_Shopee", "Liberações Shopee", "Liberacoes Shopee"),
        ("libera", "shopee"),
    )
    assert got == [esperado]
