from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


ParamTransform = Callable[[Dict[str, Any]], Dict[str, Any]]
FieldMap = Dict[str, str]
PostProcess = Callable[["pd.DataFrame", Dict[str, Any]], "pd.DataFrame"]


@dataclass
class DatasetSpec:
    dataset_id: str  # e.g., "securities.equity.cn.ohlcv_daily"
    category: str
    domain: str
    ak_functions: List[str]  # candidate akshare function names (first available will be used)
    source: Optional[str] = None
    param_transform: Optional[ParamTransform] = None
    field_mapping: Optional[FieldMap] = None
    freq_support: Optional[List[str]] = None
    adjust_support: Optional[List[str]] = None
    postprocess: Optional[PostProcess] = None


REGISTRY: Dict[str, DatasetSpec] = {}


def register(spec: DatasetSpec) -> None:
    REGISTRY[spec.dataset_id] = spec


# ---------- Parameter transformers ----------

def _noop_params(p: Dict[str, Any]) -> Dict[str, Any]:
    return p


def _ohlcv_stock_daily_params(p: Dict[str, Any]) -> Dict[str, Any]:
    symbol = p.get("symbol") or p.get("symbols")
    start = p.get("start")
    end = p.get("end")
    adjust = p.get("adjust") or ""
    return {
        "symbol": symbol,
        "period": "daily",
        "start_date": start,
        "end_date": end,
        "adjust": {"none": "", "qfq": "qfq", "hfq": "hfq"}.get(adjust, ""),
    }


def _index_daily_params(p: Dict[str, Any]) -> Dict[str, Any]:
    symbol = p.get("symbol")
    start = p.get("start")
    end = p.get("end")
    return {"symbol": symbol, "start_date": start, "end_date": end}


def _fund_nav_params(p: Dict[str, Any]) -> Dict[str, Any]:
    # fund_open_fund_daily_em has no parameters; for code-specific we must use ETF/LOF APIs
    # Prefer ETF historical for ETFs; for open funds, fallback to full market daily NAV and filter later (not ideal)
    code = p.get("fund_code") or p.get("symbol")
    start = p.get("start") or "19700101"
    end = p.get("end") or "20500101"
    return {"symbol": code, "period": "daily", "start_date": start.replace("-", ""), "end_date": end.replace("-", ""), "adjust": ""}


# ---------- Field mappings (cn -> unified) ----------

FIELD_OHLCV_CN: FieldMap = {
    "日期": "date",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
    "股票代码": "symbol",
    "代码": "symbol",
}

FIELD_INDEX_DAILY_CN: FieldMap = FIELD_OHLCV_CN

FIELD_FUND_NAV_CN: FieldMap = {
    "净值日期": "nav_date",
    "单位净值": "nav",
    "累计净值": "acc_nav",
    "日增长率": "daily_return",
    "基金代码": "fund_code",
    "基金名称": "fund_name",
}

# Additional field mappings
FIELD_TRADE_CAL: FieldMap = {"trade_date": "date"}
FIELD_FUND_FLOW_STOCK: FieldMap = {
    "日期": "date",
    "收盘价": "close",
    "涨跌幅": "pct_change",
    "主力净流入-净额": "main_inflow",
    "主力净流入-净占比": "pct_main",
}


# ---------- Macro postprocessors ----------

import pandas as pd  # noqa: E402


def _macro_ppi_post(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    series = (params.get("series") or "index").lower()
    value_col = "当月" if series in {"index", "level"} else "当月同比增长"
    out = pd.DataFrame(
        {
            "region": "CN",
            "indicator_id": "ppi" if series in {"index", "level"} else "ppi_yoy",
            "indicator_name": "PPI" if series in {"index", "level"} else "PPI同比",
            "date": df.get("月份") if "月份" in df.columns else df.iloc[:, 0],
            "value": pd.to_numeric(df[value_col], errors="coerce"),
            "unit": "index" if series in {"index", "level"} else "pct",
            "period": "M",
            "source": "akshare",
        }
    )
    return out.dropna(subset=["value"])


def _macro_pmi_post(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    segment = (params.get("segment") or "manufacturing").lower()
    if segment.startswith("non") or segment.startswith("service"):
        col = "非制造业-指数"
        indicator_id = "pmi_non_manu"
        indicator_name = "PMI非制造业"
    else:
        col = "制造业-指数"
        indicator_id = "pmi_manu"
        indicator_name = "PMI制造业"
    out = pd.DataFrame(
        {
            "region": "CN",
            "indicator_id": indicator_id,
            "indicator_name": indicator_name,
            "date": df.get("月份") if "月份" in df.columns else df.iloc[:, 0],
            "value": pd.to_numeric(df[col], errors="coerce"),
            "unit": "index",
            "period": "M",
            "source": "akshare",
        }
    )
    return out.dropna(subset=["value"])


def _macro_gdp_post(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    series = (params.get("series") or "yoy").lower()
    if series == "abs":
        col = "国内生产总值-绝对值"
        indicator_id = "gdp"
        indicator_name = "GDP"
        unit = "CNY_billion"
    else:
        col = "国内生产总值-同比增长"
        indicator_id = "gdp_yoy"
        indicator_name = "GDP同比"
        unit = "pct"
    date_col = "季度" if "季度" in df.columns else df.columns[0]
    out = pd.DataFrame(
        {
            "region": "CN",
            "indicator_id": indicator_id,
            "indicator_name": indicator_name,
            "date": df[date_col],
            "value": pd.to_numeric(df[col], errors="coerce"),
            "unit": unit,
            "period": "Q",
            "source": "akshare",
        }
    )
    return out.dropna(subset=["value"])


def _fund_open_daily_post(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    code = params.get("fund_code") or params.get("symbol")
    if code:
        df = df[df.get("基金代码") == code].copy()
    # Melt wide date columns into rows
    value_cols = [c for c in df.columns if isinstance(c, str) and ("单位净值" in c or "累计净值" in c)]
    if not value_cols:
        return pd.DataFrame(columns=["fund_code", "fund_name", "nav_date", "nav", "acc_nav"])  # empty
    base_cols = [c for c in df.columns if c not in value_cols]
    m = df.melt(id_vars=base_cols, value_vars=value_cols, var_name="col", value_name="val")
    # Extract date and type
    m["nav_date"] = m["col"].str.extract(r"(\d{4}-\d{2}-\d{2})")
    m["col_type"] = m["col"].str.split("-").str[-1]
    nav = m[m["col_type"] == "单位净值"][["基金代码", "基金简称", "nav_date", "val"]].rename(columns={"基金代码": "fund_code", "基金简称": "fund_name", "val": "nav"})
    acc = m[m["col_type"] == "累计净值"][ ["基金代码", "nav_date", "val"] ].rename(columns={"基金代码": "fund_code", "val": "acc_nav"})
    out = nav.merge(acc, on=["fund_code", "nav_date"], how="left")
    out = out.dropna(subset=["nav_date"]).copy()
    return out


def _trade_calendar_post(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    market = params.get("market") or "CN"
    out = df.rename(columns=FIELD_TRADE_CAL).copy()
    out.insert(1, "is_trading_day", True)
    out.insert(2, "market", market)
    return out


def _bond_china_yield_post(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    out_rows = []
    curve = "CGB"
    for _, row in df.iterrows():
        date = row.get("日期")
        for tenor in ["3月", "6月", "1年", "3年", "5年", "7年", "10年", "30年"]:
            if tenor in df.columns:
                val = pd.to_numeric(row.get(tenor), errors="coerce")
                if pd.notna(val):
                    out_rows.append({
                        "curve_id": curve,
                        "date": date,
                        "tenor": tenor,
                        "yield": val,
                    })
    return pd.DataFrame(out_rows)


# ---------- Dataset registrations ----------

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.ohlcv_daily",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_zh_a_hist", "stock_zh_a_hist_pre"] ,
        source="em",
        param_transform=_ohlcv_stock_daily_params,
        field_mapping=FIELD_OHLCV_CN,
        freq_support=["daily"],
        adjust_support=["none", "qfq", "hfq"],
    )
)

register(
    DatasetSpec(
        dataset_id="market.index.ohlcv",
        category="market",
        domain="market.index.cn",
        ak_functions=["stock_zh_index_daily"],
        source="em",
        param_transform=_index_daily_params,
        field_mapping=FIELD_INDEX_DAILY_CN,
        freq_support=["daily"],
    )
)

register(
    DatasetSpec(
        dataset_id="market.index.constituents",
        category="market",
        domain="market.index.cn",
        ak_functions=["index_stock_cons"],
        source="em",
        param_transform=lambda p: {"symbol": (p.get("index_code") or p.get("symbol") or "").replace(".SH", "").replace(".SZ", "")},
        field_mapping={"成分券代码": "symbol", "成分券名称": "symbol_name", "权重": "weight", "指数代码": "index_symbol"},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.quote",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_zh_a_spot_em"],
        source="em",
        param_transform=_noop_params,
        field_mapping={
            "代码": "symbol",
            "名称": "symbol_name",
            "最新价": "last",
            "涨跌幅": "pct_change",
            "涨跌额": "change",
            "成交量": "volume",
            "成交额": "amount",
        },
    )
)

register(
    DatasetSpec(
        dataset_id="securities.fund.cn.nav",
        category="securities",
        domain="securities.fund.cn",
        ak_functions=["fund_etf_hist_em"],
        source="em",
        param_transform=_fund_nav_params,
        field_mapping=FIELD_FUND_NAV_CN,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.fund.cn.nav_open",
        category="securities",
        domain="securities.fund.cn",
        ak_functions=["fund_open_fund_daily_em"],
        source="em",
        param_transform=_noop_params,
        postprocess=_fund_open_daily_post,
    )
)

# ---- Macro CN datasets ----
register(
    DatasetSpec(
        dataset_id="macro.cn.ppi",
        category="macro",
        domain="macro.cn",
        ak_functions=["macro_china_ppi", "macro_china_ppi_yearly"],
        source="stats",
        param_transform=_noop_params,
        postprocess=_macro_ppi_post,
    )
)

register(
    DatasetSpec(
        dataset_id="macro.cn.pmi",
        category="macro",
        domain="macro.cn",
        ak_functions=["macro_china_pmi", "macro_china_pmi_yearly"],
        source="stats",
        param_transform=_noop_params,
        postprocess=_macro_pmi_post,
    )
)

register(
    DatasetSpec(
        dataset_id="macro.cn.gdp",
        category="macro",
        domain="macro.cn",
        ak_functions=["macro_china_gdp", "macro_china_gdp_yearly"],
        source="stats",
        param_transform=_noop_params,
        postprocess=_macro_gdp_post,
    )
)

# ---- Market.calendar ----
register(
    DatasetSpec(
        dataset_id="market.calendar",
        category="market",
        domain="market.cn",
        ak_functions=["tool_trade_date_hist_sina"],
        source="sina",
        param_transform=_noop_params,
        postprocess=_trade_calendar_post,
    )
)

# ---- Securities.equity.cn.fund_flow ----
register(
    DatasetSpec(
        dataset_id="securities.equity.cn.fund_flow",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_individual_fund_flow"],
        source="eastmoney",
        param_transform=lambda p: {"stock": (p.get("symbol") or "").replace(".SH", "").replace(".SZ", "")},
        field_mapping=FIELD_FUND_FLOW_STOCK,
    )
)

# ---- Bond China Yield Curve ----
register(
    DatasetSpec(
        dataset_id="securities.bond.curve.cn",
        category="securities",
        domain="securities.bond.cn",
        ak_functions=["bond_china_yield"],
        source="chinabond",
        param_transform=_noop_params,
        postprocess=_bond_china_yield_post,
    )
)