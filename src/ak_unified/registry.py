from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import numpy as np  # type: ignore
from .config import load_account_key_map  # noqa: E402


ParamTransform = Callable[[Dict[str, Any]], Dict[str, Any]]
FieldMap = Dict[str, str]
PostProcess = Callable[["pd.DataFrame", Dict[str, Any]], "pd.DataFrame"]
ComputeFunc = Callable[[Dict[str, Any]], "pd.DataFrame"]


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
    compute: Optional[ComputeFunc] = None


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


def _macro_cpi_post(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    series = (params.get("series") or "yoy").lower()
    # Common CPI columns: "当月" and "当月同比增长" or similar; prefer yoy
    value_col = "当月同比增长" if series in {"yoy", "同比", "pct"} else "当月"
    out = pd.DataFrame(
        {
            "region": "CN",
            "indicator_id": "cpi_yoy" if value_col == "当月同比增长" else "cpi",
            "indicator_name": "CPI同比" if value_col == "当月同比增长" else "CPI",
            "date": df.get("月份") if "月份" in df.columns else df.iloc[:, 0],
            "value": pd.to_numeric(df[value_col], errors="coerce"),
            "unit": "pct" if value_col == "当月同比增长" else "index",
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

# Expand equity quote field mapping to capture more columns if present
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
            "换手率": "turnover_rate",
            "振幅": "amplitude",
            "市盈率-动态": "pe_ttm",
            "市净率": "pb",
            "总市值": "market_cap",
            "流通市值": "float_market_cap",
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
        dataset_id="macro.cn.cpi",
        category="macro",
        domain="macro.cn",
        ak_functions=["macro_china_cpi", "macro_china_cpi_monthly"],
        source="stats",
        param_transform=_noop_params,
        postprocess=_macro_cpi_post,
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

def _strip_suffix(code: Optional[str]) -> str:
    if not code:
        return ""
    return code.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")


def _minute_period_map(freq: Optional[str]) -> str:
    mapping = {
        "min1": "1",
        "min5": "5",
        "min15": "15",
        "min30": "30",
        "min60": "60",
        "1": "1",
        "5": "5",
        "15": "15",
        "30": "30",
        "60": "60",
    }
    return mapping.get((freq or "min1").lower(), "1")


def _eq_min_params(p: Dict[str, Any]) -> Dict[str, Any]:
    symbol = _strip_suffix(p.get("symbol") or p.get("code"))
    freq = _minute_period_map(p.get("freq") or p.get("period"))
    adjust = {"none": "", "qfq": "qfq", "hfq": "hfq"}.get((p.get("adjust") or "none"), "")
    return {"symbol": symbol, "period": freq, "adjust": adjust}


def _fut_min_params(p: Dict[str, Any]) -> Dict[str, Any]:
    contract = p.get("contract") or p.get("symbol")
    freq = _minute_period_map(p.get("freq") or p.get("period"))
    return {"symbol": contract, "period": freq}

# Common minute field mapping
FIELD_MIN_OHLCV_CN: FieldMap = {
    "时间": "datetime",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
}

# -------- Macro employment postprocessors --------

def _macro_cn_unemployment_post(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    date_col = "月份" if "月份" in df.columns else df.columns[0]
    # try common value columns
    for col in ["城镇调查失业率", "失业率", "就业人数"]:
        if col in df.columns:
            value_col = col
            break
    else:
        # fallback to second column
        value_col = df.columns[1]
    out = pd.DataFrame({
        "region": "CN",
        "indicator_id": "unemployment_rate",
        "indicator_name": "城镇调查失业率",
        "date": df[date_col],
        "value": pd.to_numeric(df[value_col], errors="coerce"),
        "unit": "pct",
        "period": "M",
        "source": "akshare",
    })
    return out.dropna(subset=["value"])


def _macro_us_unemployment_post(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    date_col = "时间" if "时间" in df.columns else ("日期" if "日期" in df.columns else df.columns[0])
    # try to find unemployment rate
    value_col = None
    for col in ["失业率", "失业人数", "就业人数", "非农就业人数"]:
        if col in df.columns:
            value_col = col
            break
    if value_col is None:
        value_col = df.columns[1]
    indicator_id = "unemployment_rate" if "率" in value_col else "employment"
    indicator_name = "失业率" if "率" in value_col else "就业/非农"
    out = pd.DataFrame({
        "region": "US",
        "indicator_id": indicator_id,
        "indicator_name": indicator_name,
        "date": df[date_col],
        "value": pd.to_numeric(df[value_col], errors="coerce"),
        "unit": "pct" if indicator_id == "unemployment_rate" else "persons_thousands",
        "period": "M",
        "source": "akshare",
    })
    return out.dropna(subset=["value"])

# -------- Boards field mappings --------
FIELD_BOARD_NAME: FieldMap = {"板块名称": "board_name", "板块代码": "board_code"}
FIELD_BOARD_SPOT: FieldMap = {"板块名称": "board_name", "板块代码": "board_code", "涨跌幅": "pct_change", "领涨股": "leader"}
FIELD_BOARD_CONS: FieldMap = {"代码": "symbol", "名称": "symbol_name"}
FIELD_BOARD_OHLCV: FieldMap = FIELD_OHLCV_CN | {"时间": "datetime"}

# -------- LHB field mappings (subset) --------
FIELD_LHB_DAILY: FieldMap = {"日期": "date", "上榜次数": "times", "买入额": "buy", "卖出额": "sell", "净额": "net"}
FIELD_LHB_STOCK_DETAIL: FieldMap = {"日期": "date", "收盘价": "close", "涨跌幅": "pct_change", "买入额": "buy", "卖出额": "sell", "净额": "net"}
FIELD_LHB_YYB_DETAIL: FieldMap = {"营业部名称": "broker_name", "买入额": "buy", "卖出额": "sell", "净额": "net"}

# -------- Fund info mappings --------
FIELD_FUND_MANAGER: FieldMap = {"姓名": "manager_name", "所属公司": "company_name", "累计从业时间": "career_years", "现任基金": "funds_current"}
FIELD_FUND_RATING: FieldMap = {"基金代码": "fund_code", "基金简称": "fund_name", "评级": "rating"}
FIELD_FUND_SCALE: FieldMap = {"基金代码": "fund_code", "基金简称": "fund_name", "基金规模": "fund_scale", "成立日期": "established"}

# ---------- New dataset registrations ----------

# Minute OHLCV for A-shares
register(
    DatasetSpec(
        dataset_id="securities.equity.cn.ohlcv_min",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_zh_a_hist_min_em", "stock_zh_a_hist_pre_min_em", "stock_zh_a_minute"],
        source="em",
        param_transform=_eq_min_params,
        field_mapping=FIELD_MIN_OHLCV_CN,
        freq_support=["min1", "min5", "min15", "min30", "min60"],
    )
)

# Futures minute
register(
    DatasetSpec(
        dataset_id="securities.futures.cn.min",
        category="securities",
        domain="securities.futures.cn",
        ak_functions=["futures_zh_minute_sina"],
        source="sina",
        param_transform=_fut_min_params,
        field_mapping=FIELD_MIN_OHLCV_CN,
        freq_support=["min1", "min5", "min15", "min30", "min60"],
    )
)

# Macro employment CN
register(
    DatasetSpec(
        dataset_id="macro.cn.employment",
        category="macro",
        domain="macro.cn",
        ak_functions=["macro_china_urban_unemployment", "macro_china_hk_rate_of_unemployment"],
        source="stats",
        param_transform=_noop_params,
        postprocess=_macro_cn_unemployment_post,
    )
)

# Macro employment US
register(
    DatasetSpec(
        dataset_id="macro.us.employment",
        category="macro",
        domain="macro.us",
        ak_functions=["macro_usa_unemployment_rate", "macro_usa_non_farm", "macro_usa_adp_employment"],
        source="bls",
        param_transform=_noop_params,
        postprocess=_macro_us_unemployment_post,
    )
)

# Boards - industry names (multi-source)
register(
    DatasetSpec(
        dataset_id="securities.board.cn.industry.list",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_industry_name_em", "stock_board_industry_name_ths"],
        source="em",
        param_transform=_noop_params,
        field_mapping=FIELD_BOARD_NAME,
    )
)

# Boards - concept names (multi-source)
register(
    DatasetSpec(
        dataset_id="securities.board.cn.concept.list",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_concept_name_em", "stock_board_concept_name_ths"],
        source="em",
        param_transform=_noop_params,
        field_mapping=FIELD_BOARD_NAME,
    )
)

# Boards - industry spot (em)
register(
    DatasetSpec(
        dataset_id="securities.board.cn.industry.spot",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_industry_spot_em"],
        source="em",
        param_transform=_noop_params,
        field_mapping=FIELD_BOARD_SPOT,
    )
)

# Boards - concept spot (em)
register(
    DatasetSpec(
        dataset_id="securities.board.cn.concept.spot",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_concept_spot_em"],
        source="em",
        param_transform=_noop_params,
        field_mapping=FIELD_BOARD_SPOT,
    )
)

# Boards - industry constituents
register(
    DatasetSpec(
        dataset_id="securities.board.cn.industry.cons",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_industry_cons_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("board_code") or p.get("symbol")},
        field_mapping=FIELD_BOARD_CONS,
    )
)

# Boards - concept constituents
register(
    DatasetSpec(
        dataset_id="securities.board.cn.concept.cons",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_concept_cons_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("board_code") or p.get("symbol")},
        field_mapping=FIELD_BOARD_CONS,
    )
)

# Boards - industry daily history
register(
    DatasetSpec(
        dataset_id="securities.board.cn.industry.ohlcv_daily",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_industry_hist_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("board_code") or p.get("symbol")},
        field_mapping=FIELD_BOARD_OHLCV,
    )
)

# Boards - concept daily history
register(
    DatasetSpec(
        dataset_id="securities.board.cn.concept.ohlcv_daily",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_concept_hist_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("board_code") or p.get("symbol")},
        field_mapping=FIELD_BOARD_OHLCV,
    )
)

# Boards - industry minute
register(
    DatasetSpec(
        dataset_id="securities.board.cn.industry.ohlcv_min",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_industry_hist_min_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("board_code") or p.get("symbol"), "period": _minute_period_map(p.get("freq"))},
        field_mapping=FIELD_MIN_OHLCV_CN,
    )
)

# Boards - concept minute
register(
    DatasetSpec(
        dataset_id="securities.board.cn.concept.ohlcv_min",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_concept_hist_min_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("board_code") or p.get("symbol"), "period": _minute_period_map(p.get("freq"))},
        field_mapping=FIELD_MIN_OHLCV_CN,
    )
)

# Beijing A-share spot
register(
    DatasetSpec(
        dataset_id="securities.equity.bj.quote",
        category="securities",
        domain="securities.equity.bj",
        ak_functions=["stock_bj_a_spot_em"],
        source="em",
        param_transform=_noop_params,
        field_mapping={"代码": "symbol", "名称": "symbol_name", "最新价": "last", "涨跌幅": "pct_change", "成交量": "volume", "成交额": "amount"},
    )
)

# LHB datasets
register(
    DatasetSpec(
        dataset_id="securities.equity.cn.lhb.daily",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_lhb_detail_em", "stock_lhb_detail_daily_sina"],
        source="em",
        param_transform=_noop_params,
        field_mapping=FIELD_LHB_DAILY,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.lhb.stock_detail",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_lhb_stock_detail_em", "stock_lhb_stock_detail_date_em"],
        source="em",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
        field_mapping=FIELD_LHB_STOCK_DETAIL,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.lhb.broker_detail",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_lhb_yyb_detail_em"],
        source="em",
        param_transform=_noop_params,
        field_mapping=FIELD_LHB_YYB_DETAIL,
    )
)

# Fund info
register(
    DatasetSpec(
        dataset_id="securities.fund.cn.manager",
        category="securities",
        domain="securities.fund.cn",
        ak_functions=["fund_manager_em"],
        source="em",
        param_transform=_noop_params,
        field_mapping=FIELD_FUND_MANAGER,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.fund.cn.rating",
        category="securities",
        domain="securities.fund.cn",
        ak_functions=["fund_rating_all", "fund_rating_ja", "fund_rating_sh", "fund_rating_zs"],
        source="multi",
        param_transform=_noop_params,
        field_mapping=FIELD_FUND_RATING,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.fund.cn.scale",
        category="securities",
        domain="securities.fund.cn",
        ak_functions=["fund_scale_open_sina", "fund_scale_close_sina", "fund_scale_structured_sina", "fund_scale_change_em"],
        source="multi",
        param_transform=_noop_params,
        field_mapping=FIELD_FUND_SCALE,
    )
)

# -------- Helpers for date formatting --------

def _yyyymmdd(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    return s.replace("-", "")


# -------- Macro monetary policy & liquidity --------

register(
    DatasetSpec(
        dataset_id="macro.cn.lpr",
        category="macro",
        domain="macro.cn",
        ak_functions=["macro_china_lpr"],
        source="pbc",
        param_transform=_noop_params,
    )
)

register(
    DatasetSpec(
        dataset_id="macro.cn.shibor",
        category="macro",
        domain="macro.cn",
        ak_functions=["macro_china_shibor_all"],
        source="pbc",
        param_transform=_noop_params,
    )
)

register(
    DatasetSpec(
        dataset_id="macro.cn.repo_rate_hist",
        category="macro",
        domain="macro.cn",
        ak_functions=["repo_rate_hist"],
        source="pbc",
        param_transform=lambda p: {"start_date": _yyyymmdd(p.get("start")), "end_date": _yyyymmdd(p.get("end"))},
    )
)

register(
    DatasetSpec(
        dataset_id="macro.cn.repo_rate_query",
        category="macro",
        domain="macro.cn",
        ak_functions=["repo_rate_query"],
        source="pbc",
        param_transform=lambda p: {"symbol": p.get("symbol") or "回购定盘利率"},
    )
)

register(
    DatasetSpec(
        dataset_id="macro.cn.central_bank_balance",
        category="macro",
        domain="macro.cn",
        ak_functions=["macro_china_central_bank_balance"],
        source="pbc",
        param_transform=_noop_params,
    )
)

register(
    DatasetSpec(
        dataset_id="macro.cn.social_financing",
        category="macro",
        domain="macro.cn",
        ak_functions=["macro_china_new_financial_credit", "macro_stock_finance"],
        source="pbc",
        param_transform=_noop_params,
    )
)

# TODO(analysis): derive economic_cycle_phase using multiple indicators (growth, inflation, policy rates). Not directly provided by AkShare.

# -------- Market valuation/liquidity/sentiment --------

register(
    DatasetSpec(
        dataset_id="market.valuation.cn.index_pe",
        category="market",
        domain="market.cn",
        ak_functions=["stock_index_pe_lg"],
        source="legulegu",
        param_transform=lambda p: {"symbol": p.get("symbol") or "沪深300"},
    )
)

register(
    DatasetSpec(
        dataset_id="market.valuation.cn.index_pb",
        category="market",
        domain="market.cn",
        ak_functions=["stock_index_pb_lg"],
        source="legulegu",
        param_transform=lambda p: {"symbol": p.get("symbol") or "上证50"},
    )
)

register(
    DatasetSpec(
        dataset_id="market.valuation.cn.market_pe",
        category="market",
        domain="market.cn",
        ak_functions=["stock_market_pe_lg"],
        source="legulegu",
        param_transform=lambda p: {"symbol": p.get("symbol") or "深证"},
    )
)

register(
    DatasetSpec(
        dataset_id="market.valuation.cn.market_pb",
        category="market",
        domain="market.cn",
        ak_functions=["stock_market_pb_lg"],
        source="legulegu",
        param_transform=lambda p: {"symbol": p.get("symbol") or "上证"},
    )
)

# Margin financing data
register(
    DatasetSpec(
        dataset_id="market.margin.cn.sse",
        category="market",
        domain="market.cn",
        ak_functions=["stock_margin_sse"],
        source="sse",
        param_transform=lambda p: {"start_date": _yyyymmdd(p.get("start")), "end_date": _yyyymmdd(p.get("end"))},
    )
)

register(
    DatasetSpec(
        dataset_id="market.margin.cn.szse",
        category="market",
        domain="market.cn",
        ak_functions=["stock_margin_szse"],
        source="szse",
        param_transform=lambda p: {"date": _yyyymmdd(p.get("date") or p.get("end"))},
    )
)

register(
    DatasetSpec(
        dataset_id="market.margin.cn.detail_sse",
        category="market",
        domain="market.cn",
        ak_functions=["stock_margin_detail_sse"],
        source="sse",
        param_transform=lambda p: {"date": _yyyymmdd(p.get("date") or p.get("end"))},
    )
)

register(
    DatasetSpec(
        dataset_id="market.margin.cn.detail_szse",
        category="market",
        domain="market.cn",
        ak_functions=["stock_margin_detail_szse"],
        source="szse",
        param_transform=lambda p: {"date": _yyyymmdd(p.get("date") or p.get("end"))},
    )
)

register(
    DatasetSpec(
        dataset_id="market.margin.cn.ratio",
        category="market",
        domain="market.cn",
        ak_functions=["stock_margin_ratio_pa"],
        source="pa",
        param_transform=lambda p: {"date": _yyyymmdd(p.get("date") or p.get("end"))},
    )
)

# Northbound/Southbound statistics
register(
    DatasetSpec(
        dataset_id="market.hsgt.institution_stats",
        category="market",
        domain="market.cn",
        ak_functions=["stock_hsgt_institution_statistics_em"],
        source="em",
        param_transform=lambda p: {"market": p.get("market") or "北向持股", "start_date": _yyyymmdd(p.get("start")), "end_date": _yyyymmdd(p.get("end"))},
    )
)

register(
    DatasetSpec(
        dataset_id="market.hsgt.board_rank",
        category="market",
        domain="market.cn",
        ak_functions=["stock_hsgt_board_rank_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("symbol") or "北向资金增持行业板块排行", "indicator": p.get("indicator") or "今日"},
    )
)

# Sentiment & volatility
register(
    DatasetSpec(
        dataset_id="market.sentiment.cn.news_scope",
        category="market",
        domain="market.cn",
        ak_functions=["index_news_sentiment_scope"],
        source="news",
        param_transform=_noop_params,
    )
)

register(
    DatasetSpec(
        dataset_id="market.volatility.cn.qvix",
        category="market",
        domain="market.cn",
        ak_functions=[
            "index_option_50etf_qvix",
            "index_option_300etf_qvix",
            "index_option_100etf_qvix",
            "index_option_500etf_qvix",
            "index_option_1000index_qvix",
            "index_option_300index_qvix",
            "index_option_50index_qvix",
            "index_option_cyb_qvix",
            "index_option_kcb_qvix",
        ],
        source="sse",
        param_transform=_noop_params,
    )
)

# TODO(analysis): retail_vs_institution_ratio not directly available; consider proxy via margin activity, average account size, turnover breakdown.
# TODO(analysis): regulatory_risk_level not directly available; consider proxy from disclosures/sanctions/CSRC actions.

# -------- Index components & industry valuation --------

register(
    DatasetSpec(
        dataset_id="securities.index.cn.sw_components",
        category="securities",
        domain="securities.index.cn",
        ak_functions=["index_component_sw"],
        source="sw",
        param_transform=lambda p: {"symbol": p.get("symbol") or "801001"},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.industry.cn.pe_ratio_cninfo",
        category="securities",
        domain="securities.industry.cn",
        ak_functions=["stock_industry_pe_ratio_cninfo"],
        source="cninfo",
        param_transform=lambda p: {"symbol": p.get("symbol") or "证监会行业分类", "date": p.get("date") or "20210910"},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.industry.cn.category_cninfo",
        category="securities",
        domain="securities.industry.cn",
        ak_functions=["stock_industry_category_cninfo"],
        source="cninfo",
        param_transform=lambda p: {"symbol": p.get("symbol") or "巨潮行业分类标准"},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.industry.cn.sw_classification_hist",
        category="securities",
        domain="securities.industry.cn",
        ak_functions=["stock_industry_clf_hist_sw"],
        source="sw",
        param_transform=_noop_params,
    )
)

# -------- ESG --------
register(
    DatasetSpec(
        dataset_id="market.esg.cn.sina_rate",
        category="market",
        domain="market.cn",
        ak_functions=["stock_esg_rate_sina"],
        source="sina",
        param_transform=_noop_params,
    )
)
register(
    DatasetSpec(
        dataset_id="market.esg.cn.msci",
        category="market",
        domain="market.cn",
        ak_functions=["stock_esg_msci_sina"],
        source="sina",
        param_transform=_noop_params,
    )
)
register(
    DatasetSpec(
        dataset_id="market.esg.cn.hz",
        category="market",
        domain="market.cn",
        ak_functions=["stock_esg_hz_sina"],
        source="sina",
        param_transform=_noop_params,
    )
)
register(
    DatasetSpec(
        dataset_id="market.esg.cn.rft",
        category="market",
        domain="market.cn",
        ak_functions=["stock_esg_rft_sina"],
        source="sina",
        param_transform=_noop_params,
    )
)
register(
    DatasetSpec(
        dataset_id="market.esg.cn.zd",
        category="market",
        domain="market.cn",
        ak_functions=["stock_esg_zd_sina"],
        source="sina",
        param_transform=_noop_params,
    )
)

# -------- Fund portfolios & holders --------

register(
    DatasetSpec(
        dataset_id="securities.fund.cn.portfolio_hold",
        category="securities",
        domain="securities.fund.cn",
        ak_functions=["fund_portfolio_hold_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("fund_code") or p.get("symbol"), "date": p.get("date") or "2024"},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.fund.cn.portfolio_change",
        category="securities",
        domain="securities.fund.cn",
        ak_functions=["fund_portfolio_change_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("fund_code") or p.get("symbol"), "indicator": p.get("indicator") or "累计买入", "date": p.get("date") or "2023"},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.fund.cn.holder_structure",
        category="securities",
        domain="securities.fund.cn",
        ak_functions=["fund_hold_structure_em"],
        source="em",
        param_transform=_noop_params,
    )
)

# -------- Stock shareholding structure --------

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.shareholders.main",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_main_stock_holder"],
        source="em",
        param_transform=lambda p: {"stock": _strip_suffix(p.get("symbol") or p.get("stock"))},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.shareholders.circulate",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_circulate_stock_holder"],
        source="em",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.shareholders.fund",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_fund_stock_holder"],
        source="em",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.sharehold_change.sse",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_share_hold_change_sse"],
        source="sse",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.sharehold_change.szse",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_share_hold_change_szse"],
        source="szse",
        param_transform=_noop_params,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.sharehold_change.bse",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_share_hold_change_bse"],
        source="bse",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.share_change.cninfo",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_share_change_cninfo"],
        source="cninfo",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol")), "start_date": _yyyymmdd(p.get("start")) or "20000101", "end_date": _yyyymmdd(p.get("end")) or "20991231"},
    )
)

# Restore datasets accidentally overwritten by edits
register(
    DatasetSpec(
        dataset_id="securities.equity.cn.fundamentals.indicators",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_financial_analysis_indicator_em", "stock_financial_analysis_indicator"],
        source="em",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol")), "indicator": p.get("indicator") or "按报告期"},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.dividends",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_dividend_cninfo", "stock_fhps_em", "stock_history_dividend"],
        source="multi",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.profile",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_profile_cninfo", "stock_sy_profile_em"],
        source="multi",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.analyst_forecast",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_profit_forecast_em", "stock_profit_forecast_ths", "stock_rank_forecast_cninfo"],
        source="multi",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
    )
)

# -------- Computed datasets (heuristics; TODO: refine models) --------

def _compute_economic_cycle_phase(params: Dict[str, Any]) -> pd.DataFrame:
    # Inputs: GDP YoY (macro.cn.gdp), CPI YoY (macro.cn.cpi), Policy rates (LPR/shibor), Social financing, PMI
    from .dispatcher import fetch_data as _fetch

    def safe_fetch(dataset: str, kwargs: Dict[str, Any], ak_fn: Optional[str] = None):
        try:
            return _fetch(dataset, kwargs, ak_function=ak_fn).data
        except Exception:
            return []

    growth = safe_fetch("macro.cn.gdp", {}, ak_fn="macro_china_gdp")
    cpi = safe_fetch("macro.cn.cpi", {"series": "yoy"}, ak_fn="macro_china_cpi")
    pmi = safe_fetch("macro.cn.pmi", {"segment": "manufacturing"}, ak_fn="macro_china_pmi")
    ppi = safe_fetch("macro.cn.ppi", {"series": "yoy"}, ak_fn="macro_china_ppi")
    lpr = safe_fetch("macro.cn.lpr", {})
    social = safe_fetch("macro.cn.social_financing", {}, ak_fn="macro_china_new_financial_credit")

    def latest_num(lst, key):
        for item in lst[:50]:
            v = item.get(key)
            if isinstance(v, (int, float)):
                return float(v)
        return np.nan

    growth_yoy = latest_num(growth, "value") if growth else np.nan
    cpi_yoy = latest_num(cpi, "value") if cpi else np.nan
    pmi_val = latest_num(pmi, "value") if pmi else np.nan
    ppi_yoy = latest_num(ppi, "value") if ppi else np.nan

    # thresholds from params or defaults
    pmi_threshold = float(params.get("pmi_threshold", 50))
    pmi_recession = float(params.get("pmi_recession", 47))
    growth_high = float(params.get("growth_high", 5))
    growth_low = float(params.get("growth_low", 0))

    phase = "unknown"
    if not np.isnan(pmi_val) and not np.isnan(growth_yoy):
        if pmi_val >= pmi_threshold and growth_yoy >= growth_high:
            phase = "expansion"
        elif pmi_val < pmi_threshold and growth_yoy > growth_low:
            phase = "slowdown"
        elif growth_yoy <= growth_low or pmi_val < pmi_recession:
            phase = "recession"
        else:
            phase = "recovery"

    return pd.DataFrame([
        {
            "phase": phase,
            "growth_yoy": growth_yoy,
            "pmi_manu": pmi_val,
            "cpi_yoy": cpi_yoy,
            "ppi_yoy": ppi_yoy,
            "note": "Heuristic; thresholds configurable via params: pmi_threshold, pmi_recession, growth_high, growth_low",
        }
    ])


def _compute_retail_vs_institution_proxy(params: Dict[str, Any]) -> pd.DataFrame:
    # Inputs: margin balance ratio/time series; turnover; northbound stats
    from .dispatcher import fetch_data as _fetch

    margin = _fetch("market.margin.cn.sse", {"start": params.get("start"), "end": params.get("end")}).data
    hsgt = _fetch("market.hsgt.institution_stats", {"market": "北向持股", "start": params.get("start"), "end": params.get("end")}).data

    margin_proxy = float(len(margin) or 0)
    hsgt_proxy = float(len(hsgt) or 0)
    # Placeholder scoring 0-1 normalized by arbitrary cap
    retail_score = max(0.0, min(1.0, (margin_proxy / 5000.0)))
    institution_score = max(0.0, min(1.0, (hsgt_proxy / 5000.0)))

    return pd.DataFrame([
        {
            "retail_activity_score": retail_score,
            "institution_activity_score": institution_score,
            "note": "TODO: replace with turnover breakdown, account-level data, and proper normalization",
        }
    ])


register(
    DatasetSpec(
        dataset_id="macro.cn.economic_cycle_phase",
        category="macro",
        domain="macro.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_economic_cycle_phase,
    )
)

register(
    DatasetSpec(
        dataset_id="market.cn.retail_vs_institution_proxy",
        category="market",
        domain="market.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_retail_vs_institution_proxy,
    )
)

# Add CPI dataset mapping (CN)
register(
    DatasetSpec(
        dataset_id="macro.cn.cpi",
        category="macro",
        domain="macro.cn",
        ak_functions=["macro_china_cpi", "macro_china_cpi_monthly"],
        source="stats",
        param_transform=_noop_params,
        postprocess=_macro_cpi_post,
    )
)

# Extend economic cycle compute to fetch CPI/PMI inside compute

def _compute_index_valuation_risk(params: Dict[str, Any]) -> pd.DataFrame:
    # Inputs: index PE/PB time series -> rolling zscore and percentile
    from .dispatcher import fetch_data as _fetch
    import numpy as _np
    import pandas as _pd

    symbol = params.get("symbol") or "沪深300"
    pe_env = _fetch("market.valuation.cn.index_pe", {"symbol": symbol})
    pb_env = _fetch("market.valuation.cn.index_pb", {"symbol": "上证50"})

    def compute_stats(df: _pd.DataFrame, value_col: str):
        series = _pd.to_numeric(df[value_col], errors="coerce")
        rolling = series.rolling(252, min_periods=63)
        mean = rolling.mean()
        std = rolling.std()
        z = (series - mean) / std
        rank = series.rank(pct=True)
        return z, rank

    df_pe = _pd.DataFrame(pe_env.data)
    if df_pe.empty or "滚动市盈率" not in df_pe.columns:
        return _pd.DataFrame([])
    z_pe, pct_pe = compute_stats(df_pe, "滚动市盈率")

    df_out = _pd.DataFrame({
        "date": df_pe.get("日期", _pd.RangeIndex(len(df_pe))).astype(str),
        "pe_rolling": _pd.to_numeric(df_pe["滚动市盈率"], errors="coerce"),
        "pe_zscore_252": z_pe,
        "pe_percentile": pct_pe,
    })

    # Optional PB
    df_pb = _pd.DataFrame(pb_env.data)
    if not df_pb.empty and "市净率" in df_pb.columns:
        series_pb = _pd.to_numeric(df_pb["市净率"], errors="coerce")
        rolling_pb = series_pb.rolling(252, min_periods=63)
        z_pb = (series_pb - rolling_pb.mean()) / rolling_pb.std()
        pct_pb = series_pb.rank(pct=True)
        df_out["pb"] = series_pb
        df_out["pb_zscore_252"] = z_pb
        df_out["pb_percentile"] = pct_pb

    return df_out.dropna(how="all")


def _compute_sentiment_dashboard(params: Dict[str, Any]) -> pd.DataFrame:
    # Inputs: QVIX proxies + margin ratio + optional news sentiment
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd

    qvix_fn = params.get("qvix_function") or "index_option_300etf_qvix"
    qvix_env = _fetch("market.volatility.cn.qvix", {}, ak_function=qvix_fn)
    margin_env = _fetch("market.margin.cn.ratio", {"date": params.get("date") or "20231013"})

    news_score = None
    try:
        news_env = _fetch("market.sentiment.cn.news_scope", {})
        news_df = _pd.DataFrame(news_env.data)
        news_score = int(len(news_df)) if not news_df.empty else None
    except Exception:
        news_score = None

    qvix_df = _pd.DataFrame(qvix_env.data)
    margin_df = _pd.DataFrame(margin_env.data)

    current_vol = _pd.to_numeric(qvix_df.get("close"), errors="coerce").tail(1).fillna(0).values
    vol_current = float(current_vol[0]) if len(current_vol) else None

    out = {
        "vol_proxy": vol_current,
        "margin_ratio_sample": int(len(margin_df)),
        "news_scope_sample": news_score,
        "note": "TODO: replace news_scope_sample with actual sentiment score; add turnover risk appetite",
    }
    return _pd.DataFrame([out])


register(
    DatasetSpec(
        dataset_id="market.cn.index_valuation_risk",
        category="market",
        domain="market.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_index_valuation_risk,
    )
)

register(
    DatasetSpec(
        dataset_id="market.cn.sentiment_dashboard",
        category="market",
        domain="market.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_sentiment_dashboard,
    )
)

def _compute_risk_appetite(params: Dict[str, Any]) -> pd.DataFrame:
    # Use market snapshot to gauge winners ratio and mean turnover
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd
    import numpy as _np

    env = _fetch("securities.equity.cn.quote", {})
    df = _pd.DataFrame(env.data)
    if df.empty:
        return _pd.DataFrame([])
    winners = _np.mean(_pd.to_numeric(df.get("pct_change"), errors="coerce") > 0)
    mean_turnover = _pd.to_numeric(df.get("换手率"), errors="coerce").mean()
    amplitude_mean = _pd.to_numeric(df.get("振幅"), errors="coerce").mean()
    return _pd.DataFrame([
        {
            "winners_ratio": float(winners) if not _np.isnan(winners) else None,
            "mean_turnover_rate": float(mean_turnover) if mean_turnover is not None else None,
            "amplitude_mean": float(amplitude_mean) if amplitude_mean is not None else None,
        }
    ])


register(
    DatasetSpec(
        dataset_id="market.cn.risk_appetite",
        category="market",
        domain="market.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_risk_appetite,
    )
)

def _compute_board_heatmap(params: Dict[str, Any]) -> pd.DataFrame:
    # board_kind: industry|concept
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd
    import numpy as _np

    kind = (params.get("board_kind") or "industry").lower()
    # prefer THS summary for richer columns when available
    ds = (
        "securities.board.cn.industry.summary_ths" if kind == "industry" else "securities.board.cn.concept.summary_ths"
    )
    try:
        env = _fetch(ds, {})
        df = _pd.DataFrame(env.data)
        if not df.empty and ("涨跌幅" in df.columns or "涨跌幅(%)" in df.columns):
            chg_col = "涨跌幅" if "涨跌幅" in df.columns else "涨跌幅(%)"
            pct = _pd.to_numeric(df[chg_col], errors="coerce")
            rank = pct.rank(pct=True)
            out = _pd.DataFrame({
                "board_name": df.get("板块") or df.get("概念名称"),
                "pct_change": pct,
                "amount": _pd.to_numeric(df.get("总成交额"), errors="coerce") if "总成交额" in df.columns else _pd.Series([_np.nan]*len(df)),
                "zscore_pct_change": (pct - pct.mean())/pct.std() if pct.std() not in (0,_np.nan) else pct*_np.nan,
                "pct_rank": rank,
                "leader": df.get("领涨股"),
            })
            return out.dropna(subset=["pct_change"], how="all")
    except Exception:
        pass

    # fallback to EM spot (limited fields)
    ds2 = "securities.board.cn.industry.spot" if kind == "industry" else "securities.board.cn.concept.spot"
    env = _fetch(ds2, {})
    df = _pd.DataFrame(env.data)
    if df.empty:
        return _pd.DataFrame([])
    pct = _pd.to_numeric(df.get("pct_change"), errors="coerce")
    if not isinstance(pct, _pd.Series):
        pct = _pd.Series(pct)
    turnover = _pd.to_numeric(df.get("换手率"), errors="coerce") if "换手率" in df.columns else _pd.Series([_np.nan]*len(df))
    amount = _pd.to_numeric(df.get("成交额"), errors="coerce") if "成交额" in df.columns else _pd.Series([_np.nan]*len(df))
    z = (pct - pct.mean()) / pct.std() if pct.std() not in (0, _np.nan) else pct * _np.nan
    rank = pct.rank(pct=True)
    out = _pd.DataFrame({
        "board_name": df.get("board_name"),
        "pct_change": pct,
        "turnover_rate": turnover,
        "amount": amount,
        "zscore_pct_change": z,
        "pct_rank": rank,
    })
    return out.dropna(subset=["pct_change"], how="all")


def _compute_cross_section_valuation(params: Dict[str, Any]) -> pd.DataFrame:
    # industry-level valuation heatmap using CNINFO pe_ratio snapshot
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd
    import numpy as _np

    date = params.get("date") or "20210910"
    env = _fetch("securities.industry.cn.pe_ratio_cninfo", {"date": date, "symbol": params.get("symbol") or "证监会行业分类"})
    df = _pd.DataFrame(env.data)
    if df.empty:
        return df
    # try detect PE column
    pe_col = None
    for c in ["市盈率", "PE", "平均市盈率", "行业市盈率"]:
        if c in df.columns:
            pe_col = c
            break
    if pe_col is None:
        return df
    pe = _pd.to_numeric(df[pe_col], errors="coerce")
    rank = pe.rank(pct=True)
    z = (pe - pe.mean()) / pe.std() if pe.std() not in (0, _np.nan) else pe * _np.nan
    # industry name column guess
    name_col = None
    for c in ["行业名称", "行业", "名称", "分类名称", "板块名称", "board_name"]:
        if c in df.columns:
            name_col = c
            break
    out = _pd.DataFrame({
        "industry": df.get(name_col, df.index.astype(str)),
        "pe": pe,
        "pe_percentile": rank,
        "pe_zscore": z,
        "date": date,
        "note": "TODO: add PB/PS if available; concept valuation not covered",
    })
    return out.dropna(subset=["pe"], how="all")


def _compute_relative_strength(params: Dict[str, Any]) -> pd.DataFrame:
    # snapshot-based RS across boards
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd

    kind = (params.get("board_kind") or "industry").lower()
    ds = "securities.board.cn.industry.spot" if kind == "industry" else "securities.board.cn.concept.spot"
    env = _fetch(ds, {})
    df = _pd.DataFrame(env.data)
    if df.empty:
        return _pd.DataFrame([])
    df_out = df[["board_code", "board_name", "pct_change"]].copy()
    df_out["rs_percentile"] = df_out["pct_change"].rank(pct=True)
    return df_out.sort_values("rs_percentile", ascending=False)


register(
    DatasetSpec(
        dataset_id="market.cn.board_heatmap",
        category="market",
        domain="market.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_board_heatmap,
    )
)

register(
    DatasetSpec(
        dataset_id="market.cn.cross_section_valuation",
        category="market",
        domain="market.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_cross_section_valuation,
    )
)

register(
    DatasetSpec(
        dataset_id="market.cn.relative_strength",
        category="market",
        domain="market.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_relative_strength,
    )
)

# THS names and summaries
register(
    DatasetSpec(
        dataset_id="securities.board.cn.industry.name_ths",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_industry_name_ths"],
        source="ths",
        param_transform=_noop_params,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.board.cn.concept.name_ths",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_concept_name_ths"],
        source="ths",
        param_transform=_noop_params,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.board.cn.industry.summary_ths",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_industry_summary_ths"],
        source="ths",
        param_transform=_noop_params,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.board.cn.concept.summary_ths",
        category="securities",
        domain="securities.board.cn",
        ak_functions=["stock_board_concept_summary_ths"],
        source="ths",
        param_transform=_noop_params,
    )
)

# ---------- Single-stock fundamentals (A-share) ----------

# ---------- Financial statements postprocessors ----------

# Common CN account name to canonical english key (non-exhaustive; extend as needed)
ACCOUNT_KEY_MAP: Dict[str, str] = {
    "营业总收入": "revenue_total",
    "营业收入": "revenue",
    "主营业务收入": "revenue_main",
    "营业总成本": "operating_cost_total",
    "营业成本": "cost_of_revenue",
    "销售费用": "selling_expense",
    "管理费用": "admin_expense",
    "财务费用": "financial_expense",
    "研发费用": "rd_expense",
    "营业利润": "operating_profit",
    "利润总额": "total_profit",
    "净利润": "net_profit",
    "归母净利润": "net_profit_parent",
    "基本每股收益": "eps_basic",
    "稀释每股收益": "eps_diluted",
    "货币资金": "cash_and_equivalents",
    "应收账款": "accounts_receivable",
    "存货": "inventory",
    "流动资产合计": "current_assets_total",
    "非流动资产合计": "noncurrent_assets_total",
    "资产总计": "assets_total",
    "流动负债合计": "current_liabilities_total",
    "非流动负债合计": "noncurrent_liabilities_total",
    "负债合计": "liabilities_total",
    "所有者权益(或股东权益)合计": "equity_total",
    "经营活动现金流量净额": "cfo_net",
    "投资活动产生的现金流量净额": "cfi_net",
    "筹资活动产生的现金流量净额": "cff_net",
    "现金及现金等价物净增加额": "cash_net_change",
}

ACCOUNT_KEY_MAP = load_account_key_map(ACCOUNT_KEY_MAP)


def _map_account_key(key: str) -> str:
    return ACCOUNT_KEY_MAP.get(key, key)


def _normalize_financial_report(df: pd.DataFrame, params: Dict[str, Any], statement_type: str) -> pd.DataFrame:
    import re
    if df.empty:
        return df
    # Choose period/date column heuristically
    date_col = None
    for c in ["报告期", "报告日期", "日期", "公告日期", "period", "date"]:
        if c in df.columns:
            date_col = c
            break
    if date_col is None:
        # try first column if looks date-like
        c0 = df.columns[0]
        if re.search(r"\d{4}-\d{2}-\d{2}", str(df.iloc[0, 0])):
            date_col = c0
        else:
            date_col = c0
    # Build values map per row by numeric columns
    records: list[dict] = []
    symbol = params.get("symbol")
    for _, row in df.iterrows():
        period_end = str(row.get(date_col))
        values: Dict[str, float] = {}
        for c in df.columns:
            if c == date_col:
                continue
            try:
                v = float(pd.to_numeric(row[c]))
                if pd.notna(v):
                    values[_map_account_key(str(c))] = v
            except Exception:
                continue
        if not values:
            continue
        records.append({
            "symbol": symbol,
            "statement_type": statement_type,
            "period_end": period_end,
            "report_type": None,
            "currency": "CNY",
            "values": values,
        })
    return pd.DataFrame(records)


def _post_is(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    return _normalize_financial_report(df, params, "IS")


def _post_bs(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    return _normalize_financial_report(df, params, "BS")


def _post_cf(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    return _normalize_financial_report(df, params, "CF")


register(
    DatasetSpec(
        dataset_id="securities.equity.cn.financials.is",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_profit_sheet_by_report_em", "stock_profit_sheet_by_yearly_em", "stock_profit_sheet_by_quarterly_em"],
        source="em",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
        postprocess=_post_is,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.financials.bs",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_balance_sheet_by_report_em", "stock_balance_sheet_by_yearly_em"],
        source="em",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
        postprocess=_post_bs,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.cn.financials.cf",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=["stock_cash_flow_sheet_by_report_em", "stock_cash_flow_sheet_by_yearly_em", "stock_cash_flow_sheet_by_quarterly_em"],
        source="em",
        param_transform=lambda p: {"symbol": _strip_suffix(p.get("symbol"))},
        postprocess=_post_cf,
    )
)

# HK & US financial reports
register(
    DatasetSpec(
        dataset_id="securities.equity.hk.financials.report",
        category="securities",
        domain="securities.equity.hk",
        ak_functions=["stock_financial_hk_report_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("symbol")},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.us.financials.report",
        category="securities",
        domain="securities.equity.us",
        ak_functions=["stock_financial_us_report_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("symbol")},
    )
)

# ---------- Fundamentals composite score (computed) ----------

def _compute_fundamentals_score(params: Dict[str, Any]) -> pd.DataFrame:
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd
    import numpy as _np

    symbol = params.get("symbol")
    if not symbol:
        return _pd.DataFrame([])
    # fetch indicators
    ind = _pd.DataFrame(_fetch("securities.equity.cn.fundamentals.indicators", {"symbol": symbol}).data)
    if ind.empty:
        return _pd.DataFrame([])
    # try to extract key metrics by fuzzy column names
    def get_col(df, keys):
        for k in keys:
            if k in df.columns:
                return _pd.to_numeric(df[k], errors="coerce")
        return _pd.Series([], dtype=float)

    # valuation (lower better): PE/PB
    pe = get_col(ind, ["市盈率TTM", "市盈率", "pe_ttm", "PE"])
    pb = get_col(ind, ["市净率", "PB"]) 
    # growth: revenue yoy, net profit yoy
    growth_rev = get_col(ind, ["营业收入同比增长率", "营收同比", "收入增长率"]) 
    growth_np = get_col(ind, ["净利润同比增长率", "净利同比", "净利润增长率"]) 
    # profitability: ROE/ROA/margin
    roe = get_col(ind, ["ROE", "净资产收益率"]) 
    margin = get_col(ind, ["净利率", "毛利率"]) 
    # leverage/health: 资产负债率、流动比率
    debt = get_col(ind, ["资产负债率", "负债率"]) 
    current = get_col(ind, ["流动比率"]) 
    # dividend
    dy = get_col(ind, ["股息率", "股息率TTM"]) 

    # reduce to last row
    def last_val(s: _pd.Series):
        return float(s.dropna().iloc[-1]) if not s.dropna().empty else _np.nan

    pe_v = last_val(pe)
    pb_v = last_val(pb)
    growth_v = _np.nanmean([last_val(growth_rev), last_val(growth_np)])
    prof_v = _np.nanmean([last_val(roe), last_val(margin)])
    health_v = _np.nanmean([ (100.0 - last_val(debt)) if not _np.isnan(last_val(debt)) else _np.nan, last_val(current)])
    div_v = last_val(dy)

    # simple normalization into 0-100; lower PE/PB => higher score
    def score_inv(x, cap=60.0):
        if _np.isnan(x):
            return _np.nan
        return max(0.0, min(100.0, (cap / max(1e-6, x)) * 100.0 / cap))

    def score_pos(x, cap=30.0):
        if _np.isnan(x):
            return _np.nan
        return max(0.0, min(100.0, (x / cap) * 100.0))

    score_valuation = _np.nanmean([score_inv(pe_v), score_inv(pb_v)])
    score_growth = score_pos(growth_v, 40.0)
    score_profitability = score_pos(prof_v, 30.0)
    score_health = score_pos(health_v, 200.0)
    score_dividend = score_pos(div_v, 8.0)

    overall = _np.nanmean([score_valuation, score_growth, score_profitability, score_health, score_dividend])

    return _pd.DataFrame([
        {
            "symbol": symbol,
            "score_overall": float(overall) if not _np.isnan(overall) else None,
            "score_valuation": float(score_valuation) if not _np.isnan(score_valuation) else None,
            "score_growth": float(score_growth) if not _np.isnan(score_growth) else None,
            "score_profitability": float(score_profitability) if not _np.isnan(score_profitability) else None,
            "score_health": float(score_health) if not _np.isnan(score_health) else None,
            "score_dividend": float(score_dividend) if not _np.isnan(score_dividend) else None,
            "note": "Heuristic scoring; TODO: calibrate caps and add analyst expectations/ESG/competitive advantage",
        }
    ])


register(
    DatasetSpec(
        dataset_id="securities.equity.cn.fundamentals.score",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_fundamentals_score,
    )
)

# ---------- Technical indicators (computed) ----------

def _ema(series: "pd.Series", span: int) -> "pd.Series":
    return series.ewm(span=span, adjust=False).mean()


def _rsi(close: "pd.Series", period: int = 14) -> "pd.Series":
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi


def _atr(high: "pd.Series", low: "pd.Series", close: "pd.Series", period: int = 14) -> "pd.Series":
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


def _obv(close: "pd.Series", volume: "pd.Series") -> "pd.Series":
    direction = close.diff().fillna(0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    return (direction * volume.fillna(0)).cumsum()


def _compute_tech_indicators(params: Dict[str, Any]) -> pd.DataFrame:
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd
    import numpy as _np

    symbol = params.get("symbol")
    if not symbol:
        return _pd.DataFrame([])
    start = params.get("start")
    end = params.get("end")
    adjust = params.get("adjust") or "none"

    sma_short = int(params.get("sma_short", 20))
    sma_mid = int(params.get("sma_mid", 50))
    sma_long = int(params.get("sma_long", 200))
    ema_fast = int(params.get("ema_fast", 12))
    ema_slow = int(params.get("ema_slow", 26))
    rsi_period = int(params.get("rsi_period", 14))
    bb_window = int(params.get("bb_window", 20))
    bb_k = float(params.get("bb_k", 2.0))
    atr_period = int(params.get("atr_period", 14))
    sr_lookback = int(params.get("sr_lookback", 20))

    env = _fetch(
        "securities.equity.cn.ohlcv_daily",
        {"symbol": symbol, "start": start, "end": end, "adjust": adjust},
        ak_function="stock_zh_a_hist",
    )
    df = _pd.DataFrame(env.data)
    if df.empty:
        return df
    df = df.sort_values("date").reset_index(drop=True)
    close = _pd.to_numeric(df["close"], errors="coerce")
    high = _pd.to_numeric(df["high"], errors="coerce")
    low = _pd.to_numeric(df["low"], errors="coerce")
    volume = _pd.to_numeric(df.get("volume"), errors="coerce")

    # MAs
    df[f"sma_{sma_short}"] = close.rolling(sma_short, min_periods=max(5, sma_short//4)).mean()
    df[f"sma_{sma_mid}"] = close.rolling(sma_mid, min_periods=max(10, sma_mid//5)).mean()
    df[f"sma_{sma_long}"] = close.rolling(sma_long, min_periods=max(20, sma_long//10)).mean()
    df[f"ema_{ema_fast}"] = _ema(close, ema_fast)
    df[f"ema_{ema_slow}"] = _ema(close, ema_slow)

    # MACD
    macd = df[f"ema_{ema_fast}"] - df[f"ema_{ema_slow}"]
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal
    df["macd"] = macd
    df["macd_signal"] = signal
    df["macd_hist"] = hist

    # RSI, Bollinger, ATR, OBV
    df[f"rsi_{rsi_period}"] = _rsi(close, rsi_period)
    ma_b = close.rolling(bb_window, min_periods=max(5, bb_window//4)).mean()
    std_b = close.rolling(bb_window, min_periods=max(5, bb_window//4)).std()
    df["bb_upper"] = ma_b + bb_k * std_b
    df["bb_lower"] = ma_b - bb_k * std_b
    df[f"atr_{atr_period}"] = _atr(high, low, close, atr_period)
    df["obv"] = _obv(close, volume)

    # Support/Resistance over lookback
    df["sr_support"] = low.rolling(sr_lookback, min_periods=max(3, sr_lookback//3)).min()
    df["sr_resistance"] = high.rolling(sr_lookback, min_periods=max(3, sr_lookback//3)).max()

    # Pivot (last bar)
    last_h = high.iloc[-1]
    last_l = low.iloc[-1]
    last_c = close.iloc[-1]
    pp = (last_h + last_l + last_c) / 3.0 if _pd.notna(last_h) and _pd.notna(last_l) and _pd.notna(last_c) else _np.nan
    df["pivot_point"] = pp

    # Signals (last bar)
    df["golden_cross"] = (df[f"ema_{ema_fast}"] > df[f"ema_{ema_slow}"]).astype(int)
    df["price_breakout"] = (close > df["bb_upper"]).astype(int)
    df["price_breakdown"] = (close < df["bb_lower"]).astype(int)
    df["rsi_overbought"] = (df[f"rsi_{rsi_period}"] >= 70).astype(int)
    df["rsi_oversold"] = (df[f"rsi_{rsi_period}"] <= 30).astype(int)
    df["uptrend"] = ((df.get(f"sma_{sma_mid}") > df.get(f"sma_{sma_long}")) & (close > df.get(f"sma_{sma_mid}")).fillna(False)).astype(int)

    return df


register(
    DatasetSpec(
        dataset_id="securities.equity.cn.tech.indicators",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_tech_indicators,
    )
)

# HK add-ons
register(
    DatasetSpec(
        dataset_id="securities.equity.hk.profile",
        category="securities",
        domain="securities.equity.hk",
        ak_functions=["stock_hk_company_profile_em", "stock_hk_security_profile_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("symbol")},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.hk.analyst_forecast",
        category="securities",
        domain="securities.equity.hk",
        ak_functions=["stock_hk_profit_forecast_et"],
        source="eastmoney",
        param_transform=lambda p: {"symbol": p.get("symbol")},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.hk.indicators",
        category="securities",
        domain="securities.equity.hk",
        ak_functions=["stock_hk_indicator_eniu"],
        source="eniu",
        param_transform=lambda p: {"symbol": p.get("symbol")},
    )
)

# Fundamentals snapshot (computed aggregator)

def _compute_fundamentals_snapshot(params: Dict[str, Any]) -> pd.DataFrame:
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd

    symbol = params.get("symbol")
    if not symbol:
        return _pd.DataFrame([])
    # Quote
    quote = _fetch("securities.equity.cn.quote", {})
    qdf = _pd.DataFrame(quote.data)
    qrow = {}
    if not qdf.empty:
        qf = qdf[qdf["symbol"] == symbol]
        if not qf.empty:
            qrow = qf.iloc[0].to_dict()
    # Indicators
    try:
        ind = _pd.DataFrame(_fetch("securities.equity.cn.fundamentals.indicators", {"symbol": symbol}, ak_function="stock_financial_analysis_indicator_em").data)
    except Exception:
        ind = _pd.DataFrame()
    # Score
    try:
        score = _pd.DataFrame(_fetch("securities.equity.cn.fundamentals.score", {"symbol": symbol}).data)
    except Exception:
        score = _pd.DataFrame()

    out = {"symbol": symbol}
    out.update({k: v for k, v in qrow.items() if k in ("last", "pe_ttm", "pb", "market_cap", "float_market_cap")})
    if not ind.empty:
        for k in ["ROE", "净利润同比增长率", "营业收入同比增长率", "资产负债率", "股息率"]:
            if k in ind.columns:
                out[k] = ind[k].dropna().iloc[-1]
    if not score.empty:
        out.update(score.iloc[0].to_dict())
    return _pd.DataFrame([out])


register(
    DatasetSpec(
        dataset_id="securities.equity.cn.fundamentals.snapshot",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_fundamentals_snapshot,
    )
)

# HK/US fundamentals indicators
register(
    DatasetSpec(
        dataset_id="securities.equity.hk.fundamentals.indicators",
        category="securities",
        domain="securities.equity.hk",
        ak_functions=["stock_financial_hk_analysis_indicator_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("symbol")},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.us.fundamentals.indicators",
        category="securities",
        domain="securities.equity.us",
        ak_functions=["stock_financial_us_analysis_indicator_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("symbol")},
    )
)

# Technical signals snapshot (computed)

def _compute_tech_signals(params: Dict[str, Any]) -> pd.DataFrame:
    import pandas as _pd
    from .dispatcher import fetch_data as _fetch

    symbol = params.get("symbol")
    if not symbol:
        return _pd.DataFrame([])
    tech = _pd.DataFrame(_fetch("securities.equity.cn.tech.indicators", params).data)
    if tech.empty:
        return tech
    last = tech.iloc[-1]
    keys = [
        "golden_cross", "price_breakout", "price_breakdown", "rsi_overbought", "rsi_oversold", "uptrend",
    ]
    out = {"symbol": symbol}
    out.update({k: int(last.get(k, 0)) for k in keys})
    out["support"] = float(last.get("sr_support")) if _pd.notna(last.get("sr_support")) else None
    out["resistance"] = float(last.get("sr_resistance")) if _pd.notna(last.get("sr_resistance")) else None
    out["pivot_point"] = float(last.get("pivot_point")) if _pd.notna(last.get("pivot_point")) else None
    return _pd.DataFrame([out])


register(
    DatasetSpec(
        dataset_id="securities.equity.cn.tech.signals",
        category="securities",
        domain="securities.equity.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_tech_signals,
    )
)

# HK/US fundamentals snapshot (computed)

def _compute_fundamentals_snapshot_hk_us(params: Dict[str, Any], market: str) -> pd.DataFrame:
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd

    symbol = params.get("symbol")
    if not symbol:
        return _pd.DataFrame([])
    # Quote source for HK/US not registered here; rely on user fetching indicators and assembling snapshot
    # Indicators
    try:
        ds = "securities.equity.hk.fundamentals.indicators" if market == "hk" else "securities.equity.us.fundamentals.indicators"
        ind = _pd.DataFrame(_fetch(ds, {"symbol": symbol}).data)
    except Exception:
        ind = _pd.DataFrame()
    out = {"symbol": symbol}
    # heuristic picks
    for k in ["PE", "PB", "ROE", "净利润同比增长率", "营业收入同比增长率", "股息率"]:
        if k in ind.columns:
            out[k] = ind[k].dropna().iloc[-1]
    return _pd.DataFrame([out])


register(
    DatasetSpec(
        dataset_id="securities.equity.hk.fundamentals.snapshot",
        category="securities",
        domain="securities.equity.hk",
        ak_functions=[],
        source="computed",
        compute=lambda p: _compute_fundamentals_snapshot_hk_us(p, "hk"),
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.us.fundamentals.snapshot",
        category="securities",
        domain="securities.equity.us",
        ak_functions=[],
        source="computed",
        compute=lambda p: _compute_fundamentals_snapshot_hk_us(p, "us"),
    )
)

# HK/US spot quotes
register(
    DatasetSpec(
        dataset_id="securities.equity.hk.quote",
        category="securities",
        domain="securities.equity.hk",
        ak_functions=["stock_hk_spot_em", "stock_hk_spot"],
        source="em",
        param_transform=_noop_params,
        field_mapping={"代码": "symbol", "名称": "symbol_name", "最新价": "last", "涨跌幅": "pct_change", "成交量": "volume", "成交额": "amount"},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.us.quote",
        category="securities",
        domain="securities.equity.us",
        ak_functions=["stock_us_spot_em", "stock_us_spot"],
        source="em",
        param_transform=_noop_params,
        field_mapping={"代码": "symbol", "名称": "symbol_name", "最新价": "last", "涨跌幅": "pct_change", "成交量": "volume", "成交额": "amount"},
    )
)

# Cross-market fundamentals snapshot

def _compute_fundamentals_snapshot_cross(params: Dict[str, Any]) -> pd.DataFrame:
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd

    symbol = params.get("symbol")
    market = (params.get("market") or "CN").upper()
    if not symbol:
        return _pd.DataFrame([])

    out = {"symbol": symbol, "market": market}

    # Quote selection
    try:
        if market == "CN":
            q = _pd.DataFrame(_fetch("securities.equity.cn.quote", {}).data)
            if not q.empty:
                hit = q[q["symbol"] == symbol]
                if not hit.empty:
                    out.update(hit.iloc[0].to_dict())
        elif market == "HK":
            q = _pd.DataFrame(_fetch("securities.equity.hk.quote", {}).data)
            # users may pass numeric code; filter if present
            if not q.empty and "symbol" in q.columns:
                hit = q[q["symbol"].astype(str).str.contains(str(symbol))]
                if not hit.empty:
                    out.update(hit.iloc[0].to_dict())
        else:  # US
            q = _pd.DataFrame(_fetch("securities.equity.us.quote", {}).data)
            if not q.empty and "symbol" in q.columns:
                hit = q[q["symbol"].astype(str).str.upper() == str(symbol).upper()]
                if not hit.empty:
                    out.update(hit.iloc[0].to_dict())
    except Exception:
        pass

    # Indicators
    try:
        if market == "CN":
            ind = _pd.DataFrame(_fetch("securities.equity.cn.fundamentals.indicators", {"symbol": symbol}, ak_function="stock_financial_analysis_indicator_em").data)
        elif market == "HK":
            ind = _pd.DataFrame(_fetch("securities.equity.hk.fundamentals.indicators", {"symbol": symbol}).data)
        else:
            ind = _pd.DataFrame(_fetch("securities.equity.us.fundamentals.indicators", {"symbol": symbol}).data)
        if not ind.empty:
            for k in ["ROE", "净利润同比增长率", "营业收入同比增长率", "股息率", "PE", "PB"]:
                if k in ind.columns:
                    out[k] = ind[k].dropna().iloc[-1]
    except Exception:
        pass

    # Score (CN only)
    try:
        if market == "CN":
            s = _pd.DataFrame(_fetch("securities.equity.cn.fundamentals.score", {"symbol": symbol}).data)
            if not s.empty:
                out.update(s.iloc[0].to_dict())
    except Exception:
        pass

    return _pd.DataFrame([out])


register(
    DatasetSpec(
        dataset_id="securities.equity.cross_market.snapshot",
        category="securities",
        domain="securities.equity",
        ak_functions=[],
        source="computed",
        compute=_compute_fundamentals_snapshot_cross,
    )
)

# HK/US OHLCV daily
register(
    DatasetSpec(
        dataset_id="securities.equity.hk.ohlcv_daily",
        category="securities",
        domain="securities.equity.hk",
        ak_functions=["stock_hk_hist"],
        source="em",
        param_transform=lambda p: {
            "symbol": p.get("symbol"),
            "period": "daily",
            "start_date": (p.get("start") or "19700101").replace("-", ""),
            "end_date": (p.get("end") or "22220101").replace("-", ""),
            "adjust": {"none": "", "qfq": "qfq", "hfq": "hfq"}.get(p.get("adjust") or "none", ""),
        },
        field_mapping=FIELD_OHLCV_CN,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.us.ohlcv_daily",
        category="securities",
        domain="securities.equity.us",
        ak_functions=["stock_us_hist"],
        source="em",
        param_transform=lambda p: {
            "symbol": p.get("symbol"),
            "period": "daily",
            "start_date": (p.get("start") or "19700101").replace("-", ""),
            "end_date": (p.get("end") or "22220101").replace("-", ""),
            "adjust": {"none": "", "qfq": "qfq", "hfq": "hfq"}.get(p.get("adjust") or "none", ""),
        },
        field_mapping=FIELD_OHLCV_CN,
    )
)

# ESG unified (computed)

def _compute_esg_unified(params: Dict[str, Any]) -> pd.DataFrame:
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd

    symbol = params.get("symbol")
    # Load raw tables from multiple sources
    tables = []
    for ds in [
        "market.esg.cn.sina_rate",
        "market.esg.cn.msci",
        "market.esg.cn.hz",
        "market.esg.cn.rft",
        "market.esg.cn.zd",
    ]:
        try:
            env = _fetch(ds, {})
            df = _pd.DataFrame(env.data)
            if not df.empty:
                df["source_ds"] = ds
                tables.append(df)
        except Exception:
            continue
    if not tables:
        return _pd.DataFrame([])
    df_all = _pd.concat(tables, ignore_index=True, sort=False)
    # normalize keys
    # heuristic: try to find a code or symbol column
    code_col = None
    for c in ["代码", "symbol", "股票代码", "证券代码"]:
        if c in df_all.columns:
            code_col = c
            break
    name_col = None
    for c in ["名称", "symbol_name", "股票名称", "证券简称"]:
        if c in df_all.columns:
            name_col = c
            break
    score_cols = [c for c in df_all.columns if any(k in str(c) for k in ["ESG", "评分", "score", "评级"])]
    m = df_all[[x for x in [code_col, name_col] if x] + score_cols + ["source_ds"]].copy()
    if symbol and code_col:
        m = m[m[code_col].astype(str).str.contains(str(symbol))]
    # melt to unified rows
    mm = m.melt(id_vars=[x for x in [code_col, name_col, "source_ds"] if x], var_name="metric", value_name="value")
    mm = mm.dropna(subset=["value"]).copy()
    # map to unified keys when possible
    mm.rename(columns={code_col or "symbol": "symbol", name_col or "symbol_name": "symbol_name"}, inplace=True)
    return mm


register(
    DatasetSpec(
        dataset_id="securities.esg.cn.unified",
        category="securities",
        domain="securities.esg.cn",
        ak_functions=[],
        source="computed",
        compute=_compute_esg_unified,
    )
)

# Cross-market tech indicators (computed wrapper)

def _compute_tech_indicators_cross(params: Dict[str, Any]) -> pd.DataFrame:
    from .dispatcher import fetch_data as _fetch
    import pandas as _pd

    market = (params.get("market") or "CN").upper()
    symbol = params.get("symbol")
    if not symbol:
        return _pd.DataFrame([])
    if market == "CN":
        return _pd.DataFrame(_fetch("securities.equity.cn.tech.indicators", params).data)
    # For HK/US reuse OHLCV and compute a subset (SMA/EMA/RSI)
    ohlcv_ds = "securities.equity.hk.ohlcv_daily" if market == "HK" else "securities.equity.us.ohlcv_daily"
    env = _fetch(ohlcv_ds, {"symbol": symbol, "start": params.get("start"), "end": params.get("end")})
    df = _pd.DataFrame(env.data)
    if df.empty:
        return df
    df = df.sort_values("date").reset_index(drop=True)
    close = _pd.to_numeric(df["close"], errors="coerce")
    df["sma_20"] = close.rolling(20, min_periods=5).mean()
    df["ema_12"] = close.ewm(span=12, adjust=False).mean()
    df["ema_26"] = close.ewm(span=26, adjust=False).mean()
    # simple RSI
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    df["rsi_14"] = 100 - (100 / (1 + (gain.rolling(14, min_periods=14).mean() / loss.rolling(14, min_periods=14).mean().replace(0, _pd.NA))))
    return df


register(
    DatasetSpec(
        dataset_id="securities.equity.cross_market.tech.indicators",
        category="securities",
        domain="securities.equity",
        ak_functions=[],
        source="computed",
        compute=_compute_tech_indicators_cross,
    )
)

# NOTE: ACCOUNT_KEY_MAP could be externalized via a YAML/JSON config loaded at runtime for extensibility.

# HK/US financial statements (raw; TODO unify mapping when sources stabilize)
register(
    DatasetSpec(
        dataset_id="securities.equity.hk.financials.is",
        category="securities",
        domain="securities.equity.hk",
        ak_functions=["stock_financial_hk_report_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("symbol")},
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.us.financials.is",
        category="securities",
        domain="securities.equity.us",
        ak_functions=["stock_financial_us_report_em"],
        source="em",
        param_transform=lambda p: {"symbol": p.get("symbol")},
    )
)

# Dividends for HK/US: TODO - AkShare lacks unified dividend endpoints for HK/US; expose placeholders

def _compute_dividends_placeholder(params: Dict[str, Any]) -> pd.DataFrame:
    import pandas as _pd
    return _pd.DataFrame([{"note": "TODO: HK/US dividend endpoints not unified in AkShare; integrate external if needed"}])

register(
    DatasetSpec(
        dataset_id="securities.equity.hk.dividends",
        category="securities",
        domain="securities.equity.hk",
        ak_functions=[],
        source="computed",
        compute=_compute_dividends_placeholder,
    )
)

register(
    DatasetSpec(
        dataset_id="securities.equity.us.dividends",
        category="securities",
        domain="securities.equity.us",
        ak_functions=[],
        source="computed",
        compute=_compute_dividends_placeholder,
    )
)

# Governance datasets (CN already added): stock_hold_management_person_em etc.
# TODO: Add HK/US governance if AkShare exposes endpoints in future.