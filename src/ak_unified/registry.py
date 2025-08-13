from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


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
import numpy as np  # type: ignore


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

# -------- Computed datasets (heuristics; TODO: refine models) --------

def _compute_economic_cycle_phase(params: Dict[str, Any]) -> pd.DataFrame:
    # Inputs: GDP YoY (macro.cn.gdp), CPI YoY (macro.cn.cpi TODO), Policy rates (LPR/shibor), Social financing
    # TODO: add CPI mapping; using available LPR + social financing as proxy for liquidity, GDP yoy for growth
    from .dispatcher import fetch_data as _fetch

    growth = _fetch("macro.cn.gdp", {}, ak_function="macro_china_gdp").data  # expects MacroIndicator gdp_yoy
    lpr = _fetch("macro.cn.lpr", {}).data
    social = _fetch("macro.cn.social_financing", {}, ak_function="macro_china_new_financial_credit").data

    # Heuristic: latest YoY growth and liquidity change
    def latest_number(lst, key):
        for item in lst[:50]:
            v = item.get(key)
            if isinstance(v, (int, float)):
                return float(v)
        return np.nan

    growth_yoy = latest_number(growth, "value") if growth else np.nan
    lpr_level = np.nan
    if lpr:
        lpr_level = latest_number(lpr, list(lpr[0].keys())[1])
    social_level = np.nan
    if social:
        social_level = latest_number(social, list(social[0].keys())[1])

    # Simple rules (placeholder):
    # boom if growth high and liquidity ample; slowdown if growth falling; recession if negative growth proxy
    phase = "unknown"
    if not np.isnan(growth_yoy) and growth_yoy >= 6:
        phase = "expansion"
    elif not np.isnan(growth_yoy) and 0 < growth_yoy < 6:
        phase = "slowdown"
    elif not np.isnan(growth_yoy) and growth_yoy <= 0:
        phase = "recession"

    return pd.DataFrame([
        {
            "phase": phase,
            "growth_yoy": growth_yoy,
            "lpr_level_proxy": lpr_level,
            "social_financing_proxy": social_level,
            "note": "TODO: incorporate CPI/PPI/PMI and rate of change with filters",
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