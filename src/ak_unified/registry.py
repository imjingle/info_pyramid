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