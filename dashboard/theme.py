"""RainWatch India — shared visual theme (dark navy + blue) for Streamlit/Plotly."""

from __future__ import annotations

import streamlit as st
import plotly.graph_objects as go
import plotly.io as pio

# ---------------------------------------------------------------- palette
BG = "#0A1428"          # app background (deep navy)
PANEL = "#101E38"       # card background
PANEL_2 = "#152744"     # elevated card
BORDER = "#20355B"
TEXT = "#E8EFFA"
MUTED = "#9FB0CC"
ACCENT = "#4DA3FF"      # primary blue
ACCENT_2 = "#7DD3FC"    # light cyan
GOOD = "#34D399"
WARN = "#FBBF24"
BAD = "#F87171"

SEQ = [ "#082344", "#0C3B6E", "#12599C", "#2E7FD4", "#4DA3FF", "#7DD3FC", "#BAE6FD" ]

MONTH_NAMES = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

CSS = f"""
<style>
    .stApp {{
        background: linear-gradient(180deg, {BG} 0%, #0C1830 60%, {BG} 100%);
        color: {TEXT};
    }}
    section[data-testid="stSidebar"] {{
        background: {PANEL};
        border-right: 1px solid {BORDER};
    }}
    section[data-testid="stSidebar"] * {{ color: {TEXT}; }}
    h1, h2, h3, h4 {{ color: {TEXT}; letter-spacing: .2px; }}
    .block-container {{ padding-top: 1.4rem; padding-bottom: 2.2rem; }}

    .rw-header {{
        background: linear-gradient(90deg, {PANEL_2} 0%, {PANEL} 70%);
        border: 1px solid {BORDER};
        border-radius: 14px;
        padding: 18px 22px;
        margin-bottom: 6px;
    }}
    .rw-header h1 {{ font-size: 1.6rem; margin: 0 0 4px 0; }}
    .rw-header p {{ color: {MUTED}; margin: 0; font-size: .95rem; }}

    .rw-kpi {{
        background: {PANEL};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 14px 16px;
        height: 100%;
    }}
    .rw-kpi .kpi-label {{ color: {MUTED}; font-size: .78rem; text-transform: uppercase; letter-spacing: .6px; }}
    .rw-kpi .kpi-value {{ color: {TEXT}; font-size: 1.45rem; font-weight: 700; margin-top: 2px; }}
    .rw-kpi .kpi-sub {{ color: {MUTED}; font-size: .78rem; margin-top: 2px; }}

    .rw-note {{
        background: rgba(77,163,255,.08);
        border: 1px solid rgba(77,163,255,.25);
        border-radius: 10px; padding: 10px 14px; color: {MUTED}; font-size: .88rem;
    }}
    .rw-disclaimer {{
        background: rgba(251,191,36,.07);
        border: 1px solid rgba(251,191,36,.35);
        border-radius: 10px; padding: 10px 14px; color: {WARN}; font-size: .84rem;
    }}
    div[data-testid="stMetric"] {{
        background: {PANEL}; border: 1px solid {BORDER};
        border-radius: 12px; padding: 12px 16px;
    }}
    div[data-testid="stMetric"] label {{ color: {MUTED} !important; }}
    div[data-testid="stMetric"] div {{ color: {TEXT}; }}
    .stTabs [data-baseweb="tab-list"] {{ gap: 6px; }}
    .stTabs [data-baseweb="tab"] {{
        background: {PANEL}; border-radius: 8px 8px 0 0; color: {MUTED};
    }}
    .stTabs [aria-selected="true"] {{ color: {ACCENT} !important; }}
    div[data-testid="stDataFrame"] {{ border: 1px solid {BORDER}; border-radius: 10px; }}
    .stRadio label, .stCheckbox label {{ color: {TEXT}; }}
</style>
"""


def apply_theme() -> None:
    st.set_page_config(
        page_title="RainWatch India",
        page_icon="🌧",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(CSS, unsafe_allow_html=True)


def header(title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="rw-header"><h1>{title}</h1><p>{subtitle}</p></div>',
        unsafe_allow_html=True,
    )


def kpi_row(items: list[tuple[str, str, str]]) -> None:
    """Render a row of KPI cards: (label, value, sub)."""
    cols = st.columns(len(items))
    for col, (label, value, sub) in zip(cols, items):
        with col:
            st.markdown(
                f'<div class="rw-kpi"><div class="kpi-label">{label}</div>'
                f'<div class="kpi-value">{value}</div>'
                f'<div class="kpi-sub">{sub}</div></div>',
                unsafe_allow_html=True,
            )


def note(msg: str) -> None:
    st.markdown(f'<div class="rw-note">{msg}</div>', unsafe_allow_html=True)


def disclaimer(msg: str) -> None:
    st.markdown(f'<div class="rw-disclaimer">{msg}</div>', unsafe_allow_html=True)


def _register_template() -> None:
    layout = go.Layout(
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT, family="Inter, 'Segoe UI', system-ui, sans-serif"),
        title=dict(font=dict(size=16, color=TEXT)),
        margin=dict(l=10, r=18, t=42, b=10),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        coloraxis=dict(colorscale=SEQ, colorbar=dict(outlinecolor=BORDER, outlinewidth=1)),
        xaxis=dict(gridcolor="#1C2E4F", zerolinecolor="#1C2E4F", linecolor=BORDER),
        yaxis=dict(gridcolor="#1C2E4F", zerolinecolor="#1C2E4F", linecolor=BORDER),
    )
    pio.templates["rainwatch"] = pio.templates["plotly_dark"]
    pio.templates["rainwatch"].layout = layout
    pio.templates.default = "rainwatch"


def figure(fig: go.Figure, height: int | None = None) -> None:
    _register_template()
    fig.layout.paper_bgcolor = PANEL
    fig.layout.plot_bgcolor = PANEL
    if height:
        fig.update_layout(height=height)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
