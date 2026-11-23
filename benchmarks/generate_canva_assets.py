"""Generate matching Blue-White minimalist visual PNGs for Canva deck DAHbuYdPunA."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

from generate_charts import load_real_results


# Palette matching Blue White Geometric Minimalist Thesis Defense Presentation
BG_WHITE = "#FFFFFF"
CARD_BG = "#F8FAFC"
CARD_SOFT = "#EFF6FF"
NAVY = "#1E3A8A"
ROYAL = "#2563EB"
SKY = "#0284C7"
SLATE_DARK = "#0F172A"
SLATE_MUTED = "#334155"
BORDER_BLUE = "#BFDBFE"
AMBER = "#D97706"
CORAL = "#DC2626"
EMERALD = "#059669"

plt.rcParams["font.sans-serif"] = ["DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def _draw_card(
    ax: Any,
    x: float,
    y: float,
    w: float,
    h: float,
    title: str,
    body: str,
    edge: str = ROYAL,
    fill: str = CARD_BG,
    title_color: str = NAVY,
    title_size: float = 18.0,
    body_size: float = 16.0,
    title_y_ratio: float = 0.72,
    body_y_ratio: float = 0.34,
    linespacing: float = 1.4,
) -> None:
    box = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle="round,pad=0.35,rounding_size=1.4",
        facecolor=fill,
        edgecolor=edge,
        linewidth=2.6,
    )
    ax.add_patch(box)
    ax.text(
        x + w / 2,
        y + h * title_y_ratio,
        title,
        ha="center",
        va="center",
        color=title_color,
        fontsize=title_size,
        fontweight="bold",
    )
    ax.text(
        x + w / 2,
        y + h * body_y_ratio,
        body,
        ha="center",
        va="center",
        color=SLATE_MUTED,
        fontsize=body_size,
        linespacing=linespacing,
    )


def render_arch_3zone_light(out_path: Path) -> None:
    """Render 3-Zone Cloud Architecture diagram matching Slide 10 slot (1740x745 aspect)."""
    fig = plt.figure(figsize=(17.4, 7.45), dpi=120)
    fig.patch.set_facecolor(BG_WHITE)
    ax = fig.add_axes([0.0, 0.0, 1.0, 1.0])
    ax.set_facecolor(BG_WHITE)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    # Zone 1: Edge
    _draw_card(
        ax,
        2.5,
        74.5,
        95.0,
        22.5,
        "VÙNG 1 · CLIENT EDGE (LAPTOP)",
        "producer/replay_producer.py  ·  Đọc 2025-C (177.319 dòng)  ·  Partition key = station_id\nGửi JSON qua SSH Tunnel mã hóa (127.0.0.1:19092 → EC2 Port 22)",
        edge=SKY,
        fill=CARD_SOFT,
        title_size=19.5,
        body_size=16.5,
        title_y_ratio=0.73,
        body_y_ratio=0.32,
    )

    # Arrow Zone 1 -> Zone 2
    ax.annotate(
        "",
        xy=(50.0, 65.2),
        xytext=(50.0, 74.0),
        arrowprops=dict(arrowstyle="-|>", color=ROYAL, lw=3.2, mutation_scale=22),
    )
    ax.text(
        73.5,
        69.8,
        "SSH Tunnel (Port 22)  |  Khóa 9092 & 19092 khỏi Internet",
        ha="center",
        va="center",
        color=ROYAL,
        fontsize=15.5,
        fontweight="bold",
    )

    # Zone 2: AWS EC2 Compute
    zone2_box = FancyBboxPatch(
        (2.5, 29.0),
        95.0,
        35.5,
        boxstyle="round,pad=0.35,rounding_size=1.4",
        facecolor=CARD_BG,
        edgecolor=NAVY,
        linewidth=2.8,
    )
    ax.add_patch(zone2_box)
    ax.text(
        50.0,
        59.8,
        "VÙNG 2 · AWS EC2 c7i-flex.large (ap-southeast-2 · Docker Compose · SG mở 22 & 3000)",
        ha="center",
        va="center",
        color=NAVY,
        fontsize=18.0,
        fontweight="bold",
    )

    _draw_card(
        ax,
        5.0,
        31.8,
        27.5,
        23.0,
        "wq-kafka (KRaft 7.7)",
        "Topic 3 Partitions\nBind 127.0.0.1:19092",
        edge=ROYAL,
        fill=BG_WHITE,
        title_size=17.5,
        body_size=15.5,
        title_y_ratio=0.69,
        body_y_ratio=0.32,
    )
    _draw_card(
        ax,
        36.25,
        31.8,
        27.5,
        23.0,
        "wq-consumer",
        "Validate · Lọc nhiễu · Alert\nBatch UNNEST (100/lô)",
        edge=ROYAL,
        fill=BG_WHITE,
        title_size=17.5,
        body_size=15.5,
        title_y_ratio=0.69,
        body_y_ratio=0.32,
    )
    _draw_card(
        ax,
        67.5,
        31.8,
        27.5,
        23.0,
        "wq-grafana 11.3",
        "Tự host Docker · Port 3000\nTruy vấn trực tiếp (5s live)",
        edge=SKY,
        fill=BG_WHITE,
        title_size=17.5,
        body_size=15.5,
        title_y_ratio=0.69,
        body_y_ratio=0.32,
    )
    ax.annotate(
        "",
        xy=(35.8, 43.3),
        xytext=(33.0, 43.3),
        arrowprops=dict(arrowstyle="-|>", color=NAVY, lw=3.0, mutation_scale=18),
    )

    # Arrows Zone 2 <-> Zone 3
    ax.annotate(
        "",
        xy=(44.0, 20.4),
        xytext=(44.0, 28.6),
        arrowprops=dict(arrowstyle="-|>", color=EMERALD, lw=3.2, mutation_scale=20),
    )
    ax.annotate(
        "",
        xy=(72.0, 28.6),
        xytext=(72.0, 20.4),
        arrowprops=dict(arrowstyle="-|>", color=SKY, lw=3.2, mutation_scale=20),
    )
    ax.text(
        24.5,
        24.6,
        "TLS 5432 · UNNEST + ON CONFLICT",
        ha="center",
        va="center",
        color=EMERALD,
        fontsize=15.5,
        fontweight="bold",
    )
    ax.text(
        86.0,
        24.6,
        "SQL Query qua TLS 5432",
        ha="center",
        va="center",
        color=SKY,
        fontsize=15.5,
        fontweight="bold",
    )

    # Zone 3: Neon Managed DB
    _draw_card(
        ax,
        6.0,
        2.5,
        88.0,
        17.5,
        "VÙNG 3 · NEON SERVERLESS POSTGRESQL 16 (ap-southeast-1 · Managed DB)",
        "PgBouncer Pooled  ·  sslmode=require  ·  PK (run_id, event_id)  ·  Auto-suspend 5 phút ($0/tháng)",
        edge=EMERALD,
        fill="#ECFDF5",
        title_color=EMERALD,
        title_size=18.5,
        body_size=16.0,
        title_y_ratio=0.69,
        body_y_ratio=0.30,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=BG_WHITE)
    plt.close(fig)


def render_pipeline_5stage_light(out_path: Path) -> None:
    """Render 5-stage Data Pipeline banner matching Slide 11 top slot (1740x425 aspect)."""
    fig = plt.figure(figsize=(17.4, 4.25), dpi=120)
    fig.patch.set_facecolor(BG_WHITE)
    ax = fig.add_axes([0.0, 0.0, 1.0, 1.0])
    ax.set_facecolor(BG_WHITE)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    stages = [
        ("1. CSV Replay", "Đọc CSV 2025\nGắn run_id, event_id\nKey = station_id", SKY, CARD_SOFT),
        ("2. Kafka KRaft", "Topic 3 partitions\nĐệm chống nghẽn\nTách Edge & Worker", ROYAL, CARD_SOFT),
        ("3. Python ETL", "Lọc NO FLOW/SAMP\nGắn WARN / CRIT\nGom batch UNNEST", NAVY, CARD_BG),
        ("4. Neon DB", "PostgreSQL 16 TLS\nPK(run_id, event_id)\nON CONFLICT bỏ trùng", EMERALD, "#ECFDF5"),
        ("5. Grafana 11.3", "Truy vấn SQL live\nKPI + Throughput\nP95 (refresh 5s)", SKY, CARD_SOFT),
    ]

    w, h, gap, x_start, y0 = 17.2, 82.0, 2.7, 1.6, 9.0
    for idx, (title, body, edge, fill) in enumerate(stages):
        x0 = x_start + idx * (w + gap)
        _draw_card(
            ax,
            x0,
            y0,
            w,
            h,
            title,
            body,
            edge=edge,
            fill=fill,
            title_color=edge,
            title_size=18.5,
            body_size=15.5,
            title_y_ratio=0.76,
            body_y_ratio=0.35,
            linespacing=1.48,
        )
        if idx < len(stages) - 1:
            ax.annotate(
                "",
                xy=(x0 + w + gap - 0.35, y0 + h / 2),
                xytext=(x0 + w + 0.35, y0 + h / 2),
                arrowprops=dict(arrowstyle="-|>", color=ROYAL, lw=3.2, mutation_scale=20),
            )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=BG_WHITE)
    plt.close(fig)


def render_recovery_light(crash: dict[str, Any], out_path: Path) -> None:
    """Render 5-step Cloud Crash Recovery diagram matching Slide 14 top slot (1740x515 aspect)."""
    fig = plt.figure(figsize=(17.4, 5.15), dpi=120)
    fig.patch.set_facecolor(BG_WHITE)
    ax = fig.add_axes([0.0, 0.0, 1.0, 1.0])
    ax.set_facecolor(BG_WHITE)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    ax.text(
        50.0,
        92.5,
        f"Run thật: {crash['run_id']}   |   Cố ý sập sau Neon DB commit, trước Kafka offset commit",
        ha="center",
        va="center",
        color=NAVY,
        fontsize=18.0,
        fontweight="bold",
    )

    steps = [
        ("1. Ghi Neon DB", "AN-2095170-7668\nvào rejected_events", ROYAL, CARD_SOFT),
        ("2. Crash (77)", "exit code 77\nchưa commit offset", CORAL, "#FEF2F2"),
        ("3. Khởi động lại", "wq-consumer\ncùng consumer group", SKY, CARD_SOFT),
        ("4. Kafka Replay", "Phát lại 100 tin\nON CONFLICT\nbỏ bản trùng", AMBER, "#FFFBEB"),
        (
            "5. Kết quả Cloud",
            f"{crash['unique_events']}/{crash['total_events']} hợp lệ\n+ 26 nhiễu\n{crash['duplicates']} trùng · lag {crash['kafka_lag']}",
            EMERALD,
            "#ECFDF5",
        ),
    ]

    w, h, gap, x_start, y0 = 17.2, 52.0, 2.7, 1.6, 33.5
    for idx, (title, body, edge, fill) in enumerate(steps):
        x0 = x_start + idx * (w + gap)
        _draw_card(
            ax,
            x0,
            y0,
            w,
            h,
            title,
            body,
            edge=edge,
            fill=fill,
            title_color=edge,
            title_size=18.0,
            body_size=15.0,
            title_y_ratio=0.76,
            body_y_ratio=0.35,
            linespacing=1.42,
        )
        if idx < len(steps) - 1:
            ax.annotate(
                "",
                xy=(x0 + w + gap - 0.35, y0 + h / 2),
                xytext=(x0 + w + 0.35, y0 + h / 2),
                arrowprops=dict(arrowstyle="-|>", color=NAVY, lw=3.2, mutation_scale=20),
            )

    _draw_card(
        ax,
        6.0,
        3.5,
        88.0,
        22.5,
        "PRIMARY KEY (run_id, event_id)  +  ON CONFLICT DO NOTHING",
        "Đạt At-Least-Once Delivery + Idempotent Write xuyên suốt Kafka (EC2) và Neon PostgreSQL (Cloud)",
        edge=ROYAL,
        fill=CARD_SOFT,
        title_color=NAVY,
        title_size=18.5,
        body_size=16.0,
        title_y_ratio=0.69,
        body_y_ratio=0.30,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=BG_WHITE)
    plt.close(fig)


def render_throughput_light(data: dict[str, Any], out_path: Path) -> None:
    """Render light-theme Burst + Single vs Batch WAN chart matching Slide 13 slot (1740x660)."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(17.4, 6.60), dpi=120)
    fig.patch.set_facecolor(BG_WHITE)
    fig.subplots_adjust(left=0.065, right=0.985, top=0.89, bottom=0.14, wspace=0.18)

    # Left: Burst
    burst = data["burst"]
    labels1 = [b["label"] for b in burst]
    achieved1 = [b["achieved"] for b in burst]
    targets1 = [b["target"] for b in burst]

    ax1.set_facecolor(CARD_BG)
    bars1 = ax1.bar(labels1, achieved1, width=0.45, color=[SKY, ROYAL, NAVY], zorder=3)
    ax1.plot(labels1, targets1, color=CORAL, marker="o", linewidth=2.8, markersize=8, label="Mục tiêu", zorder=4)
    for bar, val, tgt in zip(bars1, achieved1, targets1):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            max(val, tgt) + 6.5,
            f"{val:.2f}/s",
            ha="center",
            va="bottom",
            color=SLATE_DARK,
            fontsize=17.0,
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.18", facecolor=BG_WHITE, edgecolor="none", alpha=0.85),
            zorder=5,
        )
    ax1.set_title("Burst 300 sự kiện (Edge → EC2)", color=NAVY, fontsize=20.0, fontweight="bold", pad=14)
    ax1.set_ylabel("Sự kiện / giây", color=SLATE_MUTED, fontsize=17.0)
    ax1.set_ylim(0, max(126, max(achieved1) * 1.25))
    ax1.grid(axis="y", color=BORDER_BLUE, linestyle="--", alpha=0.85, zorder=0)
    ax1.tick_params(colors=SLATE_DARK, labelsize=15.5)
    for spine in ax1.spines.values():
        spine.set_color(BORDER_BLUE)
        spine.set_linewidth(1.5)
    ax1.legend(loc="upper left", facecolor=BG_WHITE, edgecolor=BORDER_BLUE, labelcolor=SLATE_DARK, fontsize=15.5)

    # Right: Single vs Batch
    sust = data["sustained"]
    labels2 = ["Đầu vào\n(Producer)", "Single-commit\n(batch=1)", "Batch-commit\n(batch=100)"]
    values2 = [sust["input_eps"], sust["single_eps"], sust["batch_eps"]]
    colors2 = [SKY, AMBER, EMERALD]

    ax2.set_facecolor(CARD_BG)
    bars2 = ax2.bar(labels2, values2, width=0.48, color=colors2, zorder=3)
    for bar, val in zip(bars2, values2):
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            val + 4.0,
            f"{val:.1f}/s",
            ha="center",
            va="bottom",
            color=SLATE_DARK,
            fontsize=17.0,
            fontweight="bold",
        )

    y_max = max(175.0, max(values2) * 1.22)
    single_badge_y = sust["single_eps"] + 48.0 if sust["single_eps"] < 35.0 else sust["single_eps"] * 0.48
    batch_badge_y = sust["batch_eps"] + 48.0 if sust["batch_eps"] < 35.0 else sust["batch_eps"] * 0.48

    ax2.text(
        1,
        single_badge_y,
        f"p95 = {sust['single_p95_s']:.2f} s\nmax = {sust['single_max_s']:.2f} s",
        ha="center",
        va="center",
        color=SLATE_DARK,
        fontsize=15.5,
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.45", facecolor="#FFFBEB", edgecolor=AMBER, linewidth=2.2),
        zorder=5,
    )
    ax2.text(
        2,
        batch_badge_y,
        f"p95 = {sust['batch_p95_s']:.2f} s\nmax = {sust['batch_max_s']:.2f} s",
        ha="center",
        va="center",
        color=SLATE_DARK,
        fontsize=15.5,
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.45", facecolor="#ECFDF5", edgecolor=EMERALD, linewidth=2.2),
        zorder=5,
    )

    ax2.set_title("1.000 sự kiện @ 100/s qua WAN", color=NAVY, fontsize=20.0, fontweight="bold", pad=14)
    ax2.set_ylabel("Thông lượng ghi Neon DB (evt/s)", color=SLATE_MUTED, fontsize=17.0)
    ax2.set_ylim(0, y_max)
    ax2.grid(axis="y", color=BORDER_BLUE, linestyle="--", alpha=0.85, zorder=0)
    ax2.tick_params(colors=SLATE_DARK, labelsize=15.0)
    for spine in ax2.spines.values():
        spine.set_color(BORDER_BLUE)
        spine.set_linewidth(1.5)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=BG_WHITE)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Canva light-theme visual assets.")
    parser.add_argument("--results-dir", default=str(Path(__file__).resolve().parent / "results"))
    parser.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "charts"))
    args = parser.parse_args()

    data = load_real_results(Path(args.results_dir))
    out_dir = Path(args.out_dir)

    render_arch_3zone_light(out_dir / "canva_arch_3zone_light.png")
    render_pipeline_5stage_light(out_dir / "canva_pipeline_5stage_light.png")
    render_recovery_light(data["crash"], out_dir / "canva_recovery_light.png")
    render_throughput_light(data, out_dir / "canva_throughput_light.png")
    print(f"Generated 4 Canva light-theme assets in {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
