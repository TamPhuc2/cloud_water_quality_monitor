"""Generate dark-theme slide charts matching Seminar-N12-L01.pdf from Cloud results."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


BG_COLOR = "#0B192C"
PANEL_COLOR = "#132A46"
BORDER_COLOR = "#1E3A5F"
TEAL = "#2DD4BF"
MINT = "#14B8A6"
SKY = "#38BDF8"
AMBER = "#FBBF24"
CORAL = "#F87171"
GREEN = "#4ADE80"
TEXT_MAIN = "#F8FAFC"
TEXT_MUTED = "#94A3B8"

plt.rcParams["font.sans-serif"] = ["DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def sample_preview_data() -> dict[str, Any]:
    """Return clearly labeled synthetic sample data ONLY for --preview-sample."""
    return {
        "is_preview": True,
        "burst": [
            {"label": "10/s", "target": 10.0, "achieved": 10.02},
            {"label": "50/s", "target": 50.0, "achieved": 50.08},
            {"label": "100/s", "target": 100.0, "achieved": 99.85},
        ],
        "sustained": {
            "input_eps": 100.0,
            "single_eps": 28.4,
            "single_p95_s": 18.62,
            "single_max_s": 24.15,
            "batch_eps": 98.7,
            "batch_p95_s": 0.64,
            "batch_max_s": 1.12,
        },
        "crash": {
            "run_id": "crash-recovery-preview",
            "first_event_id": "AN-2089657-0135",
            "unique_events": 100,
            "total_events": 100,
            "duplicates": 0,
            "kafka_lag": 0,
        },
    }


def _find_latest_result(results_dir: Path, prefix: str) -> dict[str, Any] | None:
    matched: list[tuple[str, dict[str, Any]]] = []
    for path in sorted(results_dir.glob("*.json")):
        if path.name.endswith("-producer.json"):
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("phase") == prefix or path.name.startswith(f"{prefix}-"):
            matched.append((path.name, data))
    if matched:
        matched.sort(key=lambda item: item[0])
        return matched[-1][1]
    return None


def load_real_results(results_dir: Path) -> dict[str, Any]:
    """Load real Cloud benchmark JSON files from results_dir."""
    required = {
        "burst-10": _find_latest_result(results_dir, "burst-10"),
        "burst-50": _find_latest_result(results_dir, "burst-50"),
        "burst-100": _find_latest_result(results_dir, "burst-100"),
        "sustained-single": _find_latest_result(results_dir, "sustained-single"),
        "sustained-batch": _find_latest_result(results_dir, "sustained-batch"),
        "crash-recovery": _find_latest_result(results_dir, "crash-recovery"),
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        raise FileNotFoundError(
            f"Missing Cloud result JSONs in {results_dir}: {', '.join(missing)}. "
            "Run benchmarks first or pass --preview-sample for layout smoke test."
        )

    def _prod_eps(entry: dict[str, Any], default: float) -> float:
        prod = entry.get("producer") or {}
        return float(prod.get("actual_rate_eps") or entry.get("db_throughput_eps") or default)

    b10, b50, b100 = required["burst-10"], required["burst-50"], required["burst-100"]
    single = required["sustained-single"]
    batch = required["sustained-batch"]
    crash = required["crash-recovery"]
    assert b10 and b50 and b100 and single and batch and crash

    return {
        "is_preview": False,
        "burst": [
            {"label": "10/s", "target": 10.0, "achieved": _prod_eps(b10, 10.0)},
            {"label": "50/s", "target": 50.0, "achieved": _prod_eps(b50, 50.0)},
            {"label": "100/s", "target": 100.0, "achieved": _prod_eps(b100, 100.0)},
        ],
        "sustained": {
            "input_eps": float((batch.get("producer") or {}).get("actual_rate_eps") or 100.0),
            "single_eps": float(single.get("db_throughput_eps") or 0.0),
            "single_p95_s": float(single.get("e2e_p95_ms") or 0.0) / 1000.0,
            "single_max_s": float(single.get("e2e_max_ms") or 0.0) / 1000.0,
            "batch_eps": float(batch.get("db_throughput_eps") or 0.0),
            "batch_p95_s": float(batch.get("e2e_p95_ms") or 0.0) / 1000.0,
            "batch_max_s": float(batch.get("e2e_max_ms") or 0.0) / 1000.0,
        },
        "crash": {
            "run_id": crash.get("run_id"),
            "first_event_id": crash.get("first_event_id") or "event-000000",
            "unique_events": int(crash.get("distinct_valid_events") or 0),
            "total_events": int(crash.get("expected_valid") or crash.get("valid_rows") or 0),
            "duplicates": int(crash.get("duplicate_rows") or 0),
            "kafka_lag": 0 if crash.get("kafka_lag") is None else int(crash.get("kafka_lag")),
        },
    }


def render_throughput_chart(data: dict[str, Any], out_path: Path) -> None:
    """Render Slide 9 Cloud Throughput & Single vs Batch WAN Comparison PNG."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(19.2, 10.8), dpi=100)
    fig.patch.set_facecolor(BG_COLOR)
    fig.subplots_adjust(left=0.06, right=0.96, top=0.80, bottom=0.14, wspace=0.20)

    fig.text(
        0.04,
        0.92,
        "MINH CHỨNG TẢI TRÊN CLOUD (AWS EC2 → NEON POSTGRESQL)",
        color=TEXT_MAIN,
        fontsize=28,
        fontweight="bold",
    )
    subtitle = "Laptop Producer → SSH Tunnel → AWS EC2 c7i-flex.large (ap-southeast-2) → Neon DB (ap-southeast-1)"
    if data.get("is_preview"):
        subtitle = "[PREVIEW MẪU — CHƯA PHẢI SỐ LIỆU CLOUD THẬT]  " + subtitle
    fig.text(
        0.04,
        0.875,
        subtitle,
        color=AMBER if data.get("is_preview") else TEXT_MUTED,
        fontsize=15,
    )

    # Left panel: Burst 300 events
    burst = data["burst"]
    labels1 = [b["label"] for b in burst]
    achieved1 = [b["achieved"] for b in burst]
    targets1 = [b["target"] for b in burst]

    ax1.set_facecolor(PANEL_COLOR)
    bars1 = ax1.bar(labels1, achieved1, width=0.45, color=[MINT, TEAL, SKY], zorder=3)
    ax1.plot(labels1, targets1, color=TEXT_MAIN, marker="o", linewidth=2.2, label="Mục tiêu", zorder=4)
    for bar, val, tgt in zip(bars1, achieved1, targets1):
        label_y = max(val, tgt) + 3.5
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            label_y,
            f"{val:.2f}",
            ha="center",
            va="bottom",
            color=TEXT_MAIN,
            fontsize=14,
            fontweight="bold",
        )
    ax1.set_title("Burst 300 sự kiện (Producer → EC2 Kafka)", color=TEXT_MAIN, fontsize=19, fontweight="bold", pad=16)
    ax1.set_ylabel("Sự kiện / giây", color=TEXT_MUTED, fontsize=14)
    ax1.set_ylim(0, max(120, max(achieved1) * 1.18))
    ax1.grid(axis="y", color=BORDER_COLOR, linestyle="--", alpha=0.6, zorder=0)
    ax1.tick_params(colors=TEXT_MAIN, labelsize=13)
    for spine in ax1.spines.values():
        spine.set_color(BORDER_COLOR)
    ax1.legend(loc="upper left", facecolor=BG_COLOR, edgecolor=BORDER_COLOR, labelcolor=TEXT_MAIN, fontsize=13)

    # Right panel: Sustained 1,000 events @ 100/s (Single vs Batch)
    sust = data["sustained"]
    labels2 = ["Đầu vào\n(Producer)", "Single-commit\n(batch=1)", "Batch-commit\n(batch=100)"]
    values2 = [sust["input_eps"], sust["single_eps"], sust["batch_eps"]]
    colors2 = [SKY, AMBER, TEAL]

    ax2.set_facecolor(PANEL_COLOR)
    bars2 = ax2.bar(labels2, values2, width=0.48, color=colors2, zorder=3)
    for bar, val in zip(bars2, values2):
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            val + 2.5,
            f"{val:.1f}/s",
            ha="center",
            va="bottom",
            color=TEXT_MAIN,
            fontsize=15,
            fontweight="bold",
        )

    single_badge_y = sust["single_eps"] + 18.0 if sust["single_eps"] < 35.0 else sust["single_eps"] * 0.48
    batch_badge_y = sust["batch_eps"] + 18.0 if sust["batch_eps"] < 35.0 else sust["batch_eps"] * 0.48

    ax2.text(
        1,
        single_badge_y,
        f"p95 = {sust['single_p95_s']:.2f} s\nmax = {sust['single_max_s']:.2f} s",
        ha="center",
        va="center",
        color=TEXT_MAIN,
        fontsize=13,
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.5", facecolor=BG_COLOR, edgecolor=AMBER, linewidth=1.8),
        zorder=5,
    )
    ax2.text(
        2,
        batch_badge_y,
        f"p95 = {sust['batch_p95_s']:.2f} s\nmax = {sust['batch_max_s']:.2f} s",
        ha="center",
        va="center",
        color=TEXT_MAIN,
        fontsize=13,
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.5", facecolor=BG_COLOR, edgecolor=TEAL, linewidth=1.8),
        zorder=5,
    )

    ax2.set_title("Tải duy trì 1.000 sự kiện @ 100/s (Single vs Batch)", color=TEXT_MAIN, fontsize=19, fontweight="bold", pad=16)
    ax2.set_ylabel("Thông lượng ghi Neon DB (sự kiện/giây)", color=TEXT_MUTED, fontsize=14)
    ax2.set_ylim(0, max(120, max(values2) * 1.18))
    ax2.grid(axis="y", color=BORDER_COLOR, linestyle="--", alpha=0.6, zorder=0)
    ax2.tick_params(colors=TEXT_MAIN, labelsize=13)
    for spine in ax2.spines.values():
        spine.set_color(BORDER_COLOR)

    fig.text(
        0.04,
        0.05,
        "Kết luận Cloud: Commit từng sự kiện qua độ trễ WAN (EC2 → Neon) tạo backlog; gộp batch=100 với UNNEST xóa nghẽn và giữ p95 thấp.",
        color=TEXT_MUTED,
        fontsize=14,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=BG_COLOR)
    plt.close(fig)


def render_recovery_chart(data: dict[str, Any], out_path: Path) -> None:
    """Render Slide 10 Cloud Crash-Recovery & Deduplication Card Diagram PNG."""
    crash = data["crash"]
    fig, ax = plt.subplots(figsize=(19.2, 10.8), dpi=100)
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    ax.text(
        4,
        91,
        "KIỂM THỬ KHÔI PHỤC & CHỐNG GHI TRÙNG TRÊN CLOUD",
        color=TEXT_MAIN,
        fontsize=28,
        fontweight="bold",
    )
    subtitle = f"Crash sau Neon DB commit, trước Kafka offset commit  |  Run: {crash['run_id']}"
    if data.get("is_preview"):
        subtitle = "[PREVIEW MẪU — CHƯA PHẢI SỐ LIỆU CLOUD THẬT]  " + subtitle
    ax.text(
        4,
        85.5,
        subtitle,
        color=AMBER if data.get("is_preview") else TEXT_MUTED,
        fontsize=15,
    )

    steps = [
        ("1. Ghi Neon DB", f"event {crash['first_event_id']}\nđã COMMIT", TEAL),
        ("2. Crash Consumer", "exit code 77\noffset chưa commit", CORAL),
        ("3. Khởi động lại", "cùng consumer group\nauto_offset_reset", SKY),
        ("4. Kafka Replay", "log DEDUP_REPLAY\nON CONFLICT bỏ qua", AMBER),
        (
            "5. Kết quả Cloud",
            f"{crash['unique_events']}/{crash['total_events']} unique\n{crash['duplicates']} trùng • lag {crash['kafka_lag']}",
            GREEN,
        ),
    ]

    card_w = 16.5
    card_h = 28.0
    gap = 2.5
    start_x = 3.75
    y0 = 40.0

    for idx, (title, body, edge_color) in enumerate(steps):
        x0 = start_x + idx * (card_w + gap)
        box = FancyBboxPatch(
            (x0, y0),
            card_w,
            card_h,
            boxstyle="round,pad=0.6,rounding_size=1.8",
            facecolor=PANEL_COLOR,
            edgecolor=edge_color,
            linewidth=2.5,
        )
        ax.add_patch(box)
        ax.text(
            x0 + card_w / 2,
            y0 + card_h * 0.68,
            title,
            ha="center",
            va="center",
            color=TEXT_MAIN,
            fontsize=15,
            fontweight="bold",
        )
        ax.text(
            x0 + card_w / 2,
            y0 + card_h * 0.34,
            body,
            ha="center",
            va="center",
            color=TEXT_MUTED,
            fontsize=13,
            linespacing=1.5,
        )
        if idx < len(steps) - 1:
            ax.annotate(
                "",
                xy=(x0 + card_w + gap - 0.3, y0 + card_h / 2),
                xytext=(x0 + card_w + 0.3, y0 + card_h / 2),
                arrowprops=dict(arrowstyle="->", color=TEAL, lw=2.5),
            )

    ax.text(
        50,
        24,
        "PRIMARY KEY (run_id, event_id)  +  ON CONFLICT DO NOTHING",
        ha="center",
        va="center",
        color=TEAL,
        fontsize=21,
        fontweight="bold",
    )
    ax.text(
        50,
        16,
        "Cơ chế đạt At-Least-Once Delivery + Idempotent Write xuyên suốt Kafka (EC2) và Neon PostgreSQL (Cloud).",
        ha="center",
        va="center",
        color=TEXT_MUTED,
        fontsize=14,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=BG_COLOR)
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate slide benchmark charts.")
    parser.add_argument(
        "--results-dir",
        default=str(Path(__file__).resolve().parent / "results"),
        help="Directory containing evaluate_run.py JSON results.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(Path(__file__).resolve().parent / "charts"),
        help="Directory to write PNG charts.",
    )
    parser.add_argument(
        "--preview-sample",
        action="store_true",
        help="Render clearly labeled sample preview charts without real Cloud data.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)

    if args.preview_sample:
        data = sample_preview_data()
    else:
        data = load_real_results(Path(args.results_dir))

    render_throughput_chart(data, out_dir / "slide9_cloud_throughput.png")
    render_recovery_chart(data, out_dir / "slide10_cloud_recovery.png")
    print(f"Generated charts in {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
