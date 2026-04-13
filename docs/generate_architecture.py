"""Architecture diagram — two-row layout, large text, clean orthogonal lines."""

from PIL import Image, ImageDraw, ImageFont

W, H = 1800, 1280
BG = (12, 18, 32)
AMBER = (245, 180, 60)
TEAL = (60, 190, 200)
SLATE = (55, 65, 85)
WHITE = (225, 230, 240)
DIM = (100, 115, 140)
CORAL = (230, 95, 85)

FONT_DIR = "/Users/xinglingao/.claude/skills/canvas-design/canvas-fonts"
img = Image.new("RGB", (W, H), BG)
draw = ImageDraw.Draw(img)

f_title = ImageFont.truetype(f"{FONT_DIR}/Jura-Medium.ttf", 44)
f_subtitle = ImageFont.truetype(f"{FONT_DIR}/Jura-Light.ttf", 22)
f_section = ImageFont.truetype(f"{FONT_DIR}/Jura-Medium.ttf", 20)
f_node = ImageFont.truetype(f"{FONT_DIR}/Jura-Medium.ttf", 18)
f_body = ImageFont.truetype(f"{FONT_DIR}/JetBrainsMono-Regular.ttf", 15)
f_small = ImageFont.truetype(f"{FONT_DIR}/JetBrainsMono-Regular.ttf", 13)
f_q = ImageFont.truetype(f"{FONT_DIR}/JetBrainsMono-Bold.ttf", 16)
f_label = ImageFont.truetype(f"{FONT_DIR}/Jura-Light.ttf", 14)
f_tiny = ImageFont.truetype(f"{FONT_DIR}/JetBrainsMono-Regular.ttf", 11)


def rrect(x, y, w, h, r=8, fill=None, outline=None, width=1):
    draw.rounded_rectangle([x, y, x+w, y+h], radius=r, fill=fill, outline=outline, width=width)

def ctxt(text, font, cx, cy, fill):
    bbox = draw.textbbox((0, 0), text, font=font)
    draw.text((cx - (bbox[2]-bbox[0])/2, cy), text, fill=fill, font=font)

def dot(x, y, r, fill):
    draw.ellipse([x-r, y-r, x+r, y+r], fill=fill)

def hline(x1, x2, y, color, w=2):
    draw.line([(x1, y), (x2, y)], fill=color, width=w)

def vline(x, y1, y2, color, w=2):
    draw.line([(x, y1), (x, y2)], fill=color, width=w)

def arrow_r(x, y, c=TEAL, s=7):
    draw.polygon([(x+s, y), (x-s, y-s), (x-s, y+s)], fill=c)

def arrow_d(x, y, c=TEAL, s=7):
    draw.polygon([(x, y+s), (x-s, y-s), (x+s, y-s)], fill=c)

def arrow_u(x, y, c=TEAL, s=7):
    draw.polygon([(x, y-s), (x-s, y+s), (x+s, y+s)], fill=c)

def arrow_l(x, y, c=TEAL, s=7):
    draw.polygon([(x-s, y), (x+s, y-s), (x+s, y+s)], fill=c)


# --- Title ---
draw.text((60, 35), "AUCKLAND TRANSPORT", fill=DIM, font=f_subtitle)
draw.text((60, 65), "Real-time Streaming Pipeline", fill=WHITE, font=f_title)
hline(60, 500, 118, SLATE, 1)

# ================================================================
R1 = 170  # row 1 top

# === AT APIs ===
api_x, api_y = 60, R1
api_w, api_h = 260, 130
rrect(api_x, api_y, api_w, api_h, outline=TEAL, width=2)
draw.text((api_x+20, api_y+14), "AT GTFS-RT APIs", fill=TEAL, font=f_node)
hline(api_x+10, api_x+api_w-10, api_y+40, SLATE, 1)
draw.text((api_x+20, api_y+52), "vehicle_positions  30s", fill=DIM, font=f_body)
draw.text((api_x+20, api_y+72), "trip_updates       30s", fill=DIM, font=f_body)
draw.text((api_x+20, api_y+92), "service_alerts    300s", fill=DIM, font=f_body)

# APIs → Producer (vertical)
a1_y = api_y + api_h + 15
vline(api_x + api_w//2, api_y + api_h, a1_y, TEAL)
arrow_d(api_x + api_w//2, a1_y, TEAL, 6)

# === Producer ===
prod_x, prod_y = 60, a1_y + 8
prod_w, prod_h = 260, 60
rrect(prod_x, prod_y, prod_w, prod_h, fill=(22, 32, 50), outline=AMBER, width=2)
ctxt("Python Producer", f_node, prod_x + prod_w//2, prod_y + 10, AMBER)
draw.text((prod_x+30, prod_y+36), "Avro + Schema Registry", fill=DIM, font=f_small)

# Producer → Kafka (horizontal)
prod_right = prod_x + prod_w
kafka_x = 420
hline(prod_right, kafka_x, prod_y + prod_h//2, TEAL)
arrow_r(kafka_x - 2, prod_y + prod_h//2, TEAL, 6)

# === Kafka ===
kafka_w, kafka_h = 300, 240
kafka_y = R1
rrect(kafka_x, kafka_y, kafka_w, kafka_h, outline=TEAL, width=2)
draw.text((kafka_x+20, kafka_y+14), "Event Hubs / Kafka", fill=TEAL, font=f_node)
hline(kafka_x+10, kafka_x+kafka_w-10, kafka_y+42, SLATE, 1)

topics = [
    ("at.vehicle_positions", TEAL),
    ("at.trip_updates", TEAL),
    ("at.service_alerts", TEAL),
    ("at.headway_metrics", AMBER),
    ("at.alerts", CORAL),
]
# Track topic y positions for input arrows
topic_ys = {}
for i, (t, c) in enumerate(topics):
    ty = kafka_y + 50 + i * 32
    topic_ys[t] = ty + 8
    dot(kafka_x+24, ty+8, 5, c)
    draw.text((kafka_x+40, ty), t, fill=c, font=f_body)

# Input/output labels
draw.text((kafka_x+20, kafka_y + 50 + 2*32 + 20), "── input", fill=TEAL, font=f_tiny)
draw.text((kafka_x+110, kafka_y + 50 + 2*32 + 20), "── output", fill=AMBER, font=f_tiny)

# Kafka → Spark (horizontal to Bronze ingestion)
kafka_right = kafka_x + kafka_w
spark_x = 820
spark_conn_y = kafka_y + 80
hline(kafka_right, spark_x, spark_conn_y, TEAL)
arrow_r(spark_x - 2, spark_conn_y, TEAL, 6)

# === Spark Streaming ===
spark_w, spark_h = 920, 420
spark_y = R1
for o in [3, 2, 1]:
    rrect(spark_x-o, spark_y-o, spark_w+o*2, spark_h+o*2, r=10+o,
          outline=(AMBER[0]//6, AMBER[1]//6, AMBER[2]//6), width=1)
rrect(spark_x, spark_y, spark_w, spark_h, r=10, outline=AMBER, width=2)
draw.text((spark_x+24, spark_y+16), "SPARK STRUCTURED STREAMING", fill=AMBER, font=f_section)
draw.text((spark_x+380, spark_y+18), "5 independent streaming jobs", fill=DIM, font=f_small)
hline(spark_x+10, spark_x+spark_w-10, spark_y+46, SLATE, 1)

# Bronze ingestion bar
rrect(spark_x+24, spark_y+58, spark_w-48, 38, r=6, fill=(25, 35, 55), outline=SLATE, width=1)
ctxt("Bronze Ingestion    vehicle_positions | trip_updates | service_alerts",
     f_body, spark_x+spark_w//2, spark_y+68, WHITE)

# Q1-Q4 jobs with source topic annotations
jobs = [
    ("Q1", "delay_alert_job", "stateless filter", "← trip_updates"),
    ("Q2", "vehicle_stall_job", "stateful per-vehicle", "← vehicle_positions"),
    ("Q3", "headway_regularity_job", "windowed aggregation", "← trip_updates"),
    ("Q4", "alert_correlation_job", "3-stream join", "← all 3 topics"),
]
for i, (q, name, pattern, source) in enumerate(jobs):
    jy = spark_y + 112 + i * 74
    rrect(spark_x+24, jy, 42, 30, r=5, fill=CORAL)
    ctxt(q, f_q, spark_x+45, jy+5, BG)
    rrect(spark_x+78, jy, spark_w-114, 58, r=6, outline=SLATE, width=1)
    draw.text((spark_x+98, jy+8), name, fill=WHITE, font=f_node)
    draw.text((spark_x+98, jy+32), pattern, fill=DIM, font=f_body)
    # Source topic annotation on right side of job box
    draw.text((spark_x+spark_w-280, jy+32), source, fill=TEAL, font=f_small)

# === RED LINE: Spark Q1-Q4 → at.alerts (exits Spark LEFT, one turn up) ===
red_exit_y = spark_y + 340
kafka_alerts_x = kafka_x + kafka_w - 40

hline(kafka_alerts_x, spark_x, red_exit_y, CORAL)
vline(kafka_alerts_x, kafka_y + kafka_h, red_exit_y, CORAL)
arrow_u(kafka_alerts_x, kafka_y + kafka_h + 2, CORAL, 6)
draw.text((kafka_alerts_x - 200, red_exit_y - 20), "Q1-Q4 alerts writeback", fill=CORAL, font=f_small)

# === BLUE LINE: Spark → dbt (straight down) ===
R2 = 700
bridge_x = 960

vline(bridge_x, spark_y + spark_h, R2, TEAL)
arrow_d(bridge_x, R2 - 2, TEAL, 6)
draw.text((bridge_x + 12, (spark_y + spark_h + R2) // 2 - 8),
          "Bronze raw + detection tables", fill=DIM, font=f_label)

# === ROW 2 ===

# === Databricks Workflows ===
wf_x, wf_y = 60, R2 - 30
wf_w, wf_h = 260, 110
rrect(wf_x, wf_y, wf_w, wf_h, outline=TEAL, width=2)
draw.text((wf_x+20, wf_y+12), "Databricks Workflows", fill=TEAL, font=f_node)
hline(wf_x+10, wf_x+wf_w-10, wf_y+38, SLATE, 1)
draw.text((wf_x+20, wf_y+48), "weekly GTFS static", fill=DIM, font=f_body)
draw.text((wf_x+20, wf_y+68), "refresh + dbt trigger", fill=DIM, font=f_body)

# Workflows → dbt (horizontal)
dbt_x = 420
wf_right = wf_x + wf_w
wf_conn_y = wf_y + wf_h // 2
hline(wf_right, dbt_x, wf_conn_y, SLATE, 2)
arrow_r(dbt_x - 2, wf_conn_y, SLATE, 6)
draw.text((wf_right + 14, wf_conn_y - 20), "schedule", fill=DIM, font=f_small)

# === GTFS Static ===
static_x, static_y = 60, R2 + 100
static_w, static_h = 260, 80
rrect(static_x, static_y, static_w, static_h, outline=SLATE, width=2)
draw.text((static_x+20, static_y+12), "GTFS Static", fill=WHITE, font=f_node)
draw.text((static_x+20, static_y+40), "weekly ZIP", fill=DIM, font=f_body)
draw.text((static_x+20, static_y+58), "routes.txt + stops.txt", fill=DIM, font=f_body)

# GTFS Static → dbt (horizontal)
static_right = static_x + static_w
hline(static_right, dbt_x, static_y + static_h//2, SLATE, 2)
arrow_r(dbt_x - 2, static_y + static_h//2, SLATE, 6)
draw.text((static_right + 14, static_y + static_h//2 - 20), "dim tables", fill=DIM, font=f_small)

# === dbt ===
dbt_w, dbt_h = 680, 380
dbt_y = R2
for o in [3, 2, 1]:
    rrect(dbt_x-o, dbt_y-o, dbt_w+o*2, dbt_h+o*2, r=10+o,
          outline=(AMBER[0]//6, AMBER[1]//6, AMBER[2]//6), width=1)
rrect(dbt_x, dbt_y, dbt_w, dbt_h, r=10, outline=AMBER, width=2)

draw.text((dbt_x+24, dbt_y+16), "dbt", fill=AMBER, font=f_section)
draw.text((dbt_x+70, dbt_y+18), "DuckDB local  |  Databricks prod", fill=DIM, font=f_body)
hline(dbt_x+10, dbt_x+dbt_w-10, dbt_y+48, SLATE, 1)

# Three columns inside dbt
col_w = (dbt_w - 60) // 3
cols = [
    ("STAGING", "views", TEAL, [
        "stg_vehicle_positions",
        "stg_trip_updates",
        "stg_stall_events",
        "stg_headway_metrics",
        "stg_alert_correlations",
    ]),
    ("CORE", "tables", WHITE, [
        "dim_routes",
        "dim_stops",
    ]),
    ("MARTS", "incremental", AMBER, [
        "fct_delay_alerts",
        "fct_stall_incidents",
        "fct_headway_regularity",
        "fct_alert_impact",
    ]),
]

for ci, (layer, mat, color, models) in enumerate(cols):
    cx = dbt_x + 20 + ci * (col_w + 10)
    cy = dbt_y + 60

    draw.text((cx, cy), layer, fill=color, font=f_section)
    draw.text((cx, cy + 26), mat, fill=DIM, font=f_small)
    hline(cx, cx + col_w - 10, cy + 46, SLATE, 1)

    for mi, m in enumerate(models):
        draw.text((cx, cy + 56 + mi * 22), m, fill=DIM, font=f_body)

    if layer == "MARTS":
        q_labels = ["Q1", "Q2", "Q3", "Q4"]
        for mi, ql in enumerate(q_labels):
            m_bbox = draw.textbbox((0, 0), models[mi], font=f_body)
            m_w = m_bbox[2] - m_bbox[0]
            draw.text((cx + m_w + 12, cy + 56 + mi * 22), ql, fill=CORAL, font=f_small)

# Arrows inside dbt: staging → core → marts
arr_y = dbt_y + 86
for ci in range(2):
    ax = dbt_x + 20 + (ci+1) * (col_w + 10) - 10
    hline(ax - 20, ax, arr_y, SLATE, 1)
    arrow_r(ax - 2, arr_y, SLATE, 4)

# dbt → Power BI (horizontal)
dbt_right = dbt_x + dbt_w
pbi_x = 1220
pbi_conn_y = dbt_y + 100
hline(dbt_right, pbi_x, pbi_conn_y, AMBER)
arrow_r(pbi_x - 2, pbi_conn_y, AMBER, 6)

# === Power BI ===
pbi_w, pbi_h = 300, 200
pbi_y = R2 + 20
rrect(pbi_x, pbi_y, pbi_w, pbi_h, r=8, outline=AMBER, width=2)
draw.text((pbi_x+24, pbi_y+14), "Power BI", fill=AMBER, font=f_node)
draw.text((pbi_x+24, pbi_y+40), "DirectQuery  <5 min", fill=DIM, font=f_body)
hline(pbi_x+10, pbi_x+pbi_w-10, pbi_y+64, SLATE, 1)

pages = [
    ("P1", "Live Operations"),
    ("P2", "Delay Analysis"),
    ("P3", "Stall & Bunching"),
    ("P4", "Disruption Impact"),
]
for i, (p, name) in enumerate(pages):
    py = pbi_y + 76 + i * 28
    draw.text((pbi_x+24, py), p, fill=TEAL, font=f_body)
    draw.text((pbi_x+60, py), name, fill=DIM, font=f_body)

# --- Signature ---
draw.text((60, H-36), "at-streaming-data-pipeline", fill=(30, 40, 58), font=f_small)
draw.text((W-200, H-36), "2026", fill=(30, 40, 58), font=f_small)

# --- Save ---
out = "/Users/xinglingao/workshop/at-streaming-data-pipeline/docs/architecture.png"
img.save(out, "PNG", quality=95)
print(f"Saved: {out} ({W}x{H})")
