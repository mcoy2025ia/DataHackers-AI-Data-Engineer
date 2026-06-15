"""
EduStream — Generador de datos sintéticos realistas
====================================================
Genera los 4 CSVs (enrollments, courses, progress, instructors) con
imperfecciones intencionales que el pipeline debe manejar:

  - Nulos en payment_amount          → silver los normaliza a 0
  - Filas con total_lessons = 0      → silver_progress las filtra
  - payment_amount negativos         → DLT expect_or_drop los descarta
  - course_id NULL en enrollments    → DLT expect los marca, no descarta
  - Duplicados de enrollment_id      → silver deduplica con MERGE
  - Cursos con campos que cambian    → SCD2 los historiza

Uso:
    python generate_edustream_data.py --out ./data --enrollments 5000
"""
from __future__ import annotations
import argparse, csv, random, datetime as dt
from pathlib import Path

random.seed(42)  # reproducibilidad — un revisor debe obtener los mismos datos

CATEGORIES = ["Data Engineering", "Machine Learning", "Cloud", "Analytics",
              "Software", "Cybersecurity", "Product"]
COURSE_NAMES = [
    "Apache Spark desde cero", "Delta Lake en producción", "MLOps con MLflow",
    "Arquitectura de datos en la nube", "SQL avanzado para analítica",
    "Python para ingeniería de datos", "Streaming con Kafka",
    "Modelado dimensional", "dbt y transformaciones", "Observabilidad de datos",
    "Seguridad en pipelines", "Fundamentos de LLMs",
]
SPECIALTIES = ["Big Data", "ML", "Cloud Architecture", "Analytics Eng", "Security"]
FIRST = ["Ana", "Carlos", "María", "Juan", "Lucía", "Diego", "Sofía", "Andrés"]
LAST  = ["García", "Rodríguez", "Martínez", "López", "Gómez", "Díaz", "Vargas"]


def daterange(start: dt.date, days: int) -> dt.date:
    return start + dt.timedelta(days=random.randint(0, days))


def gen_instructors(n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        name = f"{random.choice(FIRST)} {random.choice(LAST)}"
        rows.append({
            "instructor_id": f"INST{i:03d}",
            "name": name,
            "email": f"{name.lower().replace(' ', '.')}@edustream.io",
            "specialty": random.choice(SPECIALTIES),
        })
    return rows


def gen_courses(n: int, instructors: list[dict]) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "course_id": f"CRS{i:03d}",
            "course_name": f"{random.choice(COURSE_NAMES)} v{random.randint(1,3)}",
            "category": random.choice(CATEGORIES),
            "instructor_id": random.choice(instructors)["instructor_id"],
            "list_price": random.choice([0, 29.99, 49.99, 79.99, 99.99, 149.99]),
            # updated_at permite que SCD2 detecte cambios entre cargas
            "updated_at": daterange(dt.date(2025, 1, 1), 300).isoformat(),
        })
    return rows


def gen_enrollments(n: int, courses: list[dict]) -> list[dict]:
    rows = []
    course_ids = [c["course_id"] for c in courses]
    for i in range(1, n + 1):
        cid = random.choice(course_ids)
        price = next(c["list_price"] for c in courses if c["course_id"] == cid)

        # --- imperfecciones intencionales ---
        payment = price
        r = random.random()
        if r < 0.08:
            payment = ""                       # 8% nulos
        elif r < 0.11:
            payment = round(-abs(price or 50), 2)  # 3% negativos (corruptos)

        course_field = cid
        if random.random() < 0.04:
            course_field = ""                  # 4% course_id nulo

        rows.append({
            "enrollment_id": f"ENR{i:06d}",
            "user_id": f"USR{random.randint(1, n // 3):05d}",
            "course_id": course_field,
            "payment_amount": payment,
            "enrolled_at": daterange(dt.date(2025, 1, 1), 480).isoformat(),
            "status": random.choice(["active", "active", "active", "refunded"]),
        })

    # --- duplicados intencionales (2% de las filas se repiten) ---
    dupes = random.sample(rows, k=max(1, n // 50))
    rows.extend(dupes)
    random.shuffle(rows)
    return rows


def gen_progress(enrollments: list[dict]) -> list[dict]:
    rows = []
    seen = set()
    for e in enrollments:
        key = (e["user_id"], e["course_id"])
        if key in seen or not e["course_id"]:
            continue
        seen.add(key)

        # 5% de cursos sin lecciones cargadas → total_lessons = 0
        total = 0 if random.random() < 0.05 else random.randint(8, 40)
        completed = random.randint(0, total) if total else 0

        rows.append({
            "user_id": e["user_id"],
            "course_id": e["course_id"],
            "completed_lessons": completed,
            "total_lessons": total,
            "last_activity_at": daterange(dt.date(2025, 2, 1), 400).isoformat(),
        })
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  {path.name:24s} {len(rows):>7,} filas")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./data")
    ap.add_argument("--enrollments", type=int, default=5000)
    ap.add_argument("--courses", type=int, default=40)
    ap.add_argument("--instructors", type=int, default=12)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    print("Generando dataset EduStream (seed=42)...")
    instructors = gen_instructors(args.instructors)
    courses     = gen_courses(args.courses, instructors)
    enrollments = gen_enrollments(args.enrollments, courses)
    progress    = gen_progress(enrollments)

    write_csv(out / "instructors.csv", instructors)
    write_csv(out / "courses.csv",     courses)
    write_csv(out / "enrollments.csv", enrollments)
    write_csv(out / "progress.csv",    progress)

    neg   = sum(1 for e in enrollments if str(e["payment_amount"]).startswith("-"))
    null  = sum(1 for e in enrollments if e["payment_amount"] == "")
    nocid = sum(1 for e in enrollments if not e["course_id"])
    print(f"\nImperfecciones inyectadas (para validar el pipeline):")
    print(f"  payment_amount negativos : {neg}")
    print(f"  payment_amount nulos     : {null}")
    print(f"  course_id nulos          : {nocid}")


if __name__ == "__main__":
    main()
