"""
main.py
Punto de entrada CLI del pipeline agentic de EduStream.
Se puede correr directamente o como Databricks Job task.

Modos:
  python main.py                          # corrida normal
  python main.py --drift                  # inyecta drift para testing
  python main.py --nulls                  # inyecta datos corruptos
  python main.py --env prod               # entorno de producción
  python main.py --resume <run_id>        # retoma desde checkpoint

En Databricks, configurar como Python task apuntando a este archivo.
"""

import argparse
import logging
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("edustream.main")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="EduStream Agentic Pipeline")
    p.add_argument("--env",     default="dev",   choices=["dev", "staging", "prod"])
    p.add_argument("--drift",   action="store_true", help="Inyectar drift de distribución")
    p.add_argument("--nulls",   action="store_true", help="Inyectar datos corruptos")
    p.add_argument("--run-id",  default=None,    help="Run ID personalizado")
    p.add_argument("--resume",  default=None,    help="Reanudar run_id desde checkpoint")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    logger.info("=" * 60)
    logger.info("EduStream Agentic Pipeline v1.0.0")
    logger.info("env=%s | drift=%s | nulls=%s", args.env, args.drift, args.nulls)
    logger.info("=" * 60)

    from pipelines.graph import run_pipeline, resume_from_checkpoint

    try:
        if args.resume:
            try:
                from langgraph.checkpoint.sqlite import SqliteSaver
                cp = SqliteSaver.from_conn_string(f"checkpoints_{args.env}.db")
                final_state = resume_from_checkpoint(args.resume, cp)
            except ImportError:
                logger.warning("SqliteSaver no disponible, usando MemorySaver")
                final_state = run_pipeline(run_id=args.resume, env=args.env)
        else:
            final_state = run_pipeline(
                run_id=args.run_id,
                env=args.env,
                inject_drift=args.drift,
                inject_nulls=args.nulls,
            )

        _print_summary(final_state)
        return 0 if final_state.pipeline_complete else 1

    except Exception as exc:
        logger.error("Pipeline falló con excepción: %s", exc, exc_info=True)
        return 1


def _print_summary(state) -> None:
    logger.info("")
    logger.info("━" * 60)
    logger.info("  RESUMEN EJECUTIVO DEL PIPELINE")
    logger.info("━" * 60)
    logger.info("  run_id:    %s", state.run_id)
    logger.info("  env:       %s", state.env)
    logger.info("  completo:  %s", state.pipeline_complete)
    logger.info("")
    logger.info("  INGESTION")
    logger.info("    filas procesadas: %s", f"{state.ingestion_rows:,}")
    logger.info("    tablas bronze:    %d", len(state.ingestion_tables))

    if state.dq_result:
        logger.info("")
        logger.info("  DATA QUALITY")
        logger.info("    DQ score:      %.4f", state.dq_result.score)
        logger.info("    quarantine:    %d filas", state.dq_result.quarantine_rows)
        logger.info("    drift:         %s", state.dq_result.drift_detected)
        if state.dq_result.psi_scores:
            for col, psi in state.dq_result.psi_scores.items():
                flag = " ⚠" if psi > 0.2 else ""
                logger.info("    PSI(%s): %.4f%s", col, psi, flag)

    if state.transform_result:
        logger.info("")
        logger.info("  TRANSFORMATION")
        logger.info("    silver tables: %d", len(state.transform_result.silver_tables))
        logger.info("    gold tables:   %d", len(state.transform_result.gold_tables))
        logger.info("    SCD2 changes:  %d", state.transform_result.scd2_changes)

    if state.semantic_result:
        logger.info("")
        logger.info("  SEMANTIC AGENT")
        logger.info("    SQL auditado:  %s", state.semantic_result.audit_passed)
        logger.info("    filas query:   %d", state.semantic_result.result_rows)

    if state.obs_result:
        logger.info("")
        logger.info("  OBSERVABILITY")
        logger.info("    freshness:     %.1f min", state.obs_result.freshness_minutes)
        logger.info("    SLO ok:        %s", state.obs_result.slo_ok)
        logger.info("    tokens:        %s", f"{state.obs_result.tokens_total:,}")
        logger.info("    costo:         $%.6f USD", state.obs_result.cost_usd)
        logger.info("    mlflow_run:    %s", state.obs_result.mlflow_run_id)

    logger.info("")
    logger.info("  DECISIONES DEL SUPERVISOR (%d)", len(state.supervisor_decisions))
    for d in state.supervisor_decisions:
        logger.info("    • %s", d)
    logger.info("━" * 60)


if __name__ == "__main__":
    sys.exit(main())
