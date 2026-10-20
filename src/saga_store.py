"""Persistencia PostgreSQL del agregado Saga (US-B7)."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

import psycopg
from psycopg.types.json import Jsonb

from config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER
from saga import SagaInstance, SagaState, SagaTransition

CREATE_SAGA_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS saga_instances (
    correlation_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    content_id TEXT,
    deadline TIMESTAMPTZ,
    step_attempts JSONB NOT NULL DEFAULT '{}'::jsonb,
    history JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE saga_instances
    ADD COLUMN IF NOT EXISTS step_attempts JSONB NOT NULL DEFAULT '{}'::jsonb;
CREATE INDEX IF NOT EXISTS idx_saga_instances_deadline
    ON saga_instances (deadline) WHERE deadline IS NOT NULL;
"""


def get_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


class SagaRepository:
    def init_db(self) -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SAGA_TABLE_SQL)
            conn.commit()

    def save(self, saga: SagaInstance) -> None:
        history = [
            {
                "from_state": transition.from_state.value if transition.from_state else None,
                "to_state": transition.to_state.value,
                "reason": transition.reason,
                "at": transition.at,
            }
            for transition in saga.history
        ]
        sql = """
        INSERT INTO saga_instances
            (correlation_id, state, content_id, deadline, history, step_attempts, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (correlation_id) DO UPDATE SET
            state = EXCLUDED.state,
            content_id = EXCLUDED.content_id,
            deadline = EXCLUDED.deadline,
            history = EXCLUDED.history,
            step_attempts = EXCLUDED.step_attempts,
            updated_at = now()
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    saga.correlation_id,
                    saga.state.value,
                    saga.content_id,
                    saga.deadline,
                    Jsonb(history),
                    Jsonb(saga.step_attempts),
                ))
            conn.commit()

    def get(self, correlation_id: str) -> Optional[SagaInstance]:
        sql = """
        SELECT correlation_id, state, content_id, deadline, history, step_attempts
        FROM saga_instances WHERE correlation_id = %s
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (correlation_id,))
                row = cur.fetchone()
        if row is None:
            return None
        history = [
            SagaTransition(
                from_state=SagaState(item["from_state"]) if item["from_state"] else None,
                to_state=SagaState(item["to_state"]),
                reason=item["reason"],
                at=item["at"],
            )
            for item in row[4]
        ]
        return SagaInstance(
            correlation_id=row[0],
            state=SagaState(row[1]),
            content_id=row[2],
            deadline=row[3],
            history=history,
            step_attempts=row[5],
            max_step_retries=2,
        )

    def due(self, now: datetime) -> list[SagaInstance]:
        sql = """
        SELECT correlation_id, state, content_id, deadline, history, step_attempts
        FROM saga_instances
        WHERE deadline IS NOT NULL
          AND deadline <= %s
          AND state IN ('PUBLISHING', 'WAITING_NOTIFICATION')
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (now,))
                rows = cur.fetchall()
        return [self._from_row(row) for row in rows]

    def _from_row(self, row: tuple) -> SagaInstance:
        data = json.loads(json.dumps(row[4]))
        return SagaInstance(
            correlation_id=row[0],
            state=SagaState(row[1]),
            content_id=row[2],
            deadline=row[3],
            history=[
                SagaTransition(
                    from_state=SagaState(item["from_state"]) if item["from_state"] else None,
                    to_state=SagaState(item["to_state"]),
                    reason=item["reason"],
                    at=item["at"],
                )
                for item in data
            ],
            step_attempts=row[5],
            max_step_retries=2,
        )
